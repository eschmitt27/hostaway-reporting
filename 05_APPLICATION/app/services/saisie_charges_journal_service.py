"""APP-3b / commit 5 — Journal durable des tentatives d'écriture réelle de charges.

Sans journal, un rollback critique n'existe que dans une exception Python : si personne ne regarde
l'écran au bon moment, plus rien ne dit quels fichiers ont été touchés, dans quel ordre, ni lesquels
n'ont pas été restaurés. C'est précisément le scénario où il faut pouvoir reconstituer les faits.
Ce module écrit cette trace, dans SQLite (`saisie_charges_writes`, migration 0004).

Trois règles non négociables :

1. **Le journal ne décide de rien.** Aucune valeur métier autoritaire : la vérité reste les classeurs
   Excel. On journalise la *tentative*, pas la charge — ni montant, ni logement, ni propriétaire,
   ni payload. Seulement de quoi reconstituer ce que la machine a fait aux fichiers.
2. **Le journal ne casse jamais la transaction.** Toute panne d'écriture du journal est capturée et
   rendue à l'appelant sous forme de message (`ResultatTransaction.journal_erreur`) — jamais levée
   par-dessus une erreur transactionnelle, jamais un rollback empêché.
3. **Mais elle n'est jamais avalée non plus.** Contrairement au `_log_write_safe` d'APP-2b
   (`except Exception: pass`), un échec ici est *signalé*. Un journal muet qui échoue en silence est
   pire que pas de journal : il donne l'illusion de la traçabilité.

`statut` (verdict métier, 9 valeurs) et `code` (détail technique `E_*`) sont deux colonnes
distinctes et ne sont jamais confondues.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import apply_migrations, get_db

# ── Statuts MÉTIER (jamais un code technique) ────────────────────────────────
REFUSE_FLAGS = "REFUSE_FLAGS"
REFUSE_VERROU = "REFUSE_VERROU"
REFUSE_VALIDATION = "REFUSE_VALIDATION"
REFUSE_CHARGE_ID = "REFUSE_CHARGE_ID"
ECHEC_PREPARATION = "ECHEC_PREPARATION"
SUCCES = "SUCCES"
ROLLBACK_REUSSI = "ROLLBACK_REUSSI"
ROLLBACK_CRITIQUE = "ROLLBACK_CRITIQUE"
ETAT_INCOHERENT = "ETAT_INCOHERENT"

STATUTS = frozenset({
    REFUSE_FLAGS, REFUSE_VERROU, REFUSE_VALIDATION, REFUSE_CHARGE_ID, ECHEC_PREPARATION,
    SUCCES, ROLLBACK_REUSSI, ROLLBACK_CRITIQUE, ETAT_INCOHERENT,
})

# Sources possibles de l'identifiant.
ID_FOURNI = "FOURNI"
ID_GENERE = "GENERE"
ID_DEMANDE = "DEMANDE"      # refusé avant résolution : on ne sait pas encore s'il aurait été retenu

_schemas_assures: set[str] = set()


@dataclass
class TraceTransaction:
    """Trace en cours de constitution. Ouverte au début de la tentative, close à la fin."""

    transaction_id: str
    debut_utc: str
    token_previsualisation: str | None = None
    charge_id: str | None = None
    charge_id_source: str | None = None
    cible_saisie: str | None = None
    cible_impacts: str | None = None
    sha256_saisie_avant: str | None = None
    sha256_impacts_avant: str | None = None
    db_path: Path | None = None
    # Complétés à la clôture
    fichiers_remplaces: list[str] = field(default_factory=list)
    rollback_tente: bool = False
    rollback_reussi: bool | None = None
    fichiers_non_restaures: list[dict[str, str]] = field(default_factory=list)
    verrou_pid: int | None = None
    verrou_hostname: str | None = None
    residus: list[str] = field(default_factory=list)


def ouvrir_trace(
    token_previsualisation: str | None = None,
    charge_id: str | None = None,
    charge_id_source: str | None = None,
    cible_saisie: Path | None = None,
    cible_impacts: Path | None = None,
    db_path: Path | None = None,
) -> TraceTransaction:
    """Ouvre une trace. N'écrit rien encore : une tentative n'est journalisée qu'à sa clôture."""
    return TraceTransaction(
        transaction_id=uuid.uuid4().hex,
        debut_utc=datetime.now(timezone.utc).isoformat(),
        token_previsualisation=token_previsualisation,
        charge_id=charge_id or None,
        charge_id_source=charge_id_source,
        # NOM de fichier seulement : jamais un chemin temporaire ni un chemin utilisateur complet.
        cible_saisie=Path(cible_saisie).name if cible_saisie else None,
        cible_impacts=Path(cible_impacts).name if cible_impacts else None,
        db_path=Path(db_path) if db_path else None,
    )


def _db(db_path: Path | None) -> Path:
    """Base cible. `cfg.DB_PATH` est lu DYNAMIQUEMENT : un test qui le réassigne (fixture tmp_db)
    doit pouvoir isoler le journal — un `from app.config import DB_PATH` figerait la valeur."""
    return Path(db_path) if db_path else Path(cfg.DB_PATH)


def _git_head() -> str | None:
    """HEAD courant, best-effort. Jamais bloquant : une absence de git n'empêche rien."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(cfg.PROJECT_ROOT), capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def _assurer_schema(db_path: Path) -> None:
    """Applique les migrations une fois par base et par processus (idempotent : IF NOT EXISTS)."""
    cle = str(db_path)
    if cle in _schemas_assures:
        return
    apply_migrations(db_path)
    _schemas_assures.add(cle)


def _json(valeur: Any) -> str | None:
    return json.dumps(valeur, ensure_ascii=False) if valeur else None


def cloturer_trace(
    trace: TraceTransaction,
    statut: str,
    code: str | None = None,
    details: str | None = None,
    sha256_saisie_apres: str | None = None,
    sha256_impacts_apres: str | None = None,
) -> None:
    """Écrit la trace. **Lève** en cas de panne — l'appelant doit utiliser `cloturer_trace_signalee`.

    Fonction séparée pour rester testable : c'est elle qu'on fait échouer pour prouver que la
    transaction conserve son résultat malgré une panne du journal.
    """
    if statut not in STATUTS:
        raise ValueError(f"statut inconnu : {statut!r} (attendu parmi {sorted(STATUTS)})")

    db = _db(trace.db_path)
    _assurer_schema(db)

    conn = get_db(db)
    try:
        conn.execute(
            """INSERT INTO saisie_charges_writes (
                   transaction_id, token_previsualisation, charge_id, charge_id_source,
                   debut_utc, fin_utc, statut, code, details,
                   cible_saisie, cible_impacts,
                   sha256_saisie_avant, sha256_saisie_apres,
                   sha256_impacts_avant, sha256_impacts_apres,
                   fichiers_remplaces, rollback_tente, rollback_reussi, fichiers_non_restaures,
                   verrou_pid, verrou_hostname, residus, git_head, app_pid, hostname
               ) VALUES (?,?,?,?, ?,?,?,?,?, ?,?, ?,?, ?,?, ?,?,?,?, ?,?,?,?,?,?)""",
            (
                trace.transaction_id, trace.token_previsualisation,
                trace.charge_id, trace.charge_id_source,
                trace.debut_utc, datetime.now(timezone.utc).isoformat(),
                statut, code, details,
                trace.cible_saisie, trace.cible_impacts,
                trace.sha256_saisie_avant, sha256_saisie_apres,
                trace.sha256_impacts_avant, sha256_impacts_apres,
                _json(trace.fichiers_remplaces),
                1 if trace.rollback_tente else 0,
                None if trace.rollback_reussi is None else (1 if trace.rollback_reussi else 0),
                _json(trace.fichiers_non_restaures),
                trace.verrou_pid, trace.verrou_hostname,
                _json(trace.residus), _git_head(), os.getpid(), socket.gethostname(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def cloturer_trace_signalee(trace: TraceTransaction, statut: str, **kwargs: Any) -> str | None:
    """Clôture la trace SANS jamais lever. Retourne le message d'erreur si la journalisation a
    échoué (à remonter dans le résultat), None si tout s'est bien passé.

    Ni silence, ni interruption : c'est exactement le compromis exigé — une panne de journal ne doit
    pas empêcher un rollback, mais elle ne doit pas non plus disparaître.
    """
    try:
        cloturer_trace(trace, statut, **kwargs)
        return None
    except Exception as exc:
        return (
            f"JOURNALISATION ÉCHOUÉE ({type(exc).__name__}: {exc}). "
            f"La transaction a bien abouti au statut {statut}, mais elle n'est PAS tracée en base "
            f"(transaction_id={trace.transaction_id})."
        )


# ── Lecture (diagnostic) ─────────────────────────────────────────────────────

def traces_recentes(limit: int = 50, db_path: Path | None = None) -> list[dict[str, Any]]:
    db = _db(db_path)
    _assurer_schema(db)
    conn = get_db(db)
    try:
        rows = conn.execute(
            "SELECT * FROM saisie_charges_writes ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def traces_du_token(token: str, db_path: Path | None = None) -> list[dict[str, Any]]:
    """Toutes les tentatives portant ce token de prévisualisation (ordre chronologique)."""
    db = _db(db_path)
    _assurer_schema(db)
    conn = get_db(db)
    try:
        rows = conn.execute(
            "SELECT * FROM saisie_charges_writes WHERE token_previsualisation = ? ORDER BY id",
            (token,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def token_deja_ecrit(token: str, db_path: Path | None = None) -> dict[str, Any] | None:
    """La trace SUCCES de ce token, si elle existe. Sert de garde d'idempotence.

    Lève si la base est inaccessible : l'appelant décide quoi faire (il ne doit pas bloquer la
    transaction pour autant — voir la limite documentée côté orchestrateur).
    """
    for trace in traces_du_token(token, db_path=db_path):
        if trace.get("statut") == SUCCES:
            return trace
    return None

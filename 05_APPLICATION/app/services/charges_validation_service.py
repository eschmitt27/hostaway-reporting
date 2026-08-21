"""Validation humaine des charges (A_CONTROLER → VALIDE / REJETE).

Pourquoi ce parcours existe : une charge saisie est créée en `A_CONTROLER`. Lot9 n'ingère QUE les
charges `VALIDE` ; une charge non validée n'a donc **aucun impact financier** (ni résultat, ni net
propriétaire, ni préfacture).

La vérité est la table `charges` (migration 0052) : plus de SAISIE_Charges_Flux.xlsx, plus de MASTER
régénéré par Lot3 à relire. L'écriture est une simple transaction SQLite (déjà atomique — le
remplacement de fichier avec sauvegarde/rollback n'a plus d'objet), et chaque action reste
journalisée (qui, quand, quoi, commentaire), jamais destructive : le rejet conserve la ligne, il
change seulement `statut_controle`.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

SHEET = "SAISIE"
ST_A_CONTROLER = "A_CONTROLER"
ST_VALIDE = "VALIDE"
ST_REJETE = "REJETE"

E_INTROUVABLE = "E_CHARGE_INTROUVABLE"
E_DEJA = "E_DEJA_TRAITEE"
E_COMMENTAIRE = "E_COMMENTAIRE_OBLIGATOIRE"

MESSAGES = {
    E_INTROUVABLE: "Charge introuvable.",
    E_DEJA: "Cette charge a déjà été traitée (validée ou rejetée).",
    E_COMMENTAIRE: "Un commentaire est obligatoire pour rejeter une charge.",
}

AVERTISSEMENT_NON_VALIDEE = (
    "Cette charge n'aura aucun impact financier tant qu'elle ne sera pas validée."
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _journal_init(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS charges_validation_journal (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               charge_id TEXT NOT NULL,
               ancien_statut TEXT,
               nouveau_statut TEXT NOT NULL,
               acteur TEXT,
               commentaire TEXT,
               horodatage_utc TEXT NOT NULL
           )"""
    )


def _journaliser(charge_id: str, ancien: str, nouveau: str, acteur: str, commentaire: str,
                 db_path=None) -> None:
    conn = sqlite3.connect(str(db_path or cfg.DB_PATH))
    try:
        _journal_init(conn)
        conn.execute(
            "INSERT INTO charges_validation_journal "
            "(charge_id, ancien_statut, nouveau_statut, acteur, commentaire, horodatage_utc) "
            "VALUES (?,?,?,?,?,?)",
            (charge_id, ancien, nouveau, acteur or "local", commentaire or "", _now()),
        )
        conn.commit()
    finally:
        conn.close()


def historique(charge_id: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path or cfg.DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        _journal_init(conn)
        if charge_id:
            rows = conn.execute(
                "SELECT * FROM charges_validation_journal WHERE charge_id=? ORDER BY id DESC",
                (charge_id,)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM charges_validation_journal ORDER BY id DESC LIMIT 200").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def lister(statut: str = "", refacturable: str = "", sans_proprietaire: bool = False,
           code_impact: str = "", db_path=None) -> dict[str, Any]:
    """Liste les charges ACTIVES (table `charges`), avec filtres. Lecture seule, jamais d'écriture."""
    conn = get_db(db_path)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM charges WHERE statut = 'ACTIVE' ORDER BY date_charge DESC")]
    finally:
        conn.close()
    if not rows:
        return {"status": "VIDE", "source": "charges", "rows": [], "compteurs": {}}

    compteurs = {
        "total": len(rows),
        ST_A_CONTROLER: sum(1 for d in rows if str(d.get("statut_controle")) == ST_A_CONTROLER),
        ST_VALIDE: sum(1 for d in rows if str(d.get("statut_controle")) == ST_VALIDE),
        ST_REJETE: sum(1 for d in rows if str(d.get("statut_controle")) == ST_REJETE),
        "refacturables": sum(1 for d in rows if str(d.get("refacturable")).upper() == "OUI"),
        "sans_proprietaire": sum(1 for d in rows if not str(d.get("proprietaire_id") or "").strip()),
    }

    def _match(d: dict) -> bool:
        if statut and str(d.get("statut_controle")) != statut:
            return False
        if refacturable and str(d.get("refacturable")).upper() != refacturable.upper():
            return False
        if sans_proprietaire and str(d.get("proprietaire_id") or "").strip():
            return False
        if code_impact and str(d.get("code_impact")) != code_impact:
            return False
        return True

    filtres = [d for d in rows if _match(d)]
    return {"status": "OK", "source": "charges", "rows": filtres, "compteurs": compteurs,
            "avertissement": AVERTISSEMENT_NON_VALIDEE,
            "applied": {"statut": statut, "refacturable": refacturable,
                        "sans_proprietaire": sans_proprietaire, "code_impact": code_impact}}


def _refus(code: str, details: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "details": details}


def _transition(charge_id: str, nouveau: str, *, acteur: str = "", commentaire: str = "",
                db_path=None) -> dict[str, Any]:
    charge_id = str(charge_id or "").strip()
    if nouveau == ST_REJETE and not str(commentaire or "").strip():
        return _refus(E_COMMENTAIRE)

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT statut, statut_controle FROM charges WHERE charge_id = ?", (charge_id,)
        ).fetchone()
        if row is None or row["statut"] != "ACTIVE":
            return _refus(E_INTROUVABLE, charge_id)
        ancien = str(row["statut_controle"] or "").strip()
        if ancien in (ST_VALIDE, ST_REJETE):
            return _refus(E_DEJA, f"statut actuel {ancien}")
        conn.execute(
            "UPDATE charges SET statut_controle = ?, date_modification = ? WHERE charge_id = ?",
            (nouveau, _now(), charge_id))
        conn.commit()
    finally:
        conn.close()

    _journaliser(charge_id, ancien, nouveau, acteur, commentaire, db_path=db_path)
    return {"ok": True, "charge_id": charge_id, "ancien_statut": ancien, "nouveau_statut": nouveau,
            "horodatage_utc": _now(), "acteur": acteur or "local", "commentaire": commentaire or "",
            "recalcul_requis": nouveau == ST_VALIDE}


def valider(charge_id: str, *, acteur: str = "", commentaire: str = "", db_path=None):
    """A_CONTROLER → VALIDE. La charge devient éligible à Lot9 (impact financier)."""
    return _transition(charge_id, ST_VALIDE, acteur=acteur, commentaire=commentaire, db_path=db_path)


def rejeter(charge_id: str, *, commentaire: str, acteur: str = "", db_path=None):
    """A_CONTROLER → REJETE. Ligne conservée, jamais supprimée ; jamais ingérée par Lot9."""
    return _transition(charge_id, ST_REJETE, acteur=acteur, commentaire=commentaire, db_path=db_path)

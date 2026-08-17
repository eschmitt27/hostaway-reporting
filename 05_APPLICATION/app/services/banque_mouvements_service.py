"""Mouvements bancaires en SQLite — import, déduplication, lecture.

ARCHITECTURE CIBLE
    export externe (xlsx, ou API demain) → DTO canonique → CE SERVICE → banque_mouvements
Les traitements aval lisent la base, jamais le fichier. Le format de transport ne remonte pas
au-delà de l'adaptateur : c'est ce qui rendra une API bancaire interchangeable avec un classeur.

LE BRUT EST IMMUABLE
Ce service écrit ce que la banque a envoyé, sans l'interpréter. Catégorie, statut et décisions
vivent ailleurs (`banque_classifications`, `banque_overrides`, `banque_rapprochements`). Rejouer
une classification ne doit jamais réécrire la source.

DÉDUPLICATION — DEUX RÉGIMES, ET UN REFUS DE DEVINER
1. `external_transaction_id` fourni par la banque : il fait foi. Deux lignes qui le partagent sur
   le même compte SONT le même mouvement. Rejet certain.
2. Sinon, empreinte sur compte + date + montant signé + libellé normalisé. Elle sert d'INDICE.
   Un même montant, à la même date, avec le même libellé peut être **deux vrais mouvements** — un
   double prélèvement, deux courses identiques. Fusionner sur ce seul critère ferait disparaître un
   mouvement réel, c'est-à-dire fausser un solde.

   Le service ne trancherait donc jamais seul : il insère et marque `A_CONTROLER`, laissant la
   décision à un humain. Perdre une ligne est irréversible ; en garder une en trop se corrige.
"""
from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime
from typing import Any, Iterable

from app.db.connection import get_db

SENS_DEBIT = "DEBIT"
SENS_CREDIT = "CREDIT"

SOURCE_HISTORIQUE = "HISTORIQUE"
SOURCE_MENSUEL = "MENSUEL"
SOURCE_INCREMENTAL = "INCREMENTAL"
SOURCE_API = "API"

# Résultats possibles pour une ligne présentée à l'import.
LIGNE_INSEREE = "INSEREE"
LIGNE_DOUBLON_CERTAIN = "DOUBLON_CERTAIN"      # même identifiant bancaire
LIGNE_A_CONTROLER = "DOUBLON_A_CONTROLER"      # même empreinte, identifiant absent


def _txt(v: Any) -> str:
    return str(v or "").strip()


def _normaliser_libelle(libelle: str) -> str:
    """Libellé réduit à sa substance, pour comparer deux lignes du même mouvement.

    Les exports diffèrent sur les espaces et la casse d'une extraction à l'autre. On neutralise
    cela — et rien de plus : aucun mot n'est retiré, aucune abréviation développée. Normaliser trop
    ferait ressembler deux mouvements distincts.
    """
    return re.sub(r"\s+", " ", _txt(libelle)).upper()


def empreinte(bank_account_id: str, date_operation: str, montant: float,
              libelle: str) -> str:
    """Empreinte déterministe d'un mouvement. Indice de doublon, jamais preuve d'identité."""
    base = "|".join([
        _txt(bank_account_id),
        _txt(date_operation)[:10],
        f"{round(float(montant or 0), 2):.2f}",
        _normaliser_libelle(libelle),
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _mouvement_opaque() -> str:
    return "MVT-" + uuid.uuid4().hex[:12].upper()


def previsualiser(lignes: Iterable[dict[str, Any]], *, bank_account_id: str,
                  db_path=None) -> dict[str, Any]:
    """Ce que l'import ferait, sans rien écrire.

    Les doublons sont évalués contre la base ET à l'intérieur du lot présenté : un export peut se
    contenir lui-même deux fois.
    """
    lignes = list(lignes)
    conn = get_db(db_path)
    try:
        externes = {r[0] for r in conn.execute(
            "SELECT external_transaction_id FROM banque_mouvements "
            "WHERE bank_account_id = ? AND external_transaction_id IS NOT NULL "
            "AND external_transaction_id <> ''", (bank_account_id,))}
        empreintes = {r[0] for r in conn.execute(
            "SELECT fingerprint FROM banque_mouvements WHERE bank_account_id = ?",
            (bank_account_id,))}
    finally:
        conn.close()

    vus_externes: set[str] = set()
    vus_empreintes: set[str] = set()
    resultats: list[dict[str, Any]] = []

    for i, l in enumerate(lignes, start=1):
        ext = _txt(l.get("external_transaction_id"))
        fp = empreinte(bank_account_id, _txt(l.get("date_operation")),
                       l.get("montant"), _txt(l.get("libelle_brut")))
        if ext and (ext in externes or ext in vus_externes):
            statut = LIGNE_DOUBLON_CERTAIN
        elif not ext and (fp in empreintes or fp in vus_empreintes):
            statut = LIGNE_A_CONTROLER
        else:
            statut = LIGNE_INSEREE
        if ext:
            vus_externes.add(ext)
        vus_empreintes.add(fp)
        resultats.append({**l, "_ligne": i, "_fingerprint": fp, "_statut": statut})

    dates = [_txt(l.get("date_operation"))[:10] for l in lignes if _txt(l.get("date_operation"))]
    return {
        "bank_account_id": bank_account_id,
        "nb_lignes": len(lignes),
        "nb_inserees": sum(1 for r in resultats if r["_statut"] == LIGNE_INSEREE),
        "nb_doublons": sum(1 for r in resultats if r["_statut"] == LIGNE_DOUBLON_CERTAIN),
        "nb_a_controler": sum(1 for r in resultats if r["_statut"] == LIGNE_A_CONTROLER),
        "date_min": min(dates) if dates else "",
        "date_max": max(dates) if dates else "",
        "lignes": resultats,
    }


def importer(lignes: Iterable[dict[str, Any]], *, bank_account_id: str, source_type: str,
             source_filename: str = "", source_sha256: str = "", run_id: str = "",
             db_path=None) -> dict[str, Any]:
    """Importe un lot de mouvements. Une transaction : intégral ou inexistant.

    Les doublons certains ne sont pas insérés. Les ambigus le sont, marqués `A_CONTROLER` — perdre
    un mouvement réel serait pire que d'en présenter un de trop.
    """
    apercu = previsualiser(lignes, bank_account_id=bank_account_id, db_path=db_path)
    import_id = "IMPBQ-" + uuid.uuid4().hex[:12].upper()
    maintenant = datetime.now().isoformat(timespec="seconds")

    conn = get_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            inseres = 0
            for r in apercu["lignes"]:
                if r["_statut"] == LIGNE_DOUBLON_CERTAIN:
                    continue
                conn.execute(
                    "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, "
                    "bank_account_id, external_transaction_id, date_operation, date_valeur, "
                    "sens, montant, devise, libelle_brut, contrepartie_brute, fingerprint, "
                    "ligne_source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (_mouvement_opaque(), import_id, bank_account_id,
                     _txt(r.get("external_transaction_id")) or None,
                     _txt(r.get("date_operation")), _txt(r.get("date_valeur")) or None,
                     _txt(r.get("sens")), round(float(r.get("montant") or 0), 2),
                     _txt(r.get("devise")) or "EUR", _txt(r.get("libelle_brut")),
                     _txt(r.get("contrepartie_brute")) or None, r["_fingerprint"],
                     r.get("_ligne")))
                inseres += 1

            conn.execute(
                "INSERT INTO banque_import_source (import_id, bank_account_id, source_type, "
                "source_filename, source_sha256, date_min, date_max, nb_lignes, nb_inseres, "
                "nb_doublons, nb_a_controler, date_import, run_id, statut) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'IMPORTE')",
                (import_id, bank_account_id, source_type, source_filename, source_sha256,
                 apercu["date_min"], apercu["date_max"], apercu["nb_lignes"], inseres,
                 apercu["nb_doublons"], apercu["nb_a_controler"], maintenant, run_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    return {"ok": True, "import_id": import_id, "nb_lignes": apercu["nb_lignes"],
            "nb_inseres": inseres, "nb_doublons": apercu["nb_doublons"],
            "nb_a_controler": apercu["nb_a_controler"],
            "date_min": apercu["date_min"], "date_max": apercu["date_max"]}


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

_COLONNES = ("mouvement_id_opaque", "import_id", "bank_account_id", "external_transaction_id",
             "date_operation", "date_valeur", "sens", "montant", "devise", "libelle_brut",
             "contrepartie_brute", "fingerprint", "ligne_source", "created_at")


def mouvements(*, bank_account_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Mouvements d'un compte, ou de tous les comptes si aucun n'est précisé.

    Le filtre par compte est ce qui permettra à la future banque de ne jamais voir l'historique de
    l'ancienne.
    """
    conn = get_db(db_path)
    try:
        sql = f"SELECT {', '.join(_COLONNES)} FROM banque_mouvements"
        args: list = []
        if bank_account_id:
            sql += " WHERE bank_account_id = ?"
            args.append(bank_account_id)
        sql += " ORDER BY date_operation, ligne_source, mouvement_id_opaque"
        return [dict(zip(_COLONNES, r)) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def compter(*, bank_account_id: str = "", db_path=None) -> int:
    conn = get_db(db_path)
    try:
        sql = "SELECT COUNT(*) FROM banque_mouvements"
        args: list = []
        if bank_account_id:
            sql += " WHERE bank_account_id = ?"
            args.append(bank_account_id)
        return conn.execute(sql, args).fetchone()[0]
    finally:
        conn.close()


def periode(*, bank_account_id: str = "", db_path=None) -> dict[str, str]:
    conn = get_db(db_path)
    try:
        sql = "SELECT MIN(date_operation), MAX(date_operation) FROM banque_mouvements"
        args: list = []
        if bank_account_id:
            sql += " WHERE bank_account_id = ?"
            args.append(bank_account_id)
        r = conn.execute(sql, args).fetchone()
        return {"date_min": r[0] or "", "date_max": r[1] or ""}
    finally:
        conn.close()


def imports(*, bank_account_id: str = "", db_path=None) -> list[dict[str, Any]]:
    cols = ("import_id", "bank_account_id", "source_type", "source_filename", "source_sha256",
            "date_min", "date_max", "nb_lignes", "nb_inseres", "nb_doublons", "nb_a_controler",
            "date_import", "run_id", "statut")
    conn = get_db(db_path)
    try:
        sql = f"SELECT {', '.join(cols)} FROM banque_import_source"
        args: list = []
        if bank_account_id:
            sql += " WHERE bank_account_id = ?"
            args.append(bank_account_id)
        sql += " ORDER BY date_import DESC"
        return [dict(zip(cols, r)) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()

"""Opérations diverses — OD (migration `0023`).

Objet libre (contrairement à ACHATS/BANQUE/VENTES/CAISSE, dérivées d'un objet métier déjà typé) :
une OD porte ses propres lignes équilibrées (`od_lignes`), du même contrat que `ecriture_lignes`.
Une OD ne devient une écriture ODIVERSES qu'après validation explicite — une opération HORS_COMPTA
ne bascule jamais automatiquement en écriture.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

TYPES = ("AJUSTEMENT", "OUVERTURE", "REMBOURSEMENT_ASSOCIE", "PAIEMENT_PERSONNEL_ASSOCIE",
         "HORS_COMPTA_REGULARISE", "CORRECTION", "RECLASSEMENT")

ST_BROUILLON = "BROUILLON"
ST_VALIDEE = "VALIDEE"
ST_ANNULEE = "ANNULEE"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_TYPE_INCONNU = "V01_TYPE_OPERATION_INCONNU"
E_LIBELLE_MANQUANT = "V02_LIBELLE_MANQUANT"
E_SANS_LIGNE = "V03_OD_SANS_LIGNE"
E_DESEQUILIBRE = "V04_OD_DESEQUILIBREE"
E_COMPTE_INCONNU = "V05_COMPTE_INCONNU"
E_INTROUVABLE = "E01_OD_INTROUVABLE"
E_STATUT = "E02_TRANSITION_INTERDITE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation.",
    E_TYPE_INCONNU: "Type d'opération diverse inconnu.",
    E_LIBELLE_MANQUANT: "Le libellé est obligatoire.",
    E_SANS_LIGNE: "Une opération diverse doit porter au moins deux lignes.",
    E_DESEQUILIBRE: "Le total débit doit égaler le total crédit.",
    E_COMPTE_INCONNU: "Compte inconnu ou inactif dans le plan comptable.",
    E_INTROUVABLE: "Opération diverse introuvable.",
    E_STATUT: "Transition de statut interdite.",
}


def _flags_actifs() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _nombre(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def _compte_actif(compte: str, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute("SELECT 1 FROM plan_comptable WHERE compte=? AND actif=1",
                            (compte,)).fetchone() is not None
    finally:
        conn.close()


def creer(type_operation: str, libelle: str, lignes: list[dict[str, Any]], *,
         date_operation: str = "", justification: str = "", acteur: str = "",
         db_path=None) -> dict[str, Any]:
    """Crée une OD en BROUILLON. Les lignes doivent DÉJÀ être équilibrées (même règle qu'une
    écriture) — une OD ne peut pas rester déséquilibrée en base, contrairement à une facture."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    type_operation = _txt(type_operation)
    if type_operation not in TYPES:
        return _refus(E_TYPE_INCONNU, type_operation)
    if not _txt(libelle):
        return _refus(E_LIBELLE_MANQUANT)
    if not lignes or len(lignes) < 2:
        return _refus(E_SANS_LIGNE)

    total_debit = round(sum(_nombre(l.get("debit")) or 0 for l in lignes), 2)
    total_credit = round(sum(_nombre(l.get("credit")) or 0 for l in lignes), 2)
    if total_debit != total_credit or total_debit == 0:
        return _refus(E_DESEQUILIBRE, f"débit={total_debit} crédit={total_credit}")
    for l in lignes:
        if not _compte_actif(_txt(l.get("compte")), db_path):
            return _refus(E_COMPTE_INCONNU, _txt(l.get("compte")))

    opaque = "OD-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO operations_diverses (od_id_opaque, type_operation, date_operation, "
            "libelle, justification, statut, acteur) VALUES (?,?,?,?,?,?,?)",
            (opaque, type_operation, date_operation or date.today().isoformat(), libelle,
             _txt(justification) or None, ST_BROUILLON, acteur or "local"))
        for i, l in enumerate(lignes, start=1):
            conn.execute(
                "INSERT INTO od_lignes (od_id_opaque, ligne_num, compte, auxiliaire, debit, "
                "credit, commentaire) VALUES (?,?,?,?,?,?,?)",
                (opaque, i, _txt(l.get("compte")), _txt(l.get("auxiliaire")) or None,
                 _nombre(l.get("debit")) or 0, _nombre(l.get("credit")) or 0,
                 _txt(l.get("commentaire")) or None))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "od_id_opaque": opaque}


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM operations_diverses WHERE od_id_opaque=?",
                           (opaque,)).fetchone()
        if row is None:
            return None
        lignes = conn.execute("SELECT * FROM od_lignes WHERE od_id_opaque=? ORDER BY ligne_num",
                              (opaque,)).fetchall()
    finally:
        conn.close()
    od = dict(row)
    od["lignes"] = [dict(l) for l in lignes]
    return od


def lister(*, statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM operations_diverses ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = [dict(r) for r in rows]
    if statut:
        out = [o for o in out if o["statut"] == statut]
    return out


def valider(opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Valide une OD BROUILLON et génère aussitôt son écriture ODIVERSES — une OD validée est
    toujours reflétée en écriture, jamais laissée orpheline."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    od = charger(opaque, db_path)
    if od is None:
        return _refus(E_INTROUVABLE, opaque)
    if od["statut"] != ST_BROUILLON:
        return _refus(E_STATUT, f"{od['statut']} -> {ST_VALIDEE}")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE operations_diverses SET statut=?, version=version+1 "
                     "WHERE od_id_opaque=?", (ST_VALIDEE, opaque))
        conn.commit()
    finally:
        conn.close()
    from app.services import comptabilite_ecritures_service as compta
    res = compta.generer_ecriture_od(opaque, acteur=acteur, db_path=db_path)
    return {"ok": True, "od_id_opaque": opaque, "statut": ST_VALIDEE,
            "ecriture_id_opaque": res.get("ecriture_id_opaque") if res.get("ok") else None}


def annuler(opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    od = charger(opaque, db_path)
    if od is None:
        return _refus(E_INTROUVABLE, opaque)
    if od["statut"] == ST_VALIDEE:
        return _refus(E_STATUT, "une OD validée se contrepasse, elle ne s'annule pas")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE operations_diverses SET statut=?, version=version+1 "
                     "WHERE od_id_opaque=?", (ST_ANNULEE, opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "od_id_opaque": opaque, "statut": ST_ANNULEE}

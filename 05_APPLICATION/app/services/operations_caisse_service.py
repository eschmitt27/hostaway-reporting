"""Opérations de caisse (migration `0023`) — objet métier distinct de l'écriture CAISSE.

Sert les cas de caisse SANS objet existant déjà réel : encaissement, remboursement associé en
espèces. Un paiement fournisseur en espèces passe par `reglements_fournisseurs_service` (moyen
CAISSE), jamais dupliqué ici.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

TYPES = ("ENCAISSEMENT", "REMBOURSEMENT_ASSOCIE", "AUTRE")
ST_ENREGISTREE = "ENREGISTREE"
ST_ANNULEE = "ANNULEE"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_TYPE_INCONNU = "V01_TYPE_OPERATION_INCONNU"
E_MONTANT_INVALIDE = "V02_MONTANT_INVALIDE"
E_INTROUVABLE = "E01_OPERATION_INTROUVABLE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation.",
    E_TYPE_INCONNU: "Type d'opération de caisse inconnu.",
    E_MONTANT_INVALIDE: "Le montant doit être un nombre strictement positif.",
    E_INTROUVABLE: "Opération de caisse introuvable.",
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


def creer(type_operation: str, montant: Any, *, date_operation: str = "", tiers_type: str = "",
         tiers_id: str = "", piece: str = "", commentaire: str = "", acteur: str = "",
         db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    type_operation = _txt(type_operation)
    if type_operation not in TYPES:
        return _refus(E_TYPE_INCONNU, type_operation)
    m = _nombre(montant)
    if m is None or m <= 0:
        return _refus(E_MONTANT_INVALIDE, _txt(montant))

    opaque = "CAI-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO operations_caisse (operation_id_opaque, type_operation, date_operation, "
            "montant, tiers_type, tiers_id, piece, commentaire, statut, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (opaque, type_operation, date_operation or date.today().isoformat(), round(m, 2),
             _txt(tiers_type) or None, _txt(tiers_id) or None, _txt(piece) or None,
             _txt(commentaire) or None, ST_ENREGISTREE, acteur or "local"))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque}


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM operations_caisse WHERE operation_id_opaque=?",
                           (opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def lister(*, statut: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM operations_caisse ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = [dict(r) for r in rows]
    if statut:
        out = [o for o in out if o["statut"] == statut]
    return out


def annuler(opaque: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM operations_caisse WHERE operation_id_opaque=?",
                        (opaque,)).fetchone() is None:
            return _refus(E_INTROUVABLE, opaque)
        conn.execute("UPDATE operations_caisse SET statut=?, version=version+1 "
                     "WHERE operation_id_opaque=?", (ST_ANNULEE, opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "operation_id_opaque": opaque, "statut": ST_ANNULEE}

"""Correction manuelle de l'assiette de commission — écran ASSIETTE_NEGATIVE_RAMENEE_ZERO.

Ne remplace jamais l'assiette brute (preuve/audit, `lot10_commissions.assiette_commission`, jamais
réécrite) : la correction vit à côté, dans `assiette_corrections_manuelles` (migration 0065), avec
justification obligatoire, auteur, date, ancienne/nouvelle commission. Le moteur (`lot10_calculer_
resultats.py`) consulte cette table pour calculer la commission RÉELLE ; Lot11 (`controles_lot11_
service._groupe4_commissions`) l'utilise pour distinguer ASSIETTE_NEGATIVE_RAMENEE_ZERO (encore
automatique) de ASSIETTE_CORRIGEE_MANUELLEMENT (décidée) — jamais un masquage silencieux du contrôle.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db
from app.services import controles_actionnable_service as act

CODES_ELIGIBLES = ("ASSIETTE_NEGATIVE_RAMENEE_ZERO", "ASSIETTE_CORRIGEE_MANUELLEMENT")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def element_eligible(ctrl_opaque: str, *, db_path=None) -> dict[str, Any] | None:
    fiche = act.load_fiche(ctrl_opaque, db_path=db_path)
    if fiche is None:
        return None
    el = fiche["element"]
    if el.get("code") not in CODES_ELIGIBLES:
        return None
    return el


def correction_active(reservation_calc_id: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM assiette_corrections_manuelles WHERE reservation_calc_id = ? "
            "AND actif = 1 ORDER BY id DESC LIMIT 1", (reservation_calc_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def preparer_formulaire(ctrl_opaque: str, *, db_path=None) -> dict[str, Any] | None:
    """Données de préremplissage. Ne préempile jamais la nouvelle assiette — l'humain la saisit."""
    el = element_eligible(ctrl_opaque, db_path=db_path)
    if el is None:
        return None
    d = el["donnees"]
    rid = d.get("reservation_calc_id")
    existante = correction_active(rid, db_path=db_path)
    return {
        "ctrl_opaque": ctrl_opaque,
        "reservation_id": d.get("reservation_id"),
        "reservation_calc_id": rid,
        "logement_id": d.get("logement"),
        "proprietaire_id": d.get("proprietaire"),
        "mois": d.get("mois"),
        "payout": d.get("payout"),
        "menage": d.get("menage"),
        "assiette_brute": d.get("assiette_brute"),
        "assiette_automatique": d.get("assiette_automatique"),
        "taux_commission": d.get("taux_commission"),
        "commission_actuelle": d.get("commission_actuelle"),
        "correction_existante": existante,
    }


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _parse_montant(v: str) -> float | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return float(str(v).strip().replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def recap(ctrl_opaque: str, *, nouvelle_assiette: str, justification: str = "",
         db_path=None) -> dict[str, Any] | None:
    """Résumé de l'impact avant validation humaine — aucune écriture."""
    prep = preparer_formulaire(ctrl_opaque, db_path=db_path)
    if prep is None:
        return None
    montant = _parse_montant(nouvelle_assiette)
    taux = prep["taux_commission"] or 0.0
    prep["nouvelle_assiette_saisie"] = nouvelle_assiette
    prep["justification_saisie"] = justification
    prep["nouvelle_commission_calculee"] = round(montant * taux, 2) if montant is not None else None
    prep["ancienne_commission_affichee"] = prep["commission_actuelle"]
    return prep


def corriger(ctrl_opaque: str, *, nouvelle_assiette: str, justification: str, acteur: str = "",
            db_path=None) -> dict[str, Any]:
    if not _txt(justification):
        return _refus("JUSTIFICATION_OBLIGATOIRE", "Justification obligatoire pour corriger l'assiette.")
    montant = _parse_montant(nouvelle_assiette)
    if montant is None:
        return _refus("ASSIETTE_INVALIDE", "Nouvelle assiette invalide.")

    prep = preparer_formulaire(ctrl_opaque, db_path=db_path)
    if prep is None:
        return _refus("ELEMENT_INTROUVABLE_OU_HORS_PERIMETRE",
                      "Contrôle introuvable ou non éligible à la correction d'assiette.")

    taux = prep["taux_commission"] or 0.0
    ancienne_commission = prep["commission_actuelle"]
    nouvelle_commission = round(montant * taux, 2)

    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE assiette_corrections_manuelles SET actif = 0 "
            "WHERE reservation_calc_id = ? AND actif = 1", (prep["reservation_calc_id"],))
        conn.execute(
            "INSERT INTO assiette_corrections_manuelles "
            "(reservation_calc_id, assiette_brute, assiette_automatique, assiette_manuelle, "
            "justification, acteur, date_correction, ancienne_commission, nouvelle_commission, "
            "taux_commission, actif) VALUES (?,?,?,?,?,?,?,?,?,?,1)",
            (prep["reservation_calc_id"], prep["assiette_brute"], prep["assiette_automatique"],
             montant, justification, acteur or None, _now(), ancienne_commission,
             nouvelle_commission, taux))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "reservation_calc_id": prep["reservation_calc_id"],
           "ancienne_commission": ancienne_commission, "nouvelle_commission": nouvelle_commission}


# ── Recalcul ciblé après correction — Lot10 → Lot11 → Lot12 (DAG existant) ──────────────────────

def recalculer(*, db_path=None) -> dict[str, Any]:
    from app.services import orchestrateur_moteur
    from app.services import controles_lot11_service
    from app.services import lot12_prefactures_service

    etapes: list[dict[str, Any]] = []

    def _etape(nom: str, resultat: dict[str, Any]) -> bool:
        etapes.append({"etape": nom, "resultat": resultat})
        return bool(resultat.get("ok", True))

    if not _etape("LOT10", orchestrateur_moteur.executer_lot10(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    if not _etape("LOT11", controles_lot11_service.construire(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    if not _etape("LOT12", lot12_prefactures_service.construire(db_path=db_path)):
        return {"ok": False, "etapes": etapes}
    return {"ok": True, "etapes": etapes}

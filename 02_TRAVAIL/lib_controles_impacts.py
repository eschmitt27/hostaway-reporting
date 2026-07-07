"""Lot11 — contrôles de cohérence des impacts charges (affectations / ménage / réserve / avantage).

Contrôles purs et testables, retournant des anomalies {code, charge_id, message, severite}.
Aucune écriture ; lecture uniquement via charge_id valide.
"""
from __future__ import annotations

from typing import Any, Iterable

import lib_avantages as av

TOL = 0.01


def _n(v) -> float:
    try:
        return round(float(v or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def ctrl_affectations_somme(affectations: Iterable[dict[str, Any]], montant_par_charge: dict[str, float]) -> list[dict]:
    """Quote-parts d'affectation d'une charge = montant de la charge."""
    par_charge: dict[str, float] = {}
    for r in affectations:
        cid = str(r.get("charge_id", "") or "").strip()
        par_charge[cid] = round(par_charge.get(cid, 0.0) + _n(r.get("quote_part")), 2)
    out = []
    for cid, total in par_charge.items():
        attendu = montant_par_charge.get(cid)
        if attendu is not None and abs(total - _n(attendu)) > TOL:
            out.append({"code": "AFFECTATION_SOMME_INCOHERENTE", "charge_id": cid,
                        "message": f"Σ quotes-parts {total} ≠ montant {attendu}", "severite": "BLOQUANT"})
    return out


def ctrl_reserve_somme(reserve: Iterable[dict[str, Any]], montant_refac_par_charge: dict[str, float]) -> list[dict]:
    """Quote-parts de réserve d'une charge = montant réellement refacturable."""
    par_charge: dict[str, float] = {}
    for r in reserve:
        cid = str(r.get("charge_id", "") or "").strip()
        par_charge[cid] = round(par_charge.get(cid, 0.0) + _n(r.get("montant_refacturable")), 2)
    out = []
    for cid, total in par_charge.items():
        attendu = montant_refac_par_charge.get(cid)
        if attendu is not None and abs(total - _n(attendu)) > TOL:
            out.append({"code": "RESERVE_SOMME_INCOHERENTE", "charge_id": cid,
                        "message": f"Σ réserve {total} ≠ refacturable {attendu}", "severite": "BLOQUANT"})
    return out


def ctrl_menage_hors_reserve(menage: Iterable[dict[str, Any]], reserve: Iterable[dict[str, Any]]) -> list[dict]:
    """Une charge ménage n'est jamais présente dans la réserve refacturable."""
    charges_menage = {str(r.get("charge_id", "") or "").strip() for r in menage}
    out = []
    for r in reserve:
        cid = str(r.get("charge_id", "") or "").strip()
        if cid in charges_menage:
            out.append({"code": "MENAGE_DANS_RESERVE", "charge_id": cid,
                        "message": "Charge ménage présente dans la réserve refacturable (interdit).",
                        "severite": "BLOQUANT"})
    return out


def ctrl_avantage_unique(charges: Iterable[dict[str, Any]]) -> list[dict]:
    """Avantage unique par lien_origine (charge_id) — jamais deux fois pour la même charge."""
    charges = list(charges)
    vus: dict[str, int] = {}
    for c in charges:
        cid = str(c.get("charge_id", "") or "").strip()
        if av.charge_est_avantage(c) and cid:
            vus[cid] = vus.get(cid, 0) + 1
    return [{"code": "AVANTAGE_DUPLIQUE", "charge_id": cid,
             "message": f"Avantage compté {n} fois pour la charge (attendu 1).", "severite": "BLOQUANT"}
            for cid, n in vus.items() if n > 1]


def ctrl_pas_de_charge_recreee(affectations: Iterable[dict[str, Any]], charges_reelles_ids: set[str]) -> list[dict]:
    """Aucune affectation analytique ne doit exister comme charge réelle distincte (double charge)."""
    out = []
    for r in affectations:
        aid = str(r.get("affectation_id", "") or "").strip()
        # Un affectation_id ne doit jamais être un charge_id réel (les IDs affectation sont dérivés : {charge_id}-AFF-n)
        if aid in charges_reelles_ids:
            out.append({"code": "AFFECTATION_RECREEE_COMME_CHARGE", "charge_id": r.get("charge_id"),
                        "message": f"affectation_id {aid} présent comme charge réelle.", "severite": "BLOQUANT"})
    return out


def ctrl_charge_id_valide(rows: Iterable[dict[str, Any]], charges_valides: set[str]) -> list[dict]:
    """Chaque ligne d'impact référence un charge_id valide (jamais orpheline)."""
    out = []
    for r in rows:
        cid = str(r.get("charge_id", "") or "").strip()
        if cid and cid not in charges_valides:
            out.append({"code": "IMPACT_CHARGE_ID_INCONNU", "charge_id": cid,
                        "message": f"charge_id {cid} inconnu.", "severite": "A_CONTROLER"})
    return out

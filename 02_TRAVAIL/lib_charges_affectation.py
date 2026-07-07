"""Lot9/Lot10 — lecture des affectations analytiques d'une charge (multi-logements/propriétaires).

Une charge reste UNE charge économique unique (montant total, portée par le flux Lot9). Les affectations
sont des quotes-parts analytiques par logement (somme = montant), permettant à Lot10 d'attribuer le résultat
par logement et par propriétaire SANS multiplier la charge économique.

Lit l'onglet AFFECTATIONS de SAISIE_Charges_Impacts (lié par charge_id). Aucune écriture réelle.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

TOLERANCE = 0.01


def charger_affectations(path: Path) -> list[dict[str, Any]]:
    import openpyxl
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        ws = wb["AFFECTATIONS"]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return []
    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    out = []
    for r in rows[1:]:
        if not any(c not in (None, "") for c in r):
            continue
        out.append({headers[i]: r[i] for i in range(len(headers))})
    return out


def affectations_d_une_charge(rows: Iterable[dict[str, Any]], charge_id: str) -> list[dict[str, Any]]:
    cid = str(charge_id or "").strip()
    return [r for r in rows if str(r.get("charge_id", "") or "").strip() == cid]


def somme_quotes(rows: list[dict[str, Any]]) -> float:
    return round(sum(float(r.get("quote_part") or 0.0) for r in rows), 2)


def verifier_somme(rows_charge: list[dict[str, Any]], montant: float) -> bool:
    """La somme des quotes-parts d'une charge = montant (jamais répliqué intégralement)."""
    return abs(somme_quotes(rows_charge) - round(float(montant), 2)) <= TOLERANCE


def impacts_par_logement(rows: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for r in rows:
        lid = str(r.get("logement_id", "") or "").strip()
        if not lid:
            continue
        out[lid] = round(out.get(lid, 0.0) + float(r.get("quote_part") or 0.0), 2)
    return out


def impacts_par_proprietaire(rows: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for r in rows:
        pid = str(r.get("proprietaire_id", "") or "").strip()
        if not pid:
            continue
        out[pid] = round(out.get(pid, 0.0) + float(r.get("quote_part") or 0.0), 2)
    return out


def charge_economique_unique(rows_charge: list[dict[str, Any]], montant: float) -> dict[str, Any]:
    """Contrôle : une charge = une seule charge économique ; les affectations n'en font pas plusieurs.

    - total_analytique = somme quotes-parts = montant (pas de total global multiplié) ;
    - nb_impacts = nombre de logements distincts.
    """
    par_log = impacts_par_logement(rows_charge)
    return {
        "montant_economique": round(float(montant), 2),
        "total_analytique": somme_quotes(rows_charge),
        "coherent": verifier_somme(rows_charge, montant),
        "nb_impacts_logement": len(par_log),
        "impacts_logement": par_log,
        "impacts_proprietaire": impacts_par_proprietaire(rows_charge),
    }

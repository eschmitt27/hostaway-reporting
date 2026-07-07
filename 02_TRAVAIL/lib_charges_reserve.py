"""Lot12 — lecture PRÉPARATOIRE de la réserve de charges refacturables.

But : identifier, lors de la préparation des préfactures, les propositions refacturables EN_ATTENTE
liées à un propriétaire × mois. NE modifie JAMAIS une préfacture, n'applique rien automatiquement.
Les états APPLIQUÉ / REPORTÉ / IGNORÉ peuvent rester non opérationnels ; leur lecture ne casse pas le modèle.

Lit l'onglet RESERVE_REFACTURATION de SAISIE_Charges_Impacts (lié par charge_id). Aucune écriture réelle.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

STATUT_EN_ATTENTE = "EN_ATTENTE"
STATUTS_CONNUS = {"EN_ATTENTE", "APPLIQUE", "REPORTE", "IGNORE"}


def charger_reserve(path: Path) -> list[dict[str, Any]]:
    import openpyxl
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    try:
        ws = wb["RESERVE_REFACTURATION"]
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


def reserves_en_attente(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in rows if str(r.get("statut_traitement", "") or "").strip().upper() == STATUT_EN_ATTENTE]


def propositions_pour_prefacture(
    rows: Iterable[dict[str, Any]], proprietaire_id: str, mois: str
) -> list[dict[str, Any]]:
    """Propositions refacturables EN_ATTENTE d'un propriétaire × mois (lecture seule).

    Dédup par reserve_id : jamais deux propositions pour la même quote-part.
    """
    prop = str(proprietaire_id or "").strip()
    m = str(mois or "").strip()
    vus: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in reserves_en_attente(rows):
        if str(r.get("proprietaire_id", "") or "").strip() != prop:
            continue
        if str(r.get("mois", "") or "").strip() != m:
            continue
        rid = str(r.get("reserve_id", "") or "").strip()
        if rid in vus:
            continue
        vus.add(rid)
        out.append(r)
    return out


def montant_propositions(rows: list[dict[str, Any]]) -> float:
    return round(sum(float(r.get("montant_refacturable") or 0.0) for r in rows), 2)


def prefacture_inchangee(montant_prefacture_avant: float, montant_prefacture_apres: float) -> bool:
    """Contrôle : lire la réserve n'augmente jamais automatiquement une préfacture."""
    return round(float(montant_prefacture_avant), 2) == round(float(montant_prefacture_apres), 2)

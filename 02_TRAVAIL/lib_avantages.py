"""Agrégation des avantages associés issus des charges (réplique testable du PQ Lot7).

Le classeur Lot7 (MASTER_FACT_MAN_IK_Avantages.xlsx) transforme SOURCE_SAISIE + SAISIE_Charges_Flux
via Power Query (Excel). Cette librairie réplique la partie « avantage issu d'une charge » de façon
déterministe et testable en Python, SANS écrire dans les données réelles.

Principe : l'avantage d'une charge est PORTÉ PAR LA CHARGE (colonne `avantage_associe_id`, distincte
du moyen de paiement) ou, à défaut, dérivé du type de flux personnel (TYPE_FLUX_002). Chaque charge est
comptée EXACTEMENT UNE FOIS pour son bénéficiaire — jamais ressaisie en Lot7 (anti double comptage).
"""
from __future__ import annotations

from typing import Any, Iterable

# Flux personnels historiquement comptés comme avantage brut (parité PQ Lot7).
TYPE_FLUX_AVANTAGE_PERSO = {"TYPE_FLUX_002"}
# Flux « charge société payée perso/liquide » (déduits des avantages nets, non un avantage brut).
TYPE_FLUX_CHARGE_SOCIETE = {"TYPE_FLUX_004", "TYPE_FLUX_008"}


def beneficiaire(charge: dict[str, Any]) -> str | None:
    """Bénéficiaire de l'avantage : avantage_associe_id (explicite) sinon associe_id (paiement)."""
    av = str(charge.get("avantage_associe_id", "") or "").strip()
    if av:
        return av
    return str(charge.get("associe_id", "") or "").strip() or None


def charge_est_avantage(charge: dict[str, Any]) -> bool:
    """Une charge constitue un avantage si explicitement flaggée OU flux personnel TYPE_FLUX_002."""
    if str(charge.get("avantage_associe_id", "") or "").strip():
        return True
    return str(charge.get("type_flux_id", "") or "").strip() in TYPE_FLUX_AVANTAGE_PERSO


def _montant(charge: dict[str, Any]) -> float:
    try:
        return round(float(charge.get("montant") or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def aggregate_avantages(charges: Iterable[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Agrège les avantages par (associe_id, mois). Chaque charge comptée UNE seule fois.

    Déduplication par charge_id : un même charge_id présent deux fois en entrée n'est compté qu'une fois
    (idempotence d'une seconde exécution). Retourne {(associe, mois): {avantage_brut, nb_charges, charge_ids}}.
    """
    vus: set[str] = set()
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for c in charges:
        cid = str(c.get("charge_id", "") or "").strip()
        if not cid or cid in vus:
            continue
        vus.add(cid)
        if not charge_est_avantage(c):
            continue
        benef = beneficiaire(c)
        if not benef:
            continue
        mois = str(c.get("mois", "") or "").strip()
        key = (benef, mois)
        agg = out.setdefault(key, {"associe_id": benef, "mois": mois,
                                   "avantage_brut": 0.0, "nb_charges": 0, "charge_ids": []})
        agg["avantage_brut"] = round(agg["avantage_brut"] + _montant(c), 2)
        agg["nb_charges"] += 1
        agg["charge_ids"].append(cid)
    return out


def avantage_pour(charges: Iterable[dict[str, Any]], associe_id: str, mois: str) -> float:
    """Avantage brut total d'un associé sur un mois (0.0 si aucun)."""
    agg = aggregate_avantages(charges)
    return agg.get((str(associe_id).strip(), str(mois).strip()), {}).get("avantage_brut", 0.0)

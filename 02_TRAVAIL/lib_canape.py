"""Sofa-bed preparation charged to owners when paid by travellers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CanapeResult:
    status: str
    amount: float = 0.0
    message: str = ""


def _f(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _configured_rule(logement_id: str, ref_row: dict[str, Any] | None) -> tuple[int | None, float | None, str]:
    if ref_row:
        seuil = _f(ref_row.get("seuil_voyageurs_preparation_canape"))
        montant = _f(ref_row.get("montant_preparation_canape"))
        if seuil is not None and montant is not None and montant > 0:
            return int(seuil), float(montant), "REF_Logements"

    legacy = {
        "LOG_0006": (3, 10.0),
        "LOG_0008": (5, 10.0),
        "LOG_0011": (5, 10.0),
        "LOG_0013": (5, 10.0),
    }
    if logement_id in legacy:
        seuil, montant = legacy[logement_id]
        return seuil, montant, "REGLE_TRANSITOIRE_CANAPE_A_PARAMETRER_REF_LOGEMENTS"
    return None, None, "NON_APPLICABLE"


def calculate_canape_amount(logement_id: Any, guest_count: Any, ref_row: dict[str, Any] | None = None) -> CanapeResult:
    lid = "" if logement_id is None else str(logement_id).strip()
    seuil, montant, source = _configured_rule(lid, ref_row)
    if seuil is None or montant is None:
        return CanapeResult("NON_APPLICABLE", 0.0, source)
    guests = _f(guest_count)
    if guests is None:
        return CanapeResult("A_CONTROLER", 0.0, "Nombre de voyageurs absent pour logement avec preparation canape")
    if guests >= seuil:
        return CanapeResult("OK", montant, source)
    return CanapeResult("NON_ELIGIBLE", 0.0, source)

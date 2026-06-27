"""Settlement and reimbursement rules.

This module contains no workbook dependency. It is used by Lot10/Lot11 and by
unit tests to keep Airbnb payments, AirCover reimbursements and overpayments
out of the economic result.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

AIRBNB_IMPUTATION_REQUIRED_COLUMNS = [
    "imputation_airbnb_id",
    "transaction_banque_id",
    "reference_airbnb",
    "proprietaire_id",
    "logement_id",
    "mois",
    "document_id",
    "montant_impute",
    "date_imputation",
    "justificatif",
    "statut",
    "commentaire",
]

AIRCOVER_REQUIRED_COLUMNS = [
    "aircover_id",
    "date",
    "montant",
    "beneficiaire_reel",
    "proprietaire_id",
    "logement_id",
    "reservation_id",
    "mois",
    "justificatif",
    "traitement",
    "statut_controle",
    "commentaire",
]


@dataclass(frozen=True)
class SettlementResult:
    reste_a_payer: float
    credit_a_traiter: float
    statut: str


def _blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _money(value: Any) -> float | None:
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def validated_airbnb_imputation(row: dict[str, Any]) -> tuple[bool, str]:
    """Return whether an Airbnb payment is certain enough to reduce settlement."""

    required = ["transaction_banque_id", "proprietaire_id", "logement_id", "mois", "document_id", "justificatif"]
    missing = [c for c in required if _blank(row.get(c))]
    if missing:
        return False, "AIRBNB_IMPUTATION_INCOMPLETE"
    if str(row.get("statut") or "").strip().upper() not in {"VALIDE", "VALIDEE", "VALIDÉE"}:
        return False, "AIRBNB_IMPUTATION_NON_VALIDEE"
    if _money(row.get("montant_impute")) is None:
        return False, "AIRBNB_IMPUTATION_MONTANT_INVALIDE"
    return True, "OK"


def settle_invoice(total_due: Any, owner_advances: Any = 0, airbnb_imputed: Any = 0, other_paid: Any = 0) -> SettlementResult:
    due = _money(total_due)
    advances = _money(owner_advances)
    airbnb = _money(airbnb_imputed)
    other = _money(other_paid)
    if due is None or advances is None or airbnb is None or other is None:
        return SettlementResult(0.0, 0.0, "REGLEMENT_MONTANT_INVALIDE")
    received = round(advances + airbnb + other, 2)
    raw_rest = round(due - received, 2)
    if raw_rest < 0:
        return SettlementResult(0.0, abs(raw_rest), "TROP_PERÇU / CRÉDIT À TRAITER")
    return SettlementResult(raw_rest, 0.0, "A_CONTROLER")


def aircover_auto_impact(row: dict[str, Any]) -> tuple[float, float, float, str]:
    """AirCover never changes payout, commission or owner net automatically."""

    traitement = str(row.get("traitement") or "").strip()
    justificatif = str(row.get("justificatif") or "").strip()
    if not traitement or not justificatif:
        return 0.0, 0.0, 0.0, "AIRCOVER_A_CONTROLER"
    return 0.0, 0.0, 0.0, "AIRCOVER_TRAITEMENT_EXPLICITE_A_REVOIR"

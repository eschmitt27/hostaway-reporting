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


REFACTURABLE_CHARGE_REQUIRED_COLUMNS = [
    "charge_id",
    "mois",
    "montant",
    "refacturable",
    "statut_controle",
    "logement_id",
    "proprietaire_id",
]


def eligible_refacturable_charge(row: dict[str, Any]) -> tuple[bool, float, str]:
    """Decide whether a charge line feeds ``charges_exceptionnelles_refacturees``.

    D033/D034/D041/D042: a charge only enters the owner REGLEMENT block when it is
    explicitly marked refacturable, validated, and attributable to an owner. The
    eligibility is never inferred from the category or the type_flux — only the
    explicit ``refacturable`` flag decides.

    Returns ``(eligible, montant, code)``:
      - ``(True, montant, "OK")`` when the line must be added to the owner settlement;
      - ``(False, 0.0, "NON_REFACTURABLE")`` when ``refacturable`` != OUI;
      - ``(False, 0.0, "CHARGE_NON_VALIDE")`` when ``statut_controle`` != VALIDE;
      - ``(False, 0.0, "MONTANT_INVALIDE")`` when the amount is missing or <= 0;
      - ``(False, montant, "REFAC_SANS_PROPRIETAIRE")`` when refacturable+valid but no
        owner can be attributed (the amount is returned so the caller can trace it).
    """

    if str(row.get("refacturable") or "").strip().upper() != "OUI":
        return False, 0.0, "NON_REFACTURABLE"
    if str(row.get("statut_controle") or "").strip().upper() != "VALIDE":
        return False, 0.0, "CHARGE_NON_VALIDE"
    montant = _money(row.get("montant"))
    if montant is None or montant <= 0:
        return False, 0.0, "MONTANT_INVALIDE"
    prop = str(row.get("proprietaire_id") or "").strip()
    if not prop or prop == "None":
        return False, montant, "REFAC_SANS_PROPRIETAIRE"
    return True, montant, "OK"


def aggregate_refacturable_charges(
    charges: Any,
) -> tuple[dict[tuple, tuple[float, Any]], dict[tuple, float], list[dict[str, Any]]]:
    """Aggregate eligible refacturable charges for the owner REGLEMENT block.

    Returns ``(by_log, by_prop, controls)``:
      - ``by_log``  : ``{(mois, logement_id): (montant, proprietaire_id)}`` for charges
        attributable to a logement (main case — join into the REGLEMENT rows).
      - ``by_prop`` : ``{(mois, proprietaire_id): montant}`` for charges with an owner but
        no logement (routed to a GLOBAL_NON_AFFECTE sentinel row by the caller).
      - ``controls``: one dict per refacturable+valid charge that has no attributable
        owner, so it is never dropped silently nor assigned arbitrarily.

    A charge is never counted in both ``by_log`` and ``by_prop``.
    """

    by_log: dict[tuple, tuple[float, Any]] = {}
    by_prop: dict[tuple, float] = {}
    controls: list[dict[str, Any]] = []
    for row in charges:
        eligible, montant, code = eligible_refacturable_charge(row)
        if code == "REFAC_SANS_PROPRIETAIRE":
            controls.append({
                "charge_id": row.get("charge_id"),
                "mois": row.get("mois"),
                "montant": round(montant, 2),
                "code_controle": code,
                "message": "Charge refacturable sans proprietaire exploitable; aucun rattachement reglement.",
                "statut": "A_CONTROLER",
            })
            continue
        if not eligible:
            continue
        mois = row.get("mois")
        prop = str(row.get("proprietaire_id")).strip()
        logid = str(row.get("logement_id") or "").strip()
        if logid and logid != "None":
            key = (mois, logid)
            cur = by_log.get(key, (0.0, prop))
            by_log[key] = (round(cur[0] + montant, 2), cur[1])
        else:
            key = (mois, prop)
            by_prop[key] = round(by_prop.get(key, 0.0) + montant, 2)
    return by_log, by_prop, controls


def aircover_auto_impact(row: dict[str, Any]) -> tuple[float, float, float, str]:
    """AirCover never changes payout, commission or owner net automatically."""

    beneficiaire = str(row.get("beneficiaire_reel") or "").strip().upper()
    traitement = str(row.get("traitement") or "").strip()
    justificatif = str(row.get("justificatif") or "").strip()
    statut = str(row.get("statut_controle") or "").strip().upper()
    if beneficiaire not in {"PROPRIETAIRE", "CONCIERGERIE"}:
        return 0.0, 0.0, 0.0, "AIRCOVER_A_CONTROLER"
    if not traitement or not justificatif:
        return 0.0, 0.0, 0.0, "AIRCOVER_A_CONTROLER"
    if statut not in {"VALIDE", "VALIDEE", "VALIDÉE"}:
        return 0.0, 0.0, 0.0, "AIRCOVER_A_CONTROLER"
    return 0.0, 0.0, 0.0, "AIRCOVER_TRAITEMENT_EXPLICITE_A_REVOIR"

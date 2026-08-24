"""Dated reference resolution helpers.

The functions in this module are deliberately dependency-free so the business
rules can be unit-tested without opening the operational workbooks.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Iterable


REF_GESTION_LOGEMENTS_HIST_SHEET = "REF_Gestion_Logements_Hist"


class RefHistoryError(ValueError):
    """Raised when a dated reference cannot be resolved safely."""


@dataclass(frozen=True)
class Resolution:
    status: str
    value: Any = None
    row: dict[str, Any] | None = None
    message: str = ""


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip() == ""


def norm_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def is_active(value: Any) -> bool:
    if value is None:
        return True
    return norm_text(value).upper() in {"OUI", "YES", "TRUE", "1", "ACTIF"}


def parse_date(value: Any) -> _dt.date | None:
    if value is None or value == "":
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, (int, float)):
        # Excel serial date, 1899-12-30 convention used by openpyxl.
        return (_dt.date(1899, 12, 30) + _dt.timedelta(days=int(value)))
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return _dt.datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    try:
        return _dt.datetime.fromisoformat(s[:10]).date()
    except ValueError as exc:
        raise RefHistoryError(f"Date invalide: {value!r}") from exc


def applies_on(row: dict[str, Any], ref_date: Any, start_col: str, end_col: str) -> bool:
    d = parse_date(ref_date)
    if d is None:
        return False
    start = parse_date(row.get(start_col))
    end = parse_date(row.get(end_col))
    if start is not None and d < start:
        return False
    if end is not None and d > end:
        return False
    return True


def resolve_commission_rate(
    rows: Iterable[dict[str, Any]],
    *,
    proprietaire_id: Any,
    logement_id: Any,
    ref_date: Any,
) -> Resolution:
    """Resolve dated commission rate.

    Priority:
      2. logement-specific rate, regardless of owner field
      1. owner rate with blank logement_id
    Several rows at the selected priority are ambiguous and must block.
    """

    prop = norm_text(proprietaire_id)
    log = norm_text(logement_id)
    candidates: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        if not is_active(row.get("actif")):
            continue
        if not applies_on(row, ref_date, "date_debut", "date_fin"):
            continue
        row_log = norm_text(row.get("logement_id"))
        row_prop = norm_text(row.get("proprietaire_id"))
        if row_log and row_log == log:
            candidates.append((2, row))
        elif not row_log and row_prop == prop:
            candidates.append((1, row))

    if not candidates:
        return Resolution("MISSING", message="Aucun taux historise applicable")

    best_priority = max(priority for priority, _ in candidates)
    best = [row for priority, row in candidates if priority == best_priority]
    if len(best) > 1:
        ids = ", ".join(norm_text(r.get("taux_commission_id")) for r in best)
        return Resolution("AMBIGUOUS", message=f"Taux simultanes applicables: {ids}")

    row = best[0]
    raw_rate = row.get("taux_commission")
    try:
        rate = float(raw_rate)
    except (TypeError, ValueError):
        return Resolution("MISSING", row=row, message=f"Taux invalide: {raw_rate!r}")
    return Resolution("OK", value=rate, row=row, message="")


def resolve_canape_parametres(
    rows: Iterable[dict[str, Any]],
    *,
    logement_id: Any,
    ref_date: Any,
) -> Resolution:
    """Resolve dated sofa-bed ("canape") preparation parameters (seuil/montant) for a logement.

    No configured rule for a logement is a legitimate, common case — a logement simply doesn't
    charge for sofa-bed preparation. This resolver only distinguishes "no row applies at this
    date" (MISSING) from "exactly one row applies" (OK, `res.row` carries seuil/montant) from
    "several simultaneous rows" (AMBIGUOUS, a data error the admin layer should already prevent).
    Never falls back to whatever row happens to be "current" — a MISSING/AMBIGUOUS result must
    never be silently replaced by today's value.
    """
    log = norm_text(logement_id)
    candidates = []
    for row in rows:
        if norm_text(row.get("logement_id")) != log:
            continue
        if not is_active(row.get("actif")):
            continue
        if not applies_on(row, ref_date, "date_debut", "date_fin"):
            continue
        candidates.append(row)

    if not candidates:
        return Resolution("MISSING", message="Aucun parametre canape configure pour cette date")
    if len(candidates) > 1:
        ids = ", ".join(norm_text(r.get("canape_parametre_id")) for r in candidates)
        return Resolution("AMBIGUOUS", message=f"Parametres canape simultanes: {ids}")
    return Resolution("OK", row=candidates[0])


def resolve_regle_version(
    rows: Iterable[dict[str, Any]],
    *,
    rule_code: Any,
    ref_date: Any,
) -> Resolution:
    """Resolve which VERSION of an algorithmic rule (assiette de commission, répartition d'une
    charge commune de facture, ...) applies at a given economic date.

    Distinct from a simple historized VARIABLE (taux, montant) : a rule is identified by a stable
    `rule_code` (e.g. "ASSIETTE_COMMISSION") and resolves to a stable `version` string (e.g. "V1")
    — the actual implementation for that version lives in code, never in the database. `res.value`
    carries the version string, `res.row` the full row (parametres/commentaire included).

    Mission 6 bis: today only V1 exists for each rule_code — this resolver proves the SELECTION
    mechanism works (tested with a fixture-only "V2"), it does not itself introduce any V2.
    """
    code = norm_text(rule_code)
    candidates = []
    for row in rows:
        if norm_text(row.get("rule_code")) != code:
            continue
        if not is_active(row.get("actif")):
            continue
        if not applies_on(row, ref_date, "date_debut", "date_fin"):
            continue
        candidates.append(row)

    if not candidates:
        return Resolution("MISSING", message=f"Aucune version de regle applicable pour {code!r} a cette date")
    if len(candidates) > 1:
        versions = ", ".join(norm_text(r.get("version")) for r in candidates)
        return Resolution("AMBIGUOUS", message=f"Versions simultanees pour {code!r}: {versions}")
    row = candidates[0]
    return Resolution("OK", value=norm_text(row.get("version")), row=row)


def resolve_parametre_general(
    rows: Iterable[dict[str, Any]],
    *,
    nom_parametre: Any,
    ref_date: Any,
) -> Resolution:
    """Resolve a dated value from `ref_parametres_generaux` (grain `nom_parametre`, columns
    `date_debut_validite`/`date_fin_validite`) — same fail-closed shape as the other resolvers,
    generalized so any economically-evolving general parameter can be resolved by date instead of
    "whichever row happens to match the name" (the gap found in `lot6e_gainperte_menages.py`'s
    `TAUX_HORAIRE_MENAGE_INTERNE` lookup).
    """
    nom = norm_text(nom_parametre)
    candidates = []
    for row in rows:
        if norm_text(row.get("nom_parametre")) != nom:
            continue
        if not is_active(row.get("actif")):
            continue
        if not applies_on(row, ref_date, "date_debut_validite", "date_fin_validite"):
            continue
        candidates.append(row)

    if not candidates:
        return Resolution("MISSING", message=f"Aucun parametre {nom!r} applicable a cette date")
    if len(candidates) > 1:
        return Resolution("AMBIGUOUS", message=f"Valeurs simultanees pour le parametre {nom!r}")
    row = candidates[0]
    try:
        valeur = float(str(row.get("valeur")).replace(",", "."))
    except (TypeError, ValueError):
        return Resolution("MISSING", row=row, message=f"Valeur invalide pour {nom!r}: {row.get('valeur')!r}")
    return Resolution("OK", value=valeur, row=row)


def resolve_management_period(
    rows: Iterable[dict[str, Any]],
    *,
    logement_id: Any,
    date_arrivee: Any,
    date_depart: Any = None,
) -> Resolution:
    """Resolve owner/management period for a reservation.

    The full stay must fit within one active management period. A stay crossing
    a boundary is controlled instead of silently reassigned.
    """

    log = norm_text(logement_id)
    start = parse_date(date_arrivee)
    end = parse_date(date_depart) or start
    if start is None:
        return Resolution("MISSING", message="Date d'arrivee absente")

    dated_candidates = []
    undated_candidates = []
    for row in rows:
        if norm_text(row.get("logement_id")) != log:
            continue
        if not is_active(row.get("actif")):
            continue
        if not applies_on(row, start, "date_debut", "date_fin"):
            continue
        period_end = parse_date(row.get("date_fin"))
        if period_end is not None and end is not None and end > period_end:
            return Resolution("OUT_OF_PERIOD", row=row, message="Sejour chevauche la fin de gestion")
        if parse_date(row.get("date_debut")) is None:
            undated_candidates.append(row)
        else:
            dated_candidates.append(row)

    # A dated explicit management period is more specific than an undated
    # historical line whose start is unknown. Undated rows are only used when
    # they are the sole applicable source for the stay.
    candidates = dated_candidates or undated_candidates

    if not candidates:
        return Resolution("MISSING", message="Aucune periode de gestion applicable")
    if len(candidates) > 1:
        ids = ", ".join(norm_text(r.get("gestion_id")) for r in candidates)
        return Resolution("AMBIGUOUS", message=f"Periodes de gestion simultanees: {ids}")
    row = candidates[0]
    if is_blank(row.get("proprietaire_id")):
        return Resolution("MISSING_OWNER", row=row, message="Periode sans proprietaire")
    if parse_date(row.get("date_debut")) is None:
        return Resolution(
            "OK",
            value=row.get("proprietaire_id"),
            row=row,
            message="Date de debut de gestion absente: historique anterieur ou inconnu non bloquant",
        )
    return Resolution("OK", value=row.get("proprietaire_id"), row=row)

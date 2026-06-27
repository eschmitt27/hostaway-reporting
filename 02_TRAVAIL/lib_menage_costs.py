"""Internal cleaning cost rules.

Business switch:
  - until 2026-05-31: hours * dated worker hourly rate
  - from 2026-06-01: dated fixed cost per cleaning

No method falls back to the other one.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Iterable

from lib_ref_history import is_active, is_blank, norm_text, parse_date

PIVOT_FIXED_COST = _dt.date(2026, 6, 1)


@dataclass(frozen=True)
class CostResolution:
    status: str
    method: str
    total: float | None = None
    unit: float | None = None
    rate: float | None = None
    ref_id: str | None = None
    priority: int | None = None
    message: str = ""


def _applies(row: dict[str, Any], ref_date: _dt.date) -> bool:
    start = parse_date(row.get("date_debut") or row.get("date_debut_validite"))
    end = parse_date(row.get("date_fin") or row.get("date_fin_validite"))
    if start is not None and ref_date < start:
        return False
    if end is not None and ref_date > end:
        return False
    return True


def _f(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve_hourly_rate(
    rows: Iterable[dict[str, Any]],
    *,
    intervenant_id: Any,
    ref_date: Any,
) -> CostResolution:
    d = parse_date(ref_date)
    method = "INTERNE_HEURES_TAUX_INTERVENANT"
    if d is None:
        return CostResolution("MISSING", method, message="Date de reference absente")
    iid = norm_text(intervenant_id)
    matches = []
    for row in rows:
        if not is_active(row.get("actif")):
            continue
        if norm_text(row.get("intervenant_id")) != iid:
            continue
        if not _applies(row, d):
            continue
        matches.append(row)
    if not matches:
        return CostResolution("MISSING", method, message="Aucun taux horaire applicable")
    if len(matches) > 1:
        ids = ", ".join(norm_text(r.get("taux_horaire_id")) for r in matches)
        return CostResolution("AMBIGUOUS", method, message=f"Taux horaires simultanes: {ids}")
    row = matches[0]
    rate = _f(row.get("taux_horaire"))
    if rate is None:
        return CostResolution("MISSING", method, ref_id=norm_text(row.get("taux_horaire_id")), message="Taux horaire invalide")
    return CostResolution("OK", method, rate=rate, ref_id=norm_text(row.get("taux_horaire_id")))


def _fixed_candidate_priority(row: dict[str, Any], intervenant_id: str, logement_id: str, type_logement_id: str) -> int | None:
    row_i = norm_text(row.get("intervenant_id"))
    row_l = norm_text(row.get("logement_id"))
    row_t = norm_text(row.get("type_logement_id"))
    explicit_priority = _f(row.get("priorite"))

    if row_i and row_l and row_i == intervenant_id and row_l == logement_id:
        return int(explicit_priority or 1)
    if not row_i and row_l and row_l == logement_id:
        return int(explicit_priority or 2)
    if row_i and row_t and row_i == intervenant_id and row_t == type_logement_id:
        return int(explicit_priority or 3)
    if not row_i and row_t and row_t == type_logement_id:
        return int(explicit_priority or 4)
    if is_blank(row_i) and is_blank(row_l) and is_blank(row_t):
        return int(explicit_priority or 5)
    return None


def resolve_fixed_internal_cost(
    rows: Iterable[dict[str, Any]],
    *,
    intervenant_id: Any,
    logement_id: Any,
    type_logement_id: Any,
    ref_date: Any,
    nb_menages: Any,
) -> CostResolution:
    d = parse_date(ref_date)
    method = "INTERNE_COUT_FIXE_PARAMETRE"
    if d is None:
        return CostResolution("MISSING", method, message="Date de reference absente")
    iid = norm_text(intervenant_id)
    lid = norm_text(logement_id)
    tid = norm_text(type_logement_id)
    candidates: list[tuple[int, dict[str, Any]]] = []
    for row in rows:
        if not is_active(row.get("actif")):
            continue
        if not _applies(row, d):
            continue
        priority = _fixed_candidate_priority(row, iid, lid, tid)
        if priority is not None:
            candidates.append((priority, row))
    if not candidates:
        return CostResolution("MISSING", method, message="Aucun cout fixe applicable")

    best_priority = min(priority for priority, _ in candidates)
    best = [row for priority, row in candidates if priority == best_priority]
    if len(best) > 1:
        ids = ", ".join(norm_text(r.get("cout_interne_id") or r.get("cout_menage_interne_id")) for r in best)
        return CostResolution("AMBIGUOUS", method, priority=best_priority, message=f"Couts fixes simultanes: {ids}")

    row = best[0]
    unit = _f(row.get("cout_fixe_par_menage") or row.get("montant_interne_standard"))
    count = _f(nb_menages)
    if unit is None or count is None:
        return CostResolution("MISSING", method, priority=best_priority, message="Cout fixe ou volume invalide")
    return CostResolution(
        "OK",
        method,
        total=round(unit * count, 2),
        unit=unit,
        ref_id=norm_text(row.get("cout_interne_id") or row.get("cout_menage_interne_id")),
        priority=best_priority,
    )


def resolve_internal_cleaning_cost(
    *,
    ref_date: Any,
    intervenant_id: Any,
    logement_id: Any,
    type_logement_id: Any,
    nb_menages: Any,
    nb_heures: Any,
    hourly_rows: Iterable[dict[str, Any]],
    fixed_rows: Iterable[dict[str, Any]],
) -> CostResolution:
    d = parse_date(ref_date)
    if d is None:
        return CostResolution("MISSING", "DATE_ABSENTE", message="Date de reference absente")
    if d < PIVOT_FIXED_COST:
        rate = resolve_hourly_rate(hourly_rows, intervenant_id=intervenant_id, ref_date=d)
        if rate.status != "OK":
            return rate
        hours = _f(nb_heures)
        if hours is None:
            return CostResolution("MISSING", rate.method, ref_id=rate.ref_id, rate=rate.rate, message="Heures absentes avant juin")
        return CostResolution("OK", rate.method, total=round(hours * rate.rate, 2), rate=rate.rate, ref_id=rate.ref_id)
    return resolve_fixed_internal_cost(
        fixed_rows,
        intervenant_id=intervenant_id,
        logement_id=logement_id,
        type_logement_id=type_logement_id,
        ref_date=d,
        nb_menages=nb_menages,
    )

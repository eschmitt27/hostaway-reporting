"""Canonical guestCount resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import re
from typing import Any

SOURCE_API_LIST = "API_LIST"
SOURCE_API_DETAIL = "API_DETAIL"
SOURCE_LEGACY_SNAPSHOT = "LEGACY_SNAPSHOT"
SOURCE_ABSENT = "ABSENT"
SOURCE_CONFLICT_API = "CONFLIT_API"

CODE_CONFLICT_API = "GUEST_COUNT_CONFLICT_API_LIST_DETAIL"
CODE_INVALID_API = "GUEST_COUNT_INVALIDE_API"


@dataclass(frozen=True)
class GuestCountResolution:
    value: int | None
    source: str
    status: str
    code: str | None = None
    message: str | None = None


def is_missing(value: Any) -> bool:
    return value is None


def normalize_guest_count(value: Any) -> tuple[int | None, str | None]:
    """Return (integer guest count, error code). Missing is not an error."""
    if is_missing(value):
        return None, None
    if isinstance(value, float) and math.isnan(value):
        return None, CODE_INVALID_API
    if isinstance(value, str) and value.strip() == "":
        return None, CODE_INVALID_API
    if isinstance(value, bool):
        return None, CODE_INVALID_API
    if isinstance(value, int):
        return (value, None) if value >= 0 else (None, CODE_INVALID_API)
    if isinstance(value, float):
        if math.isnan(value) or value < 0 or not value.is_integer():
            return None, CODE_INVALID_API
        return int(value), None
    text = str(value).strip()
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return None, CODE_INVALID_API
    number = float(text)
    if number < 0 or not number.is_integer():
        return None, CODE_INVALID_API
    return int(number), None


def resolve_api_guest_count(list_value: Any, detail_value: Any = None) -> GuestCountResolution:
    list_count, list_error = normalize_guest_count(list_value)
    detail_count, detail_error = normalize_guest_count(detail_value)
    if list_error or detail_error:
        return GuestCountResolution(None, SOURCE_ABSENT, "A_CONTROLER", CODE_INVALID_API, "guestCount API invalide")
    has_list = list_count is not None
    has_detail = detail_count is not None
    if has_list and has_detail and list_count != detail_count:
        return GuestCountResolution(None, SOURCE_CONFLICT_API, "A_CONTROLER", CODE_CONFLICT_API, "Conflit numberOfGuests liste/detail")
    if has_list:
        return GuestCountResolution(list_count, SOURCE_API_LIST, "OK")
    if has_detail:
        return GuestCountResolution(detail_count, SOURCE_API_DETAIL, "OK")
    return GuestCountResolution(None, SOURCE_ABSENT, "OK")


def extract_number_of_guests_from_snapshot(snapshot: Any) -> Any:
    if is_missing(snapshot):
        return None
    text = str(snapshot)
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        data = None
    if isinstance(data, dict):
        for key in ("numberOfGuests", "guestCount"):
            value, error = normalize_guest_count(data.get(key))
            if error:
                return data.get(key)
            if value is not None:
                return value
    match = re.search(r'"(?:numberOfGuests|guestCount)"\s*:\s*(-?\d+(?:\.\d+)?)', text)
    if match:
        raw = match.group(1)
        value, error = normalize_guest_count(raw)
        return raw if error else value
    return None


def resolve_guest_count_from_sources(
    list_value: Any = None,
    detail_value: Any = None,
    legacy_snapshot_value: Any = None,
) -> GuestCountResolution:
    api = resolve_api_guest_count(list_value, detail_value)
    if api.source != SOURCE_ABSENT or api.code is not None:
        return api
    legacy_value, legacy_error = normalize_guest_count(legacy_snapshot_value)
    if legacy_error:
        return GuestCountResolution(None, SOURCE_ABSENT, "A_CONTROLER", CODE_INVALID_API, "guestCount snapshot invalide")
    if legacy_value is not None:
        return GuestCountResolution(legacy_value, SOURCE_LEGACY_SNAPSHOT, "OK")
    return api


def guest_count_from_reservation(reservation: dict[str, Any]) -> Any:
    return resolve_api_guest_count(reservation.get("numberOfGuests"), reservation.get("guestCount")).value


def guest_count_from_reservation_or_snapshot(
    reservation: dict[str, Any],
    snapshot_guest_count: Any = None,
) -> Any:
    return resolve_guest_count_from_sources(
        reservation.get("numberOfGuests"),
        reservation.get("guestCount"),
        snapshot_guest_count,
    ).value

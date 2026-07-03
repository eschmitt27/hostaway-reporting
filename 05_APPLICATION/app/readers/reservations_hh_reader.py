"""Lecteur read-only des réservations hors Hostaway — Lot APP-2a.

Source : MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx, onglet `MASTER`
(sortie générée par Power Query, consommée telle quelle par le moteur lot4bis).

Règles :
- Lecture seule stricte (excel_reader → openpyxl read_only=True).
- L'onglet MASTER contient une ligne-placeholder Power Query
  (« [Charge par Power Query ...] ») : elle est filtrée.
  Seules les lignes dont reservation_hh_id commence par 'RESHH-' sont retournées.
- Aucune transformation, aucun recalcul : lignes brutes du fichier généré.
- Ne lit JAMAIS SAISIE_*, REF_Setup, REF_Gestion, REF_Proprietaires.
"""
from typing import Any
from app.config import MASTER_RESERVATIONS_HH
from app.readers.excel_reader import read_sheet

SHEET_MASTER = "MASTER"
PK_PREFIX = "RESHH-"


def master_available() -> bool:
    return MASTER_RESERVATIONS_HH.exists()


def _is_real_row(row: dict[str, Any]) -> bool:
    pk = str(row.get("reservation_hh_id") or "").strip()
    return pk.startswith(PK_PREFIX)


def read_reservations() -> list[dict[str, Any]]:
    """Lignes réelles de réservations hors Hostaway (placeholder PQ exclu)."""
    rows = read_sheet(MASTER_RESERVATIONS_HH, SHEET_MASTER, max_rows=None)
    return [r for r in rows if _is_real_row(r)]


def find_reservation(reservation_hh_id: str) -> dict[str, Any] | None:
    target = str(reservation_hh_id).strip()
    for r in read_reservations():
        if str(r.get("reservation_hh_id") or "").strip() == target:
            return r
    return None

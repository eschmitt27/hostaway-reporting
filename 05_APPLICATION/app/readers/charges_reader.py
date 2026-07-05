"""Lecteur read-only charges fournisseurs — Lot APP-3a.

Source unique (lecture seule) :
- MASTER_FACT_MAN_Charges.xlsx onglet MASTER — sortie Power Query Lot3.
  Données présentes uniquement après refresh Excel.

Règles gravées :
- D026 : source unique = MASTER Lot3, jamais MASTER_CALC_Flux ni SAISIE.
- D025 : IK et virements associés exclus du périmètre Fournisseurs.
- D044 : statut_controle affiché tel quel, jamais recalculé.
- Lecture seule stricte via excel_reader (openpyxl read_only=True).
"""
from typing import Any
from app.config import MASTER_CHARGES, SAISIE_CHARGES
from app.readers.excel_reader import read_sheet

SHEET_MASTER = "MASTER"
SOURCE_MASTER = "MASTER_FACT_MAN_Charges.xlsx"
SOURCE_SAISIE = "SAISIE_Charges_Flux.xlsx"

_EXCLUDED_TYPE_FLUX = {"IK", "VIREMENT_ASSOCIE"}


def master_available() -> bool:
    return MASTER_CHARGES.exists()


def saisie_available() -> bool:
    return SAISIE_CHARGES.exists()


def _is_real_row(row: dict[str, Any]) -> bool:
    """Filtre les lignes placeholder Power Query (charge_id commence par '[')."""
    cid = row.get("charge_id")
    if cid is None:
        return False
    s = str(cid).strip()
    return bool(s) and not s.startswith("[")


def _is_fournisseur_row(row: dict[str, Any]) -> bool:
    """Exclut IK et virements associés — D025."""
    tfi = str(row.get("type_flux_id") or "").strip().upper()
    return tfi not in _EXCLUDED_TYPE_FLUX


def read_charges() -> list[dict[str, Any]]:
    """Toutes les charges fournisseurs depuis le MASTER Lot3 (hors IK/virements)."""
    rows = read_sheet(MASTER_CHARGES, SHEET_MASTER, max_rows=None)
    return [r for r in rows if _is_real_row(r) and _is_fournisseur_row(r)]


def find_charge(charge_id: str) -> dict[str, Any] | None:
    """Ligne unique par charge_id. None si absente."""
    cid = str(charge_id).strip()
    for row in read_charges():
        if str(row.get("charge_id") or "").strip() == cid:
            return row
    return None

"""Lecteur REF_Setup.xlsm pour APP-2b — validation saisie HH contrôlée.

Séparé de ref_setup_reader.py (APP-1) pour ne pas violer l'invariant APP-1
(interdiction de lire REF_Gestion_Logements_Hist et REF_Proprietaires dans ce module).
Lecture seule stricte. Aucun calcul métier.
"""
from pathlib import Path
from typing import Any
from app.config import REF_SETUP
from app.readers.excel_reader import read_sheet

_SHEET_CLOTURE = "REF_Cloture_Mensuelle"
_SHEET_GESTION = "REF_Gestion_Logements_Hist"
_SHEET_LOGEMENTS = "REF_Logements"
_SHEET_PROPRIETAIRES = "REF_Proprietaires"
_SHEET_ASSOCIES = "REF_Associes"
_SHEET_CANAUX_RESERVATION = "REF_Canaux_Reservation"
_SHEET_MODES_PAIEMENT = "REF_Modes_Paiement"
_SHEET_CODES_IMPACT = "REF_Codes_Impact"
_SHEET_TAUX_COMMISSION = "REF_Taux_Commission"
_SHEET_COUTS_STANDARDS_MENAGE = "REF_Couts_Standards_Menage"


def get_all_logements_hh(ref_setup_path: Path | None = None) -> list[dict[str, Any]]:
    """Toutes les lignes brutes de REF_Logements (APP-2b : éligibilité logement D8)."""
    p = ref_setup_path or REF_SETUP
    return read_sheet(p, _SHEET_LOGEMENTS, max_rows=None)


def get_gestion_hist(ref_setup_path: Path | None = None) -> list[dict[str, Any]]:
    """Toutes les lignes brutes de REF_Gestion_Logements_Hist (APP-2b : D7/D8)."""
    p = ref_setup_path or REF_SETUP
    return read_sheet(p, _SHEET_GESTION, max_rows=None)


def get_all_proprietaires_hh(ref_setup_path: Path | None = None) -> list[dict[str, Any]]:
    """Toutes les lignes brutes de REF_Proprietaires (APP-2b : divergence D9)."""
    p = ref_setup_path or REF_SETUP
    return read_sheet(p, _SHEET_PROPRIETAIRES, max_rows=None)


def get_all_associes(ref_setup_path: Path | None = None) -> list[dict[str, Any]]:
    """Toutes les lignes brutes de REF_Associes (APP-2b : D11 associé récupérateur)."""
    p = ref_setup_path or REF_SETUP
    return read_sheet(p, _SHEET_ASSOCIES, max_rows=None)


def get_canaux(ref_setup_path: Path | None = None) -> tuple[str, list[dict[str, Any]]]:
    p = ref_setup_path or REF_SETUP
    return _SHEET_CANAUX_RESERVATION, read_sheet(p, _SHEET_CANAUX_RESERVATION, max_rows=None)


def get_modes_paiement(ref_setup_path: Path | None = None) -> tuple[str, list[dict[str, Any]]]:
    p = ref_setup_path or REF_SETUP
    return _SHEET_MODES_PAIEMENT, read_sheet(p, _SHEET_MODES_PAIEMENT, max_rows=None)


def get_codes_impact(ref_setup_path: Path | None = None) -> tuple[str, list[dict[str, Any]]]:
    p = ref_setup_path or REF_SETUP
    return _SHEET_CODES_IMPACT, read_sheet(p, _SHEET_CODES_IMPACT, max_rows=None)


def get_taux_commission(ref_setup_path: Path | None = None) -> list[dict[str, Any]]:
    p = ref_setup_path or REF_SETUP
    return read_sheet(p, _SHEET_TAUX_COMMISSION, max_rows=None)


def get_couts_standards_menage(ref_setup_path: Path | None = None) -> list[dict[str, Any]]:
    p = ref_setup_path or REF_SETUP
    return read_sheet(p, _SHEET_COUTS_STANDARDS_MENAGE, max_rows=None)


def get_cloture_mois(mois_str: str, ref_setup_path: Path | None = None) -> dict[str, Any] | None:
    """Ligne de REF_Cloture_Mensuelle pour le mois donné (AAAA-MM), ou None si absent."""
    p = ref_setup_path or REF_SETUP
    target = str(mois_str).strip()
    for row in read_sheet(p, _SHEET_CLOTURE, max_rows=None):
        if str(row.get("mois", "")).strip() == target:
            return row
    return None

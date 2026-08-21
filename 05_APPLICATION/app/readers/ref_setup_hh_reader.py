"""Lecteur du référentiel pour APP-2b — validation de la saisie HH contrôlée.

Séparé de l'enrichissement fiche logement (APP-1) : ce module a le droit de lire
`REF_Gestion_Logements_Hist` et `REF_Proprietaires`, que l'autre s'interdisait.

MIGRATION EXCEL → SQLITE
Par défaut, ces fonctions lisent le référentiel SQLITE (`ref_*`, migration 0029) via
`ref_setup_repo`. `REF_Setup.xlsm` n'est plus ouvert au runtime.

POURQUOI `ref_setup_path` SUBSISTE — ET POURQUOI CE N'EST PAS UN REPLI EXCEL
Les flux « préparation / écriture réelle sur COPIE » (`saisie_hh_dryrun_service`,
`saisie_hh_real_write_service`, `saisie_hh_schema_real_prepare_service`) simulent une opération sur
une copie de travail du classeur, et doivent donc lire CETTE copie — pas la base. Le paramètre sert
uniquement à cela : il est fourni EXPLICITEMENT par un appelant qui manipule un fichier de travail.

Ce n'est pas le repli interdit par la règle « SQLite sinon Excel » : ce repli-là consisterait à
rouvrir le classeur QUAND LA BASE EST VIDE, ce qui masquerait un référentiel non initialisé. Ici,
sans chemin explicite, la lecture est SQLite et un référentiel absent se voit (liste vide), il n'est
jamais compensé en douce par un fichier.
"""
from pathlib import Path
from typing import Any

from app.readers.excel_reader import read_sheet
from app.services import ref_setup_repo as repo

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


def _lire(onglet: str, ref_setup_path: Path | None, *, db_path=None) -> list[dict[str, Any]]:
    """SQLite par défaut ; le classeur UNIQUEMENT si un chemin de travail est fourni."""
    if ref_setup_path is not None:
        return read_sheet(Path(ref_setup_path), onglet, max_rows=None)
    return repo.lire_onglet(onglet, db_path=db_path)


def get_all_logements_hh(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Logements (APP-2b : éligibilité logement D8)."""
    return _lire(_SHEET_LOGEMENTS, ref_setup_path, db_path=db_path)


def get_gestion_hist(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Gestion_Logements_Hist (APP-2b : D7/D8)."""
    return _lire(_SHEET_GESTION, ref_setup_path, db_path=db_path)


def get_all_proprietaires_hh(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Proprietaires (APP-2b : divergence D9)."""
    return _lire(_SHEET_PROPRIETAIRES, ref_setup_path, db_path=db_path)


def get_all_associes(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    """Toutes les lignes de REF_Associes (APP-2b : D11 associé récupérateur)."""
    return _lire(_SHEET_ASSOCIES, ref_setup_path, db_path=db_path)


def get_canaux(ref_setup_path: Path | None = None, *, db_path=None) -> tuple[str, list[dict[str, Any]]]:
    return _SHEET_CANAUX_RESERVATION, _lire(_SHEET_CANAUX_RESERVATION, ref_setup_path,
                                            db_path=db_path)


def get_modes_paiement(ref_setup_path: Path | None = None, *, db_path=None) -> tuple[str, list[dict[str, Any]]]:
    return _SHEET_MODES_PAIEMENT, _lire(_SHEET_MODES_PAIEMENT, ref_setup_path, db_path=db_path)


def get_codes_impact(ref_setup_path: Path | None = None, *, db_path=None) -> tuple[str, list[dict[str, Any]]]:
    return _SHEET_CODES_IMPACT, _lire(_SHEET_CODES_IMPACT, ref_setup_path, db_path=db_path)


def get_taux_commission(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    return _lire(_SHEET_TAUX_COMMISSION, ref_setup_path, db_path=db_path)


def get_couts_standards_menage(ref_setup_path: Path | None = None, *, db_path=None) -> list[dict[str, Any]]:
    return _lire(_SHEET_COUTS_STANDARDS_MENAGE, ref_setup_path, db_path=db_path)


def get_cloture_mois(mois_str: str, ref_setup_path: Path | None = None, *,
                     db_path=None) -> dict[str, Any] | None:
    """Ligne de REF_Cloture_Mensuelle pour le mois donné (AAAA-MM), ou None si absent."""
    target = str(mois_str).strip()
    for row in _lire(_SHEET_CLOTURE, ref_setup_path, db_path=db_path):
        if str(row.get("mois", "")).strip() == target:
            return row
    return None

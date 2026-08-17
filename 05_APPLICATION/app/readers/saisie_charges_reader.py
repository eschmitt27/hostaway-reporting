"""APP-3b-1 - Lecture SAISIE_Charges_Flux + référentiels charges.

Toutes les fonctions sont en lecture seule.
SAISIE_Charges_Flux.xlsx et REF_Setup.xlsm ne sont jamais modifiés ici.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

SAISIE_SHEET = "SAISIE"

# Colonnes à ne JAMAIS écrire (contiennent des formules)
FORMULA_COLS: frozenset[str] = frozenset({"C", "I", "J", "AD"})

# Mapping colonne → champ pour les colonnes manuelles
MANUAL_COL_MAP: dict[str, str] = {
    "A": "charge_id",
    "B": "date_charge",
    "D": "montant",
    "E": "sens_flux",
    "F": "categorie_charge_id",
    "G": "type_flux_id",
    "H": "code_impact",
    "K": "prise_en_compta",
    "L": "associe_id",
    "M": "mode_paiement_id",
    "N": "carte_id",
    "O": "affectation_type",
    "P": "logement_id",
    "Q": "proprietaire_id",
    "R": "reservation_id",
    "S": "refacturable",
    "T": "source_flux",
    "U": "methode_traitement",
    "V": "paye_avec_montant_recupere",
    "W": "lien_virement_banque",
    "X": "statut_controle",
    "Y": "niveau_anomalie",
    "Z": "code_anomalie",
    "AA": "statut_rapprochement",
    "AB": "justificatif",
    "AC": "commentaire",
    "AE": "date_saisie",
    "AF": "affectable_menage",
    "AG": "intervenant_concerne",
    # Profils d'impact + catégorie personnalisée (migration CHG_024)
    "AH": "profil_impact_charge",
    "AI": "libelle_categorie_personnalise",
    # Avantage associé porté par la charge (bénéficiaire, distinct du paiement) — agrégé par Lot7
    "AJ": "avantage_associe_id",
}


def _col_index(col: str) -> int:
    """Convertit une lettre de colonne Excel en index 1-based."""
    result = 0
    for ch in col.upper():
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result


FORMULA_COL_INDICES: frozenset[int] = frozenset(_col_index(c) for c in FORMULA_COLS)


def _read_sheet_rows(
    path: Path,
    sheet_name: str,
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True, keep_vba=True)
    try:
        ws = wb[sheet_name]
        all_rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if not all_rows:
        return []
    headers = [str(h).strip() if h is not None else "" for h in all_rows[0]]
    result: list[dict[str, Any]] = []
    limit = len(all_rows) if max_rows is None else min(max_rows + 1, len(all_rows))
    for row in all_rows[1:limit]:
        if not any(c is not None for c in row):
            continue
        result.append({headers[i]: row[i] for i in range(len(headers))})
    return result


def read_ref_categories_charges(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Categories_Charges")


def read_ref_types_flux(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Types_Flux")


def read_ref_codes_impact(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Codes_Impact")


def read_ref_modes_paiement(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Modes_Paiement")


def read_ref_associes(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Associes")


def read_ref_cartes_paiement(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Cartes_Paiement")


def read_ref_logements(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Logements")


def read_ref_cloture(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Cloture_Mensuelle")


def read_ref_assoc_mode(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Assoc_Mode")


def read_ref_types_affectation(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Types_Affectation")


def read_ref_statuts(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Statuts")


def read_ref_gestion_logements(ref_path: Path | None = None) -> list[dict[str, Any]]:
    """Historique de gestion : lien logement ↔ propriétaire (statut, dates)."""
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Gestion_Logements_Hist")


def read_ref_intervenants(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Intervenants")


def read_ref_couts_standards_menage(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Couts_Standards_Menage")


def read_ref_proprietaires(ref_path: Path | None = None) -> list[dict[str, Any]]:
    return _read_sheet_rows(Path(ref_path or cfg.REF_SETUP), "REF_Proprietaires")


def find_model_row(saisie_path: Path | None = None) -> int | None:
    """Trouve la première ligne dont charge_id est vide et qui contient des formules.

    Ouvre le fichier avec data_only=False pour détecter les formules.
    Retourne l'index de ligne 1-based, ou None si introuvable.
    Le fichier source n'est jamais modifié.
    """
    p = Path(saisie_path or cfg.SAISIE_CHARGES)
    wb = openpyxl.load_workbook(str(p), read_only=False, data_only=False)
    try:
        ws = wb[SAISIE_SHEET]
        for row_idx, row in enumerate(ws.iter_rows(), 1):
            if row_idx == 1:
                continue
            charge_id_val = row[0].value
            if charge_id_val is not None and str(charge_id_val).strip():
                continue
            for cell in row:
                if cell.column in FORMULA_COL_INDICES:
                    val = cell.value
                    if val is not None and str(val).startswith("="):
                        return row_idx
    finally:
        wb.close()
    return None


def read_all_charge_ids(saisie_path: Path | None = None) -> list[str]:
    """Retourne tous les charge_id non vides de la feuille SAISIE."""
    p = Path(saisie_path or cfg.SAISIE_CHARGES)
    wb = openpyxl.load_workbook(str(p), read_only=True, data_only=True)
    try:
        ws = wb[SAISIE_SHEET]
        result: list[str] = []
        for row_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
            if row_idx == 1:
                continue
            charge_id = row[0]
            if charge_id is not None and str(charge_id).strip():
                result.append(str(charge_id).strip())
    finally:
        wb.close()
    return result


def count_charges_with_prefix(prefix: str, saisie_path: Path | None = None) -> int:
    """Compte les charges existantes dont l'ID commence par {prefix}-."""
    ids = read_all_charge_ids(saisie_path)
    separator = prefix + "-"
    return sum(1 for cid in ids if cid.startswith(separator))


def reservation_id_exists(
    reservation_id: str,
    resolues_path: Path | None = None,
) -> bool:
    """La réservation est-elle connue du dataset RÉSOLU courant.

    Cherche côté Hostaway et côté hors Hostaway, comme avant. La source est désormais SQLite : le
    classeur pouvait dater d'un calcul précédent, et une réservation extraite depuis y aurait été
    jugée inexistante.

    `resolues_path` n'a plus d'objet ; le paramètre subsiste pour les appelants existants. Retourne
    False si aucun dataset n'est disponible — refuser une référence qu'on ne peut pas vérifier vaut
    mieux que l'accepter sans contrôle.
    """
    from app.services import reservations_dataset_service as ds

    return ds.existe(reservation_id, etape=ds.ETAPE_RESOLUES)

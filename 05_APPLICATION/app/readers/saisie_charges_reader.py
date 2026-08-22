"""APP-3b-1 - Lecture référentiels charges (REF_Setup) + vérification réservation.

Toutes les fonctions sont en lecture seule. `REF_Setup.xlsm` n'est jamais modifié ici.

`read_ref_categories_charges`/`read_ref_types_flux` ne sont plus appelées par l'application
(les référentiels charges sont désormais en SQLite) — conservées uniquement parce que
`tests/test_profils_impact_schema.py` les utilise encore pour vérifier, sur le classeur réel,
qu'une migration historique (CHG_024, profils d'impact) est bien en place.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

# Mapping colonne → champ pour les colonnes manuelles (utilisé par test_profils_impact_schema.py
# pour vérifier la présence des colonnes profil ; l'écriture correspondante n'existe plus).
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

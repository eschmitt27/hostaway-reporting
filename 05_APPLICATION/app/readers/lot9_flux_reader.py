"""Lecteur read-only Lot9 — `MASTER_CALC_Flux.xlsx` (flux unifiés).

Ne recalcule rien : sert uniquement la réconciliation Lot9↔Lot10 (`comptabilite_reconciliations_
service.lot9_vs_lot10`), qui relit le grain déjà produit par Lot9 pour vérifier que Lot10 l'a
correctement agrégé — jamais un second moteur de flux.
"""
from __future__ import annotations

from typing import Any

import app.config as cfg
from app.readers.excel_reader import read_sheet

SHEET = "MASTER"

VISION_COLONNE = {
    "REEL": "inclure_resultat_reel",
    "COMPTABLE": "inclure_resultat_comptable",
    "HORS_COMPTA": "inclure_resultat_hors_compta",
}


def disponible() -> bool:
    return cfg.MASTER_CALC_FLUX.exists()


def lire_flux() -> list[dict[str, Any]]:
    if not disponible():
        return []
    return read_sheet(cfg.MASTER_CALC_FLUX, SHEET, max_rows=None)

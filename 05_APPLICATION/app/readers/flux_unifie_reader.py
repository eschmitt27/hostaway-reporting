"""Lecteur Lot9 — flux unifiés (`flux_unifies`, migration 0043).

Ne recalcule rien : sert uniquement la réconciliation Lot9↔Lot10 (`comptabilite_reconciliations_
service.lot9_vs_lot10`), qui relit le grain déjà produit par Lot9 pour vérifier que Lot10 l'a
correctement agrégé — jamais un second moteur de flux.

SQLite-first (mission Ménages/Lot9) : lit `flux_unifies`, construite par `flux_unifie_service.
construire()`. Ne rouvre plus `MASTER_CALC_Flux.xlsx` (classeur devenu `LEGACY_PARITE_TEMPORAIRE` —
un outil de comparaison ponctuelle, plus une source applicative).

`ROW_HASH` en majuscules dans les lignes rendues : c'est le nom que le classeur legacy portait et
que `comptabilite_reconciliations_service` continue d'attendre — la colonne SQLite est `row_hash`
(convention minuscule des tables 0043), traduite ici à la lecture, comme `reservations_dataset_
service` le fait déjà pour `guest_count`/`guestCount`.
"""
from __future__ import annotations

from typing import Any

from app.services import flux_unifie_service as _svc

VISION_COLONNE = {
    "REEL": "inclure_resultat_reel",
    "COMPTABLE": "inclure_resultat_comptable",
    "HORS_COMPTA": "inclure_resultat_hors_compta",
}


def disponible(*, db_path=None) -> bool:
    conn_rows = _svc.lire(db_path=db_path)
    return bool(conn_rows)


def lire_flux(*, db_path=None) -> list[dict[str, Any]]:
    lignes = _svc.lire(db_path=db_path)
    out = []
    for r in lignes:
        d = dict(r)
        d["ROW_HASH"] = d.pop("row_hash", None)
        out.append(d)
    return out

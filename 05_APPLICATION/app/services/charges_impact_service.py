"""Ré-export du moteur Charges pur (Mission 8) — voir `app.moteurs.charges_engine`.

Toute la logique (catalogue, périmètre facture, répartition égale, ventilations analytiques)
vit désormais dans `app/moteurs/charges_engine.py` : c'était déjà un module pur (aucun
sqlite3/FastAPI/pandas/fichier), déplacé pour rendre cette pureté explicite et vérifiable
(même pattern que `fifo_engine.py`/`compte_proprietaire_service.py`, Mission 5).

Ce fichier ne fait plus qu'importer et ré-exposer, pour que `charges_preview_service.py` et les
tests existants (`tests/test_charges_impact.py`, `tests/test_repartition_charge_commune_facture.py`)
continuent de fonctionner sans modification (`from app.services import charges_impact_service as
impact`, puis `impact.repartir_egal(...)`, etc.) — cf. `MOTEUR_CHARGES.md` pour le détail complet.
"""
from __future__ import annotations

from app.moteurs.charges_engine import (  # noqa: F401 — ré-export, voir MOTEUR_CHARGES.md
    CATEGORIES_HORS_FORMULAIRE_EXPLICITE,
    CATEGORY_CATALOG,
    MENAGE_CHOIX,
    MENAGE_FORCE,
    MENAGE_INTERDIT,
    MENAGE_MODE_INTERVENANT,
    MENAGE_MODE_LOGEMENT,
    avantage_possible,
    build_effet_saisie,
    build_reserve_refacturation,
    catalog_entry,
    compute_perimetre_logements,
    gestion_active_pour_mois,
    logements_actifs_proprietaire,
    menage_comportement,
    menage_perimetre,
    repartir_egal,
    somme_quotes_parts,
)

"""Archivage immuable des pièces à la validation — PRÉPARÉ, INACTIF EN PHASE DE TEST.

CE QUE CE SERVICE FERA, EN PRODUCTION
À la validation d'une facture fournisseur, archiver côte à côte et de façon immuable :
  · le PDF original, pièce probante ;
  · le MD qui a servi à cette validation, s'il en existe un ;
  · à défaut de MD externe, un MD canonique produit à partir des données RÉELLEMENT validées,
    pour que la lecture retenue reste lisible même si le moteur d'extraction change ensuite.

POURQUOI IL NE FAIT RIEN AUJOURD'HUI
Tant que l'application est en phase de test, les PDF et MD manipulés sont des documents de
recette. Les archiver polluerait durablement un dépôt dont la promesse est justement qu'on n'en
retire rien. `cfg.ARCHIVAGE_PIECES_ENABLED` est donc faux par défaut, et chaque appel se refuse en
le disant — il ne crée même pas le dossier de destination.

L'activation se fera au passage officiel TEST → PRODUCTION, par cette seule variable.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import app.config as cfg

E_DESACTIVE = "ARCHIVAGE_DESACTIVE"

MESSAGE_DESACTIVE = (
    "Archivage des pièces désactivé : l'application est en phase de test. "
    "Il sera activé au passage en production (ARCHIVAGE_PIECES_ENABLED).")


def actif() -> bool:
    return bool(getattr(cfg, "ARCHIVAGE_PIECES_ENABLED", False))


def etat() -> dict[str, Any]:
    """Ce que l'écran peut dire de l'archivage, sans rien déclencher."""
    return {"actif": actif(), "destination": str(cfg.ARCHIVAGE_PIECES_DIR),
            "destination_creee": Path(cfg.ARCHIVAGE_PIECES_DIR).exists(),
            "message": "" if actif() else MESSAGE_DESACTIVE}


def archiver_a_la_validation(facture_id_opaque: str, *, pdf_path=None, md_path=None,
                             acteur: str = "", db_path=None) -> dict[str, Any]:
    """Point d'entrée unique du futur archivage. Refuse tant que l'archivage n'est pas activé.

    Aucun fichier n'est lu, copié ni créé dans ce cas : un service inactif ne doit laisser aucune
    trace, pas même un dossier vide.
    """
    if not actif():
        return {"ok": False, "code": E_DESACTIVE, "message": MESSAGE_DESACTIVE,
                "facture_id_opaque": facture_id_opaque}
    # Implémentation réelle à écrire lors du passage en production : copie immuable du PDF et du
    # MD (ou génération du MD canonique), empreintes, et trace en base.
    raise NotImplementedError(
        "L'archivage définitif sera implémenté au passage TEST → PRODUCTION.")

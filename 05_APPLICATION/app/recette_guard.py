"""Write-guard du MODE RECETTE — barrière de dernière ligne contre toute écriture réelle.

Objectif : garantir qu'en mode recette, AUCUN fichier hors du dossier de recette isolé ne peut être
remplacé, quelles que soient les erreurs de configuration en amont. Cette garde est volontairement
minimale, sans dépendance, et « fail-closed » : au moindre doute, elle refuse.

Elle NE remplace PAS les flags `*_REAL_WRITE_ENABLED` : elle s'ajoute par-dessus. Les deux doivent
être satisfaits pour écrire.
"""
from __future__ import annotations

from pathlib import Path

import app.config as cfg


class EcritureHorsRecette(Exception):
    """Levée quand une écriture viserait un chemin non autorisé en mode recette."""


# Racines réelles connues qu'il ne faut JAMAIS toucher, même par erreur de configuration.
_SEGMENTS_REELS_INTERDITS = ("01_SOURCES_BRUTES", "02_TRAVAIL", "03_EXPORTS")


def _sous_racine(cible: Path, racine: Path) -> bool:
    try:
        cible.resolve().relative_to(racine.resolve())
        return True
    except (ValueError, OSError):
        return False


def assert_ecriture_autorisee(cible: str | Path) -> None:
    """Autorise le remplacement de `cible` UNIQUEMENT si le mode recette est actif et que la cible
    est strictement sous `RECETTE_ROOT`. Sinon lève `EcritureHorsRecette`.

    En mode NON recette, la garde est neutre (elle laisse la main aux flags historiques), afin de ne
    pas modifier le comportement par défaut de l'application.
    """
    if not cfg.RECETTE_MODE:
        return  # comportement historique inchangé : les flags REAL_WRITE restent la seule garde

    cible = Path(cible)
    racine = Path(cfg.RECETTE_ROOT).resolve()

    if not _sous_racine(cible, racine):
        raise EcritureHorsRecette(
            f"MODE RECETTE : écriture refusée — la cible {cible} n'est pas sous le dossier de "
            f"recette {racine}."
        )

    # Défense en profondeur : si le dossier de recette contenait par erreur un vrai dossier source
    # partagé avec le dépôt réel, on refuse quand même les segments réels connus situés HORS recette.
    resolue = cible.resolve()
    for segment in _SEGMENTS_REELS_INTERDITS:
        # autorisé seulement si ce segment est bien à l'intérieur de la racine de recette
        if segment in resolue.parts and not _sous_racine(resolue, racine):
            raise EcritureHorsRecette(
                f"MODE RECETTE : écriture refusée — chemin réel protégé détecté ({segment})."
            )


def bandeau_actif() -> bool:
    """Vrai si l'interface doit afficher « MODE RECETTE — AUCUNE DONNÉE RÉELLE MODIFIÉE »."""
    return bool(cfg.RECETTE_MODE)

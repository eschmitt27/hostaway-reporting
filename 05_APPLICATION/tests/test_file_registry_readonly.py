"""Garde en lecture seule : le répertoire REF_Setup doit être couvert.

L'entrée du registre visait `01_SOURCES_BRUTES/REF_Setup.xlsm`, un chemin inexistant — le classeur
vit dans un sous-dossier `REF_Setup/`. Le classeur lui-même restait refusé par le contrôle de nom
(il ne commence pas par `SAISIE_`), mais un fichier `SAISIE_*.xlsx` déposé dans ce répertoire
passait au travers.
"""
from pathlib import Path

import app.config as cfg
from app.services import file_registry


def _ref_setup_dir() -> Path:
    return Path(cfg.PROJECT_ROOT) / "01_SOURCES_BRUTES" / "REF_Setup"


def test_le_classeur_setup_n_est_jamais_writable():
    assert not file_registry.is_writable(Path(cfg.REF_SETUP))


def test_un_fichier_saisie_dans_le_repertoire_setup_est_refuse():
    """Le cas que l'ancienne entrée laissait passer."""
    piege = _ref_setup_dir() / "SAISIE_Piege.xlsx"
    assert not file_registry.is_writable(piege), (
        "un fichier SAISIE_ placé dans le répertoire du référentiel doit rester non modifiable")


def test_le_registre_vise_un_chemin_qui_existe():
    """Un garde qui protège un chemin fantôme ne protège rien."""
    racines = file_registry._READONLY_ROOTS
    setup = [r for r in racines if "REF_Setup" in str(r)]
    assert setup, "le référentiel doit figurer dans les racines protégées"
    assert setup[0].exists(), f"racine protégée inexistante : {setup[0]}"


def test_une_vraie_saisie_reste_writable():
    """La garde ne doit pas bloquer les saisies légitimes."""
    saisie = Path(cfg.SAISIE_ROOT) / "Charges" / "SAISIE_Charges_Flux.xlsx"
    assert file_registry.is_writable(saisie)

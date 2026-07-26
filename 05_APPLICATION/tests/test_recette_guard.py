"""Write-guard du MODE RECETTE — barrière de dernière ligne."""
import importlib
from pathlib import Path

import pytest

import app.config as cfg
from app import recette_guard


def _reload_guard():
    importlib.reload(recette_guard)
    return recette_guard


def test_mode_inactif_laisse_passer(monkeypatch):
    """Hors mode recette : garde neutre (comportement historique inchangé)."""
    monkeypatch.setattr(cfg, "RECETTE_MODE", False)
    g = _reload_guard()
    g.assert_ecriture_autorisee(Path("/nimporte/quel/chemin/reel.xlsx"))  # ne lève pas


def test_mode_actif_refuse_hors_recette(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", (tmp_path / "data_recette").resolve())
    g = _reload_guard()
    with pytest.raises(g.EcritureHorsRecette):
        g.assert_ecriture_autorisee(tmp_path / "ailleurs" / "reel.xlsx")


def test_mode_actif_autorise_sous_recette(tmp_path, monkeypatch):
    racine = (tmp_path / "data_recette").resolve()
    racine.mkdir(parents=True)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", racine)
    g = _reload_guard()
    g.assert_ecriture_autorisee(racine / "01_SOURCES_BRUTES" / "Charges" / "SAISIE.xlsx")  # ok


def test_mode_actif_refuse_segment_reel_hors_racine(tmp_path, monkeypatch):
    racine = (tmp_path / "data_recette").resolve()
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", racine)
    g = _reload_guard()
    with pytest.raises(g.EcritureHorsRecette):
        g.assert_ecriture_autorisee(tmp_path / "02_TRAVAIL" / "Lot3_Charges" / "MASTER.xlsx")


def test_bandeau_actif_suit_le_mode(monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    assert _reload_guard().bandeau_actif() is True
    monkeypatch.setattr(cfg, "RECETTE_MODE", False)
    assert _reload_guard().bandeau_actif() is False

"""Environnement et base active — aucune confusion possible entre base RÉELLE et bases dev/test.

La « base réelle » de ces tests est FACTICE : un `.env` et un dossier sous `tmp_path`, substitués à
ceux du projet. Seul `test_04bis` touche à la vraie configuration, et uniquement pour prouver qu'une
session pytest qui la viserait est refusée AVANT toute ouverture.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app import environnement as envmod
from app.db.connection import get_db

APP_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def reel_factice(tmp_path, monkeypatch):
    """Un `.env` déclarant un dossier « réel » factice, pris pour la configuration du projet."""
    dossier = tmp_path / "donnees_reelles"
    dossier.mkdir()
    fichier_env = tmp_path / ".env"
    fichier_env.write_text(f"APP_DATA_DIR={dossier.as_posix()}\nMODE_REEL_ECRITURES=1\n",
                           encoding="utf-8")
    monkeypatch.setattr(cfg, "ENV_FILE", fichier_env)
    envmod._data_dir_reel_depuis.cache_clear()
    yield dossier
    envmod._data_dir_reel_depuis.cache_clear()


def _comme_application_reelle(monkeypatch, dossier: Path) -> None:
    """Ce que voit l'application lancée normalement : APP_DATA_DIR = dossier réel, aucune étiquette."""
    monkeypatch.delenv(envmod.VARIABLE, raising=False)
    monkeypatch.setenv("APP_DATA_DIR", str(dossier))
    monkeypatch.setattr(cfg, "DB_PATH", dossier / "app.db")


# 1. Vraie configuration → REAL

def test_01_vraie_configuration_est_reelle(reel_factice, monkeypatch):
    _comme_application_reelle(monkeypatch, reel_factice)
    assert envmod.environnement() == envmod.REAL
    envmod.refuser_base_reelle()                      # l'application réelle passe
    get_db().close()


# 2. Copie → RECETTE

def test_02_copie_est_recette(reel_factice, tmp_path, monkeypatch):
    copie = tmp_path / "copie"
    monkeypatch.delenv(envmod.VARIABLE, raising=False)
    monkeypatch.setenv("APP_DATA_DIR", str(copie))
    monkeypatch.setattr(cfg, "DB_PATH", copie / "app.db")
    assert envmod.environnement() == envmod.RECETTE
    monkeypatch.setenv(envmod.VARIABLE, "RECETTE")
    assert envmod.environnement() == envmod.RECETTE


def test_02bis_reel_ne_se_declare_pas(reel_factice, tmp_path, monkeypatch):
    """Une copie étiquetée REAL ne devient pas réelle : l'étiquette est refusée."""
    monkeypatch.setenv(envmod.VARIABLE, "REAL")
    monkeypatch.setattr(cfg, "DB_PATH", tmp_path / "copie" / "app.db")
    with pytest.raises(envmod.BaseReelleInterdite):
        envmod.refuser_base_reelle()


# 3. pytest → TEST

def test_03_la_suite_est_l_environnement_test():
    assert os.environ[envmod.VARIABLE] == "TEST"
    assert envmod.environnement() == envmod.TEST


# 4. Test pointant vers la vraie base → refus

def test_04_un_test_vers_la_base_reelle_est_refuse(reel_factice, monkeypatch):
    monkeypatch.setattr(cfg, "DB_PATH", reel_factice / "app.db")
    with pytest.raises(envmod.BaseReelleInterdite):
        get_db()
    with pytest.raises(envmod.BaseReelleInterdite):
        get_db(reel_factice / "app.db")
    assert not (reel_factice / "app.db").exists(), "rien n'a été créé dans le dossier réel"


def test_04bis_une_session_pytest_sur_le_vrai_app_data_dir_ne_demarre_pas():
    """Le vrai `APP_DATA_DIR` exporté dans le shell : la session est refusée avant tout test."""
    reel = envmod.data_dir_reel()
    if reel is None:
        pytest.skip("aucun APP_DATA_DIR réel déclaré sur cette machine")
    env = {**os.environ, "APP_DATA_DIR": str(reel)}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         "tests/test_environnement_base_active.py::test_03_la_suite_est_l_environnement_test"],
        cwd=str(APP_ROOT), env=env, capture_output=True, text=True, timeout=300)
    assert proc.returncode == 3, proc.stdout[-2000:]
    assert "REFUS — BASE RÉELLE" in proc.stdout + proc.stderr
    assert str(reel) not in proc.stdout + proc.stderr, "le chemin réel n'est pas recopié"


def test_04ter_la_garde_de_session_refuse_meme_la_lecture_du_dossier_reel():
    reel = envmod.data_dir_reel()
    if reel is None:
        pytest.skip("aucun APP_DATA_DIR réel déclaré sur cette machine")
    with pytest.raises(AssertionError, match="REEL"):
        sqlite3.connect(f"file:{(reel / 'app.db').as_posix()}?mode=ro", uri=True)
    with pytest.raises(AssertionError, match="REEL"):
        open(reel / "app.db", "rb")


# 5. Recette pointant accidentellement vers la vraie base → refus (l'application ne démarre pas)

def test_05_une_recette_sur_la_base_reelle_ne_demarre_pas(reel_factice, monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app

    monkeypatch.setenv(envmod.VARIABLE, "RECETTE")
    monkeypatch.setattr(cfg, "DB_PATH", reel_factice / "app.db")
    with pytest.raises(envmod.BaseReelleInterdite):
        with TestClient(app):
            pass
    assert not (reel_factice / "app.db").exists(), "aucune migration n'a ouvert la base réelle"


# 6. Base temporaire → fonctionnement normal

def test_06_base_temporaire_fonctionne(tmp_db, client):
    conn = get_db(tmp_db)
    try:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()
    assert client.get("/observabilite/runs").status_code == 200


# 7. APP_DATA_DIR manquant → comportement explicite

def test_07_sans_app_data_dir_c_est_la_base_dev_obsolete(reel_factice, monkeypatch):
    monkeypatch.delenv(envmod.VARIABLE, raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    monkeypatch.setattr(cfg, "DB_PATH", Path(cfg.APP_ROOT) / "data" / "app.db")
    ident = envmod.identite()
    assert ident["environnement"] == envmod.DEV
    assert "BASE_DEV_OBSOLETE" in ident["base_active"]
    assert ident["ecritures_reelles_bloquees"] is True


def test_07bis_etiquette_invalide_refusee(monkeypatch):
    monkeypatch.setenv(envmod.VARIABLE, "PROD")
    with pytest.raises(envmod.BaseReelleInterdite, match="invalide"):
        envmod.refuser_base_reelle()


# 9. Aucun secret ni chemin affiché ; 10. affichage Observabilité

def test_09_10_observabilite_affiche_l_environnement_sans_chemin(client, tmp_db):
    page = client.get("/observabilite/runs").text
    assert 'data-testid="environnement"' in page
    assert "Environnement : TEST" in page
    assert "BLOQUÉES" in page
    for interdit in (str(tmp_db.parent), os.environ.get("USERNAME", "¤¤"), "QONTO", "TOKEN"):
        assert interdit not in page, interdit
    reel = envmod.data_dir_reel()
    if reel is not None:
        assert str(reel) not in page and reel.as_posix() not in page


def test_10_affichage_reel_ecritures_actives(reel_factice, monkeypatch):
    _comme_application_reelle(monkeypatch, reel_factice)
    monkeypatch.setattr(cfg, "MODE_REEL_ECRITURES", True)
    ident = envmod.identite()
    assert ident["libelle"] == "RÉEL" and ident["est_reel"]
    assert ident["ecritures_actives"] is True and ident["ecritures_reelles_bloquees"] is False
    assert ident["base_active"] == "app.db — base réelle"
    assert str(reel_factice) not in str(ident)

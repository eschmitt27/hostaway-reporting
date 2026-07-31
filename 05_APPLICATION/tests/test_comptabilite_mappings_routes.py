"""Couche HTTP des mappings comptables."""
from __future__ import annotations

import pytest

import app.config as cfg


@pytest.fixture(autouse=True)
def _env(tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return tmp_db


def test_page_mappings_accessible(client):
    assert client.get("/comptabilite/mappings").status_code == 200


def test_creer_regle_via_http(client):
    r = client.post("/comptabilite/mappings",
                    data={"portee": "CATEGORIE", "cle": "CHG_MENAGE", "compte": "606100",
                          "statut": "VALIDE"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Règle créée" in html
    assert "CHG_MENAGE" in html and "606100" in html


def test_creer_regle_invalide_signale_erreur(client):
    r = client.post("/comptabilite/mappings", data={"portee": "CATEGORIE", "compte": "606100"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "obligatoire" in html.lower()

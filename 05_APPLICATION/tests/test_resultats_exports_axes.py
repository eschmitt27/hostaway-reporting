"""Bloc 6 (mission Analytique/Résultats — finalisation) : exports CSV par axe.
Vérifie que chaque axe expose un export réel (contenu cohérent, statut 200, pas de chemin absolu)."""
from __future__ import annotations

import app.config as cfg


def test_export_dashboard_global(client):
    r = client.get("/resultats/dashboard/export.csv")
    assert r.status_code == 200
    assert "vision" in r.text


def test_export_logement(client):
    r = client.get("/resultats/export.csv")
    assert r.status_code == 200
    assert "logement_id" in r.text


def test_export_proprietaires(client):
    r = client.get("/resultats/proprietaires/export.csv")
    assert r.status_code == 200
    assert "proprietaire_id" in r.text


def test_export_fournisseurs(client):
    r = client.get("/resultats/fournisseurs/export.csv")
    assert r.status_code == 200
    assert "fournisseur_id_opaque" in r.text


def test_export_prestataires(client):
    r = client.get("/resultats/prestataires/export.csv")
    assert r.status_code == 200
    assert "prestataire_id" in r.text


def test_export_categories(client):
    r = client.get("/resultats/categories/export.csv")
    assert r.status_code == 200
    assert "categorie" in r.text


def test_export_reconciliation(client, monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_CALC_FLUX", tmp_path / "absent_flux.xlsx")
    r = client.get("/resultats/reconciliation/export.csv")
    assert r.status_code == 200
    assert "reconciliation" in r.text
    assert "\\" not in r.text and ":\\" not in r.text


def test_exports_pas_de_chemin_absolu(client):
    for url in ("/resultats/dashboard/export.csv", "/resultats/export.csv",
               "/resultats/proprietaires/export.csv", "/resultats/fournisseurs/export.csv",
               "/resultats/prestataires/export.csv", "/resultats/categories/export.csv"):
        r = client.get(url)
        assert ":\\" not in r.text and "/home/" not in r.text and "C:\\" not in r.text

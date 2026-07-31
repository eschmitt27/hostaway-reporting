"""Écrans Résultats par axe (Bloc 4) : fournisseur/{id}, catégories(+détail), prestataires(+détail),
plateformes/{id}, activités(+détail)."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


@pytest.fixture(autouse=True)
def _env(tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "MASTER_CALC_FLUX", tmp_path / "absent_flux.xlsx")
    return tmp_db


def test_fournisseur_detail_reel(client, tmp_db):
    frs = frs_svc.creer("Fournisseur Axe Route", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-AXE-R1",
                   "date_facture": "2026-06-10", "montant_ttc": 33.0}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)
    compta.valider(res["ecriture_id_opaque"], db_path=tmp_db)

    html = client.get(f"/resultats/fournisseurs/{frs}").text
    assert "33.00" in html


def test_fournisseur_detail_inconnu(client):
    html = client.get("/resultats/fournisseurs/FRS-INCONNU").text
    assert "NON_DISPONIBLE" in html


def test_categories(client):
    assert client.get("/resultats/categories").status_code == 200


def test_categorie_detail_inconnue(client):
    html = client.get("/resultats/categories/CAT_INCONNUE").text
    assert "NON_DISPONIBLE" in html


def test_prestataires_vide(client):
    html = client.get("/resultats/prestataires").text
    assert "NON_DISPONIBLE" in html


def test_prestataires_reel(client, tmp_db):
    conn = get_db(tmp_db)
    conn.execute(
        "INSERT INTO menages (menage_id_opaque, logement_id, proprietaire_id, type_menage, mois, "
        "statut, fournisseur_id_opaque, cout_prevu, cout_reel) "
        "VALUES ('MEN-ROUTE-1','LOG_A1','PROP_A','EXTERNE','2026-06','REGLE','FRS-ROUTE-1',30.0,32.0)")
    conn.commit()
    conn.close()

    html = client.get("/resultats/prestataires?mois=2026-06").text
    assert "FRS-ROUTE-1" in html

    detail = client.get("/resultats/prestataires/FRS-ROUTE-1?mois=2026-06").text
    assert "MEN-ROUTE-1" in detail and "32.00" in detail


def test_prestataire_detail_inconnu(client):
    html = client.get("/resultats/prestataires/FRS-INCONNU").text
    assert "NON_DISPONIBLE" in html


def test_plateforme_detail_non_disponible(client):
    html = client.get("/resultats/plateformes/AIRBNB").text
    assert "NON_DISPONIBLE" in html and "AIRBNB" in html


def test_activites(client):
    html = client.get("/resultats/activites").text
    assert "NON_DISPONIBLE" in html


def test_activite_detail_non_disponible(client):
    html = client.get("/resultats/activites/HEBERGEMENT").text
    assert "NON_DISPONIBLE" in html and "HEBERGEMENT" in html

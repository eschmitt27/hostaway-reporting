"""Routes du premier socle Comptabilité : couche HTTP."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


@pytest.fixture(autouse=True)
def _env(tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return tmp_db


def _facture_validee(db_path):
    frs = frs_svc.creer("Fournisseur Route Test", "MAINTENANCE", db_path=db_path)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-ROUTE-1",
                   "date_facture": "2026-06-10", "montant_ttc": 90.0}, db_path=db_path)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db_path)
    return r["facture_id_opaque"]


def test_accueil_accessible(client):
    assert client.get("/comptabilite").status_code == 200


def test_ecritures_liste_accessible(client):
    assert client.get("/comptabilite/ecritures").status_code == 200


def test_plan_comptable_accessible(client):
    r = client.get("/comptabilite/plan-comptable")
    assert r.status_code == 200 and "401000" in r.text


def test_journaux_accessible(client):
    assert client.get("/comptabilite/journaux").status_code == 200


def test_generer_depuis_facture_puis_valider(client, tmp_db):
    opaque = _facture_validee(tmp_db)
    r = client.post(f"/factures/{opaque}/generer-ecriture-achat",
                    data={"acteur": "recette"}, follow_redirects=False)
    assert r.status_code == 303
    detail_url = r.headers["location"]
    html = client.get(detail_url).text
    assert "PROPOSEE" in html and "606000" in html and "401000" in html

    ecr_opaque = detail_url.rsplit("/", 1)[-1]
    r2 = client.post(f"/comptabilite/ecritures/{ecr_opaque}/valider", follow_redirects=False)
    assert r2.status_code == 303
    assert compta.charger(ecr_opaque, tmp_db)["statut"] == compta.ST_VALIDEE


def test_regenerer_est_idempotent_via_http(client, tmp_db):
    opaque = _facture_validee(tmp_db)
    r1 = client.post(f"/factures/{opaque}/generer-ecriture-achat", follow_redirects=False)
    r2 = client.post(f"/factures/{opaque}/generer-ecriture-achat", follow_redirects=False)
    assert r1.headers["location"] == r2.headers["location"]


def test_contrepasser_depuis_l_interface(client, tmp_db):
    opaque = _facture_validee(tmp_db)
    r = client.post(f"/factures/{opaque}/generer-ecriture-achat", follow_redirects=False)
    ecr_opaque = r.headers["location"].rsplit("/", 1)[-1]
    client.post(f"/comptabilite/ecritures/{ecr_opaque}/valider")
    r2 = client.post(f"/comptabilite/ecritures/{ecr_opaque}/contrepasser",
                     data={"commentaire": "Test"}, follow_redirects=False)
    assert r2.status_code == 303
    assert compta.charger(ecr_opaque, tmp_db)["statut"] == compta.ST_CONTREPASSEE


def test_ecriture_inconnue_404(client):
    assert client.get("/comptabilite/ecritures/ECR-INEXISTANTE").status_code == 404


def test_persistance_apres_deux_lectures(client, tmp_db):
    opaque = _facture_validee(tmp_db)
    r = client.post(f"/factures/{opaque}/generer-ecriture-achat", follow_redirects=False)
    url = r.headers["location"]
    html1 = client.get(url).text
    html2 = client.get(url).text
    assert "PROPOSEE" in html1 and "PROPOSEE" in html2

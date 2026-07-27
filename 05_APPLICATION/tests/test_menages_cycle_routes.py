"""Routes du cycle de vie Ménages : couche HTTP."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.services import menages_cycle_service as cycle

LOGEMENT = "LOG_A1"
PROPRIETAIRE = "PROP_A"


@pytest.fixture(autouse=True)
def _env(tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED", True)

    def _fake_load_detail(logement_id):
        if logement_id == LOGEMENT:
            return {"logement_id": LOGEMENT, "proprietaire_id": PROPRIETAIRE, "status": "OK"}
        return None

    from app.services import logements_service
    monkeypatch.setattr(logements_service, "load_detail", _fake_load_detail)
    return tmp_db


def test_liste_accessible(client):
    assert client.get("/menages/cycle").status_code == 200


def test_a_affecter_accessible(client):
    assert client.get("/menages/cycle/a-affecter").status_code == 200


def test_controles_accessible(client):
    r = client.get("/menages/cycle/controles")
    assert r.status_code == 200 and "Contrôles ménages" in r.text


def test_nouveau_form_accessible(client):
    assert client.get("/menages/cycle/nouveau").status_code == 200


def test_creer_puis_afficher_detail(client):
    r = client.post("/menages/cycle", data={
        "logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "EXTERNE",
        "date_prevue": "2026-06-10", "cout_prevu": "45",
    }, follow_redirects=False)
    assert r.status_code == 303
    detail_url = r.headers["location"]
    html = client.get(detail_url).text
    assert "PREVU" in html


def test_detail_menage_inconnu_404(client):
    assert client.get("/menages/cycle/MEN-INEXISTANT").status_code == 404


def test_creer_logement_inconnu_redirige_avec_erreur(client):
    r = client.post("/menages/cycle", data={
        "logement_id": "LOG_INEXISTANT", "mois": "2026-06", "type_menage": "EXTERNE",
    }, follow_redirects=False)
    assert r.status_code == 303
    assert "/menages/cycle/nouveau" in r.headers["location"]


def test_affecter_puis_realiser_puis_valider(client, tmp_db):
    from app.services import fournisseurs_referentiel_service as frs_svc
    from app.db.connection import get_db
    frs = frs_svc.creer("Presta HTTP Test", "MENAGE", db_path=tmp_db)["fournisseur_id_opaque"]
    conn = get_db(tmp_db)
    conn.execute("INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, "
                "type_menage, date_debut_validite) VALUES (?,?,?)", (frs, "EXTERNE", "2026-01-01"))
    conn.commit(); conn.close()

    r = client.post("/menages/cycle", data={
        "logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "EXTERNE",
        "date_prevue": "2026-06-10",
    }, follow_redirects=False)
    opaque = r.headers["location"].split("/menages/cycle/")[1].split("?")[0]

    r2 = client.post(f"/menages/cycle/{opaque}/affecter",
                     data={"fournisseur_id_opaque": frs}, follow_redirects=False)
    assert r2.status_code == 303
    assert cycle.charger(opaque, tmp_db)["statut"] == cycle.ST_A_REALISER

    r3 = client.post(f"/menages/cycle/{opaque}/realiser",
                     data={"cout_reel": "50", "methode_cout": "EXTERNE_FACTURE"},
                     follow_redirects=False)
    assert r3.status_code == 303
    assert cycle.charger(opaque, tmp_db)["statut"] == cycle.ST_REALISE

    r4 = client.post(f"/menages/cycle/{opaque}/statut",
                     data={"statut": cycle.ST_A_CONTROLER}, follow_redirects=False)
    assert cycle.charger(opaque, tmp_db)["statut"] == cycle.ST_A_CONTROLER

    r5 = client.post(f"/menages/cycle/{opaque}/statut",
                     data={"statut": cycle.ST_VALIDE}, follow_redirects=False)
    assert cycle.charger(opaque, tmp_db)["statut"] == cycle.ST_VALIDE

    html = client.get(f"/menages/cycle/{opaque}").text
    assert "VALIDE" in html


def test_annuler_un_menage(client, tmp_db):
    r = client.post("/menages/cycle", data={
        "logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "INTERNE",
        "date_prevue": "2026-06-12",
    }, follow_redirects=False)
    opaque = r.headers["location"].split("/menages/cycle/")[1].split("?")[0]
    client.post(f"/menages/cycle/{opaque}/statut", data={"statut": cycle.ST_ANNULE})
    assert cycle.charger(opaque, tmp_db)["statut"] == cycle.ST_ANNULE


def test_transition_interdite_redirige_avec_erreur(client, tmp_db):
    r = client.post("/menages/cycle", data={
        "logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "EXTERNE",
        "date_prevue": "2026-06-13",
    }, follow_redirects=False)
    opaque = r.headers["location"].split("/menages/cycle/")[1].split("?")[0]
    r2 = client.post(f"/menages/cycle/{opaque}/statut", data={"statut": cycle.ST_REGLE},
                     follow_redirects=False)
    html = client.get(r2.headers["location"]).text
    assert "erreur" in r2.headers["location"].lower() or "Transition" in html


def test_persistance_apres_relecture(client, tmp_db):
    r = client.post("/menages/cycle", data={
        "logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "EXTERNE",
        "date_prevue": "2026-06-14",
    }, follow_redirects=False)
    opaque = r.headers["location"].split("/menages/cycle/")[1].split("?")[0]
    html1 = client.get(f"/menages/cycle/{opaque}").text
    html2 = client.get(f"/menages/cycle/{opaque}").text
    assert opaque in html1 and opaque in html2

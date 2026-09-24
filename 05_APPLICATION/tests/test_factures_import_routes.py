"""Mission 3 — « Actualiser les factures » : UN bouton, UN importeur (le dossier de dépôt).

L'ancien import par prévisualisation/confirmation constituait un second importeur, sans hash, sans
MD ni version. Son adresse répond désormais par un message, sans rien créer. Déposer une pièce
depuis le navigateur revient à la poser dans le dossier surveillé, puis à le relire.
"""
from __future__ import annotations

import json

import pytest

import app.config as cfg
from app.services import factures_service as fact
from app.services import menages_pdf_import_service as pdf_import
from app.services import referentiel_logements_export_service as ref_export


@pytest.fixture
def depot(tmp_db, tmp_path, monkeypatch):
    d = tmp_path / "depot"
    d.mkdir()
    monkeypatch.setattr(cfg, "MENAGES_PDF_DIR", d)
    return d


@pytest.mark.parametrize("methode, url", [
    ("get", "/factures/importer"), ("post", "/factures/importer/previsualiser"),
    ("get", "/factures/importer/previsualisation/abc"), ("post", "/factures/importer/confirmer/abc"),
])
def test_ancien_import_repond_par_un_message_sans_rien_creer(client, tmp_db, methode, url):
    r = getattr(client, methode)(url, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"].startswith("/factures?message=")
    assert fact.lister(db_path=tmp_db) == []


def test_actualiser_depose_puis_relit_le_dossier(client, tmp_db, depot, monkeypatch):
    appels = []
    reel = pdf_import.recharger
    monkeypatch.setattr(pdf_import, "recharger",
                        lambda **k: appels.append(k) or reel(dossier=depot, db_path=tmp_db,
                                                             acteur="test"))
    r = client.post("/factures/recharger",
                    files=[("pieces", ("09-26-Test.pdf", b"%PDF-1.4 piece", "application/pdf"))],
                    follow_redirects=False)
    assert r.status_code == 303
    assert (depot / "09-26-Test.pdf").read_bytes() == b"%PDF-1.4 piece"
    assert len(appels) == 1, "un seul importeur : le rechargement du dossier"
    assert "1 pièce(s) déposée(s)" in client.get(r.headers["location"]).text


def test_actualiser_sans_piece_est_le_rechargement(client, tmp_db, depot, monkeypatch):
    appels = []
    monkeypatch.setattr(pdf_import, "recharger", lambda **k: appels.append(k) or {
        "ok": True, "nb_presents": 0, "nb_importees": 0, "nb_supprimees": 0, "doublons": {}})
    r = client.post("/factures/recharger", follow_redirects=False)
    assert r.status_code == 303 and len(appels) == 1
    assert "Factures actualisées" in client.get(r.headers["location"]).text


def test_depot_n_ecrase_jamais_une_piece(depot):
    (depot / "a.pdf").write_bytes(b"%PDF-1 original")
    assert pdf_import.deposer("a.pdf", b"%PDF-1 original", dossier=depot)["depose"] is False
    refus = pdf_import.deposer("a.pdf", b"%PDF-1 autre", dossier=depot)
    assert not refus["ok"] and "renommez" in refus["message"]
    assert (depot / "a.pdf").read_bytes() == b"%PDF-1 original"


@pytest.mark.parametrize("nom, contenu", [("x.exe", b"MZ"), ("x.pdf", b"pas un pdf"),
                                          ("x.pdf", b""), (".cache.pdf", b"%PDF")])
def test_depot_refuse_ce_qui_n_est_pas_une_piece(depot, nom, contenu):
    assert not pdf_import.deposer(nom, contenu, dossier=depot)["ok"]
    assert list(depot.iterdir()) == []


def test_depot_ignore_tout_chemin(depot):
    res = pdf_import.deposer("../../evasion.pdf", b"%PDF-1", dossier=depot)
    assert res["ok"] and (depot / "evasion.pdf").exists()
    assert not (depot.parent / "evasion.pdf").exists()


def test_actualisation_idempotente(tmp_db, depot):
    premier = pdf_import.recharger(acteur="t", dossier=depot, db_path=tmp_db)
    second = pdf_import.recharger(acteur="t", dossier=depot, db_path=tmp_db)
    assert second["nb_importees"] == 0 and second["nb_supprimees"] == 0
    assert premier["nb_presents"] == second["nb_presents"]


def test_export_referentiel_deplace_contrat_identique(client, tmp_db):
    ancien = client.get("/factures/referentiel-logements.json", follow_redirects=False)
    assert ancien.status_code == 307
    assert ancien.headers["location"] == "/exports/referentiel-logements.json"
    r = client.get("/exports/referentiel-logements.json")
    assert r.status_code == 200
    assert 'filename="referentiel_logements.json"' in r.headers["content-disposition"]
    assert json.loads(r.text) == json.loads(ref_export.exporter_json())
    assert json.loads(r.text)["schema"] == "REFERENTIEL_LOGEMENTS_V1"

"""Routes trésorerie propriétaires (HTTP) : liste, création, prévisualisation, cycle de vie."""
from __future__ import annotations

import pytest

from app.readers import proprietaires_reader
from app.services import proprietaires_tresorerie_service as svc


@pytest.fixture(autouse=True)
def _proprietaire_connu(monkeypatch):
    # Le référentiel vient désormais de SQLite : sans cette bascule, `ref_available()` répond
    # « non importé » et les routes rendent 404 avant même de chercher le propriétaire.
    monkeypatch.setattr(proprietaires_reader, "ref_available", lambda: True)
    monkeypatch.setattr(proprietaires_reader, "find_proprietaire",
                        lambda pid: {"proprietaire_id": pid} if pid == "PROP_0001" else None)


CHAMPS = {"sens": "PROPRIETAIRE_VERS_SOCIETE", "nature": "ACOMPTE_PROPRIETAIRE",
         "montant": "250.50", "date_mouvement": "2026-06-01", "justification": "test"}


def test_01_liste_globale_vide(client):
    r = client.get("/proprietaires/tresorerie")
    assert r.status_code == 200 and "Aucun mouvement" in r.text


def test_02_liste_globale_alimentee(client):
    client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS)
    r = client.get("/proprietaires/tresorerie")
    assert "PROP_0001" in r.text and "1 mouvement" in r.text


def test_03_filtres(client):
    client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS)
    r = client.get("/proprietaires/tresorerie?sens=SOCIETE_VERS_PROPRIETAIRE")
    assert "Aucun mouvement" in r.text
    r2 = client.get("/proprietaires/tresorerie?sens=PROPRIETAIRE_VERS_SOCIETE")
    assert "PROP_0001" in r2.text


def test_04_pagination(client):
    for _ in range(25):
        client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS)
    r = client.get("/proprietaires/tresorerie")
    assert "25 mouvements" in r.text and "Page 1" in r.text
    r2 = client.get("/proprietaires/tresorerie?page=2")
    assert r2.status_code == 200


def test_05_creation_via_confirmer(client):
    r = client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS,
                    follow_redirects=False)
    assert r.status_code == 303 and "/proprietaires/tresorerie/MTP-" in r.headers["location"]


def test_06_previsualisation(client):
    r = client.post("/proprietaires/PROP_0001/tresorerie/previsualiser", data=CHAMPS)
    assert r.status_code == 200 and "250.50" in r.text
    assert "Confirmer" in r.text


def test_07_confirmation(client):
    r = client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS,
                    follow_redirects=True)
    assert r.status_code == 200 and "BROUILLON" in r.text


def _creer_mouvement(client) -> str:
    r = client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS,
                    follow_redirects=False)
    return r.headers["location"].rsplit("/", 1)[-1]


def test_08_modification_brouillon(client):
    mid = _creer_mouvement(client)
    r = client.post(f"/proprietaires/tresorerie/{mid}/modifier",
                    data={"montant": "999.99"}, follow_redirects=True)
    assert "999.99" in r.text


def test_09_validation(client):
    mid = _creer_mouvement(client)
    r = client.post(f"/proprietaires/tresorerie/{mid}/valider", follow_redirects=True)
    assert "VALIDE" in r.text


def test_10_annulation(client):
    mid = _creer_mouvement(client)
    r = client.post(f"/proprietaires/tresorerie/{mid}/annuler",
                    data={"justification": "erreur de saisie"}, follow_redirects=True)
    assert "ANNULE" in r.text


def test_11_justification_obligatoire_pour_annulation(client):
    mid = _creer_mouvement(client)
    r = client.post(f"/proprietaires/tresorerie/{mid}/annuler", data={"justification": ""},
                    follow_redirects=True)
    assert "obligatoire" in r.text.lower()
    m = svc.charger(mid)
    assert m["statut"] != svc.ST_ANNULE   # pas annulé, refus effectif


def test_12_nature_invalide_refusee(client):
    champs = {**CHAMPS, "nature": "NATURE_INVENTEE"}
    r = client.post("/proprietaires/PROP_0001/tresorerie/previsualiser", data=champs,
                    follow_redirects=False)
    assert r.status_code == 303 and "erreur" in r.headers["location"]


def test_13_sens_invalide_refuse(client):
    champs = {**CHAMPS, "sens": "SENS_INVENTE"}
    r = client.post("/proprietaires/PROP_0001/tresorerie/previsualiser", data=champs,
                    follow_redirects=False)
    assert r.status_code == 303 and "erreur" in r.headers["location"]


def test_14_proprietaire_absent(client):
    r = client.get("/proprietaires/PROP_INEXISTANT/tresorerie/nouveau")
    assert r.status_code == 404


def test_15_proprietaire_absent_liste(client):
    r = client.get("/proprietaires/PROP_INEXISTANT/tresorerie")
    assert r.status_code == 404


def test_16_logement_facultatif_absent_ok(client):
    r = client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS,
                    follow_redirects=False)
    assert r.status_code == 303


def test_17_detail(client):
    mid = _creer_mouvement(client)
    r = client.get(f"/proprietaires/tresorerie/{mid}")
    assert r.status_code == 200 and mid in r.text


def test_18_historique(client):
    mid = _creer_mouvement(client)
    client.post(f"/proprietaires/tresorerie/{mid}/valider")
    r = client.get(f"/proprietaires/tresorerie/{mid}/historique")
    assert r.status_code == 200 and "CREATION" in r.text and "VALIDATION" in r.text


def test_19_montant_rapproche_et_reste_affiches(client):
    mid = _creer_mouvement(client)
    client.post(f"/proprietaires/tresorerie/{mid}/valider")
    r = client.get(f"/proprietaires/tresorerie/{mid}")
    assert "Montant rapproché" in r.text and "Reste à rapprocher" in r.text


def test_20_reste_egal_montant_avant_rapprochement(client):
    mid = _creer_mouvement(client)
    client.post(f"/proprietaires/tresorerie/{mid}/valider")
    m = svc.charger(mid)
    assert svc.reste_a_rapprocher(mid) == m["montant"]


def test_21_valide_non_modifiable(client):
    mid = _creer_mouvement(client)
    client.post(f"/proprietaires/tresorerie/{mid}/valider")
    r = client.post(f"/proprietaires/tresorerie/{mid}/modifier",
                    data={"montant": "1.0"}, follow_redirects=True)
    m = svc.charger(mid)
    assert m["montant"] != 1.0   # refusé, valeur inchangée


def test_22_annule_non_rapprochable(client):
    mid = _creer_mouvement(client)
    client.post(f"/proprietaires/tresorerie/{mid}/annuler", data={"justification": "x"})
    objets = svc.objets_rapprochables()
    assert mid not in [o["mouvement_opaque"] for o in objets]


def test_23_identifiant_opaque_dans_url(client):
    mid = _creer_mouvement(client)
    assert mid.startswith("MTP-")


def test_24_garde_recette_non_bloquante_en_recette(client):
    # Recette locale : la création fonctionne (pas de garde bloquante hors mode réel).
    r = client.post("/proprietaires/PROP_0001/tresorerie/confirmer", data=CHAMPS,
                    follow_redirects=False)
    assert r.status_code == 303


def test_25_aucune_pii_dans_le_rendu(client):
    mid = _creer_mouvement(client)
    r = client.get(f"/proprietaires/tresorerie/{mid}")
    assert "iban" not in r.text.lower() and "rib" not in r.text.lower()


def test_26_aucun_chemin_absolu_dans_le_rendu(client):
    mid = _creer_mouvement(client)
    r = client.get(f"/proprietaires/tresorerie/{mid}")
    assert "C:\\" not in r.text and "/home/" not in r.text


def test_27_mouvement_inexistant_404(client):
    r = client.get("/proprietaires/tresorerie/MTP-INEXISTANT")
    assert r.status_code == 404
    r2 = client.get("/proprietaires/tresorerie/MTP-INEXISTANT/historique")
    assert r2.status_code == 404


def test_28_liste_proprietaire_affiche_solde(client):
    _creer_mouvement(client)
    r = client.get("/proprietaires/PROP_0001/tresorerie")
    assert "Solde net" in r.text

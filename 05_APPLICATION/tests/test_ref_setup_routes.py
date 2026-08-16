"""Écran Référentiel Setup — l'import doit être faisable sans terminal.

Ces tests vérifient le parcours complet depuis l'interface : état, prévisualisation, import. Ils
utilisent un classeur SYNTHÉTIQUE, jamais le référentiel réel.
"""
from urllib.parse import unquote

import pytest

from app.services import ref_setup_catalogue as cat
from app.services import ref_setup_import_service as imp
from app.services import ref_setup_repo as repo
from test_ref_setup_import import _ecrire_classeur, LIGNES_SYNTHETIQUES  # noqa: F401

pytest.importorskip("openpyxl")


@pytest.fixture
def classeur(tmp_path, monkeypatch):
    """Redirige la source vers un classeur synthétique, pour toute la durée du test."""
    chemin = _ecrire_classeur(tmp_path / "REF_Setup_fixture.xlsx")
    import app.config as cfg
    monkeypatch.setattr(cfg, "REF_SETUP", chemin)
    return chemin


def test_ecran_repond_200(client):
    r = client.get("/referentiel-setup")
    assert r.status_code == 200
    assert "Référentiel Setup" in r.text


def test_referentiel_vide_est_distingue_de_referentiel_sans_donnees(client):
    """Message explicite : « jamais importé » n'est pas « aucune donnée »."""
    r = client.get("/referentiel-setup")
    assert "jamais importé" in r.text


def test_previsualisation_n_ecrit_rien(client, classeur):
    r = client.post("/referentiel-setup/previsualiser")
    assert r.status_code == 200
    assert "Rien n'a été écrit" in r.text
    assert repo.etat()["disponible"] is False


def test_previsualisation_annonce_le_volume(client, classeur):
    r = client.post("/referentiel-setup/previsualiser")
    assert "28" in r.text
    assert "Confirmer l'import" in r.text


def test_import_depuis_l_interface(client, classeur):
    r = client.post("/referentiel-setup/importer", follow_redirects=False)
    assert r.status_code == 303
    assert "Import IMP-" in unquote(r.headers["location"])

    etat = repo.etat()
    assert etat["disponible"] is True
    assert etat["compteurs"]["ref_logements"] == 2
    assert etat["compteurs"]["ref_proprietaires"] == 2

    page = client.get("/referentiel-setup")
    assert "Dernier import" in page.text


def test_import_refuse_affiche_la_raison(client, tmp_path, monkeypatch):
    lignes = {**LIGNES_SYNTHETIQUES,
              "REF_Proprietaires": [{"proprietaire_id": "P"}, {"proprietaire_id": "P"}]}
    import app.config as cfg
    monkeypatch.setattr(cfg, "REF_SETUP", _ecrire_classeur(tmp_path / "ko.xlsx", lignes))

    r = client.post("/referentiel-setup/importer", follow_redirects=False)
    assert r.status_code == 303
    assert "plusieurs fois" in unquote(r.headers["location"])
    assert repo.etat()["disponible"] is False


def test_previsualisation_refusee_explique_sans_stacktrace(client, tmp_path, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "REF_SETUP", _ecrire_classeur(
        tmp_path / "manque.xlsx", omettre="REF_Associes"))
    r = client.post("/referentiel-setup/previsualiser")
    assert r.status_code == 200
    assert imp.E_ONGLET_MANQUANT in r.text
    assert "REF_Associes" in r.text
    assert "Traceback" not in r.text


# ── Repository ──────────────────────────────────────────────────────────────────────────────────

def test_repo_lit_par_onglet_et_par_table(client, classeur):
    client.post("/referentiel-setup/importer")
    par_table = repo.lire_table("ref_logements")
    par_onglet = repo.lire_onglet("REF_Logements")
    assert par_table == par_onglet
    assert {l["logement_id"] for l in par_table} == {"LOG_9001", "LOG_9002"}
    # Les colonnes du classeur sont toutes présentes, y compris celles laissées vides.
    assert set(par_table[0]) == set(cat.PAR_TABLE["ref_logements"].colonnes)


def test_repo_lit_par_cle(client, classeur):
    client.post("/referentiel-setup/importer")
    assert repo.lire_par_cle("ref_logements", "LOG_9001")["nom_court"] == "Fixture A"
    assert repo.lire_par_cle("ref_logements", "LOG_INEXISTANT") is None


def test_repo_refuse_une_table_hors_catalogue():
    with pytest.raises(KeyError):
        repo.lire_table("ecritures")


def test_repo_rend_des_chaines(client, classeur):
    """Même convention que csv_reader : le service appelant convertit, le repository ne devine pas."""
    client.post("/referentiel-setup/importer")
    ligne = repo.lire_par_cle("ref_taux_commission", "TX_9001")
    assert ligne["taux_commission"] == "0.15"
    assert isinstance(ligne["taux_commission"], str)

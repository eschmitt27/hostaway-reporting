"""Mission 4 — Paramètres société & facturation : SQLite source canonique, émission débloquée.

Valeurs FICTIVES uniquement (« SAS DEMO ») : aucune identité réelle dans les tests.
"""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.services import facturation_config_service as fconf
from app.services import factures_proprietaires_conformite_service as conformite
from app.services import factures_proprietaires_service as fp
from app.services import parametres_societe_service as params
from app.services import referentiel_admin_service as adm

DEMO = {
    "SOCIETE_NOM": "SAS DEMO", "SOCIETE_FORME_JURIDIQUE": "SAS", "SOCIETE_CAPITAL": "1 000 EUR",
    "SOCIETE_ADRESSE": "1 rue Demo\n00000 Villedemo", "SOCIETE_SIREN": "000 000 000",
    "SOCIETE_RCS": "RCS Demo", "SOCIETE_REPRESENTANTS": "Représentant Demo",
    "FACTURATION_REGIME_TVA": "FRANCHISE_TVA",
    "FACTURATION_MENTION_FRANCHISE_TVA": "TVA non applicable, art. 293 B du CGI",
    "FACTURATION_DELAI_PAIEMENT_JOURS": "0",
}


@pytest.fixture
def sans_env(monkeypatch):
    """Aucune valeur ne vient de l'environnement ni d'`app.config` : seule la base parle."""
    for cle in params.CLES:
        monkeypatch.delenv(cle, raising=False)
        monkeypatch.setattr(cfg, cle, "", raising=False)


def _source(**kw):
    base = {"mois": "2026-07", "proprietaire_id": "PROP_DEMO", "logement_id": "LOG_DEMO",
            "source_calcul": "PREF-DEMO", "COMMISSION_CONCIERGERIE": 300.0,
            "MENAGE_FACTURE": 150.0, "CHARGE_FIXE": 50.0, "montant_du_conciergerie": 500.0}
    base.update(kw)
    return base


@pytest.fixture
def particulier(monkeypatch):
    monkeypatch.setattr(conformite, "client", lambda pid, **kw: {
        "type_client": fconf.CLIENT_PARTICULIER, "denomination": "Client Demo",
        "adresse": "2 rue Demo", "adresse_facturation": "2 rue Demo", "siren": "",
        "tva_intra": "", "numero_bon_commande": "", "telephone": "", "email": ""})


# ── Administration ──────────────────────────────────────────────────────────────────────────────

def test_administration_repond_et_aucun_lien_mort(client):
    assert client.get("/administration", follow_redirects=False).status_code == 307
    page = client.get("/administration/referentiels")
    assert page.status_code == 200 and "/administration/societe-facturation" in page.text
    # L'erreur reproduite : « Coûts ménage → parcours dédié » menait à une adresse inexistante.
    for table in adm.URLS_PARCOURS:
        url = adm.URLS_PARCOURS[table]
        assert client.get(url).status_code == 200, url
    for table in ("ref_logements", "ref_proprietaires", "ref_couts_standards_menage"):
        assert client.get(f"/administration/referentiels/{table}").status_code == 200


def test_ecran_parametres_repond(client, sans_env):
    page = client.get("/administration/societe-facturation")
    assert page.status_code == 200
    for libelle in ("Identité", "Contact", "Facturation", "SIREN", "Délai de paiement",
                    "Régime de TVA"):
        assert libelle in page.text


# ── Source canonique ────────────────────────────────────────────────────────────────────────────

def test_la_base_fait_foi_sur_l_environnement(tmp_db, monkeypatch):
    monkeypatch.setenv("SOCIETE_NOM", "Ancienne valeur env")
    assert fconf.emetteur()["denomination"] == "Ancienne valeur env"     # transition
    params.enregistrer({"SOCIETE_NOM": "SAS DEMO"}, acteur="test")
    assert fconf.emetteur()["denomination"] == "SAS DEMO"
    params.enregistrer({"SOCIETE_NOM": ""}, acteur="test")
    # Enregistré VIDE : « non configuré », et l'environnement n'est plus consulté.
    assert fconf.emetteur()["denomination"] == ""


def test_reprise_depuis_environnement_une_seule_fois(tmp_db, sans_env, monkeypatch):
    monkeypatch.setenv("SOCIETE_SIREN", "000000000")
    res = params.reprendre_depuis_environnement(acteur="test")
    assert set(res["modifiees"]) == set(params.CLES)          # toutes les clés deviennent explicites
    assert params.lire("SOCIETE_SIREN") == (True, "000000000")
    assert params.lire("SOCIETE_TVA_INTRA") == (True, None)   # absent partout : NULL, jamais inventé
    monkeypatch.setenv("SOCIETE_SIREN", "111111111")
    assert params.reprendre_depuis_environnement(acteur="test")["modifiees"] == []
    assert fconf.emetteur()["siren"] == "000000000"


def test_historique_et_audit(tmp_db, sans_env):
    params.enregistrer({"SOCIETE_NOM": "A"}, acteur="Ewan", motif="création")
    params.enregistrer({"SOCIETE_NOM": "B"}, acteur="Ewan")
    params.enregistrer({"SOCIETE_NOM": "B"}, acteur="Ewan")             # inchangé : rien d'écrit
    h = params.historique()
    assert [(x["ancienne_valeur"], x["nouvelle_valeur"]) for x in h] == [("A", "B"), (None, "A")]
    assert h[1]["motif"] == "création" and h[0]["modifie_par"] == "Ewan"


@pytest.mark.parametrize("cle, valeur", [
    ("SOCIETE_SIREN", "12345"), ("SOCIETE_SIRET", "123"), ("SOCIETE_TVA_INTRA", "??"),
    ("FACTURATION_DELAI_PAIEMENT_JOURS", "-1"), ("FACTURATION_DELAI_PAIEMENT_JOURS", "trente"),
    ("FACTURATION_REGIME_TVA", "INVENTE"),
])
def test_validation_refuse_sans_rien_ecrire(tmp_db, cle, valeur):
    with pytest.raises(params.ParametreInvalide):
        params.enregistrer({cle: valeur}, acteur="test")
    assert params.lire(cle) == (False, None)


def test_siret_doit_commencer_par_le_siren(tmp_db):
    with pytest.raises(params.ParametreInvalide):
        params.enregistrer({"SOCIETE_SIREN": "000000000", "SOCIETE_SIRET": "11111111100011"},
                           acteur="t")
    params.enregistrer({"SOCIETE_SIREN": "000 000 000", "SOCIETE_SIRET": "000000000 00011"},
                       acteur="t")
    assert params.lire("SOCIETE_SIRET") == (True, "00000000000011")


def test_tva_intra_absente_ne_bloque_pas_en_franchise(tmp_db, sans_env, particulier):
    params.enregistrer(DEMO, acteur="test")
    f = fp.creer(_source(), db_path=tmp_db)
    res = conformite.verifier(f, date_facture="2026-08-01", db_path=tmp_db)
    assert res["statut"] == conformite.PRETE, res["manques"]
    assert res["conformite"]["emetteur"]["tva_intra"] == ""


# ── Délai : 0 ≠ NULL ────────────────────────────────────────────────────────────────────────────

def test_delai_zero_paiement_a_reception_et_null_non_configure(tmp_db, sans_env):
    params.enregistrer({"FACTURATION_DELAI_PAIEMENT_JOURS": "0"}, acteur="t")
    assert fconf.delai_paiement_jours() == 0
    assert fconf.conditions_paiement() == "Paiement à réception"
    assert conformite.date_echeance("2026-08-05", fconf.delai_paiement_jours()) == "2026-08-05"
    params.enregistrer({"FACTURATION_DELAI_PAIEMENT_JOURS": ""}, acteur="t")
    assert params.lire("FACTURATION_DELAI_PAIEMENT_JOURS") == (True, None)
    assert fconf.delai_paiement_jours() is None


# ── Particulier / professionnel ─────────────────────────────────────────────────────────────────

def test_professionnel_garde_ses_exigences(tmp_db, sans_env, monkeypatch):
    params.enregistrer(DEMO, acteur="test")
    monkeypatch.setattr(conformite, "client", lambda pid, **kw: {
        "type_client": fconf.CLIENT_PROFESSIONNEL, "denomination": "SARL DEMO",
        "adresse": "3 rue Demo", "adresse_facturation": "3 rue Demo", "siren": "111111111",
        "tva_intra": "", "numero_bon_commande": "", "telephone": "", "email": ""})
    f = fp.creer(_source(), db_path=tmp_db)
    codes = {m["code"] for m in conformite.verifier(f, date_facture="2026-08-01",
                                                   db_path=tmp_db)["manques"]}
    assert codes == {conformite.C_PENALITES, conformite.C_INDEMNITE}


# ── Émission débloquée, snapshot immuable ───────────────────────────────────────────────────────

def test_avant_parametrage_les_blocages_reproduits(tmp_db, sans_env, particulier):
    f = fp.creer(_source(), db_path=tmp_db)
    codes = {m["code"] for m in conformite.verifier(f, date_facture="2026-08-01",
                                                   db_path=tmp_db)["manques"]}
    assert {conformite.C_IDENTITE_EMETTEUR, conformite.C_REGIME_TVA,
            conformite.C_ECHEANCE} <= codes


def test_particulier_franchise_delai_zero_emissible_et_snapshot_fige(client, tmp_db, sans_env,
                                                                     particulier, tmp_path,
                                                                     monkeypatch):
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    params.enregistrer(DEMO, acteur="test")
    f = fp.creer(_source(), db_path=tmp_db)
    fid = f["facture_id_opaque"]
    assert conformite.verifier(f, date_facture="2026-08-03", db_path=tmp_db)["statut"] == \
        conformite.PRETE

    # Le parcours réel, par l'interface : la route lit l'émetteur à la source canonique.
    assert client.post(f"/factures-proprietaires/{fid}/valider",
                       follow_redirects=False).status_code == 303
    assert client.post(f"/factures-proprietaires/{fid}/emettre",
                       data={"date_facture": "2026-08-03"},
                       follow_redirects=False).status_code == 303
    emise = fp.lire(fid, db_path=tmp_db)
    assert emise["statut"] == fp.ST_EMIS and re.fullmatch(r"2026-07-\d{3}", emise["numero_facture"])
    snap = fp.contenu_emis(fid, db_path=tmp_db)
    assert snap["emetteur"]["nom"] == "SAS DEMO"

    # Les paramètres changent APRÈS l'émission : la facture émise n'en voit rien.
    params.enregistrer({"SOCIETE_NOM": "AUTRE NOM", "FACTURATION_DELAI_PAIEMENT_JOURS": "30"},
                       acteur="test")
    apres = fp.contenu_emis(fid, db_path=tmp_db)
    assert apres == snap and apres["emetteur"]["nom"] == "SAS DEMO"
    assert fp.lire(fid, db_path=tmp_db)["snapshot_hash"] == emise["snapshot_hash"]


def test_route_admin_enregistre_et_refuse_sans_auteur(client, tmp_db, sans_env):
    r = client.post("/administration/societe-facturation",
                    data={"SOCIETE_NOM": "SAS DEMO", "acteur": ""}, follow_redirects=False)
    assert "erreur=" in r.headers["location"]
    r = client.post("/administration/societe-facturation",
                    data={"SOCIETE_NOM": "SAS DEMO", "FACTURATION_DELAI_PAIEMENT_JOURS": "0",
                          "acteur": "Ewan"}, follow_redirects=False)
    assert "message=" in r.headers["location"]
    assert params.lire("SOCIETE_NOM") == (True, "SAS DEMO")
    assert "Paiement à réception" in client.get("/administration/societe-facturation").text

"""Mission 6 — administration temporelle du paramètre canapé (seuil/montant).

Réutilise entièrement l'infrastructure existante (`referentiel_admin_service.transaction`/
`clore_periode`/`inserer`, journal `ref_admin_evenements`) — ce fichier couvre uniquement ce qui
est nouveau : `canape_gestion_service.py`, le verrou `COLONNES_LECTURE_SEULE` sur `ref_logements`,
et l'écran générique.
"""
import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import canape_gestion_service as canape
from app.services import referentiel_admin_service as adm


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


# ── Historisation du paramètre canapé ───────────────────────────────────────────────────────────

def test_canape_est_lecture_seule_sur_ecran_generique(ref):
    assert "ref_canape_parametres" in adm.LECTURE_SEULE
    res = adm.creer_ligne("ref_canape_parametres", {
        "canape_parametre_id": "CNP_X", "logement_id": "LOG_A1",
        "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30"},
        db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_ECRITURE


def test_changer_parametres_ouvre_une_premiere_periode(ref):
    res = canape.changer_parametres("LOG_A1", 5, 30, "2026-07-01", db_path=ref)
    assert res["ok"], res
    lignes = canape.historique("LOG_A1", db_path=ref)
    assert len(lignes) == 1
    assert int(lignes[0]["seuil_voyageurs_preparation_canape"]) == 5
    assert float(lignes[0]["montant_preparation_canape"]) == 30
    assert lignes[0]["date_debut"] == "2026-07-01" and lignes[0]["date_fin"] == ""


def test_changer_parametres_cloture_lancien_et_ouvre_le_nouveau(ref):
    canape.changer_parametres("LOG_A1", 5, 30, "2026-01-01", db_path=ref)
    res = canape.changer_parametres("LOG_A1", 6, 40, "2027-01-01", db_path=ref)
    assert res["ok"], res
    lignes = canape.historique("LOG_A1", db_path=ref)
    assert len(lignes) == 2
    ancien = [l for l in lignes if l["date_fin"]][0]
    assert float(ancien["montant_preparation_canape"]) == 30 and ancien["date_fin"] == "2026-12-31"
    nouveau = [l for l in lignes if not l["date_fin"]][0]
    assert float(nouveau["montant_preparation_canape"]) == 40


def test_changer_parametres_ne_modifie_jamais_une_ligne_close(ref):
    canape.changer_parametres("LOG_A1", 5, 30, "2026-01-01", db_path=ref)
    canape.changer_parametres("LOG_A1", 6, 40, "2027-01-01", db_path=ref)
    canape.changer_parametres("LOG_A1", 7, 50, "2028-01-01", db_path=ref)
    lignes = canape.historique("LOG_A1", db_path=ref)
    assert len(lignes) == 3
    premiere = [l for l in lignes if float(l["montant_preparation_canape"]) == 30][0]
    assert premiere["date_fin"] == "2026-12-31"      # jamais retouché


def test_changer_parametres_logement_inconnu_refuse(ref):
    res = canape.changer_parametres("LOG_ZZZ", 5, 30, "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == canape.E_LOGEMENT_INCONNU


def test_changer_parametres_seuil_invalide_refuse(ref):
    res = canape.changer_parametres("LOG_A1", "abc", 30, "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == canape.E_SEUIL_INVALIDE


def test_changer_parametres_montant_negatif_refuse(ref):
    res = canape.changer_parametres("LOG_A1", 5, -10, "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == canape.E_MONTANT_INVALIDE


def test_changer_parametres_date_invalide_refusee(ref):
    res = canape.changer_parametres("LOG_A1", 5, 30, "01/07/2026", db_path=ref)
    assert res["ok"] is False and res["code"] == canape.E_DATE_INVALIDE


# ── Verrou colonnes sur ref_logements (écran générique) ─────────────────────────────────────────

def test_ref_logements_reste_editable_pour_ses_champs_descriptifs(ref):
    res = adm.modifier_ligne("ref_logements", "LOG_A1", {"nom_court": "A1-bis"}, db_path=ref)
    assert res["ok"], res


def test_colonnes_canape_ignorees_silencieusement_sur_modifier_ligne(ref):
    """Les deux colonnes vestigiales ne doivent plus pouvoir être modifiées via l'écran générique,
    mais la modification des AUTRES champs de la même requête reste acceptée."""
    avant = adm.ligne("ref_logements", "LOG_A1", db_path=ref)
    res = adm.modifier_ligne("ref_logements", "LOG_A1", {
        "nom_court": "A1-ter",
        "seuil_voyageurs_preparation_canape": "999",
        "montant_preparation_canape": "999",
    }, db_path=ref)
    assert res["ok"], res
    apres = adm.ligne("ref_logements", "LOG_A1", db_path=ref)
    assert apres["nom_court"] == "A1-ter"
    assert apres["seuil_voyageurs_preparation_canape"] == avant.get("seuil_voyageurs_preparation_canape", "")
    assert apres["montant_preparation_canape"] == avant.get("montant_preparation_canape", "")


def test_creer_ligne_ref_logements_ignore_les_colonnes_canape(ref):
    res = adm.creer_ligne("ref_logements", {
        "logement_id": "LOG_NOUVEAU", "nom_court": "Nouveau",
        "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
    }, db_path=ref)
    assert res["ok"], res
    ligne = adm.ligne("ref_logements", "LOG_NOUVEAU", db_path=ref)
    assert ligne["seuil_voyageurs_preparation_canape"] == ""
    assert ligne["montant_preparation_canape"] == ""


def test_decrire_table_expose_les_colonnes_verrouillees(ref):
    meta = adm.decrire_table("ref_logements", db_path=ref)
    assert meta["colonnes_lecture_seule"] == sorted(
        ["seuil_voyageurs_preparation_canape", "montant_preparation_canape"])
    meta_autre = adm.decrire_table("ref_proprietaires", db_path=ref)
    assert meta_autre["colonnes_lecture_seule"] == []


# ── UI : écran générique existant ────────────────────────────────────────────────────────────────

def test_ecran_detail_canape_affiche_le_formulaire_dedie(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_canape_parametres")
    assert resp.status_code == 200
    assert "Changer le seuil/montant canapé" in resp.text
    assert "changer-parametres" in resp.text


def test_route_changer_parametres_canape_ecrit_et_redirige(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/administration/referentiels/ref_canape_parametres/changer-parametres",
                       data={"logement_id": "LOG_A1", "seuil_voyageurs_preparation_canape": "5",
                             "montant_preparation_canape": "30", "date_debut": "2026-07-01",
                             "justification": "Test"},
                       follow_redirects=False)
    assert resp.status_code == 303
    lignes = canape.historique("LOG_A1", db_path=tmp_db)
    assert len(lignes) == 1 and float(lignes[0]["montant_preparation_canape"]) == 30


def test_ecran_ref_logements_masque_les_champs_canape(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_logements")
    assert resp.status_code == 200
    assert 'name="seuil_voyageurs_preparation_canape"' not in resp.text
    assert 'name="montant_preparation_canape"' not in resp.text

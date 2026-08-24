"""Mission 6 bis — administration du versionnement des règles algorithmiques.

Réutilise entièrement l'infrastructure existante (`referentiel_admin_service.transaction`/
`clore_periode`/`inserer`, journal `ref_admin_evenements`, `TABLES_NATIVES`) — ce fichier couvre
uniquement ce qui est nouveau : `regle_version_gestion_service.py` et l'écran dédié.
"""
import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import referentiel_admin_service as adm
from app.services import regle_version_gestion_service as regv


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def test_regles_versions_est_lecture_seule_sur_ecran_generique(ref):
    assert "ref_regles_versions" in adm.LECTURE_SEULE
    res = adm.creer_ligne("ref_regles_versions", {
        "regle_version_id": "RGV_X", "rule_code": "ASSIETTE_COMMISSION", "version": "V2"},
        db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_ECRITURE


def test_changer_version_ouvre_une_premiere_version(ref):
    res = regv.changer_version("REGLE_TEST", "V1", "2026-07-01", db_path=ref)
    assert res["ok"], res
    lignes = regv.historique("REGLE_TEST", db_path=ref)
    assert len(lignes) == 1
    assert lignes[0]["version"] == "V1"
    assert lignes[0]["date_debut"] == "2026-07-01" and lignes[0]["date_fin"] == ""


def test_changer_version_cloture_lancienne_et_ouvre_la_nouvelle(ref):
    regv.changer_version("REGLE_TEST", "V1", "2026-01-01", db_path=ref)
    res = regv.changer_version("REGLE_TEST", "V2", "2027-01-01", db_path=ref)
    assert res["ok"], res
    lignes = regv.historique("REGLE_TEST", db_path=ref)
    assert len(lignes) == 2
    ancienne = [l for l in lignes if l["date_fin"]][0]
    assert ancienne["version"] == "V1" and ancienne["date_fin"] == "2026-12-31"
    nouvelle = [l for l in lignes if not l["date_fin"]][0]
    assert nouvelle["version"] == "V2"


def test_changer_version_ne_modifie_jamais_une_version_close(ref):
    regv.changer_version("REGLE_TEST", "V1", "2026-01-01", db_path=ref)
    regv.changer_version("REGLE_TEST", "V2", "2027-01-01", db_path=ref)
    regv.changer_version("REGLE_TEST", "V3", "2028-01-01", db_path=ref)
    lignes = regv.historique("REGLE_TEST", db_path=ref)
    assert len(lignes) == 3
    v1 = [l for l in lignes if l["version"] == "V1"][0]
    assert v1["date_fin"] == "2026-12-31"      # jamais retouché


def test_changer_version_rule_code_manquant_refuse(ref):
    res = regv.changer_version("", "V1", "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == regv.E_RULE_CODE_MANQUANT


def test_changer_version_version_manquante_refuse(ref):
    res = regv.changer_version("REGLE_TEST", "", "2026-07-01", db_path=ref)
    assert res["ok"] is False and res["code"] == regv.E_VERSION_MANQUANTE


def test_changer_version_date_invalide_refusee(ref):
    res = regv.changer_version("REGLE_TEST", "V1", "01/07/2026", db_path=ref)
    assert res["ok"] is False and res["code"] == regv.E_DATE_INVALIDE


def test_chevauchement_versions_refuse(ref):
    """Deux versions ouvertes simultanément pour la même règle : refusé (même garde générique que
    taux commission/coûts ménage/canapé)."""
    regv.changer_version("REGLE_TEST", "V1", "2026-01-01", db_path=ref)
    res = adm.inserer("ref_regles_versions", {
        "regle_version_id": "RGV_MANUEL", "rule_code": "REGLE_TEST", "version": "V2",
        "date_debut": "2026-06-01", "date_fin": "", "actif": "OUI"},
        action="CREATION", db_path=ref)
    assert res["ok"] is False and res["code"] == adm.E_PERIODE_INCOHERENTE


# ── Écran ────────────────────────────────────────────────────────────────────────────────────────

def test_ecran_detail_regles_versions_affiche_le_formulaire_dedie(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels/ref_regles_versions")
    assert resp.status_code == 200
    assert "Introduire une nouvelle version" in resp.text
    assert "changer-version" in resp.text


def test_route_changer_version_ecrit_et_redirige(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.post("/administration/referentiels/ref_regles_versions/changer-version",
                       data={"rule_code": "REGLE_TEST", "version": "V1",
                             "date_debut": "2026-07-01", "commentaire": "Test"},
                       follow_redirects=False)
    assert resp.status_code == 303
    lignes = regv.historique("REGLE_TEST", db_path=tmp_db)
    assert len(lignes) == 1 and lignes[0]["version"] == "V1"


def test_ecran_administration_index_liste_les_regles_versionnees(client, tmp_db):
    fx.semer_parc_standard(tmp_db)
    resp = client.get("/administration/referentiels")
    assert resp.status_code == 200
    assert "Règles versionnées" in resp.text
    assert "/administration/referentiels/ref_regles_versions" in resp.text

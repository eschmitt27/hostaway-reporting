"""Résolution des mappings comptables historisés (migration `0024`)."""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations
from app.services import comptabilite_mappings_service as maps


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


def test_seed_generique_toujours_present(db):
    res = maps.resoudre_compte(categorie_charge_id="CHG_INCONNUE", db_path=db)
    assert res["statut"] == maps.ST_PROVISOIRE
    assert res["compte"] == "606000"
    assert res["regle"] == maps.PORTEE_PROVISOIRE


def test_mapping_exact_categorie_prioritaire(db):
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606100", cle="CHG_MENAGE", statut=maps.ST_VALIDE,
                     db_path=db)
    res = maps.resoudre_compte(categorie_charge_id="CHG_MENAGE", type_flux_id="TYPE_X", db_path=db)
    assert res["statut"] == maps.ST_VALIDE
    assert res["compte"] == "606100"
    assert res["regle"] == maps.PORTEE_CATEGORIE


def test_mapping_type_flux_utilise_si_categorie_absente(db):
    maps.creer_regle(maps.PORTEE_TYPE_FLUX, "627000", cle="TYPE_BANQUE", statut=maps.ST_VALIDE,
                     db_path=db)
    res = maps.resoudre_compte(categorie_charge_id="CHG_AUTRE", type_flux_id="TYPE_BANQUE", db_path=db)
    assert res["statut"] == maps.ST_VALIDE
    assert res["compte"] == "627000"
    assert res["regle"] == maps.PORTEE_TYPE_FLUX


def test_mapping_expire_ignore(db):
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606100", cle="CHG_MENAGE", statut=maps.ST_VALIDE,
                     date_fin_validite="2026-01-01", db_path=db)
    res = maps.resoudre_compte(categorie_charge_id="CHG_MENAGE", date_reference="2026-06-01", db_path=db)
    assert res["compte"] == "606000"     # retombe sur le générique, pas le compte expiré
    assert res["statut"] == maps.ST_PROVISOIRE


def test_mapping_pas_encore_actif_ignore(db):
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606100", cle="CHG_MENAGE", statut=maps.ST_VALIDE,
                     date_debut_validite="2027-01-01", db_path=db)
    res = maps.resoudre_compte(categorie_charge_id="CHG_MENAGE", date_reference="2026-06-01", db_path=db)
    assert res["compte"] == "606000"
    assert res["statut"] == maps.ST_PROVISOIRE


def test_mapping_provisoire_categorie_specifique(db):
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606200", cle="CHG_MAINT", statut=maps.ST_PROVISOIRE,
                     db_path=db)
    res = maps.resoudre_compte(categorie_charge_id="CHG_MAINT", db_path=db)
    assert res["compte"] == "606200"
    assert res["statut"] == maps.ST_PROVISOIRE
    assert res["regle"] == maps.PORTEE_CATEGORIE


def test_historisation_deux_periodes_meme_categorie(db):
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606100", cle="CHG_MENAGE", statut=maps.ST_VALIDE,
                     date_debut_validite="2026-01-01", date_fin_validite="2026-05-31", db_path=db)
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606150", cle="CHG_MENAGE", statut=maps.ST_VALIDE,
                     date_debut_validite="2026-06-01", db_path=db)
    ancien = maps.resoudre_compte(categorie_charge_id="CHG_MENAGE", date_reference="2026-03-01", db_path=db)
    recent = maps.resoudre_compte(categorie_charge_id="CHG_MENAGE", date_reference="2026-07-01", db_path=db)
    assert ancien["compte"] == "606100"
    assert recent["compte"] == "606150"


def test_creer_regle_portee_inconnue_refusee(db):
    res = maps.creer_regle("PORTEE_INEXISTANTE", "606000", cle="X", db_path=db)
    assert res["ok"] is False and res["code"] == maps.E_PORTEE_INCONNUE


def test_creer_regle_cle_manquante_refusee(db):
    res = maps.creer_regle(maps.PORTEE_CATEGORIE, "606000", db_path=db)
    assert res["ok"] is False and res["code"] == maps.E_CLE_MANQUANTE


def test_lister_regles_filtre_par_portee(db):
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606100", cle="CHG_MENAGE", db_path=db)
    maps.creer_regle(maps.PORTEE_TYPE_FLUX, "627000", cle="TYPE_BANQUE", db_path=db)
    cats = maps.lister_regles(portee=maps.PORTEE_CATEGORIE, db_path=db)
    assert len(cats) == 1 and cats[0]["cle"] == "CHG_MENAGE"

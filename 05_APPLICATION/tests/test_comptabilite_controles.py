"""Cœur Comptabilité — catalogue de contrôles."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_controles_service as ctrl
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def test_controler_sans_donnees_ne_leve_pas(db):
    rapport = ctrl.controler(db_path=db)
    assert rapport["statut"] == "OK"
    assert rapport["fiable"] is True


def test_facture_validee_sans_ecriture_detectee(db):
    frs = frs_svc.creer("Fournisseur Ctrl Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-CTRL-1",
                   "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    rapport = ctrl.controler(db_path=db)
    codes = [a["code"] for a in rapport["anomalies"]]
    assert ctrl.C_FACTURE_SANS_ECRITURE in codes


def test_facture_avec_ecriture_ne_declenche_pas_lanomalie(db):
    frs = frs_svc.creer("Fournisseur Ctrl Test 2", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-CTRL-2",
                   "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    rapport = ctrl.controler(db_path=db)
    cibles = [a for a in rapport["anomalies"]
             if a["code"] == ctrl.C_FACTURE_SANS_ECRITURE and a["identifiant"] == "FA-CTRL-2"]
    assert cibles == []


def test_compte_absent_detecte_par_insertion_directe(db):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, piece, "
        "libelle, origine_type, origine_id_opaque, statut, total_debit, total_credit) "
        "VALUES ('ECR-X','ACHATS','2026-06-01','2026-06','P','L','MANUEL','X','VALIDEE',10,10)")
    conn.execute(
        "INSERT INTO ecriture_lignes (ecriture_id_opaque, ligne_num, compte, debit, credit) "
        "VALUES ('ECR-X', 1, '999999', 10, 0)")
    conn.execute(
        "INSERT INTO ecriture_lignes (ecriture_id_opaque, ligne_num, compte, debit, credit) "
        "VALUES ('ECR-X', 2, '401000', 0, 10)")
    conn.commit()
    conn.close()
    rapport = ctrl.controler(db_path=db)
    codes = [a["code"] for a in rapport["anomalies"]]
    assert ctrl.C_COMPTE_ABSENT in codes


def test_mapping_a_arbitrer_detecte(db):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO mapping_categorie_compte (categorie_charge_id, compte, statut) "
        "VALUES ('CHG_001', '606000', 'A_CONTROLER')")
    conn.commit()
    conn.close()
    rapport = ctrl.controler(db_path=db)
    codes = [a["code"] for a in rapport["anomalies"]]
    assert ctrl.C_MAPPING_ABSENT in codes


def test_tva_non_arbitree_toujours_presente_en_info(db):
    rapport = ctrl.controler(db_path=db)
    infos = [a for a in rapport["anomalies"] if a["code"] == ctrl.C_TVA_NON_ARBITREE]
    assert len(infos) == 1 and infos[0]["severite"] == ctrl.INFO


def test_filtre_par_periode(db):
    frs = frs_svc.creer("Fournisseur Ctrl Periode", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-CTRL-P1",
                   "date_facture": "2026-05-10", "montant_ttc": 40.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    rapport_mai = ctrl.controler(periode="2026-05", db_path=db)
    rapport_juin = ctrl.controler(periode="2026-06", db_path=db)
    assert rapport_mai["nb_ecritures"] == 1
    assert rapport_juin["nb_ecritures"] == 0

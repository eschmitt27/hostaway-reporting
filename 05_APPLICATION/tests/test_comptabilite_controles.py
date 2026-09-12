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
    """Le contrôle garde le cas « facture validée, aucune écriture d'achat ».

    Depuis la recette utilisateur n°3 (§36/§77), la validation GÉNÈRE elle-même l'écriture
    d'achat : le cas ne peut donc plus naître d'une validation normale. Il reste possible pour
    les factures validées AVANT cette correction (les deux factures PDF réelles étaient dans cet
    état exact : VALIDEE, journal ACHATS vide) et si une écriture venait à disparaître. On
    reconstitue donc la situation en retirant l'écriture après coup.
    """
    frs = frs_svc.creer("Fournisseur Ctrl Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-CTRL-1",
                   "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    conn = get_db(db)
    try:
        conn.execute("DELETE FROM ecriture_lignes WHERE ecriture_id_opaque IN "
                     "(SELECT ecriture_id_opaque FROM ecritures WHERE origine_id_opaque=?)",
                     (r["facture_id_opaque"],))
        conn.execute("DELETE FROM ecritures WHERE origine_id_opaque=?", (r["facture_id_opaque"],))
        conn.commit()
    finally:
        conn.close()
    rapport = ctrl.controler(db_path=db)
    codes = [a["code"] for a in rapport["anomalies"]]
    assert ctrl.C_FACTURE_SANS_ECRITURE in codes


def test_validation_facture_genere_ecriture_achat_et_dette(db):
    """§36/§77 — la dette fournisseur naît à la VALIDATION, pas au débit bancaire."""
    frs = frs_svc.creer("Fournisseur Achat", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-ACHAT-1",
                    "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=db)
    res = fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    assert res["ok"] is True
    assert res["ecriture_achat"]["ok"] is True

    conn = get_db(db)
    try:
        ecr = conn.execute(
            "SELECT ecriture_id_opaque, journal, total_debit, total_credit FROM ecritures "
            "WHERE origine_id_opaque=?", (r["facture_id_opaque"],)).fetchall()
        assert len(ecr) == 1 and ecr[0]["journal"] == "ACHATS"
        assert ecr[0]["total_debit"] == ecr[0]["total_credit"] == 40.0
        dette = conn.execute(
            "SELECT credit FROM ecriture_lignes WHERE ecriture_id_opaque=? AND compte='401000'",
            (ecr[0]["ecriture_id_opaque"],)).fetchone()
        assert dette["credit"] == 40.0
    finally:
        conn.close()

    # Idempotence stricte (§35) : rejouer la validation ne crée jamais une seconde dépense.
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    conn = get_db(db)
    try:
        n = conn.execute("SELECT COUNT(*) c FROM ecritures WHERE origine_id_opaque=?",
                         (r["facture_id_opaque"],)).fetchone()["c"]
    finally:
        conn.close()
    assert n == 1


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

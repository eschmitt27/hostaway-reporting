"""Cœur Comptabilité — journaux VENTES (adaptateur Lot12), CAISSE, ODIVERSES."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc
from app.services import operations_caisse_service as caisse
from app.services import operations_diverses_service as od
from app.services import reglements_fournisseurs_service as regl


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


# ── Plan comptable étendu ─────────────────────────────────────────────────────

def test_plan_comptable_0023(db):
    conn = get_db(db)
    comptes = {r["compte"] for r in conn.execute("SELECT compte FROM plan_comptable")}
    conn.close()
    assert {"530000", "467000", "706000"} <= comptes


# ── VENTES (adaptateur Lot12) ─────────────────────────────────────────────────

def test_generer_ecriture_vente_positive(db):
    res = compta.generer_ecriture_vente("PROP_A", "2026-06", 150.0,
                                        nom_proprietaire="Alpha", acteur="recette", db_path=db)
    assert res["ok"], res
    e = compta.charger(res["ecriture_id_opaque"], db)
    assert e["journal"] == "VENTES"
    assert e["total_debit"] == e["total_credit"] == 150.0
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    par_compte = {l["compte"]: l for l in lignes}
    assert par_compte["411000"]["debit"] == 150.0
    assert par_compte["411000"]["auxiliaire"] == "PROP_A"
    assert par_compte["706000"]["credit"] == 150.0


def test_generer_ecriture_vente_negative_sens_inverse(db):
    res = compta.generer_ecriture_vente("PROP_B", "2026-06", -40.0, db_path=db)
    assert res["ok"], res
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    par_compte = {l["compte"]: l for l in lignes}
    assert par_compte["706000"]["debit"] == 40.0
    assert par_compte["411000"]["credit"] == 40.0


def test_generer_ecriture_vente_montant_nul_refuse(db):
    res = compta.generer_ecriture_vente("PROP_C", "2026-06", 0.0, db_path=db)
    assert res["ok"] is False


def test_generer_ecriture_vente_idempotente(db):
    r1 = compta.generer_ecriture_vente("PROP_A", "2026-06", 150.0, db_path=db)
    r2 = compta.generer_ecriture_vente("PROP_A", "2026-06", 150.0, db_path=db)
    assert r1["ecriture_id_opaque"] == r2["ecriture_id_opaque"]
    assert r2["deja_generee"] is True


# ── CAISSE : règlement fournisseur en espèces ─────────────────────────────────

@pytest.fixture
def facture_validee(db):
    frs = frs_svc.creer("Fournisseur Caisse Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-CAISSE-1",
                   "date_facture": "2026-06-10", "montant_ttc": 80.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    return {"facture_id_opaque": r["facture_id_opaque"], "fournisseur": frs}


def test_generer_ecriture_caisse_reglement_fournisseur(db, facture_validee):
    reg = regl.enregistrer(facture_validee["fournisseur"],
                           [{"facture_id_opaque": facture_validee["facture_id_opaque"], "montant": 80.0}],
                           date_reglement="2026-06-15", moyen="CAISSE", db_path=db)
    assert reg["ok"], reg
    res = compta.generer_ecriture_caisse_reglement(reg["reglement_id_opaque"], acteur="recette", db_path=db)
    assert res["ok"], res
    e = compta.charger(res["ecriture_id_opaque"], db)
    assert e["journal"] == "CAISSE"
    assert e["total_debit"] == e["total_credit"] == 80.0
    lignes = {l["compte"] for l in compta.lignes(res["ecriture_id_opaque"], db)}
    assert lignes == {"401000", "530000"}


def test_generer_ecriture_caisse_refuse_si_moyen_different(db, facture_validee):
    reg = regl.enregistrer(facture_validee["fournisseur"],
                           [{"facture_id_opaque": facture_validee["facture_id_opaque"], "montant": 80.0}],
                           date_reglement="2026-06-15", moyen="BANQUE", db_path=db)
    res = compta.generer_ecriture_caisse_reglement(reg["reglement_id_opaque"], db_path=db)
    assert res["ok"] is False


# ── CAISSE : opération de caisse (encaissement / remboursement associé) ──────

def test_operation_caisse_encaissement_puis_ecriture(db):
    op = caisse.creer("ENCAISSEMENT", 25.0, date_operation="2026-06-20",
                      tiers_type="ASSOCIE", tiers_id="PERS_X", acteur="recette", db_path=db)
    assert op["ok"], op
    res = compta.generer_ecriture_caisse_operation(op["operation_id_opaque"], acteur="recette", db_path=db)
    assert res["ok"], res
    lignes = {l["compte"]: l for l in compta.lignes(res["ecriture_id_opaque"], db)}
    assert lignes["530000"]["debit"] == 25.0
    assert lignes["467000"]["credit"] == 25.0
    assert lignes["467000"]["auxiliaire"] == "PERS_X"


def test_operation_caisse_remboursement_associe(db):
    op = caisse.creer("REMBOURSEMENT_ASSOCIE", 30.0, tiers_id="PERS_Y", db_path=db)
    res = compta.generer_ecriture_caisse_operation(op["operation_id_opaque"], db_path=db)
    assert res["ok"], res
    lignes = {l["compte"]: l for l in compta.lignes(res["ecriture_id_opaque"], db)}
    assert lignes["467000"]["debit"] == 30.0
    assert lignes["530000"]["credit"] == 30.0


def test_operation_caisse_type_inconnu_refuse(db):
    res = caisse.creer("TYPE_INCONNU", 10.0, db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_TYPE_INCONNU


def test_operation_caisse_montant_invalide_refuse(db):
    res = caisse.creer("ENCAISSEMENT", 0, db_path=db)
    assert res["ok"] is False and res["code"] == caisse.E_MONTANT_INVALIDE


def test_operation_caisse_annulee_refuse_generation(db):
    op = caisse.creer("ENCAISSEMENT", 25.0, tiers_id="PERS_X", db_path=db)
    caisse.annuler(op["operation_id_opaque"], db_path=db)
    res = compta.generer_ecriture_caisse_operation(op["operation_id_opaque"], db_path=db)
    assert res["ok"] is False


# ── ODIVERSES ──────────────────────────────────────────────────────────────

def test_od_creation_et_validation_genere_ecriture(db):
    r = od.creer("AJUSTEMENT", "Ajustement solde ouverture", [
        {"compte": "606000", "debit": 50.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 50.0, "auxiliaire": "FRS_TEST"},
    ], justification="Correction de solde initial", db_path=db)
    assert r["ok"], r
    charge = od.charger(r["od_id_opaque"], db)
    assert charge["statut"] == od.ST_BROUILLON
    assert len(charge["lignes"]) == 2

    res = od.valider(r["od_id_opaque"], acteur="recette", db_path=db)
    assert res["ok"], res
    assert res["ecriture_id_opaque"]
    e = compta.charger(res["ecriture_id_opaque"], db)
    assert e["journal"] == "ODIVERSES"
    assert e["total_debit"] == e["total_credit"] == 50.0


def test_od_desequilibree_refusee(db):
    r = od.creer("AJUSTEMENT", "OD déséquilibrée", [
        {"compte": "606000", "debit": 50.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 40.0},
    ], db_path=db)
    assert r["ok"] is False and r["code"] == od.E_DESEQUILIBRE


def test_od_compte_inconnu_refuse(db):
    r = od.creer("AJUSTEMENT", "OD compte inconnu", [
        {"compte": "999999", "debit": 10.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 10.0},
    ], db_path=db)
    assert r["ok"] is False and r["code"] == od.E_COMPTE_INCONNU


def test_od_type_inconnu_refuse(db):
    r = od.creer("TYPE_INCONNU", "x", [
        {"compte": "606000", "debit": 10.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 10.0},
    ], db_path=db)
    assert r["ok"] is False and r["code"] == od.E_TYPE_INCONNU


def test_od_sans_ligne_refusee(db):
    r = od.creer("AJUSTEMENT", "x", [], db_path=db)
    assert r["ok"] is False and r["code"] == od.E_SANS_LIGNE


def test_od_validee_ne_peut_pas_etre_revalidee(db):
    r = od.creer("AJUSTEMENT", "x", [
        {"compte": "606000", "debit": 10.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 10.0},
    ], db_path=db)
    od.valider(r["od_id_opaque"], db_path=db)
    res = od.valider(r["od_id_opaque"], db_path=db)
    assert res["ok"] is False and res["code"] == od.E_STATUT


def test_od_annulee_ne_genere_jamais_ecriture(db):
    r = od.creer("AJUSTEMENT", "x", [
        {"compte": "606000", "debit": 10.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 10.0},
    ], db_path=db)
    od.annuler(r["od_id_opaque"], db_path=db)
    res = compta.generer_ecriture_od(r["od_id_opaque"], db_path=db)
    assert res["ok"] is False


def test_od_validee_ne_sannule_pas(db):
    r = od.creer("AJUSTEMENT", "x", [
        {"compte": "606000", "debit": 10.0, "credit": 0},
        {"compte": "401000", "debit": 0, "credit": 10.0},
    ], db_path=db)
    od.valider(r["od_id_opaque"], db_path=db)
    res = od.annuler(r["od_id_opaque"], db_path=db)
    assert res["ok"] is False and res["code"] == od.E_STATUT


# ── Refus si flags désactivés (double verrou) ─────────────────────────────────

def test_operations_refusees_sans_flags(db, monkeypatch):
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", False)
    assert caisse.creer("ENCAISSEMENT", 10.0, db_path=db)["ok"] is False
    assert od.creer("AJUSTEMENT", "x", [{"compte": "606000", "debit": 1, "credit": 0},
                                        {"compte": "401000", "debit": 0, "credit": 1}],
                    db_path=db)["ok"] is False
    assert compta.generer_ecriture_vente("PROP_A", "2026-06", 10.0, db_path=db)["ok"] is False

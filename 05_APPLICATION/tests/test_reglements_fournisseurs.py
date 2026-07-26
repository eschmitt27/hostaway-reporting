"""Règlements fournisseurs : total, partiel, multiple, groupé, avoir, annulation, contrôles."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import factures_service as fact
from app.services import reglements_fournisseurs_service as svc

FRS = "FRS-TEST01"
AUTRE_FRS = "FRS-AUTRE"


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def _facture(db, ref="FA-001", ttc=120.0, fournisseur=FRS):
    r = fact.creer({"fournisseur_id_opaque": fournisseur, "facture_ref": ref,
                    "date_facture": "2026-06-01", "date_echeance": "2026-07-01",
                    "montant_ttc": ttc}, acteur="recette", db_path=db)
    assert r["ok"], r
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, acteur="recette", db_path=db)
    return r["facture_id_opaque"]


# ── Paiement total / partiel / multiple ──────────────────────────────────────

def test_paiement_total_passe_la_facture_en_reglee(db):
    fid = _facture(db)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 120.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", acteur="recette", db_path=db)
    assert res["ok"], res
    f = fact.charger(fid, db)
    assert f["statut"] == fact.ST_REGLEE
    assert f["montant_regle"] == 120.0 and f["solde_restant"] == 0.0


def test_paiement_partiel_puis_solde(db):
    fid = _facture(db)
    svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 50.0}],
                    date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    f = fact.charger(fid, db)
    assert f["statut"] == fact.ST_PARTIELLEMENT_REGLEE
    assert f["solde_restant"] == 70.0

    svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 70.0}],
                    date_reglement="2026-07-15", moyen="CAISSE", db_path=db)
    f2 = fact.charger(fid, db)
    assert f2["statut"] == fact.ST_REGLEE and f2["solde_restant"] == 0.0
    assert len(svc.reglements_de_facture(fid, db)) == 2      # plusieurs paiements sur une facture


def test_paiement_groupe_de_plusieurs_factures(db):
    f1 = _facture(db, "FA-G1", 100.0)
    f2 = _facture(db, "FA-G2", 60.0)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": f1, "montant": 100.0},
                                {"facture_id_opaque": f2, "montant": 60.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", acteur="recette", db_path=db)
    assert res["ok"] and res["montant"] == 160.0 and res["nb_factures"] == 2
    assert fact.charger(f1, db)["statut"] == fact.ST_REGLEE
    assert fact.charger(f2, db)["statut"] == fact.ST_REGLEE


def test_paiement_personnel_associe_et_avoir(db):
    f1 = _facture(db, "FA-PA", 100.0)
    r1 = svc.enregistrer(FRS, [{"facture_id_opaque": f1, "montant": 40.0}],
                         date_reglement="2026-07-01", moyen="PERSONNEL_ASSOCIE", db_path=db)
    r2 = svc.enregistrer(FRS, [{"facture_id_opaque": f1, "montant": 60.0}],
                         date_reglement="2026-07-02", moyen="AVOIR", db_path=db)
    assert r1["ok"] and r2["ok"]
    assert fact.charger(f1, db)["statut"] == fact.ST_REGLEE


# ── Contrôles ─────────────────────────────────────────────────────────────────

def test_depassement_du_solde_refuse(db):
    fid = _facture(db, "FA-D", 100.0)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 150.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_DEPASSEMENT


def test_double_paiement_refuse_apres_solde(db):
    fid = _facture(db, "FA-DP", 100.0)
    svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 100.0}],
                    date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 10.0}],
                          date_reglement="2026-07-02", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_DEPASSEMENT


def test_facture_annulee_non_reglable(db):
    fid = _facture(db, "FA-AN", 100.0)
    fact.changer_statut(fid, fact.ST_ANNULEE, acteur="recette", db_path=db)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 10.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_FACTURE_ANNULEE


def test_fournisseur_incoherent_refuse(db):
    fid = _facture(db, "FA-FI", 100.0, fournisseur=AUTRE_FRS)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 10.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_FOURNISSEUR_INCOHERENT


def test_reglement_sans_facture_refuse(db):
    res = svc.enregistrer(FRS, [], date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_SANS_FACTURE


def test_moyen_inconnu_refuse(db):
    fid = _facture(db, "FA-MI", 100.0)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 10.0}],
                          date_reglement="2026-07-01", moyen="BITCOIN", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_MOYEN_INCONNU


def test_montant_negatif_refuse(db):
    fid = _facture(db, "FA-MN", 100.0)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": -5.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_MONTANT_INVALIDE


def test_enregistrer_refuse_si_flags_off(db, monkeypatch):
    fid = _facture(db, "FA-FL", 100.0)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    res = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 10.0}],
                          date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_FLAGS


# ── Annulation & rapprochement ───────────────────────────────────────────────

def test_annulation_libere_le_solde(db):
    fid = _facture(db, "FA-AN2", 100.0)
    r = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 100.0}],
                        date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert fact.charger(fid, db)["statut"] == fact.ST_REGLEE

    svc.annuler(r["reglement_id_opaque"], commentaire="erreur de saisie", db_path=db)
    f = fact.charger(fid, db)
    assert f["montant_regle"] == 0.0 and f["solde_restant"] == 100.0
    # Le statut est DÉRIVÉ du solde : il doit redescendre, sinon la facture reste « REGLEE » avec
    # un solde non nul (incohérence détectée en recette par CTRL_FAC_REGLEE_AVEC_SOLDE_NON_NUL).
    assert f["statut"] == fact.ST_VALIDEE


def test_annulation_partielle_repasse_en_partiellement_reglee(db):
    fid = _facture(db, "FA-AN3", 100.0)
    svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 40.0}],
                    date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    r2 = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 60.0}],
                         date_reglement="2026-07-02", moyen="CAISSE", db_path=db)
    assert fact.charger(fid, db)["statut"] == fact.ST_REGLEE

    svc.annuler(r2["reglement_id_opaque"], db_path=db)
    f = fact.charger(fid, db)
    assert f["statut"] == fact.ST_PARTIELLEMENT_REGLEE
    assert f["solde_restant"] == 60.0


def test_annulation_ne_reveille_pas_une_facture_en_litige(db):
    fid = _facture(db, "FA-AN4", 100.0)
    r = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 100.0}],
                        date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    fact.changer_statut(fid, fact.ST_LITIGE, commentaire="contesté", db_path=db)
    svc.annuler(r["reglement_id_opaque"], db_path=db)
    assert fact.charger(fid, db)["statut"] == fact.ST_LITIGE   # décision humaine préservée


def test_marquer_rapproche(db):
    fid = _facture(db, "FA-RA", 100.0)
    r = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 100.0}],
                        date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    res = svc.marquer_rapproche(r["reglement_id_opaque"], "MVT-ABC123", db_path=db)
    assert res["ok"]
    assert svc.charger(r["reglement_id_opaque"], db)["statut"] == svc.ST_RAPPROCHE


def test_lister_reglements_non_rapproches(db):
    fid = _facture(db, "FA-NR", 100.0)
    r = svc.enregistrer(FRS, [{"facture_id_opaque": fid, "montant": 100.0}],
                        date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    assert len(svc.lister(non_rapproches_seulement=True, db_path=db)) == 1
    svc.marquer_rapproche(r["reglement_id_opaque"], "MVT-X", db_path=db)
    assert svc.lister(non_rapproches_seulement=True, db_path=db) == []


def test_solde_fournisseur_integre_les_reglements(db):
    f1 = _facture(db, "FA-S1", 100.0)
    _facture(db, "FA-S2", 60.0)
    svc.enregistrer(FRS, [{"facture_id_opaque": f1, "montant": 40.0}],
                    date_reglement="2026-07-01", moyen="BANQUE", db_path=db)
    s = fact.solde_fournisseur(FRS, db_path=db)
    assert s["total_facture"] == 160.0
    assert s["total_regle"] == 40.0
    assert s["solde_a_payer"] == 120.0

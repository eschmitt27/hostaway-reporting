"""Factures fournisseurs : validations, doublons, statuts, lien charge, solde, sécurité."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import factures_service as svc

FRS = "FRS-TEST01"
FORM = {"fournisseur_id_opaque": FRS, "facture_ref": "FA-2026-001", "date_facture": "2026-06-01",
        "date_echeance": "2026-07-01", "montant_ht": 100.0, "montant_tva": 20.0,
        "montant_ttc": 120.0}


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


# ── Création et validations ──────────────────────────────────────────────────

def test_creer_facture_valide(db):
    res = svc.creer(dict(FORM), acteur="recette", db_path=db)
    assert res["ok"], res
    f = svc.charger(res["facture_id_opaque"], db)
    assert f["montant_ttc"] == 120.0
    assert f["statut"] == svc.ST_A_CONTROLER
    assert f["montant_regle"] == 0.0 and f["solde_restant"] == 120.0


def test_fournisseur_obligatoire(db):
    codes = [e["code"] for e in svc.valider(dict(FORM, fournisseur_id_opaque=""), db)]
    assert svc.E_FOURNISSEUR_MANQUANT in codes


def test_fournisseur_archive_refuse(db):
    codes = [e["code"] for e in svc.valider(dict(FORM), db, fournisseur_actif=False)]
    assert svc.E_FOURNISSEUR_ARCHIVE in codes


def test_reference_obligatoire(db):
    codes = [e["code"] for e in svc.valider(dict(FORM, facture_ref=""), db)]
    assert svc.E_REF_MANQUANTE in codes


def test_montant_invalide_refuse(db):
    codes = [e["code"] for e in svc.valider(dict(FORM, montant_ttc=0), db)]
    assert svc.E_MONTANT_INVALIDE in codes


def test_ht_plus_tva_different_ttc_refuse(db):
    codes = [e["code"] for e in svc.valider(dict(FORM, montant_ht=100, montant_tva=20,
                                                 montant_ttc=999), db)]
    assert svc.E_TVA_INCOHERENTE in codes


def test_echeance_anterieure_refusee(db):
    codes = [e["code"] for e in svc.valider(dict(FORM, date_facture="2026-06-10",
                                                 date_echeance="2026-06-01"), db)]
    assert svc.E_DATE_INCOHERENTE in codes


def test_doublon_certain_refuse(db):
    svc.creer(dict(FORM), acteur="recette", db_path=db)
    codes = [e["code"] for e in svc.valider(dict(FORM), db)]
    assert svc.E_DOUBLON_CERTAIN in codes
    res = svc.creer(dict(FORM), acteur="recette", db_path=db)
    assert res["ok"] is False


def test_doublon_probable_detecte(db):
    svc.creer(dict(FORM), acteur="recette", db_path=db)
    probables = svc.doublons_probables(FRS, 120.0, "2026-06-03", db_path=db)
    assert len(probables) == 1                    # même montant, date proche, référence différente


def test_creer_refuse_si_flags_off(db, monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    res = svc.creer(dict(FORM), acteur="recette", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_FLAGS
    assert svc.lister(db_path=db) == []


# ── Statuts ───────────────────────────────────────────────────────────────────

def test_transition_valide(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    res = svc.changer_statut(r["facture_id_opaque"], svc.ST_VALIDEE, acteur="recette", db_path=db)
    assert res["ok"] and res["statut"] == svc.ST_VALIDEE


def test_transition_interdite_refusee(db):
    r = svc.creer(dict(FORM, statut=svc.ST_BROUILLON), acteur="recette", db_path=db)
    svc.changer_statut(r["facture_id_opaque"], svc.ST_ANNULEE, acteur="recette", db_path=db)
    res = svc.changer_statut(r["facture_id_opaque"], svc.ST_VALIDEE, acteur="recette", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_STATUT


def test_facture_validee_jamais_supprimee_mais_annulable(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    svc.changer_statut(r["facture_id_opaque"], svc.ST_VALIDEE, acteur="recette", db_path=db)
    svc.changer_statut(r["facture_id_opaque"], svc.ST_ANNULEE, commentaire="erreur", db_path=db)
    f = svc.charger(r["facture_id_opaque"], db)
    assert f is not None and f["statut"] == svc.ST_ANNULEE     # toujours présente


def test_historique_conserve(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    svc.changer_statut(r["facture_id_opaque"], svc.ST_VALIDEE, acteur="recette", db_path=db)
    h = svc.historique(r["facture_id_opaque"], db)
    assert len(h) == 2 and h[0]["type_evenement"] == "VALIDATION"


# ── Lien charge ───────────────────────────────────────────────────────────────

def test_lier_charge(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    res = svc.lier_charge(r["facture_id_opaque"], "CHG_001", acteur="recette", db_path=db)
    assert res["ok"]
    assert svc.charger(r["facture_id_opaque"], db)["charge_id"] == "CHG_001"


def test_une_charge_ne_peut_pas_etre_liee_a_deux_factures(db):
    r1 = svc.creer(dict(FORM), acteur="recette", db_path=db)
    r2 = svc.creer(dict(FORM, facture_ref="FA-2026-002"), acteur="recette", db_path=db)
    svc.lier_charge(r1["facture_id_opaque"], "CHG_001", db_path=db)
    res = svc.lier_charge(r2["facture_id_opaque"], "CHG_001", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_CHARGE_DEJA_LIEE


# ── Lignes de facture (multi-charges / multi-logements) ─────────────────────

def test_ajouter_ligne(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    res = svc.ajouter_ligne(r["facture_id_opaque"], "CHG_001", logement_id="LOG_A1",
                            montant_ttc=60.0, acteur="recette", db_path=db)
    assert res["ok"], res
    f = svc.charger(r["facture_id_opaque"], db)
    assert len(f["lignes"]) == 1
    assert f["lignes"][0]["charge_id"] == "CHG_001"
    assert f["lignes"][0]["logement_id"] == "LOG_A1"
    assert f["montant_lignes_ttc"] == 60.0


def test_ajouter_plusieurs_lignes_multi_charges_multi_logements(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    svc.ajouter_ligne(r["facture_id_opaque"], "CHG_001", logement_id="LOG_A1",
                      montant_ttc=60.0, db_path=db)
    svc.ajouter_ligne(r["facture_id_opaque"], "CHG_002", logement_id="LOG_B1",
                      montant_ttc=60.0, db_path=db)
    f = svc.charger(r["facture_id_opaque"], db)
    assert len(f["lignes"]) == 2
    assert f["montant_lignes_ttc"] == 120.0


def test_ligne_charge_manquante_refusee(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    res = svc.ajouter_ligne(r["facture_id_opaque"], "", montant_ttc=60.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_LIGNE_CHARGE_MANQUANTE


def test_ligne_montant_invalide_refuse(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    res = svc.ajouter_ligne(r["facture_id_opaque"], "CHG_001", montant_ttc=0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_LIGNE_MONTANT_INVALIDE


def test_ligne_charge_deja_liee_a_autre_facture_refusee(db):
    r1 = svc.creer(dict(FORM), acteur="recette", db_path=db)
    r2 = svc.creer(dict(FORM, facture_ref="FA-2026-002"), acteur="recette", db_path=db)
    svc.ajouter_ligne(r1["facture_id_opaque"], "CHG_001", montant_ttc=60.0, db_path=db)
    res = svc.ajouter_ligne(r2["facture_id_opaque"], "CHG_001", montant_ttc=60.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_CHARGE_DEJA_LIEE


def test_ligne_charge_deja_liee_via_lier_charge_refusee(db):
    """La même charge ne peut pas être à la fois en mono-charge (facture 1) et en ligne (facture 2)."""
    r1 = svc.creer(dict(FORM), acteur="recette", db_path=db)
    r2 = svc.creer(dict(FORM, facture_ref="FA-2026-002"), acteur="recette", db_path=db)
    svc.lier_charge(r1["facture_id_opaque"], "CHG_001", db_path=db)
    res = svc.ajouter_ligne(r2["facture_id_opaque"], "CHG_001", montant_ttc=60.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_CHARGE_DEJA_LIEE


def test_facture_mono_charge_refuse_ajout_ligne(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    svc.lier_charge(r["facture_id_opaque"], "CHG_001", db_path=db)
    res = svc.ajouter_ligne(r["facture_id_opaque"], "CHG_002", montant_ttc=60.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_LIGNE_FACTURE_MONO_CHARGE


def test_facture_avec_lignes_refuse_lier_charge_mono(db):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    svc.ajouter_ligne(r["facture_id_opaque"], "CHG_001", montant_ttc=60.0, db_path=db)
    res = svc.lier_charge(r["facture_id_opaque"], "CHG_002", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_LIGNE_FACTURE_MONO_CHARGE


def test_ajouter_ligne_refuse_si_flags_off(db, monkeypatch):
    r = svc.creer(dict(FORM), acteur="recette", db_path=db)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    res = svc.ajouter_ligne(r["facture_id_opaque"], "CHG_001", montant_ttc=60.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_FLAGS


def test_ajouter_ligne_facture_introuvable(db):
    res = svc.ajouter_ligne("FAC-INEXISTANTE", "CHG_001", montant_ttc=60.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_INTROUVABLE


# ── Listes et soldes ──────────────────────────────────────────────────────────

def test_facture_echue_detectee(db):
    svc.creer(dict(FORM, facture_ref="FA-ECHUE", date_facture="2020-01-01",
                   date_echeance="2020-02-01"), acteur="recette", db_path=db)
    echues = svc.lister(echues_seulement=True, db_path=db)
    assert len(echues) == 1 and echues[0]["facture_ref"] == "FA-ECHUE"


def test_solde_fournisseur(db):
    svc.creer(dict(FORM), acteur="recette", db_path=db)
    svc.creer(dict(FORM, facture_ref="FA-2026-002", montant_ttc=80.0, montant_ht=None,
                   montant_tva=None), acteur="recette", db_path=db)
    s = svc.solde_fournisseur(FRS, db_path=db)
    assert s["total_facture"] == 200.0
    assert s["total_regle"] == 0.0
    assert s["solde_a_payer"] == 200.0
    assert s["nb_factures"] == 2

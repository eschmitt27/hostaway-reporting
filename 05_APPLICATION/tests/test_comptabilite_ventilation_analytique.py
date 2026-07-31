"""Ventilation analytique des écritures ACHATS (migration `0024`) — cases A/D/E/F du brief
Analytique §4, et branchement réel des mappings (§2)."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.readers import charges_reader
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_mappings_service as maps
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


def _charge_fixture(charge_id, logement_id="", proprietaire_id="", categorie="CHG_MENAGE",
                    type_flux="TYPE_FLUX_014"):
    return {"charge_id": charge_id, "logement_id": logement_id, "proprietaire_id": proprietaire_id,
            "categorie_charge_id": categorie, "type_flux_id": type_flux}


# ── Case A : affectation directe (mono-charge, dimension portée par la charge) ───────────────────

def test_case_a_affectation_directe(db, monkeypatch):
    frs = frs_svc.creer("Fournisseur Vent A", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-A",
                   "date_facture": "2026-06-10", "montant_ttc": 90.0}, db_path=db)
    fact.lier_charge(r["facture_id_opaque"], "CHG_A1", db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge",
                        lambda cid: _charge_fixture(cid, logement_id="LOG_A1", proprietaire_id="PROP_A"))

    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert res["ok"], res
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    debit = next(l for l in lignes if l["debit"] > 0)
    assert debit["logement_id"] == "LOG_A1"
    assert debit["proprietaire_id"] == "PROP_A"

    vent = compta.ventilation_ecriture(res["ecriture_id_opaque"], db)
    assert len(vent) == 1
    assert vent[0]["methode"] == "AFFECTATION_DIRECTE_LOGEMENT"
    assert vent[0]["statut_ventilation"] == "VALIDE"
    assert vent[0]["montant_non_arrondi"] == 90.0


def test_case_a_mapping_valide_utilise_le_bon_compte(db, monkeypatch):
    frs = frs_svc.creer("Fournisseur Vent A2", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-A2",
                   "date_facture": "2026-06-10", "montant_ttc": 50.0}, db_path=db)
    fact.lier_charge(r["facture_id_opaque"], "CHG_A2", db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge",
                        lambda cid: _charge_fixture(cid, logement_id="LOG_A1", categorie="CHG_MENAGE"))
    conn = get_db(db)
    conn.execute("INSERT INTO plan_comptable (compte, libelle, type_compte) "
                "VALUES ('606100', 'Menage externe', 'CHARGE')")
    conn.commit()
    conn.close()
    maps.creer_regle(maps.PORTEE_CATEGORIE, "606100", cle="CHG_MENAGE", statut=maps.ST_VALIDE, db_path=db)

    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    debit = next(l for l in lignes if l["debit"] > 0)
    assert debit["compte"] == "606100"
    vent = compta.ventilation_ecriture(res["ecriture_id_opaque"], db)
    assert vent[0]["mapping_regle_id_opaque"] is not None


# ── Case D : facture multi-lignes (multi-charges / multi-logements) ──────────────────────────────

def test_case_d_facture_multi_lignes(db, monkeypatch):
    frs = frs_svc.creer("Fournisseur Vent D", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-D",
                   "date_facture": "2026-06-10", "montant_ttc": 120.0}, db_path=db)
    fact.ajouter_ligne(r["facture_id_opaque"], "CHG_D1", logement_id="LOG_D1", montant_ttc=70.0, db_path=db)
    fact.ajouter_ligne(r["facture_id_opaque"], "CHG_D2", logement_id="LOG_D2", montant_ttc=50.0, db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge", lambda cid: _charge_fixture(cid))

    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert res["ok"], res
    e = compta.charger(res["ecriture_id_opaque"], db)
    assert e["total_debit"] == e["total_credit"] == 120.0

    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    debits = [l for l in lignes if l["debit"] > 0]
    assert len(debits) == 2
    assert {l["logement_id"] for l in debits} == {"LOG_D1", "LOG_D2"}
    assert sum(l["debit"] for l in debits) == 120.0

    vent = compta.ventilation_ecriture(res["ecriture_id_opaque"], db)
    assert len(vent) == 2
    assert all(v["methode"] == "FACTURE_MULTI_LIGNES" for v in vent)
    # Réconciliation : montant source = somme des ventilations (tolérance 0,01 €).
    assert abs(sum(v["montant_non_arrondi"] for v in vent) - e["total_debit"]) <= 0.01


def test_case_d_logement_de_la_ligne_prioritaire_sur_celui_de_la_charge(db, monkeypatch):
    """Le logement saisi sur la ligne de facture prime sur celui, différent, de la charge MASTER."""
    frs = frs_svc.creer("Fournisseur Vent D2", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-D2",
                   "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=db)
    fact.ajouter_ligne(r["facture_id_opaque"], "CHG_D3", logement_id="LOG_LIGNE", montant_ttc=40.0, db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge",
                        lambda cid: _charge_fixture(cid, logement_id="LOG_CHARGE"))

    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    debit = next(l for l in lignes if l["debit"] > 0)
    assert debit["logement_id"] == "LOG_LIGNE"


# ── Case E : répartition Ménages (charge sans dimension propre, liée à un ménage) ─────────────────

def test_case_e_repartition_menage(db, monkeypatch):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO menages (menage_id_opaque, logement_id, proprietaire_id, type_menage, mois, "
        "statut, charge_id) VALUES ('MEN-1','LOG_E1','PROP_E1','INTERNE','2026-06','REGLE','CHG_E1')")
    conn.commit()
    conn.close()

    frs = frs_svc.creer("Fournisseur Vent E", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-E",
                   "date_facture": "2026-06-10", "montant_ttc": 60.0}, db_path=db)
    fact.lier_charge(r["facture_id_opaque"], "CHG_E1", db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge", lambda cid: _charge_fixture(cid))   # pas de logement propre

    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    debit = next(l for l in lignes if l["debit"] > 0)
    assert debit["logement_id"] == "LOG_E1"
    assert debit["proprietaire_id"] == "PROP_E1"

    vent = compta.ventilation_ecriture(res["ecriture_id_opaque"], db)
    assert vent[0]["methode"] == "REPARTITION_MENAGE"
    assert vent[0]["statut_ventilation"] == "VALIDE"


# ── Case F : sans dimension disponible — A_CONTROLER, jamais bloquant ────────────────────────────

def test_case_f_sans_dimension_disponible(db, monkeypatch):
    frs = frs_svc.creer("Fournisseur Vent F", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-F",
                   "date_facture": "2026-06-10", "montant_ttc": 30.0}, db_path=db)
    fact.lier_charge(r["facture_id_opaque"], "CHG_F1", db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge", lambda cid: _charge_fixture(cid))   # rien du tout

    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert res["ok"], res     # jamais bloquant : l'écriture se génère quand même
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    debit = next(l for l in lignes if l["debit"] > 0)
    assert debit["logement_id"] is None

    vent = compta.ventilation_ecriture(res["ecriture_id_opaque"], db)
    assert vent[0]["methode"] == "SANS_DIMENSION"
    assert vent[0]["statut_ventilation"] == "A_CONTROLER"


def test_facture_sans_aucune_charge_liee_reste_ventilee_a_controler(db):
    frs = frs_svc.creer("Fournisseur Vent G", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-G",
                   "date_facture": "2026-06-10", "montant_ttc": 15.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert res["ok"], res
    vent = compta.ventilation_ecriture(res["ecriture_id_opaque"], db)
    assert vent[0]["methode"] == "SANS_DIMENSION"
    assert vent[0]["statut_ventilation"] == "A_CONTROLER"


# ── Idempotence : pas de double ventilation à la régénération ───────────────────────────────────

# ── VENTES : dimension propriétaire_id peuplée sur les deux lignes ────────────────────────────────

def test_ventes_porte_la_dimension_proprietaire(db):
    res = compta.generer_ecriture_vente("PROP_VENT", "2026-06", 100.0, db_path=db)
    assert res["ok"], res
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    assert all(l["proprietaire_id"] == "PROP_VENT" for l in lignes)


def test_regenerer_ne_duplique_pas_la_ventilation(db, monkeypatch):
    frs = frs_svc.creer("Fournisseur Vent Idem", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-VENT-IDEM",
                   "date_facture": "2026-06-10", "montant_ttc": 25.0}, db_path=db)
    fact.lier_charge(r["facture_id_opaque"], "CHG_IDEM", db_path=db)
    monkeypatch.setattr(charges_reader, "find_charge",
                        lambda cid: _charge_fixture(cid, logement_id="LOG_IDEM"))
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)

    r1 = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    r2 = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert r1["ecriture_id_opaque"] == r2["ecriture_id_opaque"]
    vent = compta.ventilation_ecriture(r1["ecriture_id_opaque"], db)
    assert len(vent) == 1     # jamais dupliquée par la régénération idempotente

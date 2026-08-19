"""Réconciliations (Phase 2) — statuts OK/ECART_TOLERE/A_CONTROLER/BLOQUANT/NON_DISPONIBLE."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.readers import proprietaires_reglements_reader as reader
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_reconciliations_service as recon
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc
from app.services import reglements_fournisseurs_service as regl
from app.services import banques_rapprochement_service as rappro


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


# ── A. Lot9 ↔ Lot10 : NON_DISPONIBLE si MASTER_CALC_Flux absent ──────────────
# Implémentation complète et tests dédiés : test_comptabilite_reconciliation_lot9_lot10.py.

def test_lot9_vs_lot10_non_disponible_si_source_absente(tmp_db):
    # `flux_unifies` (0043) existe (migration appliquée par tmp_db) mais est vide : NON_DISPONIBLE,
    # jamais une exception — même contrat qu'avant (MASTER_CALC_Flux absent), source SQLite.
    res = recon.lot9_vs_lot10()
    assert res["statut"] == recon.ST_NON_DISPONIBLE
    assert "raison" in res


# ── B/H. Lot10 ↔ Analytique ───────────────────────────────────────────────────

def _wb(path, sheets):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, (cols, rows) in sheets.items():
        ws = wb.create_sheet(name)
        ws.append(cols)
        for r in rows:
            ws.append([r.get(c) for c in cols])
    wb.save(str(path))
    wb.close()


@pytest.fixture
def resultats_files(tmp_path, monkeypatch):
    res = tmp_path / "RES.xlsx"
    log_cols = ["mois", "logement_id", "proprietaire_id", "total_produits", "total_charges",
               "resultat", "nb_flux", "vision", "commentaire"]
    global_cols = ["vision", "total_produits", "total_charges", "resultat", "commentaire_hc"]
    par_logement = [
        {"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 1000.0, "total_charges": 300.0, "resultat": 700.0, "nb_flux": 5,
         "vision": "REEL", "commentaire": ""},
        {"mois": "2026-06", "logement_id": "LOG_B1", "proprietaire_id": "PROP_B",
         "total_produits": 500.0, "total_charges": 100.0, "resultat": 400.0, "nb_flux": 3,
         "vision": "REEL", "commentaire": ""},
    ]
    glob = [
        {"vision": "REEL", "total_produits": 1500.0, "total_charges": 400.0, "resultat": 1100.0,
         "commentaire_hc": "OK"},
        {"vision": "COMPTABLE", "total_produits": 900.0, "total_charges": 250.0, "resultat": 650.0,
         "commentaire_hc": "OK"},
    ]
    _wb(res, {"PAR_MOIS_LOGEMENT": (log_cols, par_logement),
             "PAR_MOIS_PROPRIETAIRE": (log_cols[:2] + log_cols[3:], []),
             "GLOBAL": (global_cols, glob)})
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", res)
    reader.vider_cache()
    yield tmp_path
    reader.vider_cache()


def test_lot10_vs_analytique_ok_par_construction(resultats_files):
    res = recon.lot10_vs_analytique(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_OK
    assert res["montant_gauche"] == res["montant_droit"] == 1100.0


def test_total_analytique_vs_resultat_global_alias(resultats_files):
    res = recon.total_analytique_vs_resultat_global(vision="REEL")
    assert res["statut"] == recon.ST_OK


def test_lot10_vs_analytique_non_disponible_si_source_absente(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", tmp_path / "absent.xlsx")
    reader.vider_cache()
    res = recon.lot10_vs_analytique()
    assert res["statut"] == recon.ST_NON_DISPONIBLE
    reader.vider_cache()


# ── C. Analytique ↔ Comptabilité ──────────────────────────────────────────────

def test_analytique_vs_comptabilite_ecart_attendu_a_controler(resultats_files, db):
    # Aucune écriture générée : le côté Comptabilité est 0, un écart avec Lot10 est attendu.
    res = recon.analytique_vs_comptabilite(vision="COMPTABLE", db_path=db)
    assert res["statut"] == recon.ST_A_CONTROLER
    assert res["montant_gauche"] == 650.0
    assert res["montant_droit"] == 0.0


def test_analytique_vs_comptabilite_non_disponible_si_source_absente(tmp_path, monkeypatch, db):
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", tmp_path / "absent.xlsx")
    reader.vider_cache()
    res = recon.analytique_vs_comptabilite(db_path=db)
    assert res["statut"] == recon.ST_NON_DISPONIBLE
    reader.vider_cache()


# ── D. Banque ↔ journal BANQUE ────────────────────────────────────────────────

def test_banque_vs_journal_banque_ok(db):
    frs = frs_svc.creer("Fournisseur Recon D", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-RECON-D",
                   "date_facture": "2026-06-10", "montant_ttc": 80.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    reg = regl.enregistrer(frs, [{"facture_id_opaque": r["facture_id_opaque"], "montant": 80.0}],
                           date_reglement="2026-06-15", moyen="BANQUE", db_path=db)
    rap = rappro.enregistrer("MVT-RECON-D", "REGLEMENT_CHARGE", reg["reglement_id_opaque"], 80.0,
                             montant_mouvement=80.0, db_path=db)
    rappro.confirmer(rap["rapprochement_id_opaque"], db_path=db)
    res_banque = compta.generer_ecriture_banque(rap["rapprochement_id_opaque"], db_path=db)
    compta.valider(res_banque["ecriture_id_opaque"], db_path=db)

    res = recon.banque_vs_journal_banque(db_path=db)
    assert res["statut"] == recon.ST_OK
    assert res["montant_gauche"] == res["montant_droit"] == 80.0


def test_banque_vs_journal_banque_objet_manquant(db):
    frs = frs_svc.creer("Fournisseur Recon D2", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-RECON-D2",
                   "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    reg = regl.enregistrer(frs, [{"facture_id_opaque": r["facture_id_opaque"], "montant": 40.0}],
                           date_reglement="2026-06-15", moyen="BANQUE", db_path=db)
    rap = rappro.enregistrer("MVT-RECON-D2", "REGLEMENT_CHARGE", reg["reglement_id_opaque"], 40.0,
                             montant_mouvement=40.0, db_path=db)
    rappro.confirmer(rap["rapprochement_id_opaque"], db_path=db)
    # Pas de generer_ecriture_banque : l'écriture BANQUE manque.
    res = recon.banque_vs_journal_banque(db_path=db)
    assert res["statut"] == recon.ST_A_CONTROLER
    assert len(res["detail"]) == 1


# ── E. Factures ↔ auxiliaires ─────────────────────────────────────────────────

def test_factures_vs_auxiliaires_ok(db):
    frs = frs_svc.creer("Fournisseur Recon E", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-RECON-E",
                   "date_facture": "2026-06-10", "montant_ttc": 60.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res_achat = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    compta.valider(res_achat["ecriture_id_opaque"], db_path=db)

    res = recon.factures_vs_auxiliaires(fournisseur_id_opaque=frs, db_path=db)
    assert res["statut"] == recon.ST_OK
    assert res["montant_gauche"] == res["montant_droit"] == 60.0


# ── F. Ménages ↔ charges ──────────────────────────────────────────────────────

def test_menages_vs_charges_non_disponible_sans_menage(db):
    res = recon.menages_vs_charges(db_path=db)
    assert res["statut"] == recon.ST_NON_DISPONIBLE


def test_menages_vs_charges_manquant_detecte(db):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO menages (menage_id_opaque, logement_id, proprietaire_id, type_menage, mois, "
        "statut, charge_id) VALUES ('MEN-RECON','LOG_F1','PROP_F1','INTERNE','2026-06','REGLE','CHG_INEXISTANTE')")
    conn.commit()
    conn.close()
    res = recon.menages_vs_charges(mois="2026-06", db_path=db)
    assert res["statut"] == recon.ST_A_CONTROLER
    assert res["montant_gauche"] == 1 and res["montant_droit"] == 0


# ── G. Commissions ↔ VENTES ────────────────────────────────────────────────────

def test_commissions_vs_ventes_non_disponible_sans_lot12(db, tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MASTER_NET_PROPRIETAIRE", tmp_path / "absent.xlsx")
    reader.vider_cache()
    res = recon.commissions_vs_ventes("2026-06", db_path=db)
    assert res["statut"] == recon.ST_NON_DISPONIBLE
    reader.vider_cache()

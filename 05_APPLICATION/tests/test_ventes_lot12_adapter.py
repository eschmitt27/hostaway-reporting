"""Journal VENTES — adaptateur Lot12 (SOURCE_PROVISOIRE_LOT12). Ne recalcule jamais Lot12,
ne le modifie jamais : fixtures SQLite isolées (Lot10 + Lot12), comme
`test_proprietaires_reglements.py`."""
from __future__ import annotations

import pytest

import app.config as cfg
import fixtures_lot10 as fx
from app.db.connection import apply_migrations
from app.readers import proprietaires_reglements_reader as reader
from app.services import comptabilite_ecritures_service as compta
from app.services import ventes_lot12_adapter_service as adapter


@pytest.fixture
def lot12_files(tmp_path, monkeypatch):
    vue = [
        {"mois": "2026-06", "proprietaire_id": "PROP_A", "total_payout_mois": 2000,
         "total_menage_mois": 150, "total_commission_mois": 300, "charge_fixe_mensuelle": 0,
         "montant_du_conciergerie": 300, "reste_a_payer_conciergerie": 0,
         "net_proprietaire_avant_charge_mois": 1700, "net_proprietaire_apres_charge_mois": 1700,
         "nb_reservations": 4},
        {"mois": "2026-06", "proprietaire_id": "PROP_B", "total_payout_mois": 500,
         "total_menage_mois": 30, "total_commission_mois": 0, "charge_fixe_mensuelle": 0,
         "montant_du_conciergerie": 0, "reste_a_payer_conciergerie": 0,
         "net_proprietaire_avant_charge_mois": 500, "net_proprietaire_apres_charge_mois": 500,
         "nb_reservations": 1},
    ]
    dash = [
        {"mois": "2026-06", "proprietaire_id": "PROP_A", "nb_logements": 2, "nb_bloquants_mois": 0,
         "nb_a_controler_mois": 0, "facturation_lot12_ok": "OUI", "mode_facturation": "MENSUEL",
         "statut_facture": "EMISE", "balises_non_resolues": ""},
        {"mois": "2026-06", "proprietaire_id": "PROP_B", "nb_logements": 1, "nb_bloquants_mois": 0,
         "nb_a_controler_mois": 0, "facturation_lot12_ok": "OUI", "mode_facturation": "MENSUEL",
         "statut_facture": "EMISE", "balises_non_resolues": ""},
    ]
    # VUE_MOIS vient de Lot10 SQLite (migration 0044). DASHBOARD_FACTURATION vient de Lot12
    # SQLite (migration 0047, fixtures_lot12) — MASTER_FACT_Proprietaires.xlsx n'est plus lu.
    import fixtures_lot12 as fx12

    db = tmp_path / "app.db"
    apply_migrations(db)
    fx.seeder(db, net_vue_mois=vue)
    fx12.seeder(db, dashboard=dash)

    monkeypatch.setattr(cfg, "DB_PATH", db)
    reader.vider_cache()
    yield tmp_path
    reader.vider_cache()


@pytest.fixture
def db(lot12_files, monkeypatch):
    """Même base que `lot12_files` (Lot10/Lot12 seedés) — une base séparée laisserait
    `generer_ecritures_du_mois` interroger une base vide (PROP_A/PROP_B introuvables)."""
    p = lot12_files / "app.db"
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def test_lignes_du_mois_lit_lot12(lot12_files):
    lignes = adapter.lignes_du_mois("2026-06")
    par_prop = {l["proprietaire_id"]: l for l in lignes}
    assert par_prop["PROP_A"]["montant_du_conciergerie"] == 300
    assert par_prop["PROP_B"]["montant_du_conciergerie"] == 0


def test_source_indisponible_retourne_liste_vide(tmp_path, monkeypatch):
    """Aucun run Lot10 actif : liste vide, jamais une exception."""
    db = tmp_path / "app.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db)
    reader.vider_cache()
    assert adapter.lignes_du_mois("2026-06") == []
    reader.vider_cache()


def test_generer_ecritures_du_mois(lot12_files, db):
    res = adapter.generer_ecritures_du_mois("2026-06", acteur="recette", db_path=db)
    assert res["source"] == "SOURCE_PROVISOIRE_LOT12"
    par_prop = {r["proprietaire_id"]: r for r in res["resultats"]}
    assert par_prop["PROP_A"]["genere"] is True
    assert par_prop["PROP_B"]["genere"] is False   # montant nul, rien à constater

    e = compta.charger(par_prop["PROP_A"]["ecriture_id_opaque"], db)
    assert e["journal"] == "VENTES" and e["total_debit"] == 300.0


def test_generer_ecritures_du_mois_idempotent(lot12_files, db):
    r1 = adapter.generer_ecritures_du_mois("2026-06", db_path=db)
    r2 = adapter.generer_ecritures_du_mois("2026-06", db_path=db)
    id1 = next(r["ecriture_id_opaque"] for r in r1["resultats"] if r["proprietaire_id"] == "PROP_A")
    id2 = next(r["ecriture_id_opaque"] for r in r2["resultats"] if r["proprietaire_id"] == "PROP_A")
    assert id1 == id2

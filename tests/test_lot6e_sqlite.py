"""Lot6e — écart gain/perte vs coût standard, en SQLite (menages_gainperte, 0038).

Ne réécrit pas la formule (ecart = cout_standard_total - cout_reel_total, méthodes EXTERNE_FACTURE/
INTERNE_HEURES_M04/INTERNE_STANDARD_PARAMETRE déjà validées) — seules les entrées/sorties changent.
"""
from __future__ import annotations

import runpy
import sqlite3
import sys
from pathlib import Path

REAL_SCRIPT = (Path(__file__).resolve().parents[1] / "02_TRAVAIL" / "lot6e_gainperte_menages.py")


def _appliquer_migrations(db_path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "05_APPLICATION"))
    import app.config as cfg
    from app.db.connection import apply_migrations
    orig = cfg.DB_PATH
    cfg.DB_PATH = db_path
    try:
        apply_migrations(db_path)
    finally:
        cfg.DB_PATH = orig


def _peupler(db_path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, actif, "
            "type_logement_id, import_id) VALUES ('LOG_0001','480001','OUI','T2P','IMP1')")
        conn.execute(
            "INSERT INTO ref_types_logements (type_logement_id, type_logement, import_id) "
            "VALUES ('T2P','2 pieces','IMP1')")
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, actif, date_debut_validite, import_id) "
            "VALUES ('CSM1','T2P','50.0','OUI','2026-01-01','IMP1')")
        conn.execute(
            "INSERT INTO ref_couts_menage_interne (cout_menage_interne_id, type_logement_id, "
            "montant_interne_standard, actif, date_debut_validite, import_id) "
            "VALUES ('CIM1','T2P','40.0','OUI','2026-01-01','IMP1')")
        conn.execute(
            "INSERT INTO hostaway_cleaning_tasks_extractions (extraction_id, mode, date_debut, "
            "statut) VALUES ('HCT-1','FIXTURE','2026-08-18T00:00:00Z','SUCCES')")
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, statut_controle) VALUES ('T1','2026-07','LOG_0001','completed',"
            "'réalisé','OK')")
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, statut) VALUES "
            "('FAC-1','FRS-EXT-0001','R1','2026-07-15',55.0,'A_CONTROLER')")
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, montant_ttc, source) VALUES "
            "('FLM-1','FAC-1','MENAGE_EXTERNE','LOG_0001',55.0,'PDF_EXTRACTION')")
        conn.execute(
            "INSERT INTO facture_lignes_menage_detail (ligne_id_opaque, quantite) "
            "VALUES ('FLM-1', 1)")
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages) VALUES ('2026-07', 'LOG_0001', 'INT_0001', 3)")
        conn.commit()
    finally:
        conn.close()


def _run(db_path, *extra):
    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel", "--mois", "2026-07",
                "--db", str(db_path), *extra]
    try:
        runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    except SystemExit as exc:
        assert exc.code in (0, None)
    finally:
        sys.argv = argv_pytest


def test_externe_facture_ecart_calcule_correctement(tmp_path):
    """Logement T2P, standard 50€, facturé réel 55€ -> écart -5 = PERTE."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    _run(db)

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT cout_standard_total, cout_reel_total, ecart_total, statut_ecart "
            "FROM menages_gainperte WHERE intervenant_id='FRS-EXT-0001'").fetchone()
    finally:
        conn.close()
    assert row == (50.0, 55.0, -5.0, "PERTE")


def test_interne_standard_parametre_apres_pivot(tmp_path):
    """Mois >= 2026-06 : méthode paramétrée (nb x tarif), pas heures x taux."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    _run(db)

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT methode_cout_reel, cout_reel_total, nb_menages FROM menages_gainperte "
            "WHERE intervenant_id='INT_0001'").fetchone()
    finally:
        conn.close()
    assert row == ("INTERNE_STANDARD_PARAMETRE", 120.0, 3)  # 3 x 40€


def test_rejeu_meme_mois_pas_de_doublon(tmp_path):
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    _run(db)
    _run(db)

    conn = sqlite3.connect(db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM menages_gainperte WHERE mois='2026-07'").fetchone()[0]
    finally:
        conn.close()
    assert n == 2


def test_sans_base_designee_refuse_proprement(tmp_path, monkeypatch):
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    import pytest
    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel"]
    try:
        with pytest.raises(SystemExit):
            runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    finally:
        sys.argv = argv_pytest

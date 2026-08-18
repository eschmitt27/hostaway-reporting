"""Lot6f — coût complet ménage (quote-parts), en SQLite (menages_cout_complet, 0038).

Ne réécrit ni la formule (cout_complet = cout_direct + Σ quote_parts) ni la mécanique de
ventilation (poids = nb_menages × cout_standard_unitaire, cave interne uniquement). Seules les
entrées (référentiels, externe, interne+lavage) changent de source.
"""
from __future__ import annotations

import runpy
import sqlite3
import sys
from pathlib import Path

REAL_SCRIPT = (Path(__file__).resolve().parents[1] / "02_TRAVAIL" / "lot6f_cout_complet_menages.py")


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


def _peupler(db_path, *, avec_interne=True):
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
            "INSERT INTO ref_charges_recurrentes (charge_recurrente_id, montant_ttc, actif, "
            "date_debut_validite, import_id) VALUES ('REC_002','100.0','OUI','2026-01-01','IMP1')")
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
        if avec_interne:
            conn.execute(
                "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
                "nb_menages, nb_heures, cout_lavage_attribue) VALUES "
                "('2026-07', 'LOG_0001', 'INT_0001', 2, 0, 0)")
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


def test_externe_cave_jamais_ventilee(tmp_path):
    """La cave/local REC_002 ne concerne jamais l'externe (règle figée du script)."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db, avec_interne=False)

    _run(db)

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT cout_standard_total, cout_direct_total, quote_part_local, "
            "cout_complet_total, ecart_vs_standard_total, statut_ecart FROM menages_cout_complet "
            "WHERE intervenant_id='FRS-EXT-0001'").fetchone()
    finally:
        conn.close()
    assert row == (50.0, 55.0, 0.0, 55.0, -5.0, "PERTE")


def test_interne_recoit_quote_part_cave(tmp_path):
    """Un ménage interne reçoit une part de la cave (REC_002), pondérée par son poids."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db, avec_interne=True)

    _run(db)

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT quote_part_local, cout_direct_total, cout_complet_total FROM "
            "menages_cout_complet WHERE intervenant_id='INT_0001'").fetchone()
    finally:
        conn.close()
    # Seul poste interne présent : reçoit 100% de la cave (100€).
    assert row[0] == 100.0
    assert row[2] == round((row[1] or 0) + 100.0, 2)


def test_rejeu_meme_mois_pas_de_doublon(tmp_path):
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    _run(db)
    _run(db)

    conn = sqlite3.connect(db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM menages_cout_complet WHERE mois='2026-07'").fetchone()[0]
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

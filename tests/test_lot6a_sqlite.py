"""Lot6a — CleaningTasks comptage sur SQLite (RAW 0035 -> menages_taches_enrichies, 0038).

Couvre le bug trouvé et corrige en meme temps que la migration SQLite : `load_ref_logements()`
lisait un `proprietaire_id` que REF_Logements n'a jamais eu (verifie sur le classeur reel) — le
proprietaire se resout desormais par periode via REF_Gestion_Logements_Hist, comme partout ailleurs
dans le moteur (lot4bis).
"""
from __future__ import annotations

import runpy
import sqlite3
import sys
from pathlib import Path

import pytest

REAL_SCRIPT = (Path(__file__).resolve().parents[1] / "02_TRAVAIL" /
               "lot6a_cleaning_tasks_comptage.py")


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


def _peupler(db_path, *, taches=(("T1", "480001", "2026-07-05"),
                                  ("T2", "480001", "2026-07-06"))):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, actif, import_id) "
            "VALUES ('LOG_0001','480001','OUI','IMP1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, import_id) VALUES ('GEST1','LOG_0001','PROP_0001',"
            "'2026-01-01','','IMP1')")
        conn.execute(
            "INSERT INTO ref_types_lignes_menage (type_ligne_menage_id, compte_comme_menage, "
            "import_id) VALUES ('TLM_001','OUI','IMP1')")
        conn.execute(
            "INSERT INTO hostaway_cleaning_tasks_extractions (extraction_id, mode, date_debut, "
            "statut) VALUES ('HCT-TEST','FIXTURE','2026-08-18T00:00:00Z','SUCCES')")
        for tid, listing, date_s in taches:
            conn.execute(
                "INSERT INTO hostaway_cleaning_tasks (extraction_id, task_id, reservation_id, "
                "listing_map_id, title, status, can_start_from, assignee_user_id) "
                "VALUES ('HCT-TEST', ?, ?, ?, 'Menage', 'completed', ?, '1001')",
                (tid, f"R-{tid}", listing, date_s))
        conn.commit()
    finally:
        conn.close()


def _run(db_path, *extra_args):
    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel", "--db", str(db_path),
                *extra_args]
    try:
        runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    finally:
        sys.argv = argv_pytest


def test_comptage_sqlite_resout_logement_et_proprietaire_par_periode(tmp_path):
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)
    _run(db)

    conn = sqlite3.connect(db)
    try:
        rows = conn.execute(
            "SELECT task_id, mois, logement_id, proprietaire_id, statut_controle "
            "FROM menages_taches_enrichies ORDER BY task_id").fetchall()
    finally:
        conn.close()
    assert rows == [("T1", "2026-07", "LOG_0001", "PROP_0001", "OK"),
                    ("T2", "2026-07", "LOG_0001", "PROP_0001", "OK")]


def test_gestion_hors_periode_marque_a_controler_sans_inventer_proprietaire(tmp_path):
    """Tâche datée avant toute période de gestion connue : proprietaire non résolu, A_CONTROLER —
    jamais un propriétaire deviné."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db, taches=(("T1", "480001", "2025-01-01"),))

    _run(db)

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT logement_id, proprietaire_id, statut_controle, code_anomalie "
            "FROM menages_taches_enrichies WHERE task_id='T1'").fetchone()
    finally:
        conn.close()
    assert row[0] == "LOG_0001"
    assert row[1] is None
    assert row[2] == "A_CONTROLER"
    assert "GESTION" in row[3]


def test_rejeu_sans_doublon(tmp_path):
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)
    _run(db)
    _run(db)

    conn = sqlite3.connect(db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM menages_taches_enrichies").fetchone()[0]
    finally:
        conn.close()
    assert n == 2


def test_sans_base_designee_refuse_proprement(tmp_path, monkeypatch):
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel"]
    try:
        with pytest.raises(SystemExit):
            runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    finally:
        sys.argv = argv_pytest

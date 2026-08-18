"""Lot6b — compatibilité schéma `menages_declarations_internes` (0038).

Le script `lot6b_m04_menages_internes.py` s'exécute intégralement à l'import (fetch réseau d'une
Google Sheet, écriture du classeur M04) : le tester bout-en-bout demanderait de simuler le réseau et
un classeur M04 préexistant, hors de portée d'une vérification ciblée. Ce test vérifie à la place que
les colonnes NCOLS (moins ROW_HASH, plus row_hash/run_id — exactement ce que le bloc SQLite ajouté au
script insère) sont bien acceptées par la table réellement migrée, colonne par colonne.
"""
from __future__ import annotations

import ast
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "02_TRAVAIL" / "lot6b_m04_menages_internes.py"


def _appliquer_migrations(db_path):
    sys.path.insert(0, str(ROOT / "05_APPLICATION"))
    import app.config as cfg
    from app.db.connection import apply_migrations
    orig = cfg.DB_PATH
    cfg.DB_PATH = db_path
    try:
        apply_migrations(db_path)
    finally:
        cfg.DB_PATH = orig


def _ncols():
    texte = SCRIPT.read_text(encoding="utf-8")
    m = re.search(r"NCOLS\s*=\s*(\[.*?\])", texte, re.S)
    assert m, "NCOLS introuvable dans lot6b"
    return ast.literal_eval(m.group(1))


def test_ncols_moins_row_hash_accepte_par_la_table(tmp_path):
    db = tmp_path / "app.db"
    _appliquer_migrations(db)

    sql_cols = [c for c in _ncols() if c != "ROW_HASH"] + ["row_hash", "run_id"]

    conn = sqlite3.connect(db)
    try:
        placeholders = ", ".join(["?"] * len(sql_cols))
        conn.execute(
            f"INSERT INTO menages_declarations_internes ({', '.join(sql_cols)}) "
            f"VALUES ({placeholders})", [None] * len(sql_cols))
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM menages_declarations_internes").fetchone()[0]
    finally:
        conn.close()
    assert n == 1


def test_bloc_sqlite_present_et_conditionne_par_chemin_db():
    texte = SCRIPT.read_text(encoding="utf-8")
    assert "dbm.chemin_db(None)" in texte
    assert "menages_declarations_internes" in texte
    assert "DELETE FROM menages_declarations_internes" in texte

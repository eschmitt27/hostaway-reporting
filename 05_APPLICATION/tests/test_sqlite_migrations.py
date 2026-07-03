"""APP-0 — Migrations SQLite idempotentes."""
from pathlib import Path
from app.db.connection import apply_migrations, get_db

EXPECTED_TABLES = {
    "schema_migrations",
    "audit_events",
    "pipeline_runs",
    "snapshots",
    "screen_states",
    "drafts",
    "periods",
    "saisie_hh_writes",
}


def _get_tables(db_path: Path) -> set[str]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def test_migration_creates_all_tables(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    tables = _get_tables(db)
    assert EXPECTED_TABLES == tables, f"Tables manquantes : {EXPECTED_TABLES - tables}"


def test_migration_is_idempotent(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    apply_migrations(db)  # 2ème fois — ne doit pas planter
    tables = _get_tables(db)
    assert EXPECTED_TABLES == tables


def test_schema_migrations_has_version(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        row = conn.execute("SELECT version FROM schema_migrations WHERE version='0001'").fetchone()
        assert row is not None
    finally:
        conn.close()


def test_wal_mode(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
    finally:
        conn.close()


def test_periods_mirror_not_authoritative(tmp_path):
    """La table periods est un miroir applicatif — elle accepte des valeurs de statut libres."""
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO periods (year_month, statut_mirror) VALUES ('2024-01', 'OUVERT')"
        )
        conn.commit()
        row = conn.execute("SELECT statut_mirror FROM periods WHERE year_month='2024-01'").fetchone()
        assert row[0] == "OUVERT"
    finally:
        conn.close()

import sqlite3
from pathlib import Path
from app.config import DB_PATH, DATA_DIR

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def apply_migrations(db_path: Path = DB_PATH) -> None:
    conn = get_db(db_path)
    try:
        for migration_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            sql = migration_file.read_text(encoding="utf-8")
            conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()

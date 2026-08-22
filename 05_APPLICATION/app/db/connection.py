import sqlite3
from pathlib import Path

import app.config as cfg

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_db(db_path: Path | None = None) -> sqlite3.Connection:
    """Ouvre la base applicative.

    `db_path` non fourni → `cfg.DB_PATH` est lu **à chaud**, jamais figé comme défaut d'argument.
    Un `db_path=DB_PATH` en signature capturerait la valeur à l'import : la vraie base serait alors
    visée même quand un test monkeypatche `cfg.DB_PATH` (isolation cassée). D'où `None` + résolution
    au moment de l'appel.
    """
    resolved = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(resolved))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def apply_migrations(db_path: Path | None = None) -> None:
    """Applique les migrations. `db_path` non fourni → `cfg.DB_PATH` lu à chaud (cf. get_db).

    Court-circuit : si `schema_migrations` porte déjà la version du dernier fichier, on ne relit ni
    ne réexécute aucun des ~50 fichiers de migration. Sans cela, chaque démarrage de l'application
    (et chaque test utilisant le fixture `client`, dont le lifespan rappelle `apply_migrations` après
    que `tmp_db` l'a déjà fait) rejoue l'intégralité des migrations pour un résultat inchangé — un
    coût mesuré comme dominant la lenteur de la suite de tests HH sur ce poste.
    """
    fichiers = sorted(MIGRATIONS_DIR.glob("*.sql"))
    derniere_version = fichiers[-1].stem.split("_", 1)[0] if fichiers else None

    conn = get_db(db_path)
    try:
        if derniere_version is not None:
            try:
                row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
            except sqlite3.OperationalError:
                row = None
            if row and row[0] is not None and str(row[0]) >= derniere_version:
                return
        for migration_file in fichiers:
            sql = migration_file.read_text(encoding="utf-8")
            conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()

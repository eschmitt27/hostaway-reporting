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


def _versions_appliquees(conn) -> set[str]:
    """Versions déjà enregistrées dans `schema_migrations`. Ensemble vide si la table n'existe pas
    encore (base neuve) — c'est alors l'intégralité des migrations qui doit être jouée."""
    try:
        return {str(r[0]) for r in conn.execute("SELECT version FROM schema_migrations")}
    except sqlite3.OperationalError:
        return set()


def apply_migrations(db_path: Path | None = None) -> None:
    """Applique les migrations MANQUANTES, une par une. `db_path` non fourni → `cfg.DB_PATH` lu à
    chaud (cf. get_db).

    CHAQUE FICHIER EST SUIVI INDIVIDUELLEMENT. Une version antérieure fondée sur le seul
    `MAX(version)` n'avait que deux comportements : tout court-circuiter, ou **tout rejouer depuis
    0001**. Cette seconde branche est devenue destructrice dès qu'une migration de RECONSTRUCTION
    est entrée dans l'historique : `0071` recrée `factures_proprietaires_lignes` avec la contrainte
    `CHECK` de l'époque, et `0072` l'élargit ensuite à `EXTRA`/`REDUCTION`. Rejouer 0071 sur une
    base contenant déjà une ligne `EXTRA` échoue — autrement dit, ajouter un simple fichier 0074
    empêchait l'application de démarrer. Le défaut ne se voyait pas tant qu'aucune donnée ne violait
    une contrainte ancienne ; il attendait la première.

    On ne joue donc que ce qui manque, et on enregistre chaque version appliquée, y compris quand
    le fichier lui-même a oublié son `INSERT INTO schema_migrations` (les plus anciens ne le font
    pas). Les fichiers restent idempotents par construction (`CREATE TABLE IF NOT EXISTS`…) : ce
    suivi est une seconde garantie, pas un remplacement.
    """
    fichiers = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not fichiers:
        return

    conn = get_db(db_path)
    try:
        deja = _versions_appliquees(conn)
        # Base neuve (aucune version connue) : tout est à jouer, dans l'ordre.
        a_jouer = [f for f in fichiers if f.stem.split("_", 1)[0] not in deja] if deja else fichiers
        if not a_jouer:
            return
        for migration_file in a_jouer:
            version = migration_file.stem.split("_", 1)[0]
            conn.executescript(migration_file.read_text(encoding="utf-8"))
            # Filet : certains fichiers anciens n'enregistrent pas leur propre version. Sans cette
            # ligne ils seraient rejoués à chaque démarrage — exactement le défaut corrigé ici.
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY)")
            conn.execute("INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)", (version,))
        conn.commit()
    finally:
        conn.close()

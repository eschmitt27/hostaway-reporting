"""APP_DATA_DIR — isolation réelle A/B (mission §29-30).

`app.config.DATA_DIR`/`DB_PATH` sont calculés au chargement du module depuis la variable
d'environnement `APP_DATA_DIR` — un test in-process ne le vérifie pas vraiment (le module est
déjà importé). Preuve par 2 sous-process Python distincts, chacun avec son propre
`APP_DATA_DIR`, chacun migrant sa base : les deux `app.db` doivent exister indépendamment, avec
des chemins différents, et écrire dans l'un ne doit rien changer dans l'autre.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]

_SCRIPT = (
    "import sys; sys.path.insert(0, {app_dir!r}); "
    "from app.db.connection import get_db, apply_migrations; "
    "import app.config as cfg; "
    "apply_migrations(); "
    "conn = get_db(); "
    "conn.execute(\"CREATE TABLE IF NOT EXISTS sentinelle_ab (v TEXT)\"); "
    "conn.execute(\"INSERT INTO sentinelle_ab VALUES (?)\", ({marqueur!r},)); "
    "conn.commit(); conn.close(); "
    "print(str(cfg.DB_PATH))"
)


def _lancer(app_data_dir: Path, marqueur: str) -> str:
    env = dict(os.environ)
    env["APP_DATA_DIR"] = str(app_data_dir)
    script = _SCRIPT.format(app_dir=str(APP_DIR), marqueur=marqueur)
    result = subprocess.run([sys.executable, "-c", script], cwd=str(APP_DIR), env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    return result.stdout.strip().splitlines()[-1]


def test_app_data_dir_isolation_reelle(tmp_path):
    dir_a = tmp_path / "instance_A"
    dir_b = tmp_path / "instance_B"

    db_path_a = _lancer(dir_a, "MARQUEUR_A")
    db_path_b = _lancer(dir_b, "MARQUEUR_B")

    assert db_path_a != db_path_b
    assert Path(db_path_a).exists()
    assert Path(db_path_b).exists()
    assert Path(db_path_a).parent == dir_a
    assert Path(db_path_b).parent == dir_b

    import sqlite3
    conn_a = sqlite3.connect(db_path_a)
    conn_b = sqlite3.connect(db_path_b)
    try:
        rows_a = conn_a.execute("SELECT v FROM sentinelle_ab").fetchall()
        rows_b = conn_b.execute("SELECT v FROM sentinelle_ab").fetchall()
        assert rows_a == [("MARQUEUR_A",)]
        assert rows_b == [("MARQUEUR_B",)]
    finally:
        conn_a.close()
        conn_b.close()

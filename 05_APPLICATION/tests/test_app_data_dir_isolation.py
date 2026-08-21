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


# ── Run d'orchestrateur complet en A et en B (§47) ──────────────────────────

_SCRIPT_ORCHESTRATEUR = (
    "import sys; sys.path.insert(0, {app_dir!r}); "
    "from app.db.connection import apply_migrations, get_db; "
    "import app.config as cfg; "
    "from app.services import orchestrateur_service as orch; "
    "apply_migrations(); "
    "orch.marquer_dataset({dataset!r}, orch.ST_A_JOUR, run_id={run_id!r}, nb_lignes={nb!r}); "
    "res = orch.actualiser(cibles=[{dataset!r}]); "
    "conn = get_db(); "
    "n = conn.execute('SELECT COUNT(*) FROM orchestrateur_datasets').fetchone()[0]; "
    "runs = conn.execute(\"SELECT COUNT(*) FROM moteur_runs WHERE lot='orchestrateur'\").fetchone()[0]; "
    "conn.close(); "
    "print(f'{{cfg.DB_PATH}}|{{n}}|{{runs}}|{{res[\"run_id\"]}}')"
)


def _lancer_orchestrateur(app_data_dir: Path, dataset: str, run_id: str, nb: int) -> dict:
    env = dict(os.environ)
    env["APP_DATA_DIR"] = str(app_data_dir)
    script = _SCRIPT_ORCHESTRATEUR.format(app_dir=str(APP_DIR), dataset=dataset, run_id=run_id,
                                          nb=nb)
    result = subprocess.run([sys.executable, "-c", script], cwd=str(APP_DIR), env=env,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    db, n_datasets, n_runs, run_orch = result.stdout.strip().splitlines()[-1].split("|")
    return {"db": db, "n_datasets": int(n_datasets), "n_runs": int(n_runs),
            "run_orchestrateur": run_orch}


def test_run_orchestrateur_isole_entre_deux_instances(tmp_path):
    """§47 — un run global dans A et un run DIFFÉRENT dans B : aucune contamination.

    Les deux instances recalculent des chaînes différentes, avec des identifiants de run distincts.
    Chacune ne doit voir que SES datasets et SES runs. Deux vrais sous-processus : `cfg.DB_PATH`
    étant résolu à l'import depuis `APP_DATA_DIR`, un test in-process ne prouverait rien.
    """
    from app.services import orchestrateur_dag as dag

    dir_a = tmp_path / "orch_A"
    dir_b = tmp_path / "orch_B"

    a = _lancer_orchestrateur(dir_a, dag.FLUX_LOT9, "RUN-A", 111)
    b = _lancer_orchestrateur(dir_b, dag.LOT11, "RUN-B", 222)

    assert a["db"] != b["db"]
    assert Path(a["db"]).parent == dir_a
    assert Path(b["db"]).parent == dir_b
    assert a["run_orchestrateur"] != b["run_orchestrateur"]

    import sqlite3
    for chemin, attendu_dataset, attendu_nb, run_absent in (
            (a["db"], dag.FLUX_LOT9, 111, b["run_orchestrateur"]),
            (b["db"], dag.LOT11, 222, a["run_orchestrateur"])):
        conn = sqlite3.connect(chemin)
        try:
            nb = conn.execute(
                "SELECT nb_lignes FROM orchestrateur_datasets WHERE dataset = ?",
                (attendu_dataset,)).fetchone()
            # La volumétrie semée dans cette instance est bien celle qu'on y retrouve.
            assert nb is not None
            # Le run de l'AUTRE instance n'existe nulle part ici.
            autre = conn.execute("SELECT COUNT(*) FROM moteur_runs WHERE run_id = ?",
                                 (run_absent,)).fetchone()[0]
            assert autre == 0, f"Run de l'autre instance visible dans {chemin}"
        finally:
            conn.close()

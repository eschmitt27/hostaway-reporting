import pytest
from pathlib import Path
import sys

# Ajouter la racine de l'app au PYTHONPATH pour les tests
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


def pytest_configure(config):
    """Refuse --basetemp sous APP_ROOT : déclencherait les gardes chemin APP-2c/APP-2e."""
    import garde_sources_reelles

    garde_sources_reelles.initialiser()

    basetemp_str = getattr(config.option, "basetemp", None)
    if not basetemp_str:
        return
    basetemp = Path(str(basetemp_str)).resolve()
    app_root = APP_ROOT.resolve()
    if basetemp == app_root or app_root in basetemp.parents:
        pytest.exit(
            f"\n[ERREUR CONFIGURATION TESTS]\n"
            f"--basetemp={basetemp_str!r} place les temporaires sous APP_ROOT ({app_root}).\n"
            f"Cela déclenche les gardes de chemin APP-2c/APP-2e.\n"
            f"Utiliser un chemin externe, ex : --basetemp=\"$env:TEMP\\pytest_app\"\n",
            returncode=3,
        )


@pytest.fixture(autouse=True)
def _garde_sources_reelles(monkeypatch):
    """Aucun test n'écrit dans l'arbre réel du projet (mission « zéro Excel », §21-24).

    Autouse à dessein : une protection optionnelle serait exactement absente là où on en a besoin.
    Un incident réel — neuf classeurs Hostaway réécrits par un test de fumée — l'a démontré. Voir
    `garde_sources_reelles` pour le périmètre exact : écriture interdite, lecture permise.
    """
    import garde_sources_reelles

    garde_sources_reelles.armer(monkeypatch)


@pytest.fixture
def tmp_db(tmp_path):
    """Base SQLite temporaire isolée — zéro impact sur les données réelles."""
    from app.db.connection import apply_migrations, get_db
    db_path = tmp_path / "test_app.db"
    # Patch DATA_DIR pour l'isolation
    import app.config as cfg
    orig_db = cfg.DB_PATH
    orig_backups = cfg.BACKUPS_DIR
    cfg.DB_PATH = db_path
    # `orchestrateur_service.actualiser(cibles=None)` sauvegarde app.db via `backup_service`
    # (mission industrialisation orchestrateur) — sans cette isolation, un test appelant une
    # actualisation globale écrirait une vraie copie sous le `BACKUPS_DIR` réel du projet.
    cfg.BACKUPS_DIR = tmp_path / "backups"
    apply_migrations(db_path)
    yield db_path
    cfg.DB_PATH = orig_db
    cfg.BACKUPS_DIR = orig_backups


@pytest.fixture
def amonts_calcul_ok(tmp_db):
    """Marque RESERVATIONS/MENAGES comme `ST_A_JOUR` sur `tmp_db` (mission 14b —
    `_amonts_en_echec` bloque désormais tout `CALCUL_SQLITE` jamais produit, cf.
    `orchestrateur_service.py`). À demander dans les tests qui exercent le comportement de
    l'orchestrateur EN AVAL de `FLUX_LOT9` (rollback, verrous, reprise, propagation d'erreur) sans
    vouloir tester la fraîcheur amont elle-même — sinon ils seraient tous bloqués par un amont
    jamais calculé, ce qui n'est pas leur sujet."""
    from app.services import orchestrateur_dag as dag
    from app.services import orchestrateur_service as orch
    for dataset in (dag.RESERVATIONS, dag.MENAGES):
        orch.marquer_dataset(dataset, orch.ST_A_JOUR, run_id="SEED-TEST", db_path=tmp_db)
    return tmp_db


@pytest.fixture
def tmp_snapshot_dir(tmp_path):
    """Répertoire snapshots temporaire."""
    d = tmp_path / "snapshots"
    d.mkdir()
    import app.config as cfg
    orig = cfg.SNAPSHOTS_DIR
    cfg.SNAPSHOTS_DIR = d
    yield d
    cfg.SNAPSHOTS_DIR = orig


@pytest.fixture
def client(tmp_db):
    """Client HTTP de test FastAPI."""
    from fastapi.testclient import TestClient
    from app.main import app
    with TestClient(app) as c:
        yield c

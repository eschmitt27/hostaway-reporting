import pytest
from pathlib import Path
import sys

# Ajouter la racine de l'app au PYTHONPATH pour les tests
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


@pytest.fixture
def tmp_db(tmp_path):
    """Base SQLite temporaire isolée — zéro impact sur les données réelles."""
    from app.db.connection import apply_migrations, get_db
    db_path = tmp_path / "test_app.db"
    # Patch DATA_DIR pour l'isolation
    import app.config as cfg
    orig_db = cfg.DB_PATH
    cfg.DB_PATH = db_path
    apply_migrations(db_path)
    yield db_path
    cfg.DB_PATH = orig_db


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

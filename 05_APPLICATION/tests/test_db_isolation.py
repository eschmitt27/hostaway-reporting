"""APP-2b / correctif transversal — isolation de la vraie app.db.

Prouve que le défaut « argument par défaut figé à l'import » est corrigé :
monkeypatcher `cfg.DB_PATH` redirige réellement `get_db()` ET `apply_migrations()`,
et démarrer un TestClient ne touche jamais la vraie base.
"""
import hashlib
import sqlite3
from pathlib import Path

import pytest

import app.config as cfg

REAL_DB = cfg.APP_ROOT / "data" / "app.db"


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _tables_ro(p: Path) -> set[str]:
    """Lit les tables en lecture seule stricte (URI mode=ro) — n'écrit jamais dans le fichier."""
    conn = sqlite3.connect(f"file:{p.as_posix()}?mode=ro", uri=True)
    try:
        return {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    finally:
        conn.close()


def test_apply_migrations_suit_le_monkeypatch(tmp_path, monkeypatch):
    """apply_migrations() sans argument doit viser cfg.DB_PATH monkeypatché, pas la vraie base."""
    from app.db.connection import apply_migrations
    cible = tmp_path / "iso_migr.db"
    monkeypatch.setattr(cfg, "DB_PATH", cible)
    apply_migrations()  # aucun argument
    assert cible.exists()
    assert "menages_recalcul_runs" in _tables_ro(cible)


def test_get_db_sans_argument_suit_le_monkeypatch(tmp_path, monkeypatch):
    from app.db.connection import get_db
    cible = tmp_path / "iso_getdb.db"
    monkeypatch.setattr(cfg, "DB_PATH", cible)
    conn = get_db()  # aucun argument
    try:
        conn.execute("SELECT 1").fetchone()
    finally:
        conn.close()
    assert cible.exists()


@pytest.mark.skipif(not REAL_DB.exists(), reason="app.db réelle absente")
def test_testclient_ne_touche_jamais_la_vraie_db(tmp_path, monkeypatch):
    """Créer un TestClient (lifespan → apply_migrations) ne doit toucher NI le contenu, NI le mtime,
    NI le schéma de la vraie app.db, dès lors que cfg.DB_PATH est monkeypatché."""
    sha_avant = _sha(REAL_DB)
    mtime_avant = REAL_DB.stat().st_mtime_ns
    tables_avant = _tables_ro(REAL_DB)

    monkeypatch.setattr(cfg, "DB_PATH", tmp_path / "client_iso.db")
    from fastapi.testclient import TestClient
    from app.main import app
    with TestClient(app) as c:            # déclenche le lifespan (apply_migrations)
        c.get("/health")
        c.get("/menages")
        c.get("/menages/recalculer")

    assert _sha(REAL_DB) == sha_avant, "Contenu de la vraie app.db modifié"
    assert REAL_DB.stat().st_mtime_ns == mtime_avant, "mtime de la vraie app.db modifié"
    assert _tables_ro(REAL_DB) == tables_avant, "Schéma de la vraie app.db modifié"
    # la base de test a bien reçu les migrations
    assert (tmp_path / "client_iso.db").exists()

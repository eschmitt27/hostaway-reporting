"""Tests — migration protégée (sauvegarde + rollback automatique en cas d'échec).

Mission industrialisation socle technique. Aucune écriture réelle : tout sur `tmp_path`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import backup_service, migration_service, run_history_service as history


@pytest.fixture(autouse=True)
def _isoler_backups(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups")


def test_migration_sur_base_deja_a_jour_reussit(tmp_path):
    db = tmp_path / "app.db"
    apply_migrations(db)  # déjà à la dernière version
    res = migration_service.migrer_avec_sauvegarde(db, acteur="test")
    assert res["ok"] is True
    assert res["sauvegarde_id"]
    derniers = history.derniers(db_path=db)
    succes = [r for r in derniers if r["run_id_opaque"] == res["run_id"]][0]
    assert succes["statut"] == "SUCCESS"


def test_migration_echouee_restaure_automatiquement(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)

    def _casse(*a, **k):
        raise RuntimeError("panne simulée")

    monkeypatch.setattr(migration_service, "apply_migrations", _casse)
    res = migration_service.migrer_avec_sauvegarde(db, acteur="test")

    assert res["ok"] is False
    assert res["code"] == "E_MIGRATION_ECHOUEE"
    assert res["rollback"]["ok"] is True

    derniers = history.derniers(db_path=db)
    rollbacks = [r for r in derniers if r["statut"] == "ROLLED_BACK"]
    assert len(rollbacks) == 1
    assert rollbacks[0]["sauvegarde_id_opaque"] == res["sauvegarde_id"]

    # La base cible reste lisible et cohérente après le rollback.
    conn = get_db(db)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()

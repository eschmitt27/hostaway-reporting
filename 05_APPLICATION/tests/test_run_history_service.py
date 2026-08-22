"""Tests — historique centralisé des runs (mission industrialisation socle technique)."""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations
from app.services import run_history_service as history


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "app.db"
    apply_migrations(p)
    return p


def test_demarrer_puis_succes(db):
    run_id = history.demarrer("IMPORT_TEST", acteur="recette", db_path=db)
    assert run_id.startswith("RUNH-")
    r = history.marquer_succes(run_id, db_path=db)
    assert r["ok"] and r["statut"] == "SUCCESS"
    derniers = history.derniers(db_path=db)
    assert derniers[0]["run_id_opaque"] == run_id
    assert derniers[0]["statut"] == "SUCCESS"
    assert derniers[0]["duree_s"] is not None


def test_transitions_completes(db):
    run_id = history.demarrer("IMPORT_TEST", db_path=db)
    assert history.marquer_validating(run_id, db_path=db)["statut"] == "VALIDATING"
    assert history.marquer_echec(run_id, erreur="boom", db_path=db)["statut"] == "FAILED"
    derniers = history.derniers(db_path=db)
    assert derniers[0]["erreur"] == "boom"


def test_rollback_trace(db):
    run_id = history.demarrer("MIGRATION", db_path=db)
    r = history.marquer_rollback(run_id, erreur="integrity_check", db_path=db)
    assert r["statut"] == "ROLLED_BACK"


def test_statut_inconnu_refuse(db):
    run_id = history.demarrer("X", db_path=db)
    with pytest.raises(ValueError, match="statut inconnu"):
        history._transition(run_id, "N_IMPORTE_QUOI", db_path=db)


def test_run_introuvable_signale(db):
    r = history.marquer_succes("RUNH-INEXISTANT", db_path=db)
    assert r["ok"] is False
    assert r["code"] == "E_RUN_INTROUVABLE"


def test_derniers_respecte_la_limite(db):
    for i in range(5):
        history.demarrer(f"OP_{i}", db_path=db)
    assert len(history.derniers(limit=3, db_path=db)) == 3

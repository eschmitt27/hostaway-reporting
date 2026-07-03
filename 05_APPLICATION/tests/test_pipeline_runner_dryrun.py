"""APP-0 — Pipeline runner : dry-run forcé, garde contre exécution réelle."""
import pytest
from pathlib import Path
import app.config as cfg
from app.db.connection import apply_migrations
from app.adapters import pipeline_runner


def setup_db(tmp_path):
    db = tmp_path / "app.db"
    cfg.DB_PATH = db
    cfg.SNAPSHOTS_DIR = tmp_path / "snapshots"
    cfg.SNAPSHOTS_DIR.mkdir()
    apply_migrations(db)
    return db


def test_dry_run_returns_dry_run_status(tmp_path):
    db = setup_db(tmp_path)
    result = pipeline_runner.run_pipeline("lot1_hostaway", dry_run=True, db_path=db)
    assert result["dry_run"] is True
    assert result["status"] == "DRY_RUN"
    assert "DRY-RUN" in result["output"]


def test_dry_run_does_not_call_subprocess(tmp_path, monkeypatch):
    db = setup_db(tmp_path)
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: calls.append(a) or None)
    pipeline_runner.run_pipeline("lot1_hostaway", dry_run=True, db_path=db)
    assert calls == [], "subprocess.run ne doit pas être appelé en dry-run"


def test_real_execution_blocked_at_lot0(tmp_path):
    db = setup_db(tmp_path)
    # _REAL_EXECUTION_ENABLED est False au Lot APP-0
    assert pipeline_runner._REAL_EXECUTION_ENABLED is False
    with pytest.raises(RuntimeError, match="BLOQUÉ"):
        pipeline_runner.run_pipeline("lot1_hostaway", dry_run=False, db_path=db)


def test_dry_run_logs_to_sqlite(tmp_path):
    db = setup_db(tmp_path)
    pipeline_runner.run_pipeline("lot1_hostaway", dry_run=True, db_path=db)
    from app.db.connection import get_db
    conn = get_db(db)
    rows = conn.execute("SELECT * FROM pipeline_runs").fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0]["dry_run"] == 1
    assert rows[0]["status"] == "DRY_RUN"


def test_dry_run_logs_audit_event(tmp_path):
    db = setup_db(tmp_path)
    pipeline_runner.run_pipeline("lot3_charges", dry_run=True, db_path=db)
    from app.db.connection import get_db
    conn = get_db(db)
    rows = conn.execute("SELECT action FROM audit_events").fetchall()
    conn.close()
    actions = [r[0] for r in rows]
    assert "PIPELINE_DRY_RUN" in actions

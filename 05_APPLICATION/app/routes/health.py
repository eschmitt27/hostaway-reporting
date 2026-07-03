from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pathlib import Path
from app.config import (
    PROJECT_ROOT, MASTER_RUN_LOG, EXPORTS_POWERBI,
    REF_SETUP, DB_PATH, SNAPSHOTS_DIR,
)
from app.db.connection import get_db

router = APIRouter()


@router.get("/health")
def health_check():
    checks = {}

    checks["project_root"] = {"path": str(PROJECT_ROOT), "exists": PROJECT_ROOT.exists()}
    checks["master_run_log"] = {"path": str(MASTER_RUN_LOG), "exists": MASTER_RUN_LOG.exists()}
    checks["exports_powerbi"] = {"path": str(EXPORTS_POWERBI), "exists": EXPORTS_POWERBI.exists()}
    checks["ref_setup"] = {"path": str(REF_SETUP), "exists": REF_SETUP.exists()}
    checks["snapshots_dir"] = {"path": str(SNAPSHOTS_DIR), "exists": SNAPSHOTS_DIR.exists()}

    # SQLite
    try:
        conn = get_db(DB_PATH)
        conn.execute("SELECT 1").fetchone()
        conn.close()
        checks["sqlite"] = {"ok": True, "path": str(DB_PATH)}
    except Exception as e:
        checks["sqlite"] = {"ok": False, "error": str(e)}

    # Write guard : vérifier que l'app ne peut pas écrire dans REF_Setup
    from app.services.file_registry import is_writable
    checks["write_guard_ref_setup"] = {"writable": is_writable(REF_SETUP), "expected": False}

    all_ok = (
        checks["project_root"]["exists"]
        and checks["sqlite"]["ok"]
        and not checks["write_guard_ref_setup"]["writable"]
    )

    return JSONResponse(
        content={"status": "OK" if all_ok else "DEGRADED", "checks": checks},
        status_code=200 if all_ok else 503,
    )

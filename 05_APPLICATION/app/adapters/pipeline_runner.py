import time
from pathlib import Path
from app.adapters.pipeline_registry import get_script_path
from app.db.connection import get_db
from app.services.audit_service import log_event

# Au Lot APP-0 : dry_run toujours forcé à True.
# L'exécution réelle est débloquée à partir du Lot APP-5 avec snapshot préalable.
_REAL_EXECUTION_ENABLED = False


def run_pipeline(
    script_name: str,
    dry_run: bool = True,
    db_path: Path | None = None,
) -> dict:
    """Exécute (ou simule) un script pipeline.

    Lot APP-0 : dry_run toujours True. Toute tentative d'exécution réelle
    est bloquée par garde explicite.
    """
    if not dry_run and not _REAL_EXECUTION_ENABLED:
        raise RuntimeError(
            f"BLOQUÉ — exécution réelle du pipeline '{script_name}' interdite au Lot APP-0. "
            "L'exécution réelle est activée à partir du Lot APP-5 avec snapshot préalable et confirmation."
        )

    script_path = get_script_path(script_name)
    start = time.monotonic()

    if dry_run:
        duration_ms = int((time.monotonic() - start) * 1000)
        result = {
            "script": script_name,
            "dry_run": True,
            "status": "DRY_RUN",
            "output": f"[DRY-RUN] {script_name} — simulation uniquement. Aucun script exécuté. Chemin : {script_path}",
            "duration_ms": duration_ms,
        }
        _record_run(script_name, dry_run=True, status="DRY_RUN", output=result["output"], duration_ms=duration_ms, db_path=db_path)
        log_event("PIPELINE_DRY_RUN", {"script": script_name, "path": str(script_path)}, db_path=db_path)
        return result

    # Exécution réelle — uniquement si _REAL_EXECUTION_ENABLED (Lot APP-5+)
    import subprocess, sys
    if not script_path:
        raise FileNotFoundError(f"Script introuvable : {script_name}")

    try:
        proc = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True, text=True, timeout=600,
        )
        duration_ms = int((time.monotonic() - start) * 1000)
        status = "OK" if proc.returncode == 0 else "ERROR"
        result = {
            "script": script_name, "dry_run": False, "status": status,
            "output": proc.stdout, "error": proc.stderr, "duration_ms": duration_ms,
        }
        _record_run(script_name, False, status, proc.stdout, duration_ms, proc.stderr, db_path)
        log_event("PIPELINE_RUN", {"script": script_name, "status": status}, db_path=db_path)
        return result
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timeout — {script_name} dépasse 600 secondes.")


def _record_run(name, dry_run, status, output, duration_ms, error=None, db_path=None):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (script_name, dry_run, status, output, duration_ms, error) VALUES (?,?,?,?,?,?)",
            (name, 1 if dry_run else 0, status, output, duration_ms, error),
        )
        conn.commit()
    finally:
        conn.close()

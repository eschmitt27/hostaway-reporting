from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR, DB_PATH
from app.adapters.pipeline_registry import list_scripts
from app.adapters.pipeline_runner import run_pipeline
from app.db.connection import get_db
from app.readers.run_log_reader import get_last_runs

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/sources-calculs", response_class=HTMLResponse)
def sources_calculs(request: Request):
    scripts = list_scripts()
    runs = _get_recent_pipeline_runs()
    run_log = get_last_runs(max_rows=20)
    return templates.TemplateResponse(request, "sources_calculs.html", {
        "active_menu": "sources_calculs",
        "scripts": scripts,
        "recent_runs": runs,
        "run_log": run_log,
        "dry_run_mode": True,
    })


@router.post("/sources-calculs/run", response_class=HTMLResponse)
def run_script(request: Request, script_name: str = Form(...)):
    result = run_pipeline(script_name, dry_run=True, db_path=DB_PATH)
    runs = _get_recent_pipeline_runs()
    return templates.TemplateResponse(request, "partials/pipeline_log.html", {
        "result": result,
        "recent_runs": runs,
    })


def _get_recent_pipeline_runs(limit: int = 20) -> list[dict]:
    conn = get_db(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT id, ts, script_name, dry_run, status, duration_ms FROM pipeline_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

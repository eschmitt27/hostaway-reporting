from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.config import EXPORTS_POWERBI, MASTER_RUN_LOG, TEMPLATES_DIR
from app.readers.csv_reader import read_csv
from app.readers.run_log_reader import get_run_log_status

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _safe_count(csv_path: Path) -> str:
    rows = read_csv(csv_path, max_rows=5000)
    return str(len(rows)) if rows else "—"


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    kpis = {
        "reservations": _safe_count(EXPORTS_POWERBI / "PBI_Flux.csv"),
        "controles_ouverts": _safe_count(EXPORTS_POWERBI / "PBI_Controles_Ouverts.csv"),
        "logements": _safe_count(EXPORTS_POWERBI / "PBI_Referentiel_Logements.csv"),
        "proprietaires": _safe_count(EXPORTS_POWERBI / "PBI_Referentiel_Proprietaires.csv"),
        "run_log": get_run_log_status(),
    }
    modules = [
        {"name": "Logements", "url": "/logements", "status": "disponible"},
        {"name": "Propriétaires & règlements", "url": "/proprietaires", "status": "a_venir"},
        {"name": "Réservations", "url": "/reservations", "status": "disponible"},
        {"name": "Fournisseurs", "url": "/fournisseurs", "status": "a_venir"},
        {"name": "Banques & caisse", "url": "/banques", "status": "a_venir"},
        {"name": "Ménages", "url": "/menages", "status": "disponible"},
        {"name": "Sources & calculs", "url": "/sources-calculs", "status": "disponible"},
        {"name": "Contrôles & clôture", "url": "/controles", "status": "a_venir"},
    ]
    return templates.TemplateResponse(request, "home.html", {
        "active_menu": "accueil",
        "kpis": kpis,
        "modules": modules,
    })

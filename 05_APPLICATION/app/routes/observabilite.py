"""Écran observabilité — derniers runs et sauvegardes (mission industrialisation socle technique).

Lecture seule : liste `run_history` (vue d'ensemble centralisée, alimentée par les opérations qui
l'utilisent — cf. `run_history_service.py`) et `sauvegardes_base` (cf. `backup_service.py`).
Ne remplace aucun écran existant (Pilotage/Actualisation reste l'écran de PILOTAGE des lots).
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from app.template_env import get_templates

from app.services import backup_service, run_history_service as history

router = APIRouter()
templates = get_templates()


@router.get("/observabilite/runs", response_class=HTMLResponse)
def runs(request: Request):
    return templates.TemplateResponse(request, "observabilite_runs.html", {
        "active_menu": "observabilite",
        "runs": history.derniers(limit=50),
        "sauvegardes": backup_service.lister(),
    })

"""Écran observabilité — derniers runs et sauvegardes (mission industrialisation socle technique).

Lecture seule : liste `run_history` (vue d'ensemble centralisée, alimentée par les opérations qui
l'utilisent — cf. `run_history_service.py`) et `sauvegardes_base` (cf. `backup_service.py`).
Ne remplace aucun écran existant (Pilotage/Actualisation reste l'écran de PILOTAGE des lots).
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import backup_service, run_history_service as history

router = APIRouter()
templates = get_templates()


@router.get("/observabilite")
def observabilite(request: Request):
    """Redirige vers l'écran des runs.

    La navigation pointait vers `/observabilite`, qui n'a jamais existé : seul `/observabilite/runs`
    était défini. Le menu rendait donc un 404 — un lien mort dans la navigation principale.

    Le lien du menu est corrigé, ET cette redirection est ajoutée : une adresse déjà mise en
    favori, collée dans un message ou écrite dans une note doit continuer de fonctionner.
    `308` (permanent) plutôt que `302` : la cible est définitive, et les clients peuvent la
    mémoriser sans risque.
    """
    return RedirectResponse("/observabilite/runs", status_code=308)


@router.get("/observabilite/runs", response_class=HTMLResponse)
def runs(request: Request):
    return templates.TemplateResponse(request, "observabilite_runs.html", {
        "active_menu": "observabilite",
        "runs": history.derniers(limit=50),
        "sauvegardes": backup_service.lister(),
    })

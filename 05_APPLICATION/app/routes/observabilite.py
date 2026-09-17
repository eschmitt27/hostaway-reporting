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
    # Ce que l'écran Ménages n'affiche plus (recette utilisateur n°4, §17) : le détail fichier par
    # fichier des PDF, la provenance des données Hostaway et l'historique complet des mois
    # clôturés ayant reçu des données. Ces informations ne permettent aucune action métier
    # immédiate — elles servent à comprendre après coup, et c'est ici qu'on vient comprendre.
    from app.services import menages_actualisation_service as actualisation
    from app.services import menages_service as menages

    etat = menages.charger_etat_actualisation()
    return templates.TemplateResponse(request, "observabilite_runs.html", {
        "active_menu": "observabilite",
        "runs": history.derniers(limit=50),
        "sauvegardes": backup_service.lister(),
        "pdf_menages": {**(etat.get("pdf") or {}), **menages.load_pdf_externes_info()},
        "hostaway": etat.get("hostaway"),
        "changements_clotures": actualisation.changements_mois_clotures(statut="SIGNALE"),
    })

"""Écran Pilotage / Actualisation — lancer le pipeline sans passer par un terminal (§31).

L'utilisateur y trouve « Actualiser toute l'activité » et les actualisations ciblées principales.
Les actions POST empruntent EXACTEMENT le même service que l'ordonnanceur
(`orchestrateur_service.actualiser`) : il n'existe pas de second chemin d'exécution.

L'actualisation est lancée en TÂCHE DE FOND : un recalcul complet dure plusieurs minutes, et une
requête HTTP ne doit jamais rester bloquée dessus. L'écran affiche l'avancement en relisant l'état
des datasets et les étapes du run — jamais en devinant.
"""
from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Actions ciblées proposées à l'écran : les datasets que l'orchestrateur sait réellement recalculer.
# Construite depuis le DAG, jamais recopiée à la main — une chaîne ajoutée au DAG apparaît ici.
def _cibles_proposees() -> list[dict[str, str]]:
    return [{"dataset": nom, "libelle": dag.NOEUDS[nom].libelle}
            for nom in dag.noeuds_calculables()
            if dag.NOEUDS[nom].service]


@router.get("/actualisation", response_class=HTMLResponse)
def actualisation(request: Request):
    # Un run laissé EN_COURS par un arrêt brutal ne doit pas s'afficher comme actif.
    orch.marquer_runs_interrompus()
    etat = orch.etat_global()
    return templates.TemplateResponse(request, "actualisation.html", {
        "active_menu": "actualisation",
        "datasets": etat["datasets"],
        "dernier_run": etat["dernier_run"],
        "etapes": etat["etapes"],
        "verrous": etat["verrous"],
        "cibles": _cibles_proposees(),
        "historique": orch.historique(limite=10),
    })


@router.post("/actualisation/tout/dry-run")
def actualiser_tout_dry_run(request: Request):
    """Plan d'exécution sans rien exécuter ni activer (mission industrialisation, Phase 8).

    Synchrone (aucun service n'est réellement appelé, donc rapide) : le résultat s'affiche
    immédiatement, contrairement à une vraie actualisation qui tourne en tâche de fond.
    """
    resultat = orch.actualiser(cibles=None, declencheur=orch.DECLENCHEUR_MANUEL, dry_run=True)
    etat = orch.etat_global()
    return templates.TemplateResponse(request, "actualisation.html", {
        "active_menu": "actualisation",
        "datasets": etat["datasets"],
        "dernier_run": etat["dernier_run"],
        "etapes": etat["etapes"],
        "verrous": etat["verrous"],
        "cibles": _cibles_proposees(),
        "historique": orch.historique(limite=10),
        "dry_run_resultat": resultat,
    })


@router.post("/actualisation/tout")
def actualiser_tout(background: BackgroundTasks):
    """« Actualiser toute l'activité » — tout le DAG, dans l'ordre des dépendances."""
    orch.marquer_runs_interrompus()
    background.add_task(orch.actualiser, cibles=None,
                        declencheur=orch.DECLENCHEUR_MANUEL)
    return RedirectResponse("/actualisation", status_code=303)


@router.post("/actualisation/cible")
def actualiser_cible(background: BackgroundTasks, dataset: str = Form(...)):
    """Actualisation ciblée : le dataset demandé PUIS tous ses descendants nécessaires (§29).

    Demander explicitement une source externe (Hostaway) l'autorise pour ce run : c'est la seule
    façon de la déclencher depuis l'écran, un « tout actualiser » ne la lance jamais tout seul.
    """
    if dataset not in dag.NOEUDS:
        return RedirectResponse("/actualisation?erreur=dataset_inconnu", status_code=303)
    orch.marquer_runs_interrompus()
    background.add_task(orch.actualiser, cibles=[dataset],
                        declencheur=orch.DECLENCHEUR_MANUEL,
                        inclure_imports_externes=dag.NOEUDS[dataset].externe)
    return RedirectResponse("/actualisation", status_code=303)


@router.get("/actualisation/etat")
def etat_json():
    """État courant en JSON — pour rafraîchir l'écran sans recharger toute la page."""
    return orch.etat_global()

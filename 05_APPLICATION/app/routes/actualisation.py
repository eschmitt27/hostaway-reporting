"""Écran Pilotage / Actualisation — lancer le pipeline sans passer par un terminal (§31).

L'utilisateur y trouve « Actualiser toute l'activité » et les actualisations ciblées principales.
Les actions POST empruntent EXACTEMENT le même service que l'ordonnanceur
(`orchestrateur_service.actualiser`) : il n'existe pas de second chemin d'exécution.

L'actualisation est lancée en TÂCHE DE FOND : un recalcul complet dure plusieurs minutes, et une
requête HTTP ne doit jamais rester bloquée dessus. L'écran affiche l'avancement en relisant l'état
des datasets et les étapes du run — jamais en devinant.
"""
from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, BackgroundTasks, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import actualisation_progression_service as progression_svc
from app.services import ordonnanceur_service as ordo
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

router = APIRouter()
templates = get_templates()

# Actions ciblées proposées à l'écran : les datasets que l'orchestrateur sait réellement recalculer.
# Construite depuis le DAG, jamais recopiée à la main — une chaîne ajoutée au DAG apparaît ici.
def _cibles_proposees() -> list[dict[str, str]]:
    return [{"dataset": nom, "libelle": dag.NOEUDS[nom].nom_affiche}
            for nom in dag.noeuds_calculables()
            if dag.NOEUDS[nom].service]


@router.get("/actualisation", response_class=HTMLResponse)
def actualisation(request: Request):
    # Un run laissé EN_COURS par un arrêt brutal ne doit pas s'afficher comme actif.
    orch.marquer_runs_interrompus()
    etat = orch.etat_global()
    return templates.TemplateResponse(request, "actualisation.html", {
        "active_menu": "observabilite",
        "datasets": etat["datasets"],
        "dernier_run": etat["dernier_run"],
        "etapes": etat["etapes"],
        "verrous": etat["verrous"],
        "cibles": _cibles_proposees(),
        "historique": orch.historique(limite=10),
        "ordonnanceur": ordo.etat(),
        "progression": progression_svc.progression(),
        "message": request.query_params.get("message", ""),
    })


@router.post("/actualisation/tout/dry-run")
def actualiser_tout_dry_run(request: Request):
    """Plan d'exécution sans rien exécuter ni activer (mission industrialisation, Phase 8).

    Synchrone (aucun service n'est réellement appelé, donc rapide) : le résultat s'affiche
    immédiatement, contrairement à une vraie actualisation qui tourne en tâche de fond.
    """
    resultat = orch.actualiser(cibles=None, declencheur=orch.DECLENCHEUR_MANUEL, dry_run=True,
                               inclure_imports_externes=True)
    etat = orch.etat_global()
    return templates.TemplateResponse(request, "actualisation.html", {
        "active_menu": "observabilite",
        "datasets": etat["datasets"],
        "dernier_run": etat["dernier_run"],
        "etapes": etat["etapes"],
        "verrous": etat["verrous"],
        "cibles": _cibles_proposees(),
        "historique": orch.historique(limite=10),
        "dry_run_resultat": resultat,
        "ordonnanceur": ordo.etat(),
        "progression": progression_svc.progression(),
    })


@router.post("/actualisation/tout")
def actualiser_tout(background: BackgroundTasks):
    """« Actualiser toute l'activité » — TOUTES les sources configurées, puis TOUT le DAG.

    Sources : chaque nœud `actualisation_globale` du DAG, par son service canonique (banque Qonto
    en lecture seule, réservations et tâches de ménage Hostaway depuis le dépôt publié, déclarations
    et factures PDF de ménage). Calculs : tous rejoués dans l'ordre du DAG, sans l'optimisation
    « amont inchangé » des actualisations ciblées.

    Le run est OUVERT et PLANIFIÉ ici, avant la tâche de fond : l'écran rechargé montre aussitôt
    toutes les étapes « en attente », et un double clic trouve le verrou déjà pris.
    """
    prepare = orch.preparer_actualisation_globale(declencheur=orch.DECLENCHEUR_MANUEL)
    if not prepare["ok"]:
        return RedirectResponse("/actualisation?message=" + quote(prepare["message"]),
                                status_code=303)
    background.add_task(orch.actualiser, cibles=None, declencheur=orch.DECLENCHEUR_MANUEL,
                        inclure_imports_externes=True, run_id=prepare["run_id"])
    return RedirectResponse("/actualisation", status_code=303)


@router.post("/actualisation/cible")
def actualiser_cible(background: BackgroundTasks, dataset: str = Form(...)):
    """Actualisation ciblée : le dataset demandé PUIS tous ses descendants nécessaires (§29).

    Demander explicitement une source externe (Hostaway) l'autorise pour ce run. Un « tout
    actualiser » ne lance que les sources lues dans le dépôt publié (`actualisation_globale`).
    """
    if dataset not in dag.NOEUDS:
        return RedirectResponse("/actualisation?erreur=dataset_inconnu", status_code=303)
    orch.marquer_runs_interrompus()
    background.add_task(orch.actualiser, cibles=[dataset],
                        declencheur=orch.DECLENCHEUR_MANUEL,
                        inclure_imports_externes=dag.NOEUDS[dataset].externe)
    return RedirectResponse("/actualisation", status_code=303)


@router.get("/actualisation/progression")
def progression_json(run_id: str | None = None):
    """Avancement réel du run (dernier run par défaut), relu à chaque appel — l'écran l'interroge
    chaque seconde tant qu'une actualisation tourne."""
    return progression_svc.progression(run_id=run_id or None)


@router.get("/actualisation/etat")
def etat_json():
    """État courant en JSON — pour rafraîchir l'écran sans recharger toute la page."""
    return orch.etat_global()

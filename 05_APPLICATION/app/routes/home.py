"""Tableau de bord — aucun compteur ne vient plus d'un export Power BI.

Les quatre indicateurs se lisaient dans `03_EXPORTS/PowerBI/*.csv`, c'est-à-dire dans une sortie
d'export que l'application relisait pour s'afficher. Deux d'entre eux viennent désormais du
référentiel SQLite ; les deux autres viennent des masters du moteur, via les lecteurs déjà en
place.

Cette distinction est volontaire et se lit dans le code : les compteurs de RÉFÉRENTIEL sont
définitifs, ceux de RÉSULTAT restent adossés à des masters Excel et basculeront quand le moteur
lui-même écrira en SQLite. Aucun ne dépend plus d'un export.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from app.template_env import get_templates

from app.readers import controles_cloture_reader as controles
from app.readers import flux_unifie_reader as flux
from app.readers.run_log_reader import get_run_log_status
from app.services import referentiel_service as referentiel

router = APIRouter()
templates = get_templates()

INDISPONIBLE = "—"


def _compteur_referentiel(lecture) -> str:
    """Compteur issu du référentiel SQLite. « — » si le référentiel n'est pas encore importé."""
    if not referentiel.disponible():
        return INDISPONIBLE
    try:
        return str(len(lecture()))
    except Exception:
        # Un tableau de bord ne doit jamais tomber à cause d'un compteur.
        return INDISPONIBLE


def _compteur_moteur(lecture) -> str:
    """Compteur issu d'un master du moteur — encore Excel à ce stade (voir docstring)."""
    try:
        lignes = lecture()
        return str(len(lignes)) if lignes else INDISPONIBLE
    except Exception:
        return INDISPONIBLE


def _logements_du_parc() -> list:
    """Le parc, lignes techniques exclues — même définition que l'écran Logements.

    L'ancien compteur lisait le CSV brut et affichait 19 là où l'écran annonçait « Parc : 17 » :
    les deux lignes techniques y étaient comptées comme des logements.
    """
    from app.services.logements_service import _is_technique
    return [l for l in referentiel.logements() if not _is_technique(l.get("logement_id"))]


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    kpis = {
        "reservations": _compteur_moteur(flux.lire_flux),
        "controles_ouverts": _compteur_moteur(lambda: controles.a_controler_ouverts().lignes),
        "logements": _compteur_referentiel(_logements_du_parc),
        "proprietaires": _compteur_referentiel(referentiel.proprietaires),
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

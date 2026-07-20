"""Route Pilotage mensuel (APP-5D) — tableau de bord de clôture, agrégation LECTURE SEULE.

Aucune écriture sur les données métier réelles, aucune règle recalculée : chaque compteur provient
du service existant du module concerné (voir `pilotage_mensuel_service.py`). Chaque indicateur
renvoie vers l'écran source filtré existant (pas de liste recréée ici).
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import pilotage_mensuel_export_service as export_svc
from app.services import pilotage_mensuel_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/pilotage-mensuel", response_class=HTMLResponse)
def pilotage_mensuel(request: Request, annee: str = "", mois: str = "", statut: str = ""):
    tableau = svc.tableau_mensuel(annee=annee, mois_filtre=mois, statut_filtre=statut)
    return templates.TemplateResponse(request, "pilotage_mensuel.html", {
        "active_menu": "pilotage_mensuel", "tableau": tableau,
        "applied": {"annee": annee, "mois": mois, "statut": statut},
    })


@router.get("/pilotage-mensuel/export.csv")
def pilotage_mensuel_export(annee: str = "", mois: str = "", statut: str = ""):
    tableau = svc.tableau_mensuel(annee=annee, mois_filtre=mois, statut_filtre=statut)
    contenu = export_svc.exporter(tableau)
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f'attachment; filename="{export_svc.nom_fichier(mois)}"'})

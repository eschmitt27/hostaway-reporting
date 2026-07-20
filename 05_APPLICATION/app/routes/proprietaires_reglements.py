"""Routes Propriétaires & règlements (APP-3C) — pilotage en lecture seule.

Aucun virement, aucune génération de facture, aucun déclenchement Lot12, aucune écriture.
Les statuts et montants viennent du moteur (Lot10 / Lot12).
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import proprietaires_reglements_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/proprietaires-reglements", response_class=HTMLResponse)
def reglements_dashboard(
    request: Request,
    mois: str = "",
    proprietaire_id: str = "",
    logement_id: str = "",
    statut: str = "",
    avec_anomalie: bool = False,
    avec_reste: bool = False,
    facture: str = "",
    regle: str = "",
    tri: str = "anomalie",
    page: int = 1,
):
    data = svc.load_dashboard(
        mois=mois, proprietaire_id=proprietaire_id, logement_id=logement_id, statut=statut,
        avec_anomalie=avec_anomalie, avec_reste=avec_reste, facture=facture, regle=regle,
        tri=tri, page=page,
    )
    return templates.TemplateResponse(request, "reglements_list.html", {
        "active_menu": "proprietaires", "data": data,
    })


@router.get("/proprietaires-reglements/a-controler", response_class=HTMLResponse)
def reglements_a_controler(request: Request, mois: str = ""):
    data = svc.load_to_control(mois)
    return templates.TemplateResponse(request, "reglements_a_controler.html", {
        "active_menu": "proprietaires", "data": data,
    })


@router.get("/proprietaires-reglements/export.csv")
def reglements_export_csv(
    request: Request,
    mois: str = "",
    proprietaire_id: str = "",
    logement_id: str = "",
    statut: str = "",
    avec_anomalie: bool = False,
    avec_reste: bool = False,
    facture: str = "",
    regle: str = "",
    tri: str = "anomalie",
):
    contenu = svc.export_csv(
        mois=mois, proprietaire_id=proprietaire_id, logement_id=logement_id, statut=statut,
        avec_anomalie=avec_anomalie, avec_reste=avec_reste, facture=facture, regle=regle, tri=tri,
    )
    nom = f"proprietaires_reglements_{mois or 'tous'}.csv"
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})


@router.get("/proprietaires-reglements/{proprietaire_id}", response_class=HTMLResponse)
def reglements_detail(request: Request, proprietaire_id: str, mois: str = ""):
    detail = svc.load_owner_detail(proprietaire_id, mois)
    if detail is None:
        return templates.TemplateResponse(request, "reglements_detail.html", {
            "active_menu": "proprietaires", "detail": None, "proprietaire_id": proprietaire_id,
        }, status_code=404)
    return templates.TemplateResponse(request, "reglements_detail.html", {
        "active_menu": "proprietaires", "detail": detail, "proprietaire_id": proprietaire_id,
    })

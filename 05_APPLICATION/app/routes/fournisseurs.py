from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import charges_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/fournisseurs", response_class=HTMLResponse)
def fournisseurs_list(
    request: Request,
    mois: str = "",
    logement_id: str = "",
    categorie_charge_id: str = "",
    code_impact: str = "",
    statut_controle: str = "",
    associe_id: str = "",
):
    data = svc.load_list(
        mois=mois,
        logement_id=logement_id,
        categorie_charge_id=categorie_charge_id,
        code_impact=code_impact,
        statut_controle=statut_controle,
        associe_id=associe_id,
    )
    return templates.TemplateResponse(request, "fournisseurs_list.html", {
        "active_menu": "fournisseurs",
        "data": data,
    })


@router.get("/fournisseurs/{charge_id}", response_class=HTMLResponse)
def fournisseur_detail(request: Request, charge_id: str):
    detail = svc.load_detail(charge_id)
    if detail is None:
        return templates.TemplateResponse(
            request,
            "fournisseurs_detail.html",
            {
                "active_menu": "fournisseurs",
                "detail": None,
                "charge_id": charge_id,
            },
            status_code=404,
        )
    return templates.TemplateResponse(request, "fournisseurs_detail.html", {
        "active_menu": "fournisseurs",
        "detail": detail,
        "charge_id": charge_id,
    })

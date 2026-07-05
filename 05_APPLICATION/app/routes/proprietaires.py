from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import proprietaires_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/proprietaires", response_class=HTMLResponse)
def proprietaires_list(request: Request):
    data = svc.load_list()
    return templates.TemplateResponse(request, "proprietaires_list.html", {
        "active_menu": "proprietaires",
        "data": data,
    })


@router.get("/proprietaires/{prop_id}", response_class=HTMLResponse)
def proprietaire_detail(request: Request, prop_id: str):
    detail = svc.load_detail(prop_id)
    if detail is None:
        return templates.TemplateResponse(
            request, "proprietaires_detail.html",
            {"active_menu": "proprietaires", "detail": None, "prop_id": prop_id},
            status_code=404,
        )
    return templates.TemplateResponse(request, "proprietaires_detail.html", {
        "active_menu": "proprietaires",
        "detail": detail,
        "prop_id": prop_id,
    })


@router.get("/proprietaires/{prop_id}/{mois}", response_class=HTMLResponse)
def proprietaire_releve(request: Request, prop_id: str, mois: str):
    releve = svc.load_releve(prop_id, mois)
    status_code = 200
    if releve.get("status") in ("NOT_FOUND", "ERROR"):
        status_code = 404 if releve.get("status") == "NOT_FOUND" else 200
    return templates.TemplateResponse(request, "proprietaires_releve.html", {
        "active_menu": "proprietaires",
        "releve": releve,
        "prop_id": prop_id,
        "mois": mois,
    }, status_code=status_code)


@router.get("/proprietaires/{prop_id}/{mois}/prefacture", response_class=HTMLResponse)
def proprietaire_prefacture(request: Request, prop_id: str, mois: str):
    pref = svc.load_prefacture(prop_id, mois)
    status_code = 200
    if pref.get("status") == "NOT_FOUND":
        status_code = 404
    return templates.TemplateResponse(request, "proprietaires_prefacture.html", {
        "active_menu": "proprietaires",
        "pref": pref,
        "prop_id": prop_id,
        "mois": mois,
    }, status_code=status_code)

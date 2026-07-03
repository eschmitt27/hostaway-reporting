from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import logements_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "on", "oui", "yes")


@router.get("/logements", response_class=HTMLResponse)
def logements_list(
    request: Request,
    q: str = "",
    ville: str = "",
    type_logement_id: str = "",
    proprietaire_id: str = "",
    actif: str = "",
    include_technique: str = "",
):
    data = svc.load_list(
        q=q,
        ville=ville,
        type_logement_id=type_logement_id,
        proprietaire_id=proprietaire_id,
        actif=actif,
        include_technique=_truthy(include_technique),
    )
    return templates.TemplateResponse(request, "logements_list.html", {
        "active_menu": "logements",
        "data": data,
    })


@router.get("/logements/{logement_id}", response_class=HTMLResponse)
def logement_detail(request: Request, logement_id: str):
    detail = svc.load_detail(logement_id)
    if detail is None:
        return templates.TemplateResponse(
            request,
            "logements_detail.html",
            {"active_menu": "logements", "detail": None, "logement_id": logement_id},
            status_code=404,
        )
    return templates.TemplateResponse(request, "logements_detail.html", {
        "active_menu": "logements",
        "detail": detail,
        "logement_id": logement_id,
    })

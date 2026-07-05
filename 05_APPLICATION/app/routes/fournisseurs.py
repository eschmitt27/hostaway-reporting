from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import charges_service as svc
from app.services.charges_preview_service import (
    ChargesPreviewError,
    load_form_refs,
    load_previsualisation,
    previsualiser,
)

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


@router.get("/fournisseurs/nouvelle", response_class=HTMLResponse)
def fournisseurs_nouvelle_form(request: Request):
    refs = load_form_refs()
    return templates.TemplateResponse(request, "fournisseurs_nouvelle.html", {
        "active_menu": "fournisseurs",
        "refs": refs,
        "form": {},
        "erreurs": [],
    })


@router.post("/fournisseurs/nouvelle/previsualiser", response_class=HTMLResponse)
async def fournisseurs_nouvelle_previsualiser(request: Request):
    form_raw = await request.form()
    form_data = {k: str(v) for k, v in form_raw.items()}
    result = previsualiser(form_data)
    if not result["ok"]:
        refs = load_form_refs()
        return templates.TemplateResponse(request, "fournisseurs_nouvelle.html", {
            "active_menu": "fournisseurs",
            "refs": refs,
            "form": form_data,
            "erreurs": result["manifest"]["errors"],
        })
    return RedirectResponse(
        url=f"/fournisseurs/nouvelle/previsualisation/{result['token']}",
        status_code=303,
    )


@router.get("/fournisseurs/nouvelle/previsualisation/{token}", response_class=HTMLResponse)
def fournisseurs_previsualisation(request: Request, token: str):
    try:
        data = load_previsualisation(token)
    except ChargesPreviewError:
        return templates.TemplateResponse(
            request,
            "fournisseurs_previsualisation.html",
            {"active_menu": "fournisseurs", "token": token, "manifest": None, "not_found": True},
            status_code=404,
        )
    return templates.TemplateResponse(request, "fournisseurs_previsualisation.html", {
        "active_menu": "fournisseurs",
        "token": token,
        "manifest": data["manifest"],
        "not_found": False,
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

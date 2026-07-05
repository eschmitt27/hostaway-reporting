from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import menages_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/menages", response_class=HTMLResponse)
def menages_list(
    request: Request,
    mois: str = "",
    logement_id: str = "",
    type_intervenant: str = "",
    statut_controle: str = "",
):
    data = svc.load_list(
        mois=mois,
        logement_id=logement_id,
        type_intervenant=type_intervenant,
        statut_controle=statut_controle,
    )
    return templates.TemplateResponse(request, "menages_list.html", {
        "active_menu": "menages",
        "data": data,
    })


@router.get("/menages/{mois}/{logement_id}/{intervenant_id}", response_class=HTMLResponse)
def menage_detail(request: Request, mois: str, logement_id: str, intervenant_id: str):
    detail = svc.load_detail(mois, logement_id, intervenant_id)
    if detail is None:
        return templates.TemplateResponse(
            request,
            "menages_detail.html",
            {
                "active_menu": "menages",
                "detail": None,
                "mois": mois,
                "logement_id": logement_id,
                "intervenant_id": intervenant_id,
            },
            status_code=404,
        )
    return templates.TemplateResponse(request, "menages_detail.html", {
        "active_menu": "menages",
        "detail": detail,
        "mois": mois,
        "logement_id": logement_id,
        "intervenant_id": intervenant_id,
        "outrepassage_result": None,
        "outrepassage_error": None,
    })


@router.post("/menages/{mois}/{logement_id}/{intervenant_id}/outrepasser", response_class=HTMLResponse)
async def menage_outrepasser(request: Request, mois: str, logement_id: str, intervenant_id: str):
    form_data = await request.form()
    motif = str(form_data.get("motif", "")).strip()

    if not motif:
        detail = svc.load_detail(mois, logement_id, intervenant_id)
        if detail is None:
            return templates.TemplateResponse(
                request, "menages_detail.html",
                {"active_menu": "menages", "detail": None,
                 "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id},
                status_code=404,
            )
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages",
            "detail": detail,
            "mois": mois,
            "logement_id": logement_id,
            "intervenant_id": intervenant_id,
            "outrepassage_result": None,
            "outrepassage_error": "Le motif est obligatoire.",
        }, status_code=422)

    result = svc.enregistrer_outrepassage(mois, logement_id, intervenant_id, motif)
    if not result["ok"]:
        detail = svc.load_detail(mois, logement_id, intervenant_id)
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages",
            "detail": detail,
            "mois": mois,
            "logement_id": logement_id,
            "intervenant_id": intervenant_id,
            "outrepassage_result": None,
            "outrepassage_error": result.get("error"),
        }, status_code=422)

    return RedirectResponse(
        url=f"/menages/{mois}/{logement_id}/{intervenant_id}?outrepassage=ok",
        status_code=303,
    )

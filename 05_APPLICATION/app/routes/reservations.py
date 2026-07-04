from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import app.config as cfg
from app.config import TEMPLATES_DIR
from app.services import reservations_hh_service as svc
from app.services import saisie_hh_service as saisie_svc
from app.services import saisie_hh_dryrun_service as dryrun_svc
from app.services import saisie_hh_orchestrator as hh_orchestrator

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Séparateurs typographiques possibles dans une valeur texte en entrée (espaces à retirer avant conversion)
_SPACES = (" ", " ", " ")


def format_eur(value) -> str:
    """Formatage d'AFFICHAGE au format francais (2 343,48 EUR). Aucun recalcul, aucun arrondi metier.

    - Milliers = espace ASCII, decimale = virgule, suffixe = espace + symbole euro.
    - Valeur vide -> 'Non renseigne'. Texte non convertible -> renvoye tel quel (aucune invention).
    """
    if value in (None, ""):
        return "Non renseigné"
    raw = str(value)
    for sp in _SPACES:
        raw = raw.replace(sp, "")
    raw = raw.replace(",", ".")
    try:
        num = float(raw)
    except (TypeError, ValueError):
        return str(value)
    us = f"{num:,.2f}"                      # ex. "2,343.48"
    fr = us.replace(",", " ").replace(".", ",")  # milliers = espace ASCII, decimale = virgule
    return fr + " €"              # + " €"


templates.env.filters["eur"] = format_eur


@router.get("/reservations", response_class=HTMLResponse)
def reservations_list(
    request: Request,
    q: str = "",
    mois: str = "",
    logement_id: str = "",
    proprietaire_id: str = "",
    canal_id: str = "",
    source_financiere: str = "",
    statut_controle: str = "",
    code_impact: str = "",
    comptabilisation: str = "",
):
    data = svc.load_list(
        q=q,
        mois=mois,
        logement_id=logement_id,
        proprietaire_id=proprietaire_id,
        canal_id=canal_id,
        source_financiere=source_financiere,
        statut_controle=statut_controle,
        code_impact=code_impact,
        comptabilisation=comptabilisation,
    )
    return templates.TemplateResponse(request, "reservations_list.html", {
        "active_menu": "reservations",
        "data": data,
    })


@router.get("/reservations/nouvelle", response_class=HTMLResponse)
def reservation_nouvelle_form(request: Request):
    refs = saisie_svc.load_form_refs()
    return templates.TemplateResponse(request, "reservation_nouvelle_form.html", {
        "active_menu": "reservations",
        "refs": refs,
        "form": {"source_financiere": "SAISIE_MANUELLE"},
        "erreurs": [],
    })


@router.post("/reservations/nouvelle/verifier", response_class=HTMLResponse)
async def reservation_nouvelle_verifier(request: Request):
    form_data = await request.form()
    data = dict(form_data)
    result = saisie_svc.valider(data)
    if not result["ok"]:
        refs = saisie_svc.load_form_refs()
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_form.html",
            {
                "active_menu": "reservations",
                "refs": refs,
                "form": data,
                "erreurs": result["erreurs"],
            },
            status_code=422,
        )
    return templates.TemplateResponse(request, "reservation_nouvelle_verif.html", {
        "active_menu": "reservations",
        "preview": result["preview"],
        "pk": result["pk"],
        "form_data": data,
        "resultat_ecriture": None,
    })


@router.post("/reservations/nouvelle/confirmer", response_class=HTMLResponse)
async def reservation_nouvelle_confirmer(request: Request):
    form_data = await request.form()
    data = dict(form_data)
    if not cfg.HH_REAL_WRITE_ENABLED:
        write_result = hh_orchestrator.confirm_write(
            row_data={},
            pk=str(data.get("reservation_hh_id", "")),
            mois=str(data.get("mois", "")),
        )
        return templates.TemplateResponse(request, "reservation_nouvelle_verif.html", {
            "active_menu": "reservations",
            "preview": {},
            "pk": str(data.get("reservation_hh_id", "")),
            "form_data": data,
            "resultat_ecriture": write_result,
        })
    result = saisie_svc.valider(data)
    if not result["ok"]:
        refs = saisie_svc.load_form_refs()
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_form.html",
            {
                "active_menu": "reservations",
                "refs": refs,
                "form": data,
                "erreurs": result["erreurs"],
            },
            status_code=422,
        )
    row_data = saisie_svc.build_row_data(result["preview"])
    write_result = hh_orchestrator.confirm_write(
        row_data=row_data,
        pk=result["pk"],
        mois=result["preview"].get("mois", ""),
    )
    return templates.TemplateResponse(request, "reservation_nouvelle_verif.html", {
        "active_menu": "reservations",
        "preview": result["preview"],
        "pk": result["pk"],
        "form_data": data,
        "resultat_ecriture": write_result,
    })


@router.post("/reservations/nouvelle/previsualiser", response_class=HTMLResponse)
async def reservation_nouvelle_previsualiser(request: Request):
    form_data = await request.form()
    data = dict(form_data)
    result = dryrun_svc.run_previsualisation(data)
    if not result["ok"] and result["manifest"].get("status") == "VALIDATION_REFUSEE":
        refs = saisie_svc.load_form_refs()
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_form.html",
            {
                "active_menu": "reservations",
                "refs": refs,
                "form": data,
                "erreurs": result["manifest"].get("errors", []),
            },
            status_code=422,
        )
    return RedirectResponse(
        url=f"/reservations/nouvelle/previsualisation/{result['token']}",
        status_code=303,
    )


@router.get("/reservations/nouvelle/previsualisation/{token}", response_class=HTMLResponse)
def reservation_nouvelle_previsualisation(request: Request, token: str):
    try:
        dryrun = dryrun_svc.load_previsualisation(token)
    except dryrun_svc.DryRunError:
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_previsualisation.html",
            {"active_menu": "reservations", "dryrun": None, "token": token},
            status_code=404,
        )
    return templates.TemplateResponse(request, "reservation_nouvelle_previsualisation.html", {
        "active_menu": "reservations",
        "dryrun": dryrun,
        "token": token,
    })


@router.get("/reservations/{reservation_hh_id}", response_class=HTMLResponse)
def reservation_detail(request: Request, reservation_hh_id: str):
    detail = svc.load_detail(reservation_hh_id)
    if detail is None:
        return templates.TemplateResponse(
            request,
            "reservations_detail.html",
            {"active_menu": "reservations", "detail": None, "reservation_hh_id": reservation_hh_id},
            status_code=404,
        )
    return templates.TemplateResponse(request, "reservations_detail.html", {
        "active_menu": "reservations",
        "detail": detail,
        "reservation_hh_id": reservation_hh_id,
    })

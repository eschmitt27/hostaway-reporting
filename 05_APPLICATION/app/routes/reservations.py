from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import reservations_hh_service as svc

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

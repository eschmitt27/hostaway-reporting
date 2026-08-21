from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.config import TEMPLATES_DIR
from app.services import charges_confirmation_service as confirmation
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
    # Champs multi-valeurs (cases à cocher / multi-select) transportés en listes.
    MULTI = {"logements", "proprietaires", "menage_intervenants",
             "menage_logements", "menage_proprietaires"}
    form_data: dict = {}
    for k in form_raw.keys():
        if k in MULTI:
            form_data[k] = [str(v) for v in form_raw.getlist(k)]
        else:
            form_data[k] = str(form_raw[k])
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
        "ecriture_activee": True,
        "deja_confirme": confirmation.resultat_existe(token),
    })


@router.post("/fournisseurs/nouvelle/confirmer/{token}")
def fournisseurs_confirmer(request: Request, token: str):
    """Confirme l'écriture réelle. **Ne reçoit AUCUNE donnée métier du navigateur** : seul le token
    compte, tout le reste est relu du manifest serveur.

    Protection contre la double soumission : si un résultat existe déjà pour ce token, on redirige
    sans rien réexécuter. Et comme on répond par une redirection (POST-Redirect-Get), rafraîchir la
    page de résultat est un simple GET — l'écriture n'est jamais rejouée.
    """
    if confirmation.resultat_existe(token):
        return RedirectResponse(url=f"/fournisseurs/nouvelle/resultat/{token}", status_code=303)

    resultat = confirmation.confirmer(token)   # les flags sont gardés en aval, avant toute écriture

    if not confirmation.resultat_existe(token):
        # Refus qui ne peut pas être persisté (token inconnu, manifest illisible) : aucun dossier de
        # prévisualisation où déposer un résultat. On rend le refus directement plutôt que de
        # rediriger vers une page vide. Sans écriture, rejouer ce POST est sans conséquence.
        return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
            "active_menu": "fournisseurs",
            "token": token,
            "resultat": resultat.as_dict(),
        }, status_code=404)

    return RedirectResponse(url=f"/fournisseurs/nouvelle/resultat/{token}", status_code=303)


@router.get("/fournisseurs/nouvelle/resultat/{token}", response_class=HTMLResponse)
def fournisseurs_resultat(request: Request, token: str):
    """Affiche le résultat d'une confirmation. Lecture seule : n'écrit jamais."""
    resultat = confirmation.charger_resultat(token)
    if resultat is None:
        return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
            "active_menu": "fournisseurs",
            "token": token,
            "resultat": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
        "active_menu": "fournisseurs",
        "token": token,
        "resultat": resultat,
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

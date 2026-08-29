from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from urllib.parse import quote

from app.readers.banques_reader import date_affichage, datetime_affichage
from app.config import TEMPLATES_DIR
from app.services import reservations_hh_service as svc
from app.services import saisie_hh_service as saisie_svc
from app.services import reservations_hh_confirmation_service as confirmation
from app.services import hostaway_actualisation_service as hostaway_svc
from app.services import regularisation_hh_service as regul_svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
# Formatage des dates, partagé avec les autres écrans. Une date non convertible est affichée telle
# quelle, précédée d'une mention : jamais une date inventée pour combler un champ vide.
templates.env.filters["date_fr"] = date_affichage
templates.env.filters["datetime_fr"] = datetime_affichage

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


@router.post("/reservations/nouvelle/previsualiser", response_class=HTMLResponse)
async def reservation_nouvelle_previsualiser(request: Request):
    form_data = await request.form()
    data = dict(form_data)
    result = confirmation.previsualiser(data)
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
        data = confirmation.load_previsualisation(token)
    except confirmation.DryRunError:
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_previsualisation.html",
            {"active_menu": "reservations", "manifest": None, "token": token},
            status_code=404,
        )
    return templates.TemplateResponse(request, "reservation_nouvelle_previsualisation.html", {
        "active_menu": "reservations",
        "manifest": data["manifest"],
        "token": token,
        "deja_confirme": confirmation.resultat_existe(token),
        "resultat": None,
    })


@router.post("/reservations/nouvelle/previsualisation/{token}/enregistrer", response_class=HTMLResponse)
async def reservation_nouvelle_ecriture_reelle(request: Request, token: str):
    if confirmation.resultat_existe(token):
        return RedirectResponse(
            url=f"/reservations/nouvelle/previsualisation/{token}", status_code=303)
    resultat = confirmation.confirmer(token, acteur="local")
    try:
        data = confirmation.load_previsualisation(token)
        manifest = data["manifest"]
    except confirmation.DryRunError:
        manifest = None
    return templates.TemplateResponse(request, "reservation_nouvelle_previsualisation.html", {
        "active_menu": "reservations",
        "manifest": manifest,
        "token": token,
        "deja_confirme": False,
        "resultat": resultat.as_dict(),
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


# ── Régularisation DIRECT_SANS_SAISIE_HH / VRBO_MONTANT_NON_RENSEIGNE (Mission 18b) ──────────────
# Réutilise reservations_hh_saisie_service (aucun second moteur de saisie). Voir
# regularisation_hh_service pour la RÈGLE ABSOLUE (jamais total_price copié dans montant_percu).

@router.get("/reservations/regulariser/{ctrl_opaque}", response_class=HTMLResponse)
def reservation_regulariser_form(request: Request, ctrl_opaque: str):
    prep = regul_svc.preparer_formulaire(ctrl_opaque)
    if prep is None:
        return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
            "active_menu": "controles", "prep": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
        "active_menu": "controles", "prep": prep, "ctrl_opaque": ctrl_opaque,
        "recap": None, "erreur": "",
    })


@router.post("/reservations/regulariser/{ctrl_opaque}/previsualiser", response_class=HTMLResponse)
async def reservation_regulariser_previsualiser(request: Request, ctrl_opaque: str):
    form = await request.form()
    recap = regul_svc.recap(
        ctrl_opaque,
        montant_percu=(form.get("montant_percu") or "").strip(),
        menage=(form.get("menage") or "").strip(),
        code_impact=(form.get("code_impact") or "").strip(),
        commentaire=(form.get("commentaire") or "").strip(),
        canal_id=(form.get("canal_id") or "").strip(),
        source_financiere=(form.get("source_financiere") or "").strip(),
    )
    if recap is None:
        return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
            "active_menu": "controles", "prep": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
        "active_menu": "controles", "prep": recap, "ctrl_opaque": ctrl_opaque,
        "recap": recap, "erreur": "",
    })


@router.post("/reservations/regulariser/{ctrl_opaque}/confirmer", response_class=HTMLResponse)
async def reservation_regulariser_confirmer(request: Request, ctrl_opaque: str):
    form = await request.form()
    resultat = regul_svc.regulariser(
        ctrl_opaque,
        montant_percu=(form.get("montant_percu") or "").strip(),
        menage=(form.get("menage") or "").strip(),
        code_impact=(form.get("code_impact") or "").strip(),
        commentaire=(form.get("commentaire") or "").strip(),
        canal_id=(form.get("canal_id") or "").strip(),
        source_financiere=(form.get("source_financiere") or "").strip(),
        acteur="local",
    )
    if not resultat.get("ok"):
        prep = regul_svc.preparer_formulaire(ctrl_opaque)
        return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
            "active_menu": "controles", "prep": prep, "ctrl_opaque": ctrl_opaque,
            "recap": None, "erreur": resultat.get("message", "Régularisation refusée."),
        }, status_code=422)
    return templates.TemplateResponse(request, "reservation_regulariser_confirmation.html", {
        "active_menu": "controles", "ctrl_opaque": ctrl_opaque, "resultat": resultat,
        "recalcul": None,
    })


@router.post("/reservations/regulariser/{ctrl_opaque}/recalculer", response_class=HTMLResponse)
def reservation_regulariser_recalculer(request: Request, ctrl_opaque: str):
    recalcul = regul_svc.recalculer()
    return templates.TemplateResponse(request, "reservation_regulariser_confirmation.html", {
        "active_menu": "controles", "ctrl_opaque": ctrl_opaque, "resultat": {"ok": True},
        "recalcul": recalcul,
    })


# ── Actualisation Hostaway (APP-6A) ──────────────────────────────────────────
# Le bouton appelle `hostaway_actualisation_service.actualiser()`, qui est aussi le point d'entrée
# prévu pour un déclenchement automatique : un seul chemin métier, donc un seul comportement.
# La route ne fait qu'appeler et rendre l'état — aucune logique d'extraction ici.

@router.get("/hostaway", response_class=HTMLResponse)
def hostaway_actualisation(request: Request, message: str = "", message_type: str = ""):
    return templates.TemplateResponse(request, "hostaway_actualisation.html", {
        "active_menu": "reservations",
        "etat": hostaway_svc.etat(),
        "historique": hostaway_svc.historique(limite=10),
        "message": message,
        "message_type": message_type,
    })


@router.post("/hostaway/actualiser")
def hostaway_actualiser(request: Request):
    """Lance une actualisation et redirige. La requête n'attend jamais la fin de l'extraction."""
    resultat = hostaway_svc.actualiser(declencheur=hostaway_svc.DECLENCHEUR_MANUEL)
    if resultat.get("ok"):
        message = "Actualisation Hostaway lancée. Rechargez la page pour suivre l'avancement."
        type_message = "info"
    else:
        message = resultat.get("message", "Actualisation impossible.")
        type_message = "error"
    return RedirectResponse(
        url=f"/hostaway?message={quote(message)}&message_type={type_message}", status_code=303)

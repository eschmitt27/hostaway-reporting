from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import app.config as cfg
from app.config import TEMPLATES_DIR
from app.services import impact_preview_service as preview_svc
from app.services import logements_service as svc
from app.services import logements_creation_service as creation_svc
from app.services import logements_gestion_service as gestion_svc
from app.services import referentiel_admin_service as adm

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "on", "oui", "yes")


def _ecriture_active() -> bool:
    return bool(cfg.CHARGES_REAL_WRITE_ENABLED and cfg.CHARGES_REAL_WRITE_CONFIRMATION_ENABLED)


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
        "ecriture_active": _ecriture_active(),
    })


@router.get("/logements/nouveau", response_class=HTMLResponse)
def logement_nouveau(request: Request, message: str = "", erreur: str = ""):
    refs = creation_svc.referentiels()
    return templates.TemplateResponse(request, "logements_nouveau.html", {
        "active_menu": "logements",
        "refs": refs,
        "ecriture_active": _ecriture_active(),
        "message": message,
        "erreur": erreur,
    })


@router.post("/logements")
async def logement_creer(request: Request):
    form = dict(await request.form())
    res = creation_svc.creer(form)
    if not res.get("ok"):
        msgs = "; ".join(e["message"] for e in res.get("erreurs", [])) or res.get("message", "")
        return RedirectResponse(url=f"/logements/nouveau?erreur={msgs}", status_code=303)
    return RedirectResponse(
        url=f"/logements/{res['logement_id']}?message=Logement créé.", status_code=303)


@router.get("/logements/{logement_id}", response_class=HTMLResponse)
def logement_detail(request: Request, logement_id: str, message: str = "", erreur: str = ""):
    detail = svc.load_detail(logement_id)
    etat = gestion_svc.etat_actuel(logement_id)

    if detail is None:
        if etat.get("status") != "OK":
            # Ni dans l'export PBI, ni dans REF_Setup : le logement n'existe vraiment pas.
            return templates.TemplateResponse(
                request,
                "logements_detail.html",
                {"active_menu": "logements", "detail": None, "logement_id": logement_id},
                status_code=404,
            )
        # Créé via l'application mais pas encore repris par l'export PBI (Lot13) : fiche minimale
        # construite directement depuis REF_Setup, jamais un 404 pour un logement qui existe.
        histo = gestion_svc.historique(logement_id)
        refs = creation_svc.referentiels()
        return templates.TemplateResponse(request, "logements_detail.html", {
            "active_menu": "logements",
            "detail": None,
            "fiche_minimale": True,
            "logement_id": logement_id,
            "etat": etat,
            "histo": histo,
            "refs": refs,
            "ecriture_active": _ecriture_active(),
            "message": message,
            "erreur": erreur,
        })

    histo = gestion_svc.historique(logement_id)
    refs = creation_svc.referentiels()
    return templates.TemplateResponse(request, "logements_detail.html", {
        "active_menu": "logements",
        "detail": detail,
        "logement_id": logement_id,
        "etat": etat,
        "histo": histo,
        "refs": refs,
        "ecriture_active": _ecriture_active(),
        "message": message,
        "erreur": erreur,
    })


def _retour(logement_id: str, res: dict, message_ok: str) -> RedirectResponse:
    if not res.get("ok"):
        return RedirectResponse(
            url=f"/logements/{logement_id}?erreur={res.get('message', 'Erreur.')}", status_code=303)
    return RedirectResponse(url=f"/logements/{logement_id}?message={message_ok}", status_code=303)


@router.post("/logements/{logement_id}/modifier")
async def logement_modifier(request: Request, logement_id: str):
    form = dict(await request.form())
    res = gestion_svc.modifier(logement_id, form)
    return _retour(logement_id, res, "Logement modifié.")


@router.post("/logements/{logement_id}/archiver")
async def logement_archiver(request: Request, logement_id: str):
    form = await request.form()
    date_fin = str(form.get("date_fin", "") or "")
    justification = str(form.get("justification", "") or "")
    blocage = adm.verifier_justification_retroactive(date_fin, justification)
    if blocage:
        return _retour(logement_id, blocage, "")
    res = gestion_svc.archiver(logement_id, date_fin, justification=justification)
    return _retour(logement_id, res,
                   "Logement archivé — il n'apparaîtra plus dans les nouvelles réservations/charges, "
                   "mais reste visible dans l'historique.")


@router.post("/logements/{logement_id}/reactiver")
async def logement_reactiver(request: Request, logement_id: str):
    form = await request.form()
    date_debut = str(form.get("date_debut", "") or "")
    justification = str(form.get("justification", "") or "")
    blocage = adm.verifier_justification_retroactive(date_debut, justification)
    if blocage:
        return _retour(logement_id, blocage, "")
    res = gestion_svc.reactiver(logement_id, date_debut, str(form.get("proprietaire_id", "") or ""),
                                justification=justification)
    return _retour(logement_id, res, "Logement réactivé.")


@router.post("/logements/{logement_id}/changer-proprietaire")
async def logement_changer_proprietaire(request: Request, logement_id: str):
    form = await request.form()
    date_debut = str(form.get("date_debut", "") or "")
    justification = str(form.get("justification", "") or "")
    blocage = adm.verifier_justification_retroactive(date_debut, justification)
    if blocage:
        return _retour(logement_id, blocage, "")
    res = gestion_svc.changer_proprietaire(logement_id, str(form.get("proprietaire_id", "") or ""),
                                           date_debut, justification=justification)
    return _retour(logement_id, res,
                   "Propriétaire changé — pensez à relancer les calculs (Lot9/Lot10) pour la "
                   "période concernée.")


@router.post("/logements/{logement_id}/changer-taux-commission")
async def logement_changer_taux(request: Request, logement_id: str):
    form = await request.form()
    date_debut = str(form.get("date_debut", "") or "")
    justification = str(form.get("justification", "") or "")
    blocage = adm.verifier_justification_retroactive(date_debut, justification)
    if blocage:
        return _retour(logement_id, blocage, "")
    res = gestion_svc.changer_taux_commission(
        logement_id, str(form.get("taux_commission", "") or ""), date_debut,
        justification=justification)
    return _retour(logement_id, res,
                   "Taux de commission changé — pensez à relancer Lot10 pour la période concernée.")


@router.get("/logements/{logement_id}/impacts-taux", response_class=HTMLResponse)
def logement_impacts_taux(request: Request, logement_id: str, date_debut: str = ""):
    """Aperçu structurel (Mission 6 quater §10) avant confirmation d'un changement de taux —
    aucun montant recalculé, seulement des comptages."""
    aperçu = preview_svc.previsualiser_taux_commission(logement_id, date_debut) if date_debut else None
    return templates.TemplateResponse(request, "logements_impacts.html", {
        "active_menu": "logements", "logement_id": logement_id, "date_debut": date_debut,
        "apercu": aperçu,
    })

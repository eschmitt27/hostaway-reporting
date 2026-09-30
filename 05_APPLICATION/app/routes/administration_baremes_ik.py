"""Administration › Barème des indemnités kilométriques — consulter, créer une année, modifier.

Le barème sert au contrôle INDICATIF des IK (jamais bloquant). Versionné par année, jamais
supprimé : actif / archivé ; une IK validée garde le barème qui a servi à son contrôle.
"""
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import bareme_ik_service as svc

router = APIRouter()
templates = get_templates()
_MENU = "administration_referentiels"
_BASE = "/administration/baremes-ik"


def _retour(url: str, res: dict, succes: str) -> RedirectResponse:
    cle, texte = ("message", succes) if res.get("ok") else ("erreur", res.get("message") or "Refusé.")
    return RedirectResponse(f"{url}?{cle}={quote(texte)}", status_code=303)


@router.get(_BASE, response_class=HTMLResponse)
def liste(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "administration_baremes_ik.html", {
        "active_menu": _MENU, "baremes": svc.lister(), "message": message, "erreur": erreur,
    })


@router.post(f"{_BASE}/nouveau")
async def nouveau(request: Request):
    f = await request.form()
    res = svc.creer_annee(f.get("annee", ""), acteur="administration")
    if not res.get("ok"):
        return _retour(_BASE, res, "")
    return _retour(f"{_BASE}/{res['bareme_id_opaque']}", res,
                   "Barème créé en brouillon à partir du dernier barème : vérifiez puis activez.")


@router.get(_BASE + "/{bid}", response_class=HTMLResponse)
def detail(request: Request, bid: str, message: str = "", erreur: str = ""):
    b = svc.charger(bid)
    return templates.TemplateResponse(request, "administration_bareme_ik.html", {
        "active_menu": _MENU, "b": b, "types": svc.TYPES_VEHICULE, "message": message,
        "erreur": erreur, "erreurs": [],
    }, status_code=200 if b else 404)


@router.post(_BASE + "/{bid}/enregistrer", response_class=HTMLResponse)
async def enregistrer(request: Request, bid: str):
    f = await request.form()
    colonnes = {k: [str(v) for v in f.getlist(k)] for k in
                ("type_vehicule", "cv_min", "cv_max", "km_min", "km_max", "coefficient", "constante",
                 "retirer")}
    tranches, erreurs = svc.lire_tranches_formulaire(colonnes)
    res = (svc.enregistrer(bid, tranches, f.get("majoration_electrique", ""), str(f.get("source", "")))
           if not erreurs else {"ok": False, "erreurs": erreurs, "message": erreurs[0]})
    if res.get("ok"):
        return _retour(f"{_BASE}/{bid}", res, "Barème enregistré.")
    b = svc.charger(bid)
    return templates.TemplateResponse(request, "administration_bareme_ik.html", {
        "active_menu": _MENU, "b": b, "types": svc.TYPES_VEHICULE, "message": "",
        "erreur": res.get("message"), "erreurs": res.get("erreurs", []),
    })


@router.post(_BASE + "/{bid}/activer")
def activer(bid: str):
    res = svc.activer(bid)
    return _retour(f"{_BASE}/{bid}", res, "Barème activé : il sert désormais au contrôle des IK de "
                                          "son année.")


@router.post(_BASE + "/{bid}/archiver")
def archiver(bid: str):
    return _retour(f"{_BASE}/{bid}", svc.archiver(bid), "Barème archivé (conservé, plus utilisé).")


@router.post(_BASE + "/{bid}/nouvelle-version")
def nouvelle_version(bid: str):
    res = svc.nouvelle_version(bid, acteur="administration")
    if not res.get("ok"):
        return _retour(f"{_BASE}/{bid}", res, "")
    return _retour(f"{_BASE}/{res['bareme_id_opaque']}", res,
                   "Nouvelle version créée en brouillon ; l'ancienne reste intacte.")

"""Page « Charges à contrôler » — validation / rejet applicatifs des charges.

Une charge saisie est en `A_CONTROLER` : elle n'a AUCUN impact financier tant qu'elle n'est pas
validée (Lot9 n'ingère que les charges VALIDE). Cette page rend ce parcours utilisable depuis le
navigateur ; les écritures passent par `charges_validation_service` (write-guard + flags).
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import charges_controles_integrite_service as ctrl
from app.services import charges_validation_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _contexte(statut: str = "", refacturable: str = "", sans_proprietaire: bool = False,
              code_impact: str = "", message: str = "", erreur: str = ""):
    data = svc.lister(statut=statut, refacturable=refacturable,
                      sans_proprietaire=sans_proprietaire, code_impact=code_impact)
    try:
        controles = ctrl.controler()
    except Exception as exc:                      # jamais de page en erreur pour un contrôle
        controles = {"statut": "INDISPONIBLE", "anomalies": [], "nb_bloquants": 0,
                     "detail": f"{type(exc).__name__}"}
    bloquantes = {a.get("charge_id") for a in controles.get("anomalies", [])
                  if a.get("severite") == ctrl.BLOQUANT}
    return {"active_menu": "charges_controle", "data": data, "controles": controles,
            "bloquantes": bloquantes, "message": message, "erreur": erreur,
            "avertissement": svc.AVERTISSEMENT_NON_VALIDEE,
            "applied": {"statut": statut, "refacturable": refacturable,
                        "sans_proprietaire": sans_proprietaire, "code_impact": code_impact}}


@router.get("/charges-controle", response_class=HTMLResponse)
def charges_controle(request: Request, statut: str = "", refacturable: str = "",
                     sans_proprietaire: str = "", code_impact: str = "",
                     message: str = "", erreur: str = ""):
    ctx = _contexte(statut, refacturable, bool(sans_proprietaire), code_impact, message, erreur)
    return templates.TemplateResponse(request, "charges_controle.html", ctx)


@router.get("/charges-controle/{charge_id}/historique", response_class=HTMLResponse)
def charge_historique(request: Request, charge_id: str):
    ctx = _contexte()
    ctx["historique"] = svc.historique(charge_id)
    ctx["charge_id"] = charge_id
    return templates.TemplateResponse(request, "charges_controle.html", ctx)


@router.post("/charges-controle/{charge_id}/valider")
async def valider(request: Request, charge_id: str):
    form = await request.form()
    # Une charge signalée BLOQUANTE par les contrôles ne peut pas être validée.
    try:
        controles = ctrl.controler()
        bloquantes = {a.get("charge_id") for a in controles.get("anomalies", [])
                      if a.get("severite") == ctrl.BLOQUANT}
    except Exception:
        bloquantes = set()
    if charge_id in bloquantes:
        return RedirectResponse(
            url="/charges-controle?erreur=" + "Charge bloquante : corrigez l'anomalie avant validation.",
            status_code=303)
    res = svc.valider(charge_id, acteur=str(form.get("acteur", "") or "local"),
                      commentaire=str(form.get("commentaire", "") or ""))
    if not res.get("ok"):
        return RedirectResponse(url=f"/charges-controle?erreur={res.get('message')}", status_code=303)
    return RedirectResponse(
        url="/charges-controle?message=Charge validée — les calculs doivent être relancés (Lot3/9/10/11).",
        status_code=303)


@router.post("/charges-controle/{charge_id}/rejeter")
async def rejeter(request: Request, charge_id: str):
    form = await request.form()
    motif = str(form.get("motif", "") or "").strip()
    commentaire = str(form.get("commentaire", "") or "").strip()
    complet = (motif + (" — " if motif and commentaire else "") + commentaire).strip()
    res = svc.rejeter(charge_id, commentaire=complet,
                      acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/charges-controle?erreur={res.get('message')}", status_code=303)
    return RedirectResponse(url="/charges-controle?message=Charge rejetée (ligne conservée).",
                            status_code=303)

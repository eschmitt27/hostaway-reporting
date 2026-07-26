"""Routes Factures fournisseurs et règlements.

Les écritures passent par `factures_service` / `reglements_fournisseurs_service`, tous deux gardés
par `FACTURES_REAL_WRITE_*` (double verrou RECETTE_MODE). Le rapprochement bancaire n'est pas
réimplémenté ici : il réutilise `banques_rapprochement_service` (service unique et partagé).
"""
import app.config as cfg
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import factures_service as svc
from app.services import reglements_fournisseurs_service as reg
from app.services import fournisseurs_referentiel_service as frs

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _ecriture_active() -> bool:
    return bool(cfg.FACTURES_REAL_WRITE_ENABLED and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED)


def _fournisseurs_actifs() -> list[dict]:
    try:
        return [f for f in frs.lister(actif_seul=True)]
    except Exception:
        return []


def _libelles_fournisseurs() -> dict[str, str]:
    try:
        return {f["fournisseur_id_opaque"]: f.get("nom", "") for f in frs.lister()}
    except Exception:
        return {}


@router.get("/factures", response_class=HTMLResponse)
def factures_list(request: Request, statut: str = "", fournisseur: str = "",
                  echues: str = "", message: str = "", erreur: str = ""):
    factures = svc.lister(statut=statut, fournisseur=fournisseur,
                          echues_seulement=bool(echues))
    return templates.TemplateResponse(request, "factures_list.html", {
        "active_menu": "factures", "factures": factures, "statuts": svc.STATUTS,
        "fournisseurs": _fournisseurs_actifs(), "libelles": _libelles_fournisseurs(),
        "applied": {"statut": statut, "fournisseur": fournisseur, "echues": echues},
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.get("/factures/a-payer", response_class=HTMLResponse)
def factures_a_payer(request: Request):
    toutes = svc.lister()
    a_payer = [f for f in toutes
               if f["statut"] in svc.STATUTS_OUVERTS and f["solde_restant"] > 0.005]
    return templates.TemplateResponse(request, "factures_a_payer.html", {
        "active_menu": "factures", "factures": a_payer, "libelles": _libelles_fournisseurs(),
        "total_du": round(sum(f["solde_restant"] for f in a_payer), 2),
        "nb_echues": sum(1 for f in a_payer if f["echue"]),
    })


@router.get("/factures/nouvelle", response_class=HTMLResponse)
def facture_nouvelle(request: Request, erreur: str = ""):
    return templates.TemplateResponse(request, "factures_nouvelle.html", {
        "active_menu": "factures", "fournisseurs": _fournisseurs_actifs(),
        "ecriture_active": _ecriture_active(), "erreur": erreur,
    })


@router.post("/factures")
async def facture_creer(request: Request):
    form = dict(await request.form())
    fid = str(form.get("fournisseur_id_opaque", "") or "")
    actif = any(f["fournisseur_id_opaque"] == fid for f in _fournisseurs_actifs()) if fid else None
    res = svc.creer(form, acteur=str(form.get("acteur", "") or "local"), fournisseur_actif=actif)
    if not res.get("ok"):
        msgs = "; ".join(e["message"] for e in res.get("erreurs", [])) or res.get("message", "")
        return RedirectResponse(url=f"/factures/nouvelle?erreur={msgs}", status_code=303)
    return RedirectResponse(url=f"/factures/{res['facture_id_opaque']}?message=Facture enregistrée.",
                            status_code=303)


@router.get("/factures/{opaque}", response_class=HTMLResponse)
def facture_detail(request: Request, opaque: str, message: str = "", erreur: str = ""):
    facture = svc.charger(opaque)
    if facture is None:
        return templates.TemplateResponse(request, "factures_detail.html", {
            "active_menu": "factures", "facture": None, "opaque": opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "factures_detail.html", {
        "active_menu": "factures", "facture": facture,
        "libelles": _libelles_fournisseurs(),
        "reglements": reg.reglements_de_facture(opaque),
        "historique": svc.historique(opaque),
        "statuts": svc.STATUTS, "moyens": reg.MOYENS,
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/factures/{opaque}/statut")
async def facture_statut(request: Request, opaque: str):
    form = await request.form()
    res = svc.changer_statut(opaque, str(form.get("statut", "") or ""),
                             commentaire=str(form.get("commentaire", "") or ""),
                             acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Statut mis à jour." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


@router.post("/factures/{opaque}/lier-charge")
async def facture_lier_charge(request: Request, opaque: str):
    form = await request.form()
    res = svc.lier_charge(opaque, str(form.get("charge_id", "") or ""),
                          acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Charge rattachée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


@router.post("/factures/{opaque}/reglement")
async def facture_reglement(request: Request, opaque: str):
    form = await request.form()
    facture = svc.charger(opaque)
    if facture is None:
        return RedirectResponse(url=f"/factures/{opaque}?erreur=Facture introuvable.", status_code=303)
    montant_txt = str(form.get("montant", "") or "").strip()
    res = reg.enregistrer(
        facture["fournisseur_id_opaque"],
        [{"facture_id_opaque": opaque, "montant": montant_txt}],
        date_reglement=str(form.get("date_reglement", "") or ""),
        moyen=str(form.get("moyen", "") or ""), compte=str(form.get("compte", "") or ""),
        commentaire=str(form.get("commentaire", "") or ""),
        acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Règlement enregistré." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


# ── Règlements ──────────────────────────────────────────────────────────────

@router.get("/reglements", response_class=HTMLResponse)
def reglements_list(request: Request, non_rapproches: str = ""):
    reglements = reg.lister(non_rapproches_seulement=bool(non_rapproches))
    return templates.TemplateResponse(request, "reglements_fournisseurs_list.html", {
        "active_menu": "factures", "reglements": reglements,
        "libelles": _libelles_fournisseurs(),
        "applied": {"non_rapproches": non_rapproches},
    })


@router.post("/reglements/{opaque}/annuler")
async def reglement_annuler(request: Request, opaque: str):
    form = await request.form()
    res = reg.annuler(opaque, commentaire=str(form.get("commentaire", "") or ""),
                      acteur=str(form.get("acteur", "") or "local"))
    retour = str(form.get("retour", "") or "/reglements")
    msg = "message=Règlement annulé." if res.get("ok") else f"erreur={res.get('message')}"
    sep = "&" if "?" in retour else "?"
    return RedirectResponse(url=f"{retour}{sep}{msg}", status_code=303)

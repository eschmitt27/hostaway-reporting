"""Routes du premier socle Comptabilité (migration `0021`).

Première verticale : consultation des journaux/écritures/plan comptable/auxiliaires, génération
d'une écriture ACHATS depuis une facture validée, validation, contrepassation. Aucun écran
Résultats ni tableau analytique — hors périmètre de cette mission (cadrage `43`).
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import app.config as cfg
from app.config import TEMPLATES_DIR
from app.services import comptabilite_ecritures_service as compta

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _ecriture_active() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


def _plan_comptable(db_path=None):
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY compte")]
    finally:
        conn.close()


@router.get("/comptabilite", response_class=HTMLResponse)
def comptabilite_accueil(request: Request):
    ecritures = compta.lister()
    return templates.TemplateResponse(request, "comptabilite_accueil.html", {
        "active_menu": "comptabilite", "journaux": compta.JOURNAUX,
        "nb_ecritures": len(ecritures),
        "nb_proposees": sum(1 for e in ecritures if e["statut"] == compta.ST_PROPOSEE),
        "ecriture_active": _ecriture_active(),
    })


@router.get("/comptabilite/journaux", response_class=HTMLResponse)
def comptabilite_journaux(request: Request):
    par_journal = {j: compta.lister(journal=j) for j in compta.JOURNAUX}
    return templates.TemplateResponse(request, "comptabilite_journaux.html", {
        "active_menu": "comptabilite", "par_journal": par_journal,
    })


@router.get("/comptabilite/ecritures", response_class=HTMLResponse)
def comptabilite_ecritures(request: Request, journal: str = "", periode: str = "", statut: str = ""):
    ecritures = compta.lister(journal=journal, periode=periode, statut=statut)
    return templates.TemplateResponse(request, "comptabilite_ecritures.html", {
        "active_menu": "comptabilite", "ecritures": ecritures, "journaux": compta.JOURNAUX,
        "statuts": compta.STATUTS,
        "applied": {"journal": journal, "periode": periode, "statut": statut},
    })


@router.get("/comptabilite/a-controler", response_class=HTMLResponse)
def comptabilite_a_controler(request: Request):
    proposees = compta.lister(statut=compta.ST_PROPOSEE)
    return templates.TemplateResponse(request, "comptabilite_ecritures.html", {
        "active_menu": "comptabilite", "ecritures": proposees, "journaux": compta.JOURNAUX,
        "statuts": compta.STATUTS, "applied": {}, "titre": "Écritures à contrôler",
    })


@router.get("/comptabilite/plan-comptable", response_class=HTMLResponse)
def comptabilite_plan_comptable(request: Request):
    return templates.TemplateResponse(request, "comptabilite_plan_comptable.html", {
        "active_menu": "comptabilite", "comptes": _plan_comptable(),
    })


@router.get("/comptabilite/auxiliaires", response_class=HTMLResponse)
def comptabilite_auxiliaires(request: Request, auxiliaire: str = ""):
    solde = compta.solde_auxiliaire(auxiliaire) if auxiliaire else None
    return templates.TemplateResponse(request, "comptabilite_auxiliaires.html", {
        "active_menu": "comptabilite", "auxiliaire": auxiliaire, "solde": solde,
    })


@router.get("/comptabilite/ecritures/{opaque}", response_class=HTMLResponse)
def comptabilite_ecriture_detail(request: Request, opaque: str, message: str = "", erreur: str = ""):
    e = compta.charger(opaque)
    if e is None:
        return templates.TemplateResponse(request, "comptabilite_ecriture_detail.html", {
            "active_menu": "comptabilite", "ecriture": None, "opaque": opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "comptabilite_ecriture_detail.html", {
        "active_menu": "comptabilite", "ecriture": e, "opaque": opaque,
        "lignes": compta.lignes(opaque), "ecriture_active": _ecriture_active(),
        "message": message, "erreur": erreur,
    })


@router.post("/comptabilite/ecritures/{opaque}/valider")
async def comptabilite_valider(request: Request, opaque: str):
    form = await request.form()
    res = compta.valider(opaque, acteur=str(form.get("acteur", "") or "local"))
    suffixe = "" if res.get("ok") else f"&erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/ecritures/{opaque}?message=Validée.{suffixe}",
                            status_code=303)


@router.post("/comptabilite/ecritures/{opaque}/contrepasser")
async def comptabilite_contrepasser(request: Request, opaque: str):
    form = await request.form()
    res = compta.contrepasser(opaque, commentaire=str(form.get("commentaire", "") or ""),
                              acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/comptabilite/ecritures/{opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(
        url=f"/comptabilite/ecritures/{res['miroir_id_opaque']}?message=Contrepassation générée.",
        status_code=303)


@router.post("/factures/{facture_opaque}/generer-ecriture-achat")
async def comptabilite_generer_depuis_facture(request: Request, facture_opaque: str):
    """Point d'entrée depuis la fiche facture — cf. brief §22 : « générer l'écriture ACHATS »."""
    form = await request.form()
    res = compta.generer_ecriture_achat(facture_opaque, acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/factures/{facture_opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(url=f"/comptabilite/ecritures/{res['ecriture_id_opaque']}", status_code=303)

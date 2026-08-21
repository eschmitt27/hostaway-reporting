"""Écrans Administration → Référentiels.

Le référentiel se modifiait jusqu'ici en ouvrant `REF_Setup.xlsm`. Il se modifie désormais ici :
c'est la condition pour que le classeur ne soit plus qu'un import initial et une archive.

28 tables, mais PAS 28 écrans : elles sont regroupées par usage métier
(`referentiel_admin_service.CATEGORIES`). Un écran générique liste et édite la table choisie, en
s'appuyant sur le catalogue — ajouter un référentiel au catalogue suffit à l'exposer, sans écrire
un écran de plus.

Les tables HISTORISÉES (rattachement de gestion, taux de commission) sont consultables mais non
éditables ici : elles se modifient par la fiche logement, qui clôt la période courante et en ouvre
une nouvelle. Les éditer librement permettrait de réécrire une période passée.
"""
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import referentiel_admin_service as adm

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

_MENU = "administration_referentiels"


@router.get("/administration/referentiels", response_class=HTMLResponse)
def index(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "administration_referentiels.html", {
        "active_menu": _MENU,
        "disponible": adm.disponible(),
        "categories": adm.categories(),
        "message": message,
        "erreur": erreur,
    })


@router.get("/administration/referentiels/{table}", response_class=HTMLResponse)
def detail(request: Request, table: str, message: str = "", erreur: str = ""):
    meta = adm.decrire_table(table)
    if not meta.get("ok"):
        return RedirectResponse("/administration/referentiels?erreur=Référentiel inconnu",
                                status_code=303)
    return templates.TemplateResponse(request, "administration_referentiel_detail.html", {
        "active_menu": _MENU,
        "disponible": adm.disponible(),
        "meta": meta,
        "lignes": adm.lignes(table) if adm.disponible() else [],
        "evenements": adm.historique_evenements(table, limite=20),
        "message": message,
        "erreur": erreur,
    })


@router.post("/administration/referentiels/{table}/creer")
async def creer(request: Request, table: str):
    """Création. Les champs viennent du formulaire généré depuis le catalogue."""
    form = dict(await request.form())
    res = adm.creer_ligne(table, form, acteur="ui")
    return _retour(table, res, "Ligne créée.")


@router.post("/administration/referentiels/{table}/modifier")
async def modifier(request: Request, table: str):
    form = dict(await request.form())
    meta = adm.decrire_table(table)
    if not meta.get("ok"):
        return RedirectResponse("/administration/referentiels?erreur=Référentiel inconnu",
                                status_code=303)
    cle_valeur = str(form.get(meta["cle"], "")).strip()
    res = adm.modifier_ligne(table, cle_valeur, form, acteur="ui")
    return _retour(table, res, "Ligne modifiée.")


@router.post("/administration/referentiels/{table}/activation")
def activation(table: str, cle: str = Form(...), actif: str = Form(...)):
    """Activation/désactivation — jamais de suppression : une donnée déjà référencée doit rester
    lisible, sinon les objets qui la citent deviennent orphelins."""
    res = adm.basculer_activation(table, cle, actif.upper() == "OUI", acteur="ui")
    return _retour(table, res, "Statut mis à jour.")


def _retour(table: str, res: dict, message_ok: str) -> RedirectResponse:
    base = f"/administration/referentiels/{table}"
    if res.get("ok"):
        return RedirectResponse(f"{base}?message={message_ok}", status_code=303)
    detail = res.get("detail") or ""
    message = res.get("message", res.get("code", "Échec"))
    return RedirectResponse(f"{base}?erreur={message} {detail}".strip(), status_code=303)

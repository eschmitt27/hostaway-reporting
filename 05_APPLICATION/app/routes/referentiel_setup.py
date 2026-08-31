"""Écran Référentiel Setup — état, prévisualisation et import depuis l'interface.

L'import du référentiel se faisait jusqu'ici en ouvrant Excel. Il se fait désormais ici : c'est la
condition pour que l'exploitation quotidienne ne réclame plus de terminal.

Deux temps imposés par l'interface, comme dans le service : on prévisualise, on lit ce qui va être
écrit, on confirme. Aucun bouton ne déclenche un import sans avoir montré son effet d'abord.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.template_env import get_templates

from app.services import ref_setup_import_service as imp
from app.services import ref_setup_repo as repo

router = APIRouter()
templates = get_templates()

_MENU = "referentiel_setup"


@router.get("/referentiel-setup", response_class=HTMLResponse)
def referentiel_setup(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "referentiel_setup.html", {
        "active_menu": _MENU,
        "etat": repo.etat(),
        "historique": imp.historique_imports(),
        "message": message,
        "erreur": erreur,
    })


@router.post("/referentiel-setup/previsualiser", response_class=HTMLResponse)
def previsualiser(request: Request):
    """Lit et contrôle le classeur. N'écrit rien — pas même une ligne de journal."""
    return templates.TemplateResponse(request, "referentiel_setup_previsualisation.html", {
        "active_menu": _MENU,
        "previsualisation": imp.previsualiser(),
    })


@router.post("/referentiel-setup/importer")
async def importer(request: Request):
    """Rejoue les contrôles puis écrit, ou refuse. Le service tranche, la route ne fait que router."""
    resultat = imp.importer()
    if not resultat.get("ok"):
        detail = resultat.get("detail") or ""
        message = resultat.get("message", "Import refusé.")
        return RedirectResponse(
            f"/referentiel-setup?erreur={message} {detail}".strip(), status_code=303)
    avertissements = len(resultat.get("avertissements") or [])
    suffixe = f" — {avertissements} point(s) à contrôler" if avertissements else ""
    return RedirectResponse(
        f"/referentiel-setup?message=Import {resultat['import_id']} : "
        f"{resultat['nb_feuilles']} onglets, {resultat['nb_lignes']} lignes{suffixe}",
        status_code=303)

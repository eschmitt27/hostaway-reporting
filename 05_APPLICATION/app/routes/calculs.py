"""Routes de pilotage des calculs et de clôture mensuelle.

Aucune exécution sans confirmation : la prévisualisation produit un token, et seule la confirmation
de ce token lance la chaîne. Le mode réel reste refusé par le service tant que les flags dédiés ne
sont pas actifs.
"""
import app.config as cfg
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import calculs_executeur_service as ex
from app.services import calculs_pipeline_service as pipe

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# La chaîne ménages n'est PAS proposée ici : elle démarre par lot6b, qui interroge réellement la
# feuille Google des déclarations internes. Elle se lance depuis /menages/chaine, qui copie les
# sources dans un workspace isolé et substitue un stub à l'accès réseau. `executer_lot` refuse
# d'ailleurs ces lots explicitement — cette liste ne fait que ne pas les proposer.
CHAINES = {
    "aval": [l.nom for l in ex.CHAINE_AVAL],
    "charges": [l.nom for l in ex.CHAINE_CHARGES],
}


def _mois_defaut() -> str:
    from datetime import date
    return date.today().strftime("%Y-%m")


@router.get("/calculs", response_class=HTMLResponse)
def calculs_accueil(request: Request, mois: str = "", chaine: str = "aval",
                    message: str = "", erreur: str = ""):
    mois = mois or _mois_defaut()
    lots = CHAINES.get(chaine, CHAINES["aval"])
    return templates.TemplateResponse(request, "calculs_accueil.html", {
        "active_menu": "calculs", "mois": mois, "chaine": chaine, "chaines": CHAINES,
        "lots": [ex.TOUS_LES_LOTS[n] for n in lots if n in ex.TOUS_LES_LOTS],
        "prerequis": ex.verifier_prerequis(lots),
        "interpreteur": ex.verifier_interpreteur(),
        "mode": pipe.MODE_RECETTE if cfg.RECETTE_MODE else pipe.MODE_REEL,
        "racine": str(cfg.PROJECT_ROOT),
        "runs": pipe.lister_runs(mois, limit=10),
        "cloture": pipe.statut_cloture(mois),
        "conditions": pipe.conditions_cloture(mois),
        "comparaison": pipe.comparer(mois),
        "statuts_cloture": pipe.CLOTURE_STATUTS,
        "message": message, "erreur": erreur,
    })


def _lots_demandes(form) -> list[str]:
    """Lots à exécuter : sélection explicite si fournie, sinon la chaîne entière.

    La sélection permet de rejouer un lot isolé (ou un segment) sans réexécuter toute la chaîne.
    Elle reste ordonnée selon la chaîne : on ne laisse pas l'ordre d'un formulaire décider de
    l'ordre d'exécution des moteurs.
    """
    chaine = str(form.get("chaine", "aval") or "aval")
    chaine_lots = CHAINES.get(chaine, CHAINES["aval"])
    demandes = {str(v) for v in form.getlist("lots")} if hasattr(form, "getlist") else set()
    if not demandes:
        return chaine_lots
    return [n for n in chaine_lots if n in demandes] or chaine_lots


@router.post("/calculs/previsualiser")
async def calculs_previsualiser(request: Request):
    form = await request.form()
    mois = str(form.get("mois", "") or _mois_defaut())
    lots = _lots_demandes(form)
    res = pipe.previsualiser(mois, lots)
    if not res.get("ok"):
        return RedirectResponse(url=f"/calculs?mois={mois}&erreur=Prévisualisation impossible.",
                                status_code=303)
    return RedirectResponse(url=f"/calculs/previsualisation/{res['token']}", status_code=303)


@router.get("/calculs/previsualisation/{token}", response_class=HTMLResponse)
def calculs_previsualisation(request: Request, token: str, erreur: str = ""):
    manifest = pipe.charger_manifest(token)
    if manifest is None:
        return templates.TemplateResponse(request, "calculs_previsualisation.html", {
            "active_menu": "calculs", "manifest": None, "token": token,
        }, status_code=404)
    return templates.TemplateResponse(request, "calculs_previsualisation.html", {
        "active_menu": "calculs", "manifest": manifest, "token": token, "erreur": erreur,
    })


@router.post("/calculs/lancer/{token}")
async def calculs_lancer(request: Request, token: str):
    form = await request.form()
    res = pipe.lancer(token, acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok") and res.get("code"):
        return RedirectResponse(
            url=f"/calculs/previsualisation/{token}?erreur={res.get('message')}", status_code=303)
    return RedirectResponse(url=f"/calculs/runs/{res['run_id_opaque']}", status_code=303)


@router.get("/calculs/runs/{run_id}", response_class=HTMLResponse)
def calculs_run(request: Request, run_id: str, message: str = "", erreur: str = ""):
    run = pipe.charger_run(run_id)
    if run is None:
        return templates.TemplateResponse(request, "calculs_run.html", {
            "active_menu": "calculs", "run": None, "run_id": run_id,
        }, status_code=404)
    return templates.TemplateResponse(request, "calculs_run.html", {
        "active_menu": "calculs", "run": run,
        "comparaison": pipe.comparer(run["mois"]),
        "message": message, "erreur": erreur,
    })


@router.post("/calculs/runs/{run_id}/restaurer")
async def calculs_restaurer(request: Request, run_id: str):
    res = pipe.restaurer(run_id)
    nb = len(res.get("fichiers_restaures", []))
    return RedirectResponse(url=f"/calculs/runs/{run_id}?message={nb} fichier(s) restauré(s).",
                            status_code=303)


@router.post("/calculs/cloture/{mois}")
async def calculs_cloture(request: Request, mois: str):
    form = await request.form()
    res = pipe.changer_statut_cloture(mois, str(form.get("statut", "") or ""),
                                     commentaire=str(form.get("commentaire", "") or ""),
                                     acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        detail = res.get("detail", "")
        return RedirectResponse(
            url=f"/calculs?mois={mois}&erreur={res.get('message')} {detail}".strip(),
            status_code=303)
    return RedirectResponse(url=f"/calculs?mois={mois}&message=Clôture : {res['statut']}.",
                            status_code=303)

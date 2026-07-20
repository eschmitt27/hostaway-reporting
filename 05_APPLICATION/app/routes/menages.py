"""Routes Ménages (APP-2a) — lecture, rapprochement, contrôle.

Aucune route n'écrit dans une source métier. La seule écriture est l'outrepassage,
tracé en SQLite. Le recalcul réel du pipeline n'est pas exposé : /menages/diagnostic
est une page de diagnostic, sans exécution.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import menages_service as svc
from app.services import menages_recalcul_service as recalc
from app.services import menages_chaine_service as chaine

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _filtres(mois: str, logement_id: str, proprietaire_id: str, intervenant_id: str,
             type_intervenant: str, statut: str, ecart_seul: bool,
             identification_incomplete: bool, tri: str, page: int) -> dict:
    return {
        "logement_id": logement_id,
        "proprietaire_id": proprietaire_id,
        "intervenant_id": intervenant_id,
        "type_intervenant": type_intervenant,
        "statut": statut,
        "ecart_seul": ecart_seul,
        "identification_incomplete": identification_incomplete,
        "tri": tri if tri in svc.TRIS else "anomalie",
        "page": page,
    }


@router.get("/menages", response_class=HTMLResponse)
def menages_dashboard(
    request: Request,
    mois: str = "",
    logement_id: str = "",
    proprietaire_id: str = "",
    intervenant_id: str = "",
    type_intervenant: str = "",
    statut: str = "",
    ecart_seul: bool = False,
    identification_incomplete: bool = False,
    tri: str = "anomalie",
    page: int = 1,
):
    data = svc.load_dashboard(
        mois=mois,
        **_filtres(mois, logement_id, proprietaire_id, intervenant_id, type_intervenant,
                   statut, ecart_seul, identification_incomplete, tri, page),
    )
    return templates.TemplateResponse(request, "menages_list.html", {
        "active_menu": "menages",
        "data": data,
    })


@router.get("/menages/export.csv")
def menages_export_csv(
    request: Request,
    mois: str = "",
    logement_id: str = "",
    proprietaire_id: str = "",
    intervenant_id: str = "",
    type_intervenant: str = "",
    statut: str = "",
    ecart_seul: bool = False,
    identification_incomplete: bool = False,
    tri: str = "anomalie",
):
    """Export CSV de la vue filtrée — généré en mémoire, aucun fichier écrit sur disque."""
    contenu = svc.export_reconciliation_csv(
        mois=mois, logement_id=logement_id, proprietaire_id=proprietaire_id,
        intervenant_id=intervenant_id, type_intervenant=type_intervenant, statut=statut,
        ecart_seul=ecart_seul, identification_incomplete=identification_incomplete, tri=tri,
    )
    nom = f"menages_rapprochement_{mois or 'tous'}.csv"
    return Response(
        content=contenu, media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{nom}"'},
    )


@router.get("/menages/a-controler", response_class=HTMLResponse)
def menages_a_controler(request: Request, mois: str = ""):
    if not mois:
        mois = svc.periode_par_defaut()
    data = svc.load_anomalies(mois)
    return templates.TemplateResponse(request, "menages_controler.html", {
        "active_menu": "menages",
        "data": data,
        "options": svc.load_filter_options(mois),
    })


@router.get("/menages/diagnostic", response_class=HTMLResponse)
def menages_diagnostic(request: Request):
    """Diagnostic du pipeline ménages. Ne lance rien : le recalcul reste désactivé."""
    return templates.TemplateResponse(request, "menages_diagnostic.html", {
        "active_menu": "menages",
        "dernier_calcul": svc.load_dernier_calcul(),
    })


# ── Recalcul du rapprochement (APP-2b) — sur copies, mode réel gardé ─────────
# Ces routes sont déclarées AVANT le détail /{mois}/{logement}/{intervenant} :
# « /menages/recalculer/runs/{id} » a 3 segments et serait sinon capté par le détail.

@router.get("/menages/recalculer", response_class=HTMLResponse)
def menages_recalculer_form(request: Request, mode: str = recalc.MODE_COPIES, mois: str = ""):
    plan = recalc.preparer(mode=mode, mois=mois or None)
    return templates.TemplateResponse(request, "menages_recalculer.html", {
        "active_menu": "menages", "plan": plan, "runs": recalc.load_runs(),
    })


@router.post("/menages/recalculer/preparer", response_class=HTMLResponse)
async def menages_recalculer_preparer(request: Request):
    form = await request.form()
    mode = str(form.get("mode", recalc.MODE_COPIES))
    mois = str(form.get("mois", "")).strip()
    plan = recalc.preparer(mode=mode, mois=mois or None)
    return templates.TemplateResponse(request, "menages_recalculer.html", {
        "active_menu": "menages", "plan": plan, "runs": recalc.load_runs(), "prepare": True,
    })


@router.post("/menages/recalculer/confirmer")
async def menages_recalculer_confirmer(request: Request):
    form = await request.form()
    mode = str(form.get("mode", recalc.MODE_COPIES))
    mois = str(form.get("mois", "")).strip() or None
    # confirmer() rend TOUJOURS un dict avec run_id (erreurs métier + garde d'exception incluses).
    # Défense de dernier recours côté route : si run_id manque, page d'erreur lisible (jamais un 500).
    resultat = recalc.confirmer(mode=mode, mois=mois)
    run_id = resultat.get("run_id")
    if run_id is None:
        return templates.TemplateResponse(request, "menages_run.html", {
            "active_menu": "menages", "run": None, "run_id": None,
            "erreur": resultat.get("message") or "La simulation n'a pas pu être enregistrée.",
        }, status_code=200)
    # POST-Redirect-GET : un rafraîchissement de la page résultat ne relance jamais le recalcul.
    return RedirectResponse(url=f"/menages/recalculer/runs/{run_id}", status_code=303)


@router.get("/menages/recalculer/runs/{run_id}", response_class=HTMLResponse)
def menages_recalculer_run(request: Request, run_id: int):
    run = recalc.load_run(run_id)
    if run is None:
        return templates.TemplateResponse(request, "menages_run.html", {
            "active_menu": "menages", "run": None, "run_id": run_id,
        }, status_code=404)
    return templates.TemplateResponse(request, "menages_run.html", {
        "active_menu": "menages", "run": run,
    })


# ── Action A — Actualiser l'affichage (invalide le cache, aucun script, aucun réseau) ─
@router.post("/menages/actualiser-affichage")
async def menages_actualiser_affichage(request: Request):
    """Vide le cache de lecture puis renvoie sur le tableau (POST-Redirect-GET).

    Aucune exécution de script, aucune requête réseau, aucune écriture : la prochaine
    lecture rouvre les MASTER et reflète l'état du disque.
    """
    svc.invalidate_menages_cache()
    mois = ""
    try:
        form = await request.form()
        mois = str(form.get("mois", "")).strip()
    except Exception:
        mois = ""
    cible = f"/menages?mois={mois}&affichage=actualise" if mois else "/menages?affichage=actualise"
    return RedirectResponse(url=cible, status_code=303)


# ── Action C — Chaîne COMPLÈTE : recette sur copies (mode réel gardé, flag False) ────
# Régénère les 3 sources (Hostaway/déclarations/PDF-copiés) puis 6d/6e/6f/11 dans un
# workspace isolé. Aucune écriture réelle. Déclaré AVANT le détail (3 segments) : sinon
# « /menages/chaine/executer » serait capté par « /menages/{mois}/{logement}/{intervenant} ».

@router.get("/menages/chaine", response_class=HTMLResponse)
def menages_chaine_form(request: Request, mode: str = chaine.MODE_COPIES):
    plan = chaine.preparer_chaine(mode=mode)
    return templates.TemplateResponse(request, "menages_chaine.html", {
        "active_menu": "menages", "plan": plan, "runs": recalc.load_runs(),
        "pdf_externes": svc.load_pdf_externes_info(),
    })


@router.post("/menages/chaine/executer")
async def menages_chaine_executer(request: Request):
    """Exécute la RECETTE de la chaîne complète sur copies. Le mode réel reste refusé."""
    resultat = chaine.executer_chaine(mode=chaine.MODE_COPIES)
    run_id = resultat.get("run_id")
    if run_id is None:
        return templates.TemplateResponse(request, "menages_run.html", {
            "active_menu": "menages", "run": None, "run_id": None,
            "erreur": resultat.get("message") or "La recette n'a pas pu être enregistrée.",
        }, status_code=200)
    return RedirectResponse(url=f"/menages/recalculer/runs/{run_id}", status_code=303)


@router.get("/menages/{mois}/{logement_id}/{intervenant_id}", response_class=HTMLResponse)
def menage_detail(request: Request, mois: str, logement_id: str, intervenant_id: str):
    detail = svc.load_reconciliation_detail(mois, logement_id, intervenant_id)
    if detail is None:
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages",
            "detail": None,
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
        }, status_code=404)

    return templates.TemplateResponse(request, "menages_detail.html", {
        "active_menu": "menages",
        "detail": detail,
        "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
        "outrepassage_error": None,
    })


@router.post("/menages/{mois}/{logement_id}/{intervenant_id}/outrepasser",
             response_class=HTMLResponse)
async def menage_outrepasser(request: Request, mois: str, logement_id: str,
                             intervenant_id: str):
    form_data = await request.form()
    motif = str(form_data.get("motif", "")).strip()

    if not motif:
        detail = svc.load_reconciliation_detail(mois, logement_id, intervenant_id)
        if detail is None:
            return templates.TemplateResponse(request, "menages_detail.html", {
                "active_menu": "menages", "detail": None,
                "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            }, status_code=404)
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages",
            "detail": detail,
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            "outrepassage_error": "Le motif est obligatoire.",
        }, status_code=422)

    result = svc.enregistrer_outrepassage(mois, logement_id, intervenant_id, motif)
    if not result["ok"]:
        detail = svc.load_reconciliation_detail(mois, logement_id, intervenant_id)
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages",
            "detail": detail,
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            "outrepassage_error": result.get("error"),
        }, status_code=422)

    return RedirectResponse(
        url=f"/menages/{mois}/{logement_id}/{intervenant_id}?outrepassage=ok",
        status_code=303,
    )

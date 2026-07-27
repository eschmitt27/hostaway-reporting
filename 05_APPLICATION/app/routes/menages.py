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
import app.config as cfg
from app.services import menages_cycle_service as cycle
from app.services import menages_controles_service as cycle_controles
from app.services import fournisseurs_referentiel_service as frs_svc

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


def _cycle_ecriture_active() -> bool:
    return bool(cfg.MENAGES_CYCLE_REAL_WRITE_ENABLED and cfg.MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED)


def _fournisseurs_menage_actifs():
    return [f for f in frs_svc.lister(actif_seul=True) if f["type"] == "MENAGE"]


# ── Cycle de vie opérationnel du ménage unitaire ─────────────────────────────
# Distinct du comptage/rapprochement ci-dessus : voir menages_cycle_service.py.

@router.get("/menages/cycle", response_class=HTMLResponse)
def menages_cycle_liste(request: Request, mois: str = "", logement_id: str = "",
                        fournisseur: str = "", type_menage: str = "", statut: str = ""):
    menages = cycle.lister(mois=mois, logement_id=logement_id, fournisseur=fournisseur,
                           type_menage=type_menage, statut=statut)
    return templates.TemplateResponse(request, "menages_cycle_liste.html", {
        "active_menu": "menages", "menages": menages, "statuts": cycle.STATUTS,
        "types": cycle.TYPES, "applied": {"mois": mois, "logement_id": logement_id,
        "fournisseur": fournisseur, "type_menage": type_menage, "statut": statut},
    })


@router.get("/menages/cycle/a-affecter", response_class=HTMLResponse)
def menages_cycle_a_affecter(request: Request):
    menages = [m for m in cycle.lister() if m["statut"] in (cycle.ST_PREVU, cycle.ST_A_AFFECTER)]
    return templates.TemplateResponse(request, "menages_cycle_liste.html", {
        "active_menu": "menages", "menages": menages, "statuts": cycle.STATUTS,
        "types": cycle.TYPES, "applied": {}, "titre": "Ménages à affecter",
    })


@router.get("/menages/cycle/controles", response_class=HTMLResponse)
def menages_cycle_controles(request: Request, severite: str = ""):
    data = cycle_controles.controler()
    anomalies = data["anomalies"]
    if severite:
        anomalies = [a for a in anomalies if a["severite"] == severite]
    return templates.TemplateResponse(request, "menages_cycle_controles.html", {
        "active_menu": "menages", "data": data, "anomalies": anomalies,
        "niveaux": cycle_controles.NIVEAUX, "applied": {"severite": severite},
    })


@router.get("/menages/cycle/nouveau", response_class=HTMLResponse)
def menages_cycle_nouveau_form(request: Request, erreur: str = ""):
    return templates.TemplateResponse(request, "menages_cycle_nouveau.html", {
        "active_menu": "menages", "types": cycle.TYPES,
        "ecriture_active": _cycle_ecriture_active(), "erreur": erreur,
    })


@router.post("/menages/cycle")
async def menages_cycle_creer(request: Request):
    form = dict(await request.form())
    res = cycle.creer(form, acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/menages/cycle/nouveau?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(
        url=f"/menages/cycle/{res['menage_id_opaque']}?message=Ménage créé.", status_code=303)


@router.get("/menages/cycle/{opaque}", response_class=HTMLResponse)
def menages_cycle_detail(request: Request, opaque: str, message: str = "", erreur: str = ""):
    m = cycle.charger(opaque)
    if m is None:
        return templates.TemplateResponse(request, "menages_cycle_detail.html", {
            "active_menu": "menages", "menage": None, "opaque": opaque,
        }, status_code=404)
    contexte = cycle.contexte_facture_charge_reglement_banque(opaque)
    return templates.TemplateResponse(request, "menages_cycle_detail.html", {
        "active_menu": "menages", "menage": m, "opaque": opaque,
        "historique": cycle.historique(opaque), "contexte": contexte,
        "fournisseurs": _fournisseurs_menage_actifs(), "transitions": cycle.TRANSITIONS.get(m["statut"], set()),
        "ecriture_active": _cycle_ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/menages/cycle/{opaque}/affecter")
async def menages_cycle_affecter(request: Request, opaque: str):
    form = await request.form()
    res = cycle.affecter(opaque, str(form.get("fournisseur_id_opaque", "") or ""),
                         acteur=str(form.get("acteur", "") or "local"),
                         commentaire=str(form.get("commentaire", "") or ""))
    suffixe = "" if res.get("ok") else f"&erreur={res.get('message')}"
    msg = "Prestataire affecté." if res.get("ok") else ""
    return RedirectResponse(url=f"/menages/cycle/{opaque}?message={msg}{suffixe}", status_code=303)


@router.post("/menages/cycle/{opaque}/remplacer")
async def menages_cycle_remplacer(request: Request, opaque: str):
    form = await request.form()
    res = cycle.remplacer(opaque, str(form.get("nouveau_fournisseur_id_opaque", "") or ""),
                          acteur=str(form.get("acteur", "") or "local"),
                          commentaire=str(form.get("commentaire", "") or ""))
    if not res.get("ok"):
        return RedirectResponse(url=f"/menages/cycle/{opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(
        url=f"/menages/cycle/{res['nouveau_menage_id_opaque']}?message=Ménage remplacé "
            f"(ancien : {opaque}).", status_code=303)


@router.post("/menages/cycle/{opaque}/realiser")
async def menages_cycle_realiser(request: Request, opaque: str):
    form = await request.form()
    res = cycle.realiser(opaque, duree_reelle_h=form.get("duree_reelle_h"),
                         cout_reel=form.get("cout_reel"), methode_cout=str(form.get("methode_cout", "") or ""),
                         ecart_justification=str(form.get("ecart_justification", "") or ""),
                         acteur=str(form.get("acteur", "") or "local"))
    suffixe = "" if res.get("ok") else f"&erreur={res.get('message')}"
    msg = "Réalisation enregistrée." if res.get("ok") else ""
    return RedirectResponse(url=f"/menages/cycle/{opaque}?message={msg}{suffixe}", status_code=303)


@router.post("/menages/cycle/{opaque}/statut")
async def menages_cycle_statut(request: Request, opaque: str):
    form = await request.form()
    nouveau = str(form.get("statut", "") or "")
    res = cycle.changer_statut(opaque, nouveau, commentaire=str(form.get("commentaire", "") or ""),
                               acteur=str(form.get("acteur", "") or "local"))
    suffixe = "" if res.get("ok") else f"&erreur={res.get('message')}"
    msg = f"Statut : {nouveau}." if res.get("ok") else ""
    return RedirectResponse(url=f"/menages/cycle/{opaque}?message={msg}{suffixe}", status_code=303)


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

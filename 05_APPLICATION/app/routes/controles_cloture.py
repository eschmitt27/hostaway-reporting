"""Routes Contrôles & clôture — APP-5A (lecture moteur) + APP-5B (détail actionnable + suivi humain).

APP-5A : aucune clôture, aucun changement d'état de mois, aucune écriture — contrôles moteur (Lot11).
APP-5B : ouvre les contrôles agrégés en éléments détaillés (identifiant public opaque CTRL-<hash>),
journalise le SUIVI HUMAIN dans l'app.db isolée (jamais une nouvelle vérité, jamais un masquage du
moteur) et propose des liens de correction vers les modules métier. Recalcul moteur sur COPIES.
Flags réels False.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from app.template_env import get_templates
from app.services import controles_cloture_service as svc
from app.services import controles_actionnable_service as act
from app.services import controles_suivi_service as suivi
from app.services import controles_runner_service as runner
from app.services import banques_controle_service as banque_ctrl
from app.services import assiette_correction_service as assiette_svc

router = APIRouter()
templates = get_templates()


# ── APP-5B — Écran principal actionnable ─────────────────────────────────────

@router.get("/controles-cloture", response_class=HTMLResponse)
def controles_dashboard(
    request: Request,
    vue: str = "tous",
    mois: str = "",
    module: str = "",
    niveau: str = "",
    code: str = "",
    statut_suivi: str = "",
    responsable: str = "",
    proprietaire: str = "",
    logement: str = "",
    recherche: str = "",
    actionnables_seul: bool = False,
    cloture_bloquee: bool = False,
    page: int = 1,
    classification: str = "",
):
    data = act.load_dashboard(
        vue=vue, mois=mois, module=module, niveau=niveau, code=code, statut_suivi=statut_suivi,
        responsable=responsable, proprietaire=proprietaire, logement=logement, recherche=recherche,
        actionnables_seul=actionnables_seul, cloture_bloquee=cloture_bloquee, page=page,
        classification=classification,
    )
    return templates.TemplateResponse(request, "controles_actionnable_list.html", {
        "active_menu": "controles", "data": data,
    })


# ── APP-5A — sous-écrans moteur (inchangés) ──────────────────────────────────

@router.get("/controles-cloture/bloquants", response_class=HTMLResponse)
def controles_bloquants(request: Request):
    return templates.TemplateResponse(request, "controles_bloquants.html", {
        "active_menu": "controles", "data": svc.load_blockers(),
    })


@router.get("/controles-cloture/mois/{mois}", response_class=HTMLResponse)
def controles_mois(request: Request, mois: str):
    return templates.TemplateResponse(request, "controles_mois.html", {
        "active_menu": "controles", "data": svc.load_month_status(mois),
    })


@router.get("/controles-cloture/export.csv")
def controles_export_csv(
    request: Request,
    vue: str = "tous",
    mois: str = "",
    module: str = "",
    niveau: str = "",
    code: str = "",
    statut_suivi: str = "",
    responsable: str = "",
    proprietaire: str = "",
    logement: str = "",
    recherche: str = "",
    actionnables_seul: bool = False,
    cloture_bloquee: bool = False,
    classification: str = "",
):
    contenu = act.export_csv(
        vue=vue, mois=mois, module=module, niveau=niveau, code=code, statut_suivi=statut_suivi,
        responsable=responsable, proprietaire=proprietaire, logement=logement, recherche=recherche,
        actionnables_seul=actionnables_seul, cloture_bloquee=cloture_bloquee, classification=classification,
    )
    nom = f"controles_{vue}_{mois or 'tous'}.csv"
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})


# ── APP-5B — Fiche détaillée actionnable ─────────────────────────────────────

def _fiche_ctx(request: Request, ctrl_opaque: str, erreur: str = "", status: int = 200):
    fiche = act.load_fiche(ctrl_opaque)
    if fiche is None:
        return templates.TemplateResponse(request, "controles_element_fiche.html", {
            "active_menu": "controles", "fiche": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "controles_element_fiche.html", {
        "active_menu": "controles", "fiche": fiche, "erreur": erreur,
    }, status_code=status)


@router.get("/controles-cloture/element/{ctrl_opaque}", response_class=HTMLResponse)
def controle_element(request: Request, ctrl_opaque: str):
    return _fiche_ctx(request, ctrl_opaque)


def _element(ctrl_opaque: str):
    f = act.load_fiche(ctrl_opaque)
    return f["element"] if f else None


@router.post("/controles-cloture/element/{ctrl_opaque}/action")
async def controle_element_action(request: Request, ctrl_opaque: str):
    form = await request.form()
    action = (form.get("action") or "").strip()
    el = _element(ctrl_opaque)
    if el is None:
        return _fiche_ctx(request, ctrl_opaque, status=404)
    kw = dict(
        responsable=(form.get("responsable") or "").strip(),
        commentaire=(form.get("commentaire") or "").strip(),
        justification=(form.get("justification") or "").strip(),
        preuve_reference=(form.get("preuve_reference") or "").strip(),
        motif=(form.get("motif") or "").strip(),
        portee=(form.get("portee") or "ENTITE").strip(),
        expiration=(form.get("expiration") or "").strip(),
    )
    amp = el["etat"]["anomalie_moteur_presente"]
    try:
        if action == "prendre_en_charge":
            suivi.prendre_en_charge(el, responsable=kw["responsable"], commentaire=kw["commentaire"])
        elif action == "commenter":
            suivi.commenter(el, responsable=kw["responsable"], commentaire=kw["commentaire"])
        elif action == "marquer_corrige":
            suivi.marquer_corrige(el, responsable=kw["responsable"], preuve_reference=kw["preuve_reference"],
                                  anomalie_moteur_presente=amp, commentaire=kw["commentaire"])
        elif action == "accepter_exception":
            suivi.accepter_exception(el, responsable=kw["responsable"], justification=kw["justification"],
                                     portee=kw["portee"], expiration=kw["expiration"],
                                     anomalie_moteur_presente=amp)
        elif action == "rouvrir":
            suivi.rouvrir(el, responsable=kw["responsable"], motif=kw["motif"])
        elif action == "annuler":
            suivi.annuler_decision(el, responsable=kw["responsable"])
        else:
            return _fiche_ctx(request, ctrl_opaque, erreur="Action inconnue.", status=200)
    except suivi.SuiviRefuse as exc:
        return _fiche_ctx(request, ctrl_opaque, erreur=str(exc), status=200)
    return RedirectResponse(url=f"/controles-cloture/element/{ctrl_opaque}", status_code=303)


@router.post("/controles-cloture/element/{ctrl_opaque}/recalcul-copie")
async def controle_element_recalcul(request: Request, ctrl_opaque: str):
    el = _element(ctrl_opaque)
    if el is None:
        return _fiche_ctx(request, ctrl_opaque, status=404)
    # La classification appliquée sur la copie reflète la décision APP-4B RÉELLE du mouvement :
    # « Contrôlé » / « Rapproché » = classé (le contrôle peut disparaître) ; sinon non classé.
    classifie = False
    if el.get("module") == "BANQUE":
        fiche_bq = banque_ctrl.load_fiche(el.get("entite_id", ""))
        statut = (fiche_bq or {}).get("statut_effectif")
        classifie = statut in (banque_ctrl.ST_CONTROLE, banque_ctrl.ST_RAPPROCHE)
    resultat = runner.recalculer_sur_copie(el, appliquer_classification=classifie)
    return templates.TemplateResponse(request, "controles_recalcul_run.html", {
        "active_menu": "controles", "resultat": resultat, "ctrl_opaque": ctrl_opaque,
    })


# ── ASSIETTE_NEGATIVE_RAMENEE_ZERO — modification manuelle de l'assiette ─────────────────────────
# Deux actions seulement (commenter / modifier l'assiette). Jamais total_price ni aucune valeur
# préemplie pour la nouvelle assiette — l'humain la saisit. Assiette brute/automatique JAMAIS
# réécrites (voir assiette_correction_service). Recalcul RÉEL Lot10→Lot11→Lot12, pas une copie.

@router.get("/controles-cloture/element/{ctrl_opaque}/modifier-assiette", response_class=HTMLResponse)
def assiette_modifier_form(request: Request, ctrl_opaque: str):
    prep = assiette_svc.preparer_formulaire(ctrl_opaque)
    if prep is None:
        return templates.TemplateResponse(request, "assiette_modifier_form.html", {
            "active_menu": "controles", "prep": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "assiette_modifier_form.html", {
        "active_menu": "controles", "prep": prep, "ctrl_opaque": ctrl_opaque,
        "recap": None, "erreur": "",
    })


@router.post("/controles-cloture/element/{ctrl_opaque}/modifier-assiette/previsualiser",
            response_class=HTMLResponse)
async def assiette_modifier_previsualiser(request: Request, ctrl_opaque: str):
    form = await request.form()
    recap = assiette_svc.recap(
        ctrl_opaque,
        nouvelle_assiette=(form.get("nouvelle_assiette") or "").strip(),
        justification=(form.get("justification") or "").strip(),
    )
    if recap is None:
        return templates.TemplateResponse(request, "assiette_modifier_form.html", {
            "active_menu": "controles", "prep": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    erreur = ""
    if not recap["justification_saisie"]:
        erreur = "Justification obligatoire."
    return templates.TemplateResponse(request, "assiette_modifier_form.html", {
        "active_menu": "controles", "prep": recap, "ctrl_opaque": ctrl_opaque,
        "recap": None if erreur else recap, "erreur": erreur,
    })


@router.post("/controles-cloture/element/{ctrl_opaque}/modifier-assiette/confirmer",
            response_class=HTMLResponse)
async def assiette_modifier_confirmer(request: Request, ctrl_opaque: str):
    form = await request.form()
    resultat = assiette_svc.corriger(
        ctrl_opaque,
        nouvelle_assiette=(form.get("nouvelle_assiette") or "").strip(),
        justification=(form.get("justification") or "").strip(),
        acteur="local",
    )
    if not resultat.get("ok"):
        prep = assiette_svc.preparer_formulaire(ctrl_opaque)
        return templates.TemplateResponse(request, "assiette_modifier_form.html", {
            "active_menu": "controles", "prep": prep, "ctrl_opaque": ctrl_opaque,
            "recap": None, "erreur": resultat.get("message", "Correction refusée."),
        }, status_code=422)
    return templates.TemplateResponse(request, "assiette_modifier_confirmation.html", {
        "active_menu": "controles", "ctrl_opaque": ctrl_opaque, "resultat": resultat,
        "recalcul": None,
    })


@router.post("/controles-cloture/element/{ctrl_opaque}/modifier-assiette/recalculer",
            response_class=HTMLResponse)
def assiette_modifier_recalculer(request: Request, ctrl_opaque: str):
    recalcul = assiette_svc.recalculer()
    return templates.TemplateResponse(request, "assiette_modifier_confirmation.html", {
        "active_menu": "controles", "ctrl_opaque": ctrl_opaque, "resultat": {"ok": True},
        "recalcul": recalcul,
    })


# ── APP-5A — Fiche moteur agrégée (compat ; DOIT rester après /element/*) ─────

@router.get("/controles-cloture/{stable_id}", response_class=HTMLResponse)
def controle_detail(request: Request, stable_id: str):
    detail = svc.load_control_detail(stable_id)
    if detail is None:
        return templates.TemplateResponse(request, "controles_detail.html", {
            "active_menu": "controles", "detail": None, "stable_id": stable_id,
        }, status_code=404)
    return templates.TemplateResponse(request, "controles_detail.html", {
        "active_menu": "controles", "detail": detail, "stable_id": stable_id,
    })

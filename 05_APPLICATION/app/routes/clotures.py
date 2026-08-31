"""Routes Clôtures mensuelles (APP-5C) — suivi humain uniquement, jamais la clôture réelle.

La clôture RÉELLE reste exclusivement REF_Cloture_Mensuelle (moteur, D024), jamais écrite ici.
Identifiants opaques CLO-/DOC-, jamais un id SQLite brut dans l'URL. Flags réels toujours False.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from app.template_env import get_templates

from app.readers.banques_reader import date_affichage, datetime_affichage
from app.services import clotures_service as cs
from app.services import clotures_export_service as ces
from app.services import controles_cloture_service as ctrl_cloture

router = APIRouter()
templates = get_templates()
templates.env.filters["date_fr"] = date_affichage
templates.env.filters["datetime_fr"] = datetime_affichage


def _mois_disponibles() -> list[str]:
    """Mois connus du moteur (contrôles APP-5A/5B), triés décroissant."""
    return ctrl_cloture.load_periods()


@router.get("/clotures", response_class=HTMLResponse)
def clotures_liste(request: Request, statut: str = "", annee: str = "", avec_bloqueurs: bool = False,
                   erreur: str = ""):
    lignes = []
    clotures_par_mois = {c["mois"]: c for c in cs.lister()}
    for mois in _mois_disponibles():
        if annee and not mois.startswith(annee):
            continue
        c = clotures_par_mois.get(mois)
        prog = cs.calcul_progression(mois)
        statut_c = c["statut"] if c else cs.ST_NON_DEMARREE
        if statut and statut_c != statut:
            continue
        if avec_bloqueurs and prog["nb_bloqueurs"] == 0:
            continue
        lignes.append({
            "mois": mois, "statut": statut_c, "statut_libelle": cs.STATUTS_LIBELLES.get(statut_c, statut_c),
            "cloture_id_opaque": c["cloture_id_opaque"] if c else None,
            "progression": prog,
            "derniere_action": c["date_validation"] or c["date_preparation"] or c["date_creation"] if c else None,
        })
    return templates.TemplateResponse(request, "clotures_list.html", {
        "active_menu": "clotures", "lignes": lignes, "erreur": erreur,
        "statuts": cs.STATUTS_LIBELLES, "applied": {"statut": statut, "annee": annee,
                                                     "avec_bloqueurs": avec_bloqueurs},
    })


_STATUTS_AVEC_SNAPSHOT = {cs.ST_A_VALIDER, cs.ST_VALIDEE, cs.ST_ROUVERTE, cs.ST_ARCHIVEE}


def _normaliser_snapshot(rows: list[dict]) -> list[dict]:
    """Adapte les lignes `cloture_elements` (figées) au même format que les éléments live APP-5B,
    pour un rendu template identique — le contenu, lui, reste celui figé au moment du snapshot."""
    out = []
    for s in rows:
        out.append({
            "code": s["code_controle"], "module": s["entite_type"], "niveau": s["severite"],
            "entite_id": s["entite_opaque"], "ctrl_opaque": s["ctrl_opaque"],
            "est_info": s["severite"] == "INFO",
            "etat": {
                "anomalie_moteur_presente": s["statut_moteur"] == "PRESENTE",
                "statut_suivi_libelle": s["statut_humain"],
            },
        })
    return out


def _fiche_ctx(cloture_opaque: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return None
    prog = cs.calcul_progression(c["mois"])
    live = cs.elements_du_mois(c["mois"])
    snap_derive = False
    elements_affiches = live
    if c["statut"] in _STATUTS_AVEC_SNAPSHOT:
        snap = cs.snapshot_actif(cloture_opaque)
        if snap:
            elements_affiches = _normaliser_snapshot(snap)
            snap_bloquants = {s["ctrl_opaque"] for s in snap if s["bloque_cloture"]}
            live_bloquants = {e["ctrl_opaque"] for e in live
                              if not e["est_info"] and e["etat"]["anomalie_moteur_presente"]
                              and not e["etat"]["exception_active"]}
            snap_derive = snap_bloquants != live_bloquants
    return {"cloture": c, "progression": prog, "elements": elements_affiches,
            "snapshot_derive": snap_derive, "a_snapshot": c["statut"] in _STATUTS_AVEC_SNAPSHOT,
            "documents": cs.documents(cloture_opaque), "actions": _actions_possibles(c)}


def _actions_possibles(c: dict) -> list[str]:
    suivantes = cs.TRANSITIONS.get(c["statut"], set())
    actions = []
    if cs.ST_EN_PREPARATION in suivantes and c["statut"] != cs.ST_A_VALIDER:
        actions.append("demarrer_preparation")
    if cs.ST_A_VALIDER in suivantes:
        actions.append("passer_a_valider")
    if cs.ST_VALIDEE in suivantes:
        actions.append("valider")
    if cs.ST_ROUVERTE in suivantes:
        actions.append("rouvrir")
    if cs.ST_ARCHIVEE in suivantes:
        actions.append("archiver")
    return actions


@router.get("/clotures/{cloture_opaque}", response_class=HTMLResponse)
def cloture_fiche(request: Request, cloture_opaque: str, erreur: str = ""):
    ctx = _fiche_ctx(cloture_opaque)
    if ctx is None:
        return templates.TemplateResponse(request, "cloture_fiche.html", {
            "active_menu": "clotures", "cloture": None, "cloture_opaque": cloture_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_fiche.html", {
        "active_menu": "clotures", "erreur": erreur, **ctx})


@router.post("/clotures/demarrer")
async def cloture_demarrer(request: Request):
    from urllib.parse import quote
    form = await request.form()
    mois = (form.get("mois") or "").strip()
    if not mois:
        return RedirectResponse(url="/clotures", status_code=303)
    try:
        c = cs.creer_ou_charger(mois, acteur="local")
        if c["statut"] == cs.ST_NON_DEMARREE:
            c = cs.demarrer_preparation(c, acteur="local", version_attendue=c["version"])
    except cs.ClotureRefusee as exc:
        return RedirectResponse(url=f"/clotures?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/clotures/{c['cloture_id_opaque']}", status_code=303)


@router.get("/clotures/{cloture_opaque}/preparation", response_class=HTMLResponse)
def cloture_preparation(request: Request, cloture_opaque: str):
    ctx = _fiche_ctx(cloture_opaque)
    if ctx is None:
        return templates.TemplateResponse(request, "cloture_preparation.html", {
            "active_menu": "clotures", "cloture": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_preparation.html", {"active_menu": "clotures", **ctx})


@router.post("/clotures/{cloture_opaque}/passer-a-valider")
async def cloture_passer_a_valider(request: Request, cloture_opaque: str):
    from urllib.parse import quote
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    try:
        cs.passer_a_valider(c, acteur="local", version_attendue=c["version"])
    except cs.ClotureRefusee as exc:
        return RedirectResponse(
            url=f"/clotures/{cloture_opaque}?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/clotures/{cloture_opaque}/validation", status_code=303)


@router.get("/clotures/{cloture_opaque}/validation", response_class=HTMLResponse)
def cloture_validation(request: Request, cloture_opaque: str, erreur: str = ""):
    ctx = _fiche_ctx(cloture_opaque)
    if ctx is None:
        return templates.TemplateResponse(request, "cloture_validation.html", {
            "active_menu": "clotures", "cloture": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_validation.html", {
        "active_menu": "clotures", "erreur": erreur, **ctx})


@router.post("/clotures/{cloture_opaque}/valider")
async def cloture_valider(request: Request, cloture_opaque: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    form = await request.form()
    commentaire = (form.get("commentaire") or "").strip()
    try:
        cs.valider(c, acteur="local", commentaire=commentaire, version_attendue=c["version"])
    except cs.ClotureRefusee as exc:
        from urllib.parse import quote
        return RedirectResponse(
            url=f"/clotures/{cloture_opaque}/validation?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/clotures/{cloture_opaque}", status_code=303)


@router.get("/clotures/{cloture_opaque}/historique", response_class=HTMLResponse)
def cloture_historique(request: Request, cloture_opaque: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return templates.TemplateResponse(request, "cloture_historique.html", {
            "active_menu": "clotures", "cloture": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_historique.html", {
        "active_menu": "clotures", "cloture": c, "historique": cs.historique(cloture_opaque)})


@router.get("/clotures/{cloture_opaque}/reouvrir", response_class=HTMLResponse)
def cloture_reouvrir_form(request: Request, cloture_opaque: str, erreur: str = ""):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return templates.TemplateResponse(request, "cloture_reouvrir.html", {
            "active_menu": "clotures", "cloture": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_reouvrir.html", {
        "active_menu": "clotures", "cloture": c, "erreur": erreur})


@router.post("/clotures/{cloture_opaque}/reouvrir")
async def cloture_reouvrir(request: Request, cloture_opaque: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    form = await request.form()
    justification = (form.get("justification") or "").strip()
    try:
        cs.rouvrir(c, acteur="local", justification=justification, version_attendue=c["version"])
    except cs.ClotureRefusee as exc:
        from urllib.parse import quote
        return RedirectResponse(
            url=f"/clotures/{cloture_opaque}/reouvrir?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/clotures/{cloture_opaque}", status_code=303)


@router.get("/clotures/{cloture_opaque}/export.csv")
def cloture_export(cloture_opaque: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return Response(content="Clôture introuvable.", status_code=404)
    contenu = ces.exporter_dossier(c)
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{ces.nom_fichier(c["mois"])}"'})

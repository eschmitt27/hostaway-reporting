"""Routes Clôtures mensuelles (APP-5C) — préparation, validation humaine, clôture définitive.

« Valider » (VALIDEE) enregistre la validation humaine de la préparation : le mois n'est pas gelé.
« Clôturer définitivement » (mission 33) appelle `clotures_service.archiver()` — archive économique,
`ref_cloture_mensuelle` à CLOTURE, statut ARCHIVEE, en une transaction — après une page de
confirmation ; le service refait tous les contrôles, la route n'en décide aucun.
Identifiants opaques CLO-/DOC-, jamais un id SQLite brut dans l'URL.

CLÔTURE PAR MODULES : la fiche d'une clôture est un tableau de bord — un bloc par domaine (réservations,
ménages, charges, banque, factures clients, créances, comptabilité), ses bloqueurs lus dans les vrais
modules, et « Clôturer ce module ». Le mois entier se clôture quand tous les modules le sont (même route
de confirmation que la clôture définitive d'avant).
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from app.template_env import get_templates

from app.readers.banques_reader import date_affichage, datetime_affichage
from app.services import cloture_flux_service as cloture_flux
from app.services import cloture_modules_service as cm
from app.services import clotures_service as cs
from app.services import clotures_export_service as ces
from app.services import controles_cloture_service as ctrl_cloture

router = APIRouter()
templates = get_templates()


_FEEDBACK_ACTUALISATION = {
    "succes": "Actualisation terminée.",
    "erreur": "L'actualisation n'a pas pu aboutir. Les calculs restant à actualiser sont affichés ci-dessous.",
    "erreur_referentiel": "L'actualisation n'a pas pu aboutir. Le référentiel des intervenants de ménage doit d'abord être renseigné.",
    "occupe": "Une actualisation est déjà en cours. Attendez sa fin, puis rechargez cette page.",
    "indisponible": "Ce module ne possède pas d'actualisation automatique. Consultez son module pour traiter les éléments.",
}


def _avec_feedback_actualisation(request: Request, tableau: dict) -> dict:
    module = request.query_params.get("module_actualise", "")
    code = request.query_params.get("actualisation", "")
    if module in cm.PAR_CLE and code in _FEEDBACK_ACTUALISATION:
        tableau["actualisation"] = {"module": module, "code": code,
                                   "message": _FEEDBACK_ACTUALISATION[code]}
    return tableau


def _actualiser_depuis_cloture(request: Request, mois: str, cle: str, retour: str):
    from app.services import cloture_actualisation_service as refresh
    if cle not in cm.PAR_CLE or not cs.mois_valide(mois) or mois not in _mois_disponibles():
        return Response("Mois ou module introuvable.", status_code=404)
    # Aucun message brut du moteur ne traverse la route : ils peuvent contenir du SQL
    # ou des chemins. Le détail technique reste dans son journal existant.
    try:
        code = refresh.actualiser(mois, cle)
    except Exception:
        import logging
        logging.getLogger(__name__).exception("Actualisation Clôture indisponible")
        code = "erreur"
    url = f"{retour}?actualisation={code}&module_actualise={cle}#module-{cle.lower()}"
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"retour": url})
    return RedirectResponse(url=url, status_code=303)


@router.post("/clotures/mois/{mois}/modules/{cle}/actualiser")
def cloture_mois_actualiser(request: Request, mois: str, cle: str):
    return _actualiser_depuis_cloture(request, mois, cle, f"/clotures/mois/{mois}")


@router.post("/clotures/{cloture_opaque}/modules/{cle}/actualiser")
def cloture_module_actualiser(request: Request, cloture_opaque: str, cle: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return Response("Clôture introuvable.", status_code=404)
    return _actualiser_depuis_cloture(request, c["mois"], cle, f"/clotures/{cloture_opaque}")


def _mois_disponibles(contexte_flux: dict | None = None) -> list[str]:
    """Mois connus du moteur (contrôles APP-5A/5B), de Flux financiers (mouvements, écritures,
    charges) et le mois courant — triés décroissant. Sans Flux, un mois portant de l'argent réel
    mais aucun constat moteur (septembre 2026) n'apparaîtrait jamais ici."""
    from app.services import perimetre_v1_service as v1
    mois = set(ctrl_cloture.load_periods())
    mois |= cloture_flux.mois_concernes(contexte_flux=contexte_flux)
    courant = cs.aujourdhui().strftime("%Y-%m")
    mois.add(courant)
    mois |= {c["mois"] for c in cs.lister()}          # une clôture démarrée ne disparaît jamais de la liste
    # Après le cutover V1, les mois antérieurs ne sont plus des périodes comptables : la liste ne
    # demande plus de les préparer ni de les clôturer (ils restent consultables ailleurs).
    premier = v1.premier_mois()
    # Tous les mois, du premier mois V1 au mois courant, sont offerts à la clôture : un mois terminé
    # sans aucun constat du moteur ni mouvement (donc absent des sources ci-dessus) reste à clôturer.
    if premier and cs.mois_valide(premier) and premier <= courant:
        annee, m = int(premier[:4]), int(premier[5:7])
        while f"{annee:04d}-{m:02d}" <= courant:
            mois.add(f"{annee:04d}-{m:02d}")
            annee, m = (annee + 1, 1) if m == 12 else (annee, m + 1)
    return sorted((m for m in mois if cs.mois_valide(m) and not (premier and m < premier)),
                  reverse=True)


@router.get("/clotures", response_class=HTMLResponse)
def clotures_liste(request: Request, statut: str = "", annee: str = "", avec_bloqueurs: bool = False,
                   erreur: str = ""):
    lignes = []
    clotures_par_mois = {c["mois"]: c for c in cs.lister()}
    contexte_flux = cloture_flux.contexte()          # Flux lu une fois pour tous les mois
    for mois in _mois_disponibles(contexte_flux):
        if annee and not mois.startswith(annee):
            continue
        c = clotures_par_mois.get(mois)
        prog = cs.calcul_progression(mois, contexte_flux=contexte_flux)
        statut_c = c["statut"] if c else cs.ST_NON_DEMARREE
        if statut and statut_c != statut:
            continue
        if avec_bloqueurs and prog["nb_bloqueurs"] == 0:
            continue
        pseudo = c or {"mois": mois, "statut": cs.ST_NON_DEMARREE, "cloture_id_opaque": None}
        lignes.append({
            "mois": mois, "statut": statut_c, "statut_libelle": cs.STATUTS_LIBELLES.get(statut_c, statut_c),
            "cloture_id_opaque": c["cloture_id_opaque"] if c else None,
            "progression": prog, "tableau": cm.tableau_de_bord(pseudo, progression=prog),
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


def _fiche_ctx(cloture_opaque: str, *, avec_controles: bool = True):
    """Contexte d'une clôture. `avec_controles=False` : le tableau de bord par modules n'affiche pas la liste
    détaillée des contrôles du moteur — on ne la relit pas (les pages de préparation et de validation, elles,
    la demandent encore)."""
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return None
    prog = cs.calcul_progression(c["mois"])
    live = cs.elements_du_mois(c["mois"]) if avec_controles else []
    snap_derive = False
    elements_affiches = live
    if avec_controles and c["statut"] in _STATUTS_AVEC_SNAPSHOT:
        snap = cs.snapshot_actif(cloture_opaque)
        if snap:
            elements_affiches = _normaliser_snapshot(snap)
            snap_bloquants = {s["ctrl_opaque"] for s in snap if s["bloque_cloture"]}
            live_bloquants = {e["ctrl_opaque"] for e in live
                              if not e["est_info"] and e["etat"]["anomalie_moteur_presente"]
                              and not e["etat"]["exception_active"]}
            snap_derive = snap_bloquants != live_bloquants
    return {"cloture": c, "progression": prog, "elements": elements_affiches,
            "statut_libelle": cs.STATUTS_LIBELLES.get(c["statut"], c["statut"]),
            "snapshot_derive": snap_derive, "a_snapshot": c["statut"] in _STATUTS_AVEC_SNAPSHOT,
            "documents": cs.documents(cloture_opaque), "actions": _actions_possibles(c),
            # Sections du mois, TOUTES pré-filtrées sur CE mois (§44-46). Les modules Pilotage
            # mensuel et Contrôles & clôture gardent leurs moteurs et leurs routes ; ce qui change,
            # c'est qu'on y entre depuis le mois qu'on regarde, au lieu de re-choisir une période
            # dans un troisième écran. C'était la redondance signalée en recette : trois entrées de
            # menu pour un seul objet — le mois.
            "sections_du_mois": [
                {"cle": "synthese", "libelle": "Synthèse / Pilotage",
                 "url": f"/pilotage-mensuel?mois={c['mois']}",
                 "detail": "Indicateurs économiques du mois"},
                {"cle": "controles", "libelle": "Contrôles",
                 "url": f"/controles-cloture/mois/{c['mois']}",
                 "detail": "Points de contrôle et anomalies à traiter"},
                {"cle": "menages", "libelle": "Ménages du mois",
                 "url": f"/menages?mois={c['mois']}",
                 "detail": "Rapprochement des ménages"},
                {"cle": "factures", "libelle": "Factures propriétaires",
                 "url": f"/factures-proprietaires?mois={c['mois']}",
                 "detail": "Documents émis pour ce mois"},
                {"cle": "historique", "libelle": "Historique / preuves",
                 "url": f"/clotures/{cloture_opaque}/historique",
                 "detail": "Journal des décisions de clôture"},
            ]}


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


# ── Tableau de contrôle d'un mois (Mission 32) — LECTURE SEULE ───────────────────────────────
# Déclaré avant `/clotures/{cloture_opaque}` : sinon « mois » serait pris pour un identifiant.

@router.get("/clotures/mois")
def cloture_mois_choix(mois: str = ""):
    """Sélecteur de période (formulaire GET) → la page du mois."""
    mois = mois.strip()[:7]
    if not cs.mois_valide(mois):
        return RedirectResponse(url="/clotures", status_code=303)
    return RedirectResponse(url=f"/clotures/mois/{mois}", status_code=303)


@router.get("/clotures/mois/{mois}", response_class=HTMLResponse)
def cloture_mois(request: Request, mois: str):
    """État du mois, synthèse et détail de chaque bloqueur, recalculés à l'affichage. N'écrit rien :
    ni clôture démarrée, ni mouvement qualifié, ni écriture validée."""
    if not cs.mois_valide(mois):
        return RedirectResponse(url="/clotures", status_code=303)
    c = cs.charger_par_mois(mois)
    prog = cs.calcul_progression(mois)
    pseudo = c or {"mois": mois, "statut": cs.ST_NON_DEMARREE, "cloture_id_opaque": None}
    return templates.TemplateResponse(request, "cloture_mois.html", {
        "active_menu": "clotures", "progression": prog, "cloture": c,
        "tableau": _avec_feedback_actualisation(request, cm.tableau_de_bord(pseudo, progression=prog)),
        "cloture_statut_libelle": cs.STATUTS_LIBELLES.get(c["statut"], c["statut"]) if c else "",
    })


@router.get("/clotures/{cloture_opaque}", response_class=HTMLResponse)
def cloture_fiche(request: Request, cloture_opaque: str, erreur: str = "", message: str = ""):
    ctx = _fiche_ctx(cloture_opaque, avec_controles=False)
    if ctx is None:
        return templates.TemplateResponse(request, "cloture_fiche.html", {
            "active_menu": "clotures", "cloture": None, "cloture_opaque": cloture_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_fiche.html", {
        "active_menu": "clotures", "erreur": erreur, "message": message,
        "tableau": _avec_feedback_actualisation(
            request, cm.tableau_de_bord(ctx["cloture"], progression=ctx["progression"])), **ctx})


# ── Modules : clôturer, rouvrir ──────────────────────────────────────────────────────────────────
# Une page de confirmation (GET, n'écrit rien) puis un POST qui refait TOUS les contrôles côté serveur.

def _module_ou_404(request: Request, cloture_opaque: str, cle: str, gabarit: str):
    c = cs.charger_par_opaque(cloture_opaque)
    cle = cle.upper()
    if c is None or cle not in cm.PAR_CLE:
        return None, None, templates.TemplateResponse(request, gabarit, {
            "active_menu": "clotures", "cloture": None, "module": None}, status_code=404)
    tableau = cm.tableau_de_bord(c)
    return c, next(m for m in tableau["modules"] if m["cle"] == cle), None


@router.get("/clotures/{cloture_opaque}/modules/{cle}/cloturer", response_class=HTMLResponse)
def module_cloturer_confirmation(request: Request, cloture_opaque: str, cle: str, erreur: str = ""):
    """Ce que « clôturé » veut dire, et ce qui reste éventuellement à traiter. N'écrit rien."""
    c, module, reponse = _module_ou_404(request, cloture_opaque, cle, "cloture_module_confirmer.html")
    if reponse is not None:
        return reponse
    return templates.TemplateResponse(request, "cloture_module_confirmer.html", {
        "active_menu": "clotures", "cloture": c, "module": module, "erreur": erreur,
        "mois_fr": cs.mois_fr(c["mois"]), "du_mois": cs.du_mois(c["mois"])})


@router.post("/clotures/{cloture_opaque}/modules/{cle}/cloturer")
async def module_cloturer(request: Request, cloture_opaque: str, cle: str):
    from urllib.parse import quote
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    form = await request.form()
    retour = f"/clotures/{cloture_opaque}/modules/{cle}/cloturer?erreur="
    if (form.get("confirmation") or "") != "oui":
        return RedirectResponse(url=retour + quote("Cochez la confirmation pour clôturer ce module."),
                                status_code=303)
    try:
        cm.cloturer_module(c, cle, acteur="local", commentaire=(form.get("commentaire") or "").strip())
    except cs.ClotureRefusee as exc:
        return RedirectResponse(url=retour + quote(str(exc)), status_code=303)
    nom = cm.PAR_CLE[cle.upper()].libelle
    texte = quote("Module « " + nom + " » clôturé.")
    return RedirectResponse(url=f"/clotures/{cloture_opaque}?message={texte}", status_code=303)


@router.get("/clotures/{cloture_opaque}/modules/{cle}/rouvrir", response_class=HTMLResponse)
def module_rouvrir_formulaire(request: Request, cloture_opaque: str, cle: str, erreur: str = ""):
    c, module, reponse = _module_ou_404(request, cloture_opaque, cle, "cloture_module_rouvrir.html")
    if reponse is not None:
        return reponse
    return templates.TemplateResponse(request, "cloture_module_rouvrir.html", {
        "active_menu": "clotures", "cloture": c, "module": module, "erreur": erreur,
        "mois_fr": cs.mois_fr(c["mois"]), "du_mois": cs.du_mois(c["mois"])})


@router.post("/clotures/{cloture_opaque}/modules/{cle}/rouvrir")
async def module_rouvrir(request: Request, cloture_opaque: str, cle: str):
    from urllib.parse import quote
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    form = await request.form()
    retour = f"/clotures/{cloture_opaque}/modules/{cle}/rouvrir?erreur="
    try:
        cm.rouvrir_module(c, cle, acteur="local", justification=(form.get("justification") or "").strip())
    except cs.ClotureRefusee as exc:
        return RedirectResponse(url=retour + quote(str(exc)), status_code=303)
    nom = cm.PAR_CLE[cle.upper()].libelle
    texte = quote("Module « " + nom + " » rouvert.")
    return RedirectResponse(url=f"/clotures/{cloture_opaque}?message={texte}", status_code=303)


@router.post("/clotures/demarrer")
async def cloture_demarrer(request: Request):
    from urllib.parse import quote
    form = await request.form()
    mois = (form.get("mois") or "").strip()
    if not mois:
        return RedirectResponse(url="/clotures", status_code=303)
    try:
        c = cs.demarrer(mois, acteur="local")
    except cs.ClotureRefusee as exc:
        return RedirectResponse(url=f"/clotures?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/clotures/{c['cloture_id_opaque']}", status_code=303)


@router.get("/clotures/{cloture_opaque}/preparation", response_class=HTMLResponse)
def cloture_preparation(request: Request, cloture_opaque: str):
    ctx = _fiche_ctx(cloture_opaque, avec_controles=False)
    if ctx is None:
        return templates.TemplateResponse(request, "cloture_preparation.html", {
            "active_menu": "clotures", "cloture": None,
        }, status_code=404)
    # §17 — le suivi de facturation propriétaire est un AVANCEMENT MENSUEL : sa place est dans la
    # préparation de la clôture, pas dans un relevé économique. Le moteur ne bouge pas ; seule la
    # porte d'entrée est ici.
    from app.services import proprietaires_suivi_service as suivi_prop

    avancement = suivi_prop.avancement_mois(ctx["cloture"]["mois"]) if ctx.get("cloture") else None
    return templates.TemplateResponse(request, "cloture_preparation.html", {
        "active_menu": "clotures", "avancement_facturation": avancement,
        "tableau": cm.tableau_de_bord(ctx["cloture"], progression=ctx["progression"]), **ctx})


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
        "active_menu": "clotures", "erreur": erreur,
        "tableau": cm.tableau_de_bord(ctx["cloture"], progression=ctx["progression"]), **ctx})


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


@router.get("/clotures/{cloture_opaque}/cloture-definitive", response_class=HTMLResponse)
def cloture_definitive_confirmation(request: Request, cloture_opaque: str, erreur: str = ""):
    """Étape de confirmation — n'écrit rien. Le bouton n'y figure que si la clôture paraît
    éligible ; le POST refait de toute façon chaque contrôle."""
    ctx = _fiche_ctx(cloture_opaque, avec_controles=False)
    if ctx is None:
        return templates.TemplateResponse(request, "cloture_definitive.html", {
            "active_menu": "clotures", "cloture": None}, status_code=404)
    tableau = cm.tableau_de_bord(ctx["cloture"], progression=ctx["progression"])
    return templates.TemplateResponse(request, "cloture_definitive.html", {
        "active_menu": "clotures", "erreur": erreur, "motif_indisponible": tableau["motif_mois"],
        "tableau": tableau, **ctx})


@router.post("/clotures/{cloture_opaque}/cloture-definitive")
async def cloture_definitive(request: Request, cloture_opaque: str):
    from urllib.parse import quote
    from app.services import cloture_archivage_service as arch
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    form = await request.form()
    retour = f"/clotures/{cloture_opaque}/cloture-definitive?erreur="
    if (form.get("confirmation") or "") != "oui":
        return RedirectResponse(url=retour + quote("Cochez la confirmation pour clôturer "
                                                   "définitivement le mois."), status_code=303)
    try:
        version = int(form.get("version") or "")
    except ValueError:
        return RedirectResponse(url=retour + quote(cs.MSG_ETAT_PERIME), status_code=303)
    try:
        cs.cloturer_mois(c, acteur="local", commentaire=(form.get("commentaire") or "").strip(),
                         version_attendue=version)
    except (cs.ClotureRefusee, arch.ArchivageRefuse) as exc:
        return RedirectResponse(url=retour + quote(str(exc)), status_code=303)
    return RedirectResponse(url=f"/clotures/{cloture_opaque}", status_code=303)


@router.get("/clotures/{cloture_opaque}/historique", response_class=HTMLResponse)
def cloture_historique(request: Request, cloture_opaque: str):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return templates.TemplateResponse(request, "cloture_historique.html", {
            "active_menu": "clotures", "cloture": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "cloture_historique.html", {
        "active_menu": "clotures", "cloture": c, "historique": cs.historique(cloture_opaque),
        "mois_fr": cs.mois_fr(c["mois"]), "du_mois": cs.du_mois(c["mois"])})


# ── Réouverture EXCEPTIONNELLE d'un mois clôturé définitivement ──────────────────────────────────────
# Une page de confirmation forte (GET, n'écrit rien : elle dit ce qui sera rouvert, et refuse si un mois postérieur
# est clôturé) puis un POST qui refait TOUS les contrôles côté serveur. Rien n'est supprimé.

def _normaliser(texte: str) -> str:
    return " ".join((texte or "").lower().split())


@router.get("/clotures/{cloture_opaque}/reouverture-exceptionnelle", response_class=HTMLResponse)
def cloture_reouverture_exceptionnelle_form(request: Request, cloture_opaque: str, erreur: str = ""):
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return templates.TemplateResponse(request, "cloture_reouverture_exceptionnelle.html", {
            "active_menu": "clotures", "cloture": None}, status_code=404)
    posterieurs = cs.mois_posterieurs_figes(c["mois"])
    return templates.TemplateResponse(request, "cloture_reouverture_exceptionnelle.html", {
        "active_menu": "clotures", "cloture": c, "erreur": erreur, "mois_fr": cs.mois_fr(c["mois"]),
        "du_mois": cs.du_mois(c["mois"]), "clos": c["statut"] == cs.ST_ARCHIVEE,
        "posterieurs": [{"mois": m, "mois_fr": cs.mois_fr(m)} for m in sorted(posterieurs, reverse=True)],
        "message_posterieurs": cs.message_mois_posterieurs(c["mois"], posterieurs) if posterieurs else ""})


@router.post("/clotures/{cloture_opaque}/reouverture-exceptionnelle")
async def cloture_reouverture_exceptionnelle(request: Request, cloture_opaque: str):
    from urllib.parse import quote
    c = cs.charger_par_opaque(cloture_opaque)
    if c is None:
        return RedirectResponse(url="/clotures", status_code=303)
    form = await request.form()
    retour = f"/clotures/{cloture_opaque}/reouverture-exceptionnelle?erreur="
    if (form.get("confirmation") or "") != "oui":
        return RedirectResponse(url=retour + quote("Cochez la confirmation pour rouvrir exceptionnellement ce "
                                                   "mois."), status_code=303)
    if _normaliser(form.get("mois_saisi") or "") != _normaliser(cs.mois_fr(c["mois"])):
        return RedirectResponse(url=retour + quote(f"Pour confirmer, saisissez le mois à rouvrir : "
                                                   f"« {cs.mois_fr(c['mois'])} »."), status_code=303)
    try:
        cs.rouvrir_exceptionnellement(c, acteur="local", justification=(form.get("justification") or "").strip())
    except cs.ClotureRefusee as exc:
        return RedirectResponse(url=retour + quote(str(exc)), status_code=303)
    texte = quote(f"Le mois {cs.mois_fr(c['mois'])} est rouvert exceptionnellement : ses modules sont rouverts, "
                  "vous pouvez corriger puis reclôturer chaque module et le mois.")
    return RedirectResponse(url=f"/clotures/{cloture_opaque}?message={texte}", status_code=303)


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

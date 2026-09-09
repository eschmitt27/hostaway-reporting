"""Routes Ménages (APP-2a) — lecture, rapprochement, contrôle.

Aucune route n'écrit dans une source métier. La seule écriture est l'outrepassage,
tracé en SQLite. Le recalcul réel du pipeline n'est pas exposé : /menages/diagnostic
est une page de diagnostic, sans exécution.
"""
import asyncio
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from app.template_env import get_templates

from app.services import menages_actualisation_service as actualisation
from app.services import menages_service as svc
from app.services import menages_recalcul_service as recalc
from app.services import menages_chaine_service as chaine
from app.services import menages_pdf_import_service as pdf_import
from app.services import menages_declarations_service as declarations
from app.services import orchestrateur_service as orch
import app.config as cfg
from app.services import menages_cycle_service as cycle
from app.services import menages_controles_service as cycle_controles
from app.services import fournisseurs_referentiel_service as frs_svc

router = APIRouter()
templates = get_templates()


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


def _url_contexte(mois: str, filtres: dict) -> str:
    """URL /menages reconstruite avec le mois et les filtres réellement appliqués.

    Les valeurs vides/fausses sont omises : une URL de retour lisible vaut mieux qu'une URL exacte
    mais illisible, et un filtre absent est exactement équivalent à un filtre vide.
    """
    parametres = [("mois", mois)] + [
        (cle, ("true" if valeur is True else str(valeur)))
        for cle, valeur in filtres.items()
        if valeur not in (None, "", False) and not (cle == "page" and valeur == 1)
    ]
    utiles = [(c, v) for c, v in parametres if v not in ("", "None")]
    return "/menages" + (("?" + urlencode(utiles)) if utiles else "")


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
    actualisation_statut: str = "",
    resume: str = "",
):
    data = svc.load_dashboard(
        mois=mois,
        **_filtres(mois, logement_id, proprietaire_id, intervenant_id, type_intervenant,
                   statut, ecart_seul, identification_incomplete, tri, page),
    )
    return templates.TemplateResponse(request, "menages_list.html", {
        "active_menu": "menages",
        "data": data,
        "actualisation": svc.charger_etat_actualisation(),
        # Compte rendu de la dernière actualisation : lignes courtes, calculées par le workflow
        # à partir de ce qu'il a réellement fait. Jamais un compteur écrit en dur dans le gabarit.
        # Contexte de retour, calculé UNE FOIS ici : chaque lien de ligne l'emporte avec lui, si
        # bien qu'après une correction l'utilisateur retrouve son mois ET ses filtres.
        "retour_contexte": _url_contexte(mois, _filtres(
            mois, logement_id, proprietaire_id, intervenant_id, type_intervenant, statut,
            ecart_seul, identification_incomplete, tri, page)),
        "actualisation_terminee": request.query_params.get("actualisation") == "terminee",
        "actualisation_partielle": request.query_params.get("actualisation") == "partielle",
        "actualisation_echec": request.query_params.get("actualisation") == "echec",
        "resume_actualisation": [p for p in resume.split(" · ") if p],
        "changements_clotures": actualisation.changements_mois_clotures(statut="SIGNALE"),
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


# ── Actualisation RÉELLE (parcours normal utilisateur) ───────────────────────
# Deux actions, réelles (SQLite, zéro Excel), distinctes de la recette sur copies ci-dessous :
#   - « Importer les nouvelles factures » : PDF du dossier surveillé → factures + lignes réelles
#     (facture_menage_pdf_service, déjà idempotent — un PDF déjà importé ne duplique jamais).
#   - « Actualiser les ménages » : la même action, puis déclenche le nœud MENAGES de l'orchestrateur
#     RÉEL (`orchestrateur_service.actualiser`, exactement le service utilisé par /actualisation —
#     aucun second chemin d'exécution), qui exécute lot6d/6e/6f en SQLite pur (--sans-excel).

@router.post("/menages/pdf/importer")
async def menages_pdf_importer(request: Request):
    """Importe les PDF du dossier surveillé en factures réelles (idempotent, zéro Excel)."""
    pdf_import.importer_nouveaux(acteur="ui:menages")
    return RedirectResponse(url="/menages?actualisation=pdf", status_code=303)


def _retour_contexte(form, defaut: str) -> str:
    """URL de retour après une action de ligne — TOUJOURS le contexte d'où l'utilisateur venait.

    Le formulaire transporte son propre contexte (`retour`, champ caché contenant le mois et les
    filtres de l'écran d'origine). Sans lui, une correction faite depuis un rapprochement filtré
    renverrait sur un écran non filtré : l'utilisateur perdrait sa place et devrait refaire ses
    filtres à chaque ligne traitée. Seul un chemin interne est accepté : une valeur externe serait
    une redirection ouverte.
    """
    brut = str(form.get("retour", "") or "").strip()
    if brut.startswith("/") and not brut.startswith("//"):
        return brut
    return defaut


@router.post("/menages/actualiser")
async def menages_actualiser(request: Request):
    """« Actualiser le rapprochement des ménages » — L'UNIQUE action de l'écran, SYNCHRONE pour
    l'utilisateur, MAIS HORS BOUCLE ÉVÉNEMENTIELLE pour le serveur.

    Le workflow complet (PDF → Google Sheet → Hostaway → ciblage → recalcul des mois OUVERTS →
    rapprochement) s'exécute avant que la redirection n'ait lieu : l'écran affiche l'état réel dès
    son affichage, comme avant. La différence : `actualisation.actualiser(...)` est une fonction
    SYNCHRONE BLOQUANTE (sous-processus PDF/Sheet, appel API Hostaway) — l'appeler directement dans une
    route `async def` bloquerait l'UNIQUE boucle événementielle d'uvicorn pendant toute sa durée,
    empêchant même un `GET /` sans rapport d'être servi (constaté en recette : le serveur entier
    cessait de répondre). `asyncio.to_thread` déporte l'appel bloquant sur un thread du pool ; le
    reste du serveur continue de répondre pendant ce temps. `asyncio.wait_for` borne la durée que
    CETTE requête attend avant de rendre un message d'échec propre — le thread sous-jacent n'est PAS
    tué au dépassement (impossible en CPython) : il continue jusqu'à sa fin naturelle, protégé
    contre un second déclenchement concurrent par le verrou DB pris dans le service lui-même.

    `mois` est le mois filtré à l'écran, transmis par un champ caché : il arrive en corps
    `application/x-www-form-urlencoded`, jamais en query string, d'où la lecture via
    `request.form()` plutôt qu'un paramètre de fonction FastAPI. Absent, on retombe sur le mois par
    défaut de l'écran — jamais sur le comportement implicite de lot6d/6e/6f.
    """
    form = await request.form()
    mois_cible = str(form.get("mois", "") or "").strip() or svc.periode_par_defaut()
    orch.marquer_runs_interrompus()
    try:
        resultat = await asyncio.wait_for(
            asyncio.to_thread(actualisation.actualiser, mois_affiche=mois_cible,
                              acteur="ui:menages", declencheur=orch.DECLENCHEUR_MANUEL),
            timeout=cfg.MENAGES_ACTUALISER_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        # Le thread continue en tâche de fond (non tuable) — le verrou DB (§36, pris dans le
        # service) empêche un nouveau clic de lancer une seconde chaîne tant qu'il tourne encore.
        message = (f"Délai dépassé ({cfg.MENAGES_ACTUALISER_TIMEOUT_SECONDS}s) : l'actualisation "
                   "n'a pas répondu à temps. Réessayez dans quelques instants.")
        return RedirectResponse(
            url=f"/menages?mois={mois_cible}&actualisation=echec&resume={quote(message)}",
            status_code=303)

    # Le résumé est passé en query string parce qu'il est COURT et calculé à partir de ce qui a
    # réellement été fait — jamais un compteur figé dans le gabarit.
    resume = " · ".join(resultat["statistiques"])
    # « terminee » (succès plein) n'est utilisé QUE si les trois sources et tous les mois ciblés ont
    # réellement réussi (mission §8) : un statut PARTIEL/ECHEC ne doit JAMAIS afficher le message de
    # succès, même si des statistiques partielles existent.
    statut = resultat.get("statut", "SUCCES" if resultat.get("ok") else "ECHEC")
    marqueur = {"SUCCES": "terminee", "PARTIEL": "partielle", "ECHEC": "echec"}.get(statut, "echec")
    return RedirectResponse(
        url=f"/menages?mois={mois_cible}&actualisation={marqueur}&resume={quote(resume)}",
        status_code=303)


# « Actualiser toute l'activité » (mode GLOBAL, mission §10) existe déjà : /actualisation/tout
# (app/routes/actualisation.py, `orch.actualiser(cibles=None, ...)`). Pas de second chemin ici —
# l'écran /menages y renvoie un lien distinct plutôt que de dupliquer l'implémentation.


# Saisie manuelle des déclarations internes retirée de l'UI (mission « simplifier ménages ») :
# le Google Sheet reste la seule source de déclaration. `declarations.modifier()` reste utilisé
# par /menages/{mois}/{logement}/{intervenant}/modifier-declaration (correction, pas création).


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


# « Actualiser l'affichage » a été SUPPRIMÉ, bouton et route. Il n'existait que pour compenser un
# recalcul en tâche de fond : l'écran revenait avant la fin du traitement, et l'utilisateur devait
# rafraîchir lui-même pour voir le résultat. Le workflow étant désormais synchrone, l'écran affiché
# après « Actualiser le rapprochement des ménages » est déjà à jour — un bouton pour actualiser
# l'affichage n'aurait plus rien à actualiser. Le cache est invalidé par le workflow lui-même.


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
def menage_detail(request: Request, mois: str, logement_id: str, intervenant_id: str,
                  retour: str = ""):
    detail = svc.load_reconciliation_detail(mois, logement_id, intervenant_id)
    # `retour` est reçu du tableau et RENVOYÉ dans les formulaires : le contexte de l'utilisateur
    # traverse l'aller-retour au lieu d'être perdu à la première action.
    retour = retour if retour.startswith("/") and not retour.startswith("//") else ""
    if detail is None:
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages",
            "detail": None, "retour": retour,
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
        }, status_code=404)

    return templates.TemplateResponse(request, "menages_detail.html", {
        "active_menu": "menages",
        "detail": detail, "retour": retour,
        "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
        "outrepassage_error": None,
    })


@router.post("/menages/{mois}/{logement_id}/{intervenant_id}/modifier-declaration",
             response_class=HTMLResponse)
async def menage_modifier_declaration(request: Request, mois: str,
                                      logement_id: str, intervenant_id: str):
    """Modifie nb_menages/supplément d'une déclaration interne existante (§A/A1/A2).

    Justification obligatoire dès que le supplément final est != 0 (refus sinon, code
    E_JUSTIFICATION_REQUISE) ; jamais de facture/charge/écriture créée ici (§A3). Le mois impacté
    est TOUJOURS celui de la déclaration modifiée (`mois`, dans l'URL) — le recalcul ciblé déclenché
    en tâche de fond porte sur ce seul mois, jamais sur le dernier mois disponible ni sur tout
    l'historique (mission « recalcul ménages réellement mensuel et ciblé », §5/§14)."""
    form = await request.form()
    nb_menages_raw = str(form.get("nb_menages", "")).strip()
    supplement_raw = str(form.get("supplement", "")).strip()
    justification = str(form.get("justification_supplement", "")).strip()

    resultat = declarations.modifier(
        mois=mois, logement_id=logement_id, intervenant_id=intervenant_id,
        nb_menages=int(nb_menages_raw) if nb_menages_raw else None,
        supplement=float(supplement_raw) if supplement_raw else None,
        justification_supplement=justification, acteur="ui:menages",
    )
    if not resultat.get("ok"):
        detail = svc.load_reconciliation_detail(mois, logement_id, intervenant_id)
        return templates.TemplateResponse(request, "menages_detail.html", {
            "active_menu": "menages", "detail": detail,
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            "outrepassage_error": None, "declaration_error": resultat.get("message"),
        }, status_code=422)
    # Recalcul SYNCHRONE, comme le bouton unique : l'écran suivant montre l'état réel, jamais un
    # état intermédiaire que l'utilisateur devrait rafraîchir lui-même. Même chemin bloquant que
    # /menages/actualiser : déporté hors boucle événementielle (`asyncio.to_thread`), même plafond
    # de requête (§1/§2 mission spinner infini) — cette route appelle la MÊME fonction bloquante.
    try:
        await asyncio.wait_for(
            asyncio.to_thread(actualisation.actualiser, mois_affiche=mois, acteur="ui:menages",
                              declencheur=orch.DECLENCHEUR_MANUEL),
            timeout=cfg.MENAGES_ACTUALISER_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        pass  # Le thread continue ; le verrou DB protège contre une seconde chaîne concurrente.
    retour = _retour_contexte(form, f"/menages/{mois}/{logement_id}/{intervenant_id}")
    separateur = "&" if "?" in retour else "?"
    return RedirectResponse(url=f"{retour}{separateur}declaration=modifiee", status_code=303)


@router.get("/menages/conflits", response_class=HTMLResponse)
def menages_conflits_liste(request: Request, statut: str = "OUVERT"):
    return templates.TemplateResponse(request, "menages_conflits.html", {
        "active_menu": "menages", "conflits": declarations.lister_conflits(statut=statut),
        "statut": statut,
    })


@router.post("/menages/conflits/{conflit_id}/resoudre")
async def menages_conflit_resoudre(request: Request, conflit_id: int):
    form = await request.form()
    choix = str(form.get("choix", "")).strip()
    res = declarations.resoudre_conflit(conflit_id, choix=choix, acteur="ui:menages")
    suffixe = "" if res.get("ok") else f"&erreur={res.get('message')}"
    return RedirectResponse(url=f"/menages/conflits?statut=OUVERT{suffixe}", status_code=303)


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

    retour = _retour_contexte(form_data, f"/menages/{mois}/{logement_id}/{intervenant_id}")
    separateur = "&" if "?" in retour else "?"
    return RedirectResponse(url=f"{retour}{separateur}outrepassage=ok", status_code=303)

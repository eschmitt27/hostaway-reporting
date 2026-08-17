"""Routes Banques & Caisse (APP-4A/4B) + import et rapprochement (module Banque, suite APP-3F).

Les routes de LECTURE (dashboard, à rapprocher, export, fiche APP-4A) n'écrivent jamais : elles
lisent les sorties du pipeline banque (lot8a/8b/8c). Les routes d'IMPORT et de RAPPROCHEMENT
écrivent, mais uniquement sous garde (`BANQUE_REAL_WRITE_*` + write-guard mode recette) — jamais de
connexion bancaire, jamais de virement, jamais d'écriture hors `data_recette`.
"""
import app.config as cfg
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import banques_service as svc
from app.services import banques_controle_service as ctrl
from app.services import banques_controle_writer as writer
from app.services import banques_import_service as imp
from app.services import banques_rapprochement_service as rappro
from app.services import banques_suggestions_service as sugg
from app.services import banques_candidats_service as candidats
from app.services import banques_controles_catalogue_service as catalogue
from app.services import banques_classement_service as classement
from app.readers.banques_reader import date_affichage, datetime_affichage

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["date_fr"] = date_affichage
templates.env.filters["datetime_fr"] = datetime_affichage
# Identifiant mouvement opaque pour tout lien généré (jamais le mouvement_id brut, qui contient le
# compte, dans un href/option/HTML visible). Les nouvelles pages ne génèrent que des MVT-<hash>.
templates.env.filters["mvt_opaque"] = ctrl.id_opaque


def _ecriture_active() -> bool:
    return bool(cfg.BANQUE_REAL_WRITE_ENABLED and cfg.BANQUE_REAL_WRITE_CONFIRMATION_ENABLED)


def _mouvement_pour_suggestions(id_opaque: str) -> dict | None:
    """Vue minimale d'un mouvement, suffisante pour le moteur de suggestions."""
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return None
    return {"id_opaque": id_opaque, "montant": fiche["montant"],
            "date_operation": fiche["date_operation"], "libelle": fiche["libelle"],
            "sens": fiche.get("sens", "")}


def _suggestions(id_opaque: str) -> list[dict]:
    mvt = _mouvement_pour_suggestions(id_opaque)
    if mvt is None:
        return []
    try:
        return sugg.suggerer(mvt, candidats.candidats_pour(mvt))
    except Exception:
        return []      # une source de candidats indisponible ne casse jamais la page


def _groupes(id_opaque: str) -> dict:
    """Propositions de rapprochement groupé (≥2 objets) — jamais une confirmation automatique,
    jamais un second moteur : délègue entièrement à banques_rapprochement_service.

    La recherche est faite SÉPARÉMENT par type d'objet (deux types d'objets différents n'ont
    jamais vocation à être groupés ensemble) — sinon un pool hétérogène de candidats épuiserait
    la limite d'itérations bornée avant même d'atteindre les objets réellement pertinents pour ce
    mouvement. `candidats_pour()` ne fournit jamais de candidat de type RESERVATION (règle métier
    2026-08-08 : un virement bancaire n'est jamais rapproché d'une réservation individuelle)."""
    mvt = _mouvement_pour_suggestions(id_opaque)
    if mvt is None:
        return {"groupes": [], "ambigu": False, "limite_atteinte": False}
    try:
        tous_candidats = candidats.candidats_pour(mvt)
    except Exception:
        return {"groupes": [], "ambigu": False, "limite_atteinte": False}

    import json as _json
    from collections import defaultdict
    par_type: dict[str, list] = defaultdict(list)
    for c in tous_candidats:
        par_type[c["type_objet"]].append(c)

    groupes: list[dict] = []
    ambigu = False
    limite_atteinte = False
    for type_objet, sous_liste in par_type.items():
        if len(sous_liste) < 2:
            continue
        try:
            res = rappro.proposer_groupes(mvt, sous_liste)
        except Exception:
            continue
        ambigu = ambigu or res["ambigu"]
        limite_atteinte = limite_atteinte or res["limite_atteinte"]
        for g in res["groupes"]:
            if g["nb_objets"] < 2:
                continue
            g["affectations_json"] = _json.dumps([
                {"type_objet": o["type_objet"], "objet_id": o["objet_id"],
                 "montant_affecte": o["montant_affecte"]} for o in g["objets"]])
            groupes.append(g)
    return {"groupes": groupes, "ambigu": ambigu, "limite_atteinte": limite_atteinte}


def _form_to_decision(form) -> dict:
    """Extrait les champs de décision d'un formulaire (valeurs vides -> None)."""
    def g(k):
        v = str(form.get(k, "")).strip()
        return v or None
    return {
        "categorie": g("categorie"), "type_flux_id": g("type_flux_id"),
        "proprietaire_id": g("proprietaire_id"), "logement_id": g("logement_id"),
        "reservation_id": g("reservation_id"), "facture_id": g("facture_id"),
        "statut_controle": g("statut_controle"), "commentaire": g("commentaire"),
        "justification": g("justification"),
    }


@router.get("/banques-caisse", response_class=HTMLResponse)
def banques_dashboard(
    request: Request,
    mois: str = "",
    compte_id: str = "",
    sens: str = "",
    statut: str = "",
    non_rapproche: bool = False,
    montant_min: str = "",
    montant_max: str = "",
    recherche: str = "",
    tri: str = "anomalie",
    page: int = 1,
):
    data = svc.load_dashboard(
        mois=mois, compte_id=compte_id, sens=sens, statut=statut, non_rapproche=non_rapproche,
        montant_min=montant_min, montant_max=montant_max, recherche=recherche, tri=tri, page=page,
    )
    return templates.TemplateResponse(request, "banques_list.html", {
        "active_menu": "banques", "data": data, "nb_a_controler": ctrl.compter_a_controler(),
        "ecriture_active": _ecriture_active(), "airbnb": svc.categorisation_versements_airbnb(),
        "nb_a_classer": classement.compter(),
    })


@router.get("/banques-caisse/a-rapprocher", response_class=HTMLResponse)
def banques_a_rapprocher(request: Request, mois: str = ""):
    if not mois:
        mois = svc.periode_par_defaut()
    data = svc.load_unmatched(mois)
    # Compteurs applicatifs (journal SQLite banque_rapprochements) — distincts des statuts du
    # moteur affichés dans `data` : jamais mélangés, toujours étiquetés séparément dans le template.
    compteurs_appli = {"non_rapproches": 0, "partiels": 0, "rapproches": 0}
    resume_suggestions: dict[str, dict] = {}
    for m in data.get("lignes", []):
        opaque = ctrl.id_opaque(m["mouvement_id"])
        etat = rappro.etat_rapprochement(opaque, m["montant"] or 0)
        compteurs_appli[{"NON_RAPPROCHE": "non_rapproches", "PARTIEL": "partiels",
                         "RAPPROCHE": "rapproches"}[etat["statut"]]] += 1
        props = _suggestions(opaque)
        resume_suggestions[m["mouvement_id"]] = {
            "opaque": opaque, "nb": len(props), "meilleure": props[0] if props else None,
            "montant_disponible": etat["montant_restant"],
        }
    return templates.TemplateResponse(request, "banques_a_rapprocher.html", {
        "active_menu": "banques", "data": data, "compteurs_appli": compteurs_appli,
        "resume_suggestions": resume_suggestions,
    })


@router.get("/banques-caisse/export.csv")
def banques_export_csv(
    request: Request,
    mois: str = "",
    compte_id: str = "",
    sens: str = "",
    statut: str = "",
    non_rapproche: bool = False,
    montant_min: str = "",
    montant_max: str = "",
    recherche: str = "",
    tri: str = "anomalie",
):
    contenu = svc.export_movements_csv(
        mois=mois, compte_id=compte_id, sens=sens, statut=statut, non_rapproche=non_rapproche,
        montant_min=montant_min, montant_max=montant_max, recherche=recherche, tri=tri,
    )
    nom = f"banques_mouvements_{mois or 'tous'}.csv"
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})


# ── APP-4B — Contrôle & catégorisation (fiche actionnable, ID opaque, writer copies) ──
# Déclaré AVANT « /mouvements/{stable_id} » : « /mouvements/{id}/modifier » etc. ont un segment
# supplémentaire ; la fiche opaque est servie par banque_detail (délégation ci-dessous).

@router.get("/banques-caisse/mouvements/{id_opaque}/modifier", response_class=HTMLResponse)
def banque_mouvement_modifier(request: Request, id_opaque: str):
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return templates.TemplateResponse(request, "banques_mouvement.html", {
            "active_menu": "banques", "fiche": None, "id_opaque": id_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_mouvement_modifier.html", {
        "active_menu": "banques", "fiche": fiche, "options": ctrl.options_reference(),
    })


@router.post("/banques-caisse/mouvements/{id_opaque}/previsualiser", response_class=HTMLResponse)
async def banque_mouvement_previsualiser(request: Request, id_opaque: str):
    form = await request.form()
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return templates.TemplateResponse(request, "banques_mouvement.html", {
            "active_menu": "banques", "fiche": None, "id_opaque": id_opaque,
        }, status_code=404)
    decision = _form_to_decision(form)
    # Prévisualisation : ne journalise rien, montre la décision proposée à côté de la proposition moteur.
    return templates.TemplateResponse(request, "banques_mouvement_modifier.html", {
        "active_menu": "banques", "fiche": fiche, "options": ctrl.options_reference(),
        "previsualisation": decision,
    })


@router.post("/banques-caisse/mouvements/{id_opaque}/enregistrer-copie")
async def banque_mouvement_enregistrer(request: Request, id_opaque: str):
    form = await request.form()
    decision = _form_to_decision(form)
    try:
        ctrl.enregistrer_decision(id_opaque, version_attendue=None, **decision)
    except ctrl.DecisionRefusee as exc:
        fiche = ctrl.load_fiche(id_opaque)
        return templates.TemplateResponse(request, "banques_mouvement_modifier.html", {
            "active_menu": "banques", "fiche": fiche, "options": ctrl.options_reference(),
            "erreur": str(exc), "previsualisation": decision,
        }, status_code=200)
    # Journalise l'application des décisions actives, puis affiche le run. Rien n'est réécrit : les
    # décisions vivent en base, et la vue de lecture les applique par-dessus la classification.
    resultat = writer.appliquer_decisions()
    run_id = resultat.get("run_id")
    if run_id is None:
        return templates.TemplateResponse(request, "banques_action_run.html", {
            "active_menu": "banques", "run": None, "erreur": resultat.get("message"),
        }, status_code=200)
    return RedirectResponse(url=f"/banques-caisse/actions/{run_id}?mvt={id_opaque}", status_code=303)


@router.get("/banques-caisse/actions/{run_id}", response_class=HTMLResponse)
def banque_action_run(request: Request, run_id: int, mvt: str = ""):
    run = writer.load_run(run_id)
    if run is None:
        return templates.TemplateResponse(request, "banques_action_run.html", {
            "active_menu": "banques", "run": None, "run_id": run_id,
        }, status_code=404)
    fiche = ctrl.load_fiche(mvt) if mvt else None
    return templates.TemplateResponse(request, "banques_action_run.html", {
        "active_menu": "banques", "run": run, "fiche": fiche,
    })


@router.get("/banques-caisse/mouvements/{stable_id}", response_class=HTMLResponse)
def banque_detail(request: Request, stable_id: str, message: str = "", erreur: str = ""):
    # Délégation APP-4B : un identifiant OPAQUE (MVT-<hash12>) ouvre la fiche actionnable.
    if ctrl.resoudre_opaque(stable_id) is not None:
        fiche = ctrl.load_fiche(stable_id)
        liens = rappro.lister(stable_id) if fiche else []
        etat = rappro.etat_rapprochement(stable_id, fiche["montant"]) if fiche else None
        # Contexte métier (facture / règlement / fournisseur) — importé paresseusement pour ne pas
        # créer de dépendance circulaire entre les routes Banque et le module Factures.
        try:
            from app.services import factures_banque_service as pont_factures
            contexte_metier = pont_factures.contexte_metier_du_mouvement(stable_id)
        except Exception:
            contexte_metier = []
        groupes_ctx = _groupes(stable_id)
        return templates.TemplateResponse(request, "banques_mouvement.html", {
            "active_menu": "banques", "fiche": fiche, "liens_rapprochement": liens,
            "etat_rapprochement": etat, "types_objet": rappro.TYPES_OBJET,
            "suggestions": _suggestions(stable_id),
            "historique_suggestions": sugg.historique_decisions(stable_id),
            "contexte_metier": contexte_metier,
            "groupes_proposes": groupes_ctx["groupes"], "groupes_ambigu": groupes_ctx["ambigu"],
            "groupes_limite_atteinte": groupes_ctx["limite_atteinte"],
            "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
        })
    # Sinon : ancienne fiche APP-4A (lecture seule) — non utilisée dans la nouvelle interface.
    detail = svc.load_detail(stable_id)
    if detail is None:
        return templates.TemplateResponse(request, "banques_detail.html", {
            "active_menu": "banques", "detail": None, "stable_id": stable_id,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_detail.html", {
        "active_menu": "banques", "detail": detail, "stable_id": stable_id,
    })


@router.get("/banques-caisse/controle", response_class=HTMLResponse)
def banque_controle_liste(request: Request, statut: str = "", categorie: str = "",
                          proprietaire_id: str = "", logement_id: str = "",
                          anomalie: str = "", mois: str = ""):
    data = ctrl.load_liste(statut=statut, categorie=categorie, proprietaire_id=proprietaire_id,
                           logement_id=logement_id, anomalie=anomalie, mois=mois)
    return templates.TemplateResponse(request, "banques_controle_liste.html", {
        "active_menu": "banques", "data": data,
        "applied": {"statut": statut, "categorie": categorie, "proprietaire_id": proprietaire_id,
                    "logement_id": logement_id, "anomalie": anomalie, "mois": mois},
    })


# ── Import bancaire (suite APP-3F) — upload → prévisualisation → confirmation ──

@router.get("/banques-caisse/importer", response_class=HTMLResponse)
def banque_importer_form(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "banques_importer.html", {
        "active_menu": "banques", "ecriture_active": _ecriture_active(),
        "historique": imp.historique_imports(limit=10), "message": message, "erreur": erreur,
    })


@router.post("/banques-caisse/importer/previsualiser", response_class=HTMLResponse)
async def banque_importer_previsualiser(request: Request):
    form = await request.form()
    compte_id = str(form.get("compte_id", "") or "").strip()
    fichier = form.get("fichier")
    if fichier is None or not getattr(fichier, "filename", ""):
        return RedirectResponse(url="/banques-caisse/importer?erreur=Aucun fichier sélectionné.",
                                status_code=303)
    contenu = await fichier.read()
    res = imp.previsualiser(contenu, fichier.filename, compte_id)
    if not res.get("ok"):
        return RedirectResponse(url=f"/banques-caisse/importer?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(url=f"/banques-caisse/importer/previsualisation/{res['token']}",
                            status_code=303)


@router.get("/banques-caisse/importer/previsualisation/{token}", response_class=HTMLResponse)
def banque_importer_previsualisation(request: Request, token: str):
    manifest = imp._charger_manifest(token)
    if manifest is None:
        return templates.TemplateResponse(request, "banques_importer_previsualisation.html", {
            "active_menu": "banques", "manifest": None, "token": token,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_importer_previsualisation.html", {
        "active_menu": "banques", "manifest": manifest, "token": token,
        "ecriture_active": _ecriture_active(),
    })


@router.post("/banques-caisse/importer/confirmer/{token}")
async def banque_importer_confirmer(request: Request, token: str):
    form = await request.form()
    justifier = str(form.get("justifier_doublons_probables", "") or "") in ("1", "on", "true")
    res = imp.confirmer(token, justifier_doublons_probables=justifier,
                        acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(
            url=f"/banques-caisse/importer/previsualisation/{token}?erreur={res.get('message')}",
            status_code=303)
    return templates.TemplateResponse(request, "banques_importer_resultat.html", {
        "active_menu": "banques", "resultat": res,
    })


# ── Rapprochement (suite APP-4A/4B) — lien mouvement <-> objet métier ────────

@router.post("/banques-caisse/mouvements/{id_opaque}/rapprocher")
async def banque_mouvement_rapprocher(request: Request, id_opaque: str):
    form = await request.form()
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?erreur=Mouvement introuvable.",
                                status_code=303)
    res = rappro.enregistrer(
        id_opaque, str(form.get("type_objet", "") or ""), str(form.get("objet_id", "") or "").strip(),
        float(form.get("montant_rapproche") or 0) if str(form.get("montant_rapproche", "")).strip() else 0,
        montant_mouvement=fiche["montant"], statut=rappro.ST_PROPOSE, source="MANUEL",
        commentaire=str(form.get("commentaire", "") or ""), acteur=str(form.get("acteur", "") or "local"),
    )
    if not res.get("ok"):
        return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?message=Rapprochement proposé.",
                            status_code=303)


@router.post("/banques-caisse/mouvements/{id_opaque}/groupes/confirmer")
async def banque_groupe_confirmer(request: Request, id_opaque: str):
    """Confirme UN groupe proposé par `proposer_groupes` — jamais automatique, jamais un
    sur-règlement (revalidé côté service), jamais une consommation double."""
    import json as _json
    form = await request.form()
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?erreur=Mouvement introuvable.",
                                status_code=303)
    try:
        affectations = _json.loads(str(form.get("affectations", "") or "[]"))
    except (ValueError, TypeError):
        return RedirectResponse(
            url=f"/banques-caisse/mouvements/{id_opaque}?erreur=Groupe invalide.", status_code=303)
    res = rappro.confirmer_groupe(id_opaque, fiche["montant"], affectations, statut=rappro.ST_PROPOSE,
                                  source="MANUEL", acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(
        url=f"/banques-caisse/mouvements/{id_opaque}?message=Groupe proposé ({res['nb_objets']} objets) — à confirmer individuellement.",
        status_code=303)


@router.post("/banques-caisse/rapprochements/{opaque}/confirmer")
async def banque_rapprochement_confirmer(request: Request, opaque: str, mvt: str = ""):
    form = await request.form()
    res = rappro.confirmer(opaque, commentaire=str(form.get("commentaire", "") or ""),
                           acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Rapprochement confirmé." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/banques-caisse/mouvements/{mvt}?{msg}", status_code=303)


@router.post("/banques-caisse/rapprochements/{opaque}/refuser")
async def banque_rapprochement_refuser(request: Request, opaque: str, mvt: str = ""):
    form = await request.form()
    res = rappro.refuser(opaque, commentaire=str(form.get("commentaire", "") or ""),
                         acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Rapprochement refusé." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/banques-caisse/mouvements/{mvt}?{msg}", status_code=303)


@router.post("/banques-caisse/rapprochements/{opaque}/annuler")
async def banque_rapprochement_annuler(request: Request, opaque: str, mvt: str = ""):
    form = await request.form()
    res = rappro.annuler(opaque, commentaire=str(form.get("commentaire", "") or ""),
                         acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Rapprochement annulé." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/banques-caisse/mouvements/{mvt}?{msg}", status_code=303)


# ── Suggestions de rapprochement — jamais de validation silencieuse ──────────

def _retrouver_suggestion(id_opaque: str, type_objet: str, objet_id: str) -> dict | None:
    for s in _suggestions(id_opaque):
        if s["type_objet"] == type_objet and str(s.get("objet_id") or "") == objet_id:
            return s
    return None


@router.post("/banques-caisse/mouvements/{id_opaque}/suggestions/accepter")
async def banque_suggestion_accepter(request: Request, id_opaque: str):
    form = await request.form()
    mvt = _mouvement_pour_suggestions(id_opaque)
    s = _retrouver_suggestion(id_opaque, str(form.get("type_objet", "") or ""),
                              str(form.get("objet_id", "") or ""))
    if mvt is None or s is None:
        return RedirectResponse(
            url=f"/banques-caisse/mouvements/{id_opaque}?erreur=Suggestion introuvable ou expirée.",
            status_code=303)
    montant_txt = str(form.get("montant", "") or "").strip()
    montant = float(montant_txt) if montant_txt else None
    res = sugg.accepter(mvt, s, montant=montant, acteur=str(form.get("acteur", "") or "local"),
                        commentaire=str(form.get("commentaire", "") or ""))
    if not res.get("ok"):
        return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(
        url=f"/banques-caisse/mouvements/{id_opaque}?message=Suggestion acceptée — rapprochement proposé, à confirmer.",
        status_code=303)


@router.post("/banques-caisse/mouvements/{id_opaque}/suggestions/refuser")
async def banque_suggestion_refuser(request: Request, id_opaque: str):
    form = await request.form()
    mvt = _mouvement_pour_suggestions(id_opaque)
    s = _retrouver_suggestion(id_opaque, str(form.get("type_objet", "") or ""),
                              str(form.get("objet_id", "") or ""))
    if mvt is None or s is None:
        return RedirectResponse(
            url=f"/banques-caisse/mouvements/{id_opaque}?erreur=Suggestion introuvable ou expirée.",
            status_code=303)
    definitif = str(form.get("definitif", "1") or "1") in ("1", "on", "true")
    sugg.refuser(mvt, s, definitif=definitif, commentaire=str(form.get("commentaire", "") or ""),
                 acteur=str(form.get("acteur", "") or "local"))
    libelle = "refusée" if definitif else "ignorée temporairement"
    return RedirectResponse(url=f"/banques-caisse/mouvements/{id_opaque}?message=Suggestion {libelle}.",
                            status_code=303)


# ── Contrôles Banque (catalogue applicatif, complète les contrôles moteur) ───

@router.get("/banques-caisse/controles", response_class=HTMLResponse)
def banque_controles(request: Request, severite: str = ""):
    data = catalogue.controler()
    anomalies = data["anomalies"]
    if severite:
        anomalies = [a for a in anomalies if a["severite"] == severite]
    return templates.TemplateResponse(request, "banques_controles.html", {
        "active_menu": "banques", "data": data, "anomalies": anomalies,
        "applied": {"severite": severite},
        "severites": [catalogue.BLOQUANT, catalogue.CRITIQUE, catalogue.AVERTISSEMENT, catalogue.INFO],
    })


# ── File humaine de classement (mouvements A_ENVOYER_IA) — aucune IA externe ─

@router.get("/banques-caisse/a-classer", response_class=HTMLResponse)
def banque_a_classer_liste(
    request: Request, periode: str = "", montant_min: float | None = None,
    montant_max: float | None = None, sens: str = "", categorie: str = "",
    statut_humain: str = "", sans_decision: bool = False, page: int = 1,
):
    lignes = classement.lister(
        periode=periode, montant_min=montant_min, montant_max=montant_max, sens=sens,
        categorie=categorie, statut_humain=statut_humain, uniquement_sans_decision=sans_decision)
    page = max(1, page)
    taille = 20
    total = len(lignes)
    page_lignes = lignes[(page - 1) * taille: page * taille]
    return templates.TemplateResponse(request, "banques_a_classer_list.html", {
        "active_menu": "banques", "lignes": page_lignes, "total": total, "page": page,
        "page_size": taille, "periode": periode, "montant_min": montant_min,
        "montant_max": montant_max, "sens": sens, "categorie": categorie,
        "statut_humain": statut_humain, "sans_decision": sans_decision,
        "categories": classement.categories_disponibles(), "types_decision": classement.TYPES_DECISION,
    })


@router.get("/banques-caisse/a-classer/{id_opaque}", response_class=HTMLResponse)
def banque_a_classer_detail(request: Request, id_opaque: str, erreur: str = ""):
    d = classement.charger(id_opaque)
    if d is None:
        return templates.TemplateResponse(request, "banques_a_classer_detail.html", {
            "active_menu": "banques", "mouvement": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_a_classer_detail.html", {
        "active_menu": "banques", "mouvement": d, "erreur": erreur,
        "categories": classement.categories_disponibles(), "types_decision": classement.TYPES_DECISION,
    })


@router.post("/banques-caisse/a-classer/{id_opaque}/previsualiser", response_class=HTMLResponse)
async def banque_a_classer_previsualiser(request: Request, id_opaque: str):
    form = await request.form()
    champs = {
        "type_decision": str(form.get("type_decision", "") or ""),
        "nouvelle_categorie": str(form.get("nouvelle_categorie", "") or ""),
        "justification": str(form.get("justification", "") or ""),
        "anomalie_moteur": str(form.get("anomalie_moteur", "") or ""),
        "future_regle": str(form.get("future_regle", "") or ""),
    }
    res = classement.previsualiser(id_opaque, champs["type_decision"],
                                   nouvelle_categorie=champs["nouvelle_categorie"],
                                   justification=champs["justification"],
                                   anomalie_moteur=champs["anomalie_moteur"],
                                   future_regle=champs["future_regle"])
    if not res["ok"]:
        return RedirectResponse(
            url=f"/banques-caisse/a-classer/{id_opaque}?erreur={res['message']}", status_code=303)
    return templates.TemplateResponse(request, "banques_a_classer_previsualisation.html", {
        "active_menu": "banques", "id_opaque": id_opaque, "apercu": res["apercu"], "champs": champs,
    })


@router.post("/banques-caisse/a-classer/{id_opaque}/decision")
async def banque_a_classer_decision(request: Request, id_opaque: str):
    form = await request.form()
    res = classement.decider(
        id_opaque, str(form.get("type_decision", "") or ""),
        nouvelle_categorie=str(form.get("nouvelle_categorie", "") or ""),
        justification=str(form.get("justification", "") or ""),
        anomalie_moteur=str(form.get("anomalie_moteur", "") or ""),
        future_regle=str(form.get("future_regle", "") or ""), acteur="local")
    if not res["ok"]:
        return RedirectResponse(
            url=f"/banques-caisse/a-classer/{id_opaque}?erreur={res['message']}", status_code=303)
    return RedirectResponse(url=f"/banques-caisse/a-classer/{id_opaque}", status_code=303)


@router.get("/banques-caisse/a-classer/{id_opaque}/historique", response_class=HTMLResponse)
def banque_a_classer_historique(request: Request, id_opaque: str):
    d = classement.charger(id_opaque)
    if d is None:
        return templates.TemplateResponse(request, "banques_a_classer_historique.html", {
            "active_menu": "banques", "mouvement": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_a_classer_historique.html", {
        "active_menu": "banques", "mouvement": d, "historique": classement.historique(id_opaque),
    })

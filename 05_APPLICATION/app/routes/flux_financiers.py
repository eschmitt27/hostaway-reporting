"""Module « Flux financiers » : Banque | Caisse | Charges | Rapprochement.

Une seule entrée de navigation, quatre pages techniquement distinctes. On fusionne l'EXPÉRIENCE,
pas le modèle : chaque page lit ses objets canoniques par `flux_financiers_service`, et toute
décision passe par `flux_lettrage_service` (rapprochement + comptabilisation, tout ou rien).

Qonto reste en lecture seule : le seul appel bancaire est l'actualisation, qui ne fait que des GET.
"""
from __future__ import annotations

from urllib.parse import quote_plus, urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from app.services import flux_financiers_service as flux
from app.services import flux_lettrage_service as lettrage
from app.services import flux_matching_service as matching
from app.template_env import get_templates

router = APIRouter()
templates = get_templates()

ONGLETS = (("banque", "Banque", "/flux-financiers/banque"),
           ("caisse", "Caisse", "/flux-financiers/caisse"),
           ("charges", "Charges", "/flux-financiers/charges"),
           ("rapprochement", "Rapprochement", "/flux-financiers/rapprochement"))

FILTRES_RAPPROCHEMENT = (("", "Tous"), ("MATCHE", "Matchés"), ("NON_MATCHE", "Non matchés"),
                         ("PARTIEL", "Partiels"), ("RAPPROCHE", "Rapprochés"))


def _contexte(onglet: str, **extra) -> dict:
    return {"active_menu": "flux", "flux_onglet": onglet, "flux_onglets": ONGLETS,
            "statuts_rapprochement": flux.STATUTS_RAPPROCHEMENT,
            "statuts_compta": flux.STATUTS_COMPTA_MOUVEMENT, **extra}


def _montant(v: str) -> float | None:
    try:
        return round(float(str(v).replace(",", ".").replace(" ", "")), 2) if str(v).strip() else None
    except ValueError:
        return None


def _filtrer_mouvements(lignes: list[dict], *, mois="", rapprochement="", compta="", sens="",
                        q="", montant_min="", montant_max="", source="") -> list[dict]:
    mini, maxi = _montant(montant_min), _montant(montant_max)
    q = q.strip().lower()
    out = []
    for m in lignes:
        if source and m["source"] != source:
            continue
        if mois and m["mois"] != mois:
            continue
        if rapprochement and m["statut_rapprochement"] != rapprochement:
            continue
        if compta == "A_TRAITER":
            if not m.get("a_traiter"):
                continue
        elif compta and m["statut_compta"] != compta:
            continue
        if sens and m["sens"] != sens:
            continue
        if mini is not None and m["montant"] < mini:
            continue
        if maxi is not None and m["montant"] > maxi:
            continue
        if q and q not in (m["libelle"] + " " + m.get("reference", "") + " "
                           + m.get("texte_recherche", "")).lower():
            continue
        out.append(m)
    return out


def _kpis(lignes: list[dict]) -> dict:
    return {
        "a_qualifier": sum(1 for m in lignes if m["statut_compta"] == flux.A_QUALIFIER
                           and m["definitif"] and not m["sans_effet"]),
        "a_comptabiliser": sum(1 for m in lignes if m["statut_compta"] == flux.A_COMPTABILISER),
        "erreurs": sum(1 for m in lignes if m["statut_compta"] == flux.ERREUR),
        "matches": sum(1 for m in lignes if m["statut_rapprochement"] == flux.MATCHE),
    }


@router.get("/flux-financiers")
def flux_accueil():
    return RedirectResponse("/flux-financiers/banque", status_code=307)


# ══ Banque ════════════════════════════════════════════════════════════════════════════════════

@router.get("/flux-financiers/banque", response_class=HTMLResponse)
def flux_banque(request: Request, mois: str = "", rapprochement: str = "", compta: str = "",
                sens: str = "", q: str = "", message: str = "", erreur: str = ""):
    from app.adapters import qonto_client
    from app.services import qonto_ecran_service as ecran
    from app.services import qonto_raw_service as raw

    toutes = flux.mouvements(source=flux.BANQUE)
    lignes = _filtrer_mouvements(toutes, mois=mois, rapprochement=rapprochement, compta=compta,
                                 sens=sens, q=q)
    derniere = raw.derniere_synchronisation() if raw.tables_presentes() else {}
    try:
        from app.services import banques_service
        airbnb = banques_service.categorisation_versements_airbnb()
    except Exception:      # noqa: BLE001 — alerte métier facultative : jamais bloquante
        airbnb = None
    # Relevés Crédit Mutuel importés : leurs écrans dédiés restent joignables, compteurs compris.
    try:
        from app.services import banques_classement_service as classement
        from app.services import banques_controle_service as ctrl
        nb_a_controler, nb_a_classer = ctrl.compter_a_controler(), classement.compter()
    except Exception:      # noqa: BLE001
        nb_a_controler = nb_a_classer = 0
    return templates.TemplateResponse(request, "flux_banque.html", _contexte(
        "banque", lignes=lignes, nb_total=len(toutes), kpis=_kpis(toutes),
        soldes=flux.soldes(), encaisse=ecran.encaisse_caisse(),
        comptes=raw.comptes() if raw.tables_presentes() else [],
        identifiants_presents=qonto_client.identifiants_presents(), airbnb=airbnb,
        nb_a_controler=nb_a_controler, nb_a_classer=nb_a_classer,
        derniere_synchro=ecran._horodatage_lisible((derniere or {}).get("termine_le")
                                                   or (derniere or {}).get("demarre_le")),
        mois_disponibles=sorted({m["mois"] for m in toutes if m["mois"]}, reverse=True),
        filtres={"mois": mois, "rapprochement": rapprochement, "compta": compta, "sens": sens,
                 "q": q},
        message=message, erreur=erreur))


@router.post("/flux-financiers/banque/actualiser")
def flux_banque_actualiser():
    """Synchronisation Qonto (GET seulement), puis retour à la page Banque."""
    from app.services import qonto_ecran_service as ecran
    resultat = ecran.actualiser()
    if not resultat.get("ok"):
        return RedirectResponse("/flux-financiers/banque?erreur=" + quote_plus(
            resultat.get("message", "La synchronisation Qonto a échoué.")), status_code=303)
    return RedirectResponse("/flux-financiers/banque?message=" + quote_plus(
        "Données Qonto actualisées."), status_code=303)


def _detail(request: Request, source: str, identifiant: str, message: str, erreur: str):
    m = flux.mouvement(source, identifiant)
    onglet = "banque" if source == flux.BANQUE else "caisse"
    if m is None:
        return templates.TemplateResponse(request, "flux_mouvement.html", _contexte(
            onglet, m=None), status_code=404)
    props = matching.propositions(mouvement_id=identifiant)
    meilleure = None
    if props:
        p = props[0]
        meilleure = lettrage.preparer([f"{x['source']}:{x['id']}" for x in p["mouvements"]],
                                      [f"{x['type']}:{x['id']}" for x in p["objets"]],
                                      traitement_ecart=lettrage.SOLDE_OUVERT)
    candidats = [o for o in flux.objets()
                 if matching.compatibles(m, o)][:15] if m["lettrable"] else []
    return templates.TemplateResponse(request, "flux_mouvement.html", _contexte(
        onglet, m=m, propositions=props, meilleure=meilleure, candidats=candidats,
        historique=lettrage.lettrages(element_id=identifiant),
        liens=_liens_historiques(identifiant),
        message=message, erreur=erreur))


def _liens_historiques(identifiant: str) -> list[dict]:
    """Liens de rapprochement antérieurs au module (sans lettrage) : affichés, jamais cachés."""
    from app.services import banques_rapprochement_service as rappro
    from app.services import qonto_ecran_service as ecran
    noms = flux.noms_tiers()
    out = []
    for r in rappro.lister(identifiant):
        if r.get("lettrage_id_opaque"):
            continue
        objet = r.get("objet_id") or ""
        nature = ecran.LIBELLES_RAPPROCHEMENT.get(r["type_objet"], r["type_objet"].replace("_", " ").capitalize())
        out.append({"libelle": nature + (f" — {noms.get(objet, '')}" if noms.get(objet) else ""),
                    "montant": r["montant_rapproche"], "statut": r["statut"],
                    "date": flux.date_fr(r["date_creation"])})
    return out


@router.get("/flux-financiers/banque/{identifiant}", response_class=HTMLResponse)
def flux_banque_detail(request: Request, identifiant: str, message: str = "", erreur: str = ""):
    return _detail(request, flux.BANQUE, identifiant, message, erreur)


# ══ Caisse ════════════════════════════════════════════════════════════════════════════════════

@router.get("/flux-financiers/caisse", response_class=HTMLResponse)
def flux_caisse(request: Request, mois: str = "", rapprochement: str = "", compta: str = "",
                message: str = "", erreur: str = ""):
    from app.services import qonto_ecran_service as ecran
    toutes = flux.mouvements(source=flux.CAISSE)
    lignes = _filtrer_mouvements(toutes, mois=mois, rapprochement=rapprochement, compta=compta)
    return templates.TemplateResponse(request, "flux_caisse.html", _contexte(
        "caisse", lignes=lignes, nb_total=len(toutes), kpis=_kpis(toutes),
        encaisse=ecran.encaisse_caisse(), soldes=flux.soldes(),
        associes=flux.associes_connus(), proprietaires=flux.proprietaires_connus(),
        mois_disponibles=sorted({m["mois"] for m in toutes if m["mois"]}, reverse=True),
        filtres={"mois": mois, "rapprochement": rapprochement, "compta": compta},
        libelles_types=flux.LIBELLES_OPERATION_CAISSE, message=message, erreur=erreur))


@router.get("/flux-financiers/caisse/{identifiant}", response_class=HTMLResponse)
def flux_caisse_detail(request: Request, identifiant: str, message: str = "", erreur: str = ""):
    return _detail(request, flux.CAISSE, identifiant, message, erreur)


def _retour_caisse(ok: bool, texte: str) -> RedirectResponse:
    cle = "message" if ok else "erreur"
    return RedirectResponse(f"/flux-financiers/caisse?{cle}={quote_plus(texte)}", status_code=303)


@router.post("/flux-financiers/caisse/operations")
async def flux_caisse_creer(request: Request):
    """Nouveau mouvement de caisse — workflow existant : il naît BROUILLON, sans écriture."""
    from app.services import operations_caisse_service as caisse
    form = await request.form()
    type_op = str(form.get("type_operation", "") or "")
    tiers_type, tiers_id = "", ""
    if type_op == "REMBOURSEMENT_ASSOCIE" or str(form.get("tiers_type", "")) == "ASSOCIE":
        tiers_type, tiers_id = "ASSOCIE", str(form.get("associe_id", "") or "")
    elif str(form.get("tiers_type", "")) == "PROPRIETAIRE":
        tiers_type, tiers_id = "PROPRIETAIRE", str(form.get("proprietaire_id", "") or "")
    if not str(form.get("acteur", "") or "").strip():
        return _retour_caisse(False, "Indiquez votre nom : chaque mouvement de caisse est signé.")
    res = caisse.creer(type_op, form.get("montant"), date_operation=str(form.get("date_operation", "") or ""),
                       tiers_type=tiers_type, tiers_id=tiers_id, piece=str(form.get("piece", "") or ""),
                       commentaire=str(form.get("commentaire", "") or ""),
                       acteur=str(form.get("acteur", "") or "").strip())
    if not res.get("ok"):
        return _retour_caisse(False, res.get("message") or "Création refusée.")
    return _retour_caisse(True, "Mouvement de caisse enregistré en brouillon : qualifiez-le "
                                "(rapprochement) ou validez-le pour le comptabiliser.")


@router.post("/flux-financiers/caisse/operations/{identifiant}/valider")
async def flux_caisse_valider(request: Request, identifiant: str):
    """Valider = comptabiliser (§72 du service de caisse). Réservé aux opérations qualifiées par
    leur tiers ; une dépense se qualifie par le rapprochement avec une charge."""
    from app.services import operations_caisse_service as caisse
    form = await request.form()
    acteur = str(form.get("acteur", "") or "").strip()
    if not acteur:
        return _retour_caisse(False, "Indiquez votre nom pour valider.")
    op = caisse.charger(identifiant)
    if op is None:
        return _retour_caisse(False, "Opération introuvable.")
    if op["type_operation"] == "AUTRE":
        return _retour_caisse(False, "Une dépense en espèces se qualifie par le rapprochement "
                                     "avec une charge (onglet Rapprochement).")
    motif = flux.mois_cloture(str(op["date_operation"])[:7])
    if motif:
        return _retour_caisse(False, motif)
    res = caisse.valider(identifiant, acteur=acteur)
    return _retour_caisse(bool(res.get("ok")), "Mouvement de caisse comptabilisé." if res.get("ok")
                          else (res.get("detail") or res.get("message") or "Validation refusée."))


@router.post("/flux-financiers/caisse/operations/{identifiant}/abandonner")
async def flux_caisse_abandonner(request: Request, identifiant: str):
    from app.services import operations_caisse_service as caisse
    form = await request.form()
    res = caisse.annuler(identifiant, acteur=str(form.get("acteur", "") or "local"))
    return _retour_caisse(bool(res.get("ok")), "Brouillon abandonné." if res.get("ok")
                          else (res.get("detail") or res.get("message") or "Refusé."))


# ══ Charges ═══════════════════════════════════════════════════════════════════════════════════

@router.get("/flux-financiers/charges", response_class=HTMLResponse)
def flux_charges(request: Request, mois: str = "", logement_id: str = "",
                 categorie_charge_id: str = "", code_impact: str = "", statut_controle: str = "",
                 associe_id: str = "", compta: str = ""):
    from app.services import charges_service as svc
    data = svc.load_list(mois=mois, logement_id=logement_id,
                         categorie_charge_id=categorie_charge_id, code_impact=code_impact,
                         statut_controle=statut_controle, associe_id=associe_id)
    statuts = flux.statuts_charges()
    if compta and data.get("rows") is not None:
        data["rows"] = [r for r in data["rows"]
                        if statuts.get(r.get("charge_id"), {}).get("compta", {}).get("code") == compta]
        data["count_affiches"] = len(data["rows"])
    return templates.TemplateResponse(request, "fournisseurs_list.html", _contexte(
        "charges", data=data, statuts=statuts, filtre_compta=compta,
        statuts_compta_charge=flux.STATUTS_COMPTA_CHARGE))


# ══ Rapprochement ═════════════════════════════════════════════════════════════════════════════

@router.get("/flux-financiers/rapprochement", response_class=HTMLResponse)
def flux_rapprochement(request: Request, statut: str = "", mois: str = "", source: str = "",
                       sens: str = "", compta: str = "", tiers: str = "", montant_min: str = "",
                       montant_max: str = "", m: list[str] | None = Query(default=None),
                       message: str = "", erreur: str = ""):
    toutes = flux.mouvements()
    toutes = [x for x in toutes if not x["sans_effet"]]
    mouvements = _filtrer_mouvements(toutes, mois=mois, rapprochement=statut, compta=compta,
                                     sens=sens, q="", montant_min=montant_min,
                                     montant_max=montant_max, source=source)
    objets = flux.objets(inclure_non_rapprochables=True)
    tiers_q = tiers.strip().lower()
    if tiers_q:
        objets = [o for o in objets if tiers_q in (o.get("tiers", "") + " " + o["libelle"]).lower()]
        mouvements = [x for x in mouvements if tiers_q in (x["libelle"] + " "
                                                           + x.get("reference", "")).lower()]
    if sens:
        objets = [o for o in objets if o["sens"] == sens]
    if source:
        objets = [o for o in objets if source in o["sources"]]
    propositions = matching.propositions()
    ids_visibles = {x["id"] for x in mouvements}
    propositions = [p for p in propositions if any(x["id"] in ids_visibles for x in p["mouvements"])]
    compteurs = {code: sum(1 for x in toutes if x["statut_rapprochement"] == code)
                 for code, _ in FILTRES_RAPPROCHEMENT if code}
    compteurs[""] = len(toutes)
    autres = {"mois": mois, "source": source, "sens": sens, "compta": compta, "tiers": tiers,
              "montant_min": montant_min, "montant_max": montant_max}
    liens_statut = [(code, libelle, "/flux-financiers/rapprochement?" + urlencode(
        [(k, v) for k, v in {"statut": code, **autres}.items() if v]))
        for code, libelle in FILTRES_RAPPROCHEMENT]
    return templates.TemplateResponse(request, "flux_rapprochement.html", _contexte(
        "rapprochement", mouvements=mouvements, objets=objets, propositions=propositions,
        filtres={"statut": statut, "mois": mois, "source": source, "sens": sens, "compta": compta,
                 "tiers": tiers, "montant_min": montant_min, "montant_max": montant_max},
        filtres_statut=FILTRES_RAPPROCHEMENT, liens_statut=liens_statut, compteurs=compteurs,
        preselection=set(m or []),
        mois_disponibles=sorted({x["mois"] for x in toutes if x["mois"]}, reverse=True),
        recents=lettrage.lettrages()[:10], message=message, erreur=erreur))


def _modes_auxiliaires() -> dict:
    from app.services import comptabilite_plan_service as plan
    return plan.modes_auxiliaires()


def _page_validation(request: Request, m: list[str], o: list[str], *, traitement_ecart: str = "",
                     compte_ecart: str = "", proposition: str = "", erreurs: list | None = None,
                     lignes_saisies: list | None = None, justification: str = "", acteur: str = "",
                     message: str = "", erreur_flash: str = "", status_code: int = 200):
    prep = lettrage.preparer(m, o, traitement_ecart=traitement_ecart, compte_ecart=compte_ecart)
    prop = matching.proposition(proposition) if proposition else \
        matching.proposition(prep["empreinte_selection"])
    ecritures = prep["ecritures"]
    if lignes_saisies is not None and len(lignes_saisies) == len(ecritures):
        ecritures = [dict(e, lignes=l) for e, l in zip(ecritures, lignes_saisies)]
    return templates.TemplateResponse(request, "flux_valider.html", _contexte(
        "rapprochement", prep=prep, ecritures=ecritures, proposition=prop,
        erreurs=(erreurs or []) or prep["erreurs"], comptes=flux.comptes_actifs(),
        fournisseurs=flux.fournisseurs_connus(), proprietaires=flux.proprietaires_connus(),
        associes=flux.associes_connus(), traitements_ecart=lettrage.LIBELLES_TRAITEMENT_ECART,
        modes_aux=_modes_auxiliaires(),
        justification=justification, acteur=acteur, message=message, erreur_flash=erreur_flash,
        retour_query=urlencode([("m", x) for x in m] + [("o", x) for x in o]
                               + ([("proposition", proposition)] if proposition else []))),
        status_code=status_code)


@router.get("/flux-financiers/rapprochement/valider", response_class=HTMLResponse)
def flux_valider_form(request: Request, m: list[str] | None = Query(default=None),
                      o: list[str] | None = Query(default=None),
                      traitement_ecart: str = "", compte_ecart: str = "", proposition: str = "",
                      message: str = "", erreur: str = ""):
    return _page_validation(request, m or [], o or [], traitement_ecart=traitement_ecart,
                            compte_ecart=compte_ecart, proposition=proposition,
                            message=message, erreur_flash=erreur)


@router.post("/flux-financiers/rapprochement/valider", response_class=HTMLResponse)
async def flux_valider(request: Request):
    form = await request.form()
    m = [str(x) for x in form.getlist("m")]
    o = [str(x) for x in form.getlist("o")]
    traitement = str(form.get("traitement_ecart", "") or "")
    compte_ecart = str(form.get("compte_ecart", "") or "")
    nb = int(str(form.get("nb_ecritures", "0") or "0") or 0)
    brut = {k: [str(v) for v in form.getlist(k)] for k in form.keys() if k.startswith("l_")}
    lignes = lettrage.lignes_depuis_formulaire(brut, nb) if nb else None
    acteur = str(form.get("acteur", "") or "")
    justification = str(form.get("justification", "") or "")
    proposition = str(form.get("proposition", "") or "")
    res = lettrage.valider(m, o, acteur=acteur, traitement_ecart=traitement,
                           compte_ecart=compte_ecart, lignes=lignes, justification=justification,
                           proposition=proposition)
    if not res.get("ok"):
        return _page_validation(request, m, o, traitement_ecart=traitement,
                                compte_ecart=compte_ecart, proposition=proposition,
                                erreurs=res.get("erreurs"), lignes_saisies=lignes,
                                justification=justification, acteur=acteur, status_code=200)
    texte = ("Ce rapprochement était déjà validé : rien n'a été rejoué." if res.get("deja_valide")
             else "Rapprochement validé et comptabilisé.")
    return RedirectResponse(f"/flux-financiers/rapprochement?message={quote_plus(texte)}",
                            status_code=303)


@router.post("/flux-financiers/rapprochement/refuser")
async def flux_refuser(request: Request):
    form = await request.form()
    res = lettrage.refuser(str(form.get("empreinte", "") or ""),
                           acteur=str(form.get("acteur", "") or ""),
                           motif=str(form.get("motif", "") or ""))
    cle, texte = (("message", "Proposition refusée : rien d'autre n'a changé.") if res.get("ok")
                  else ("erreur", res.get("message", "Refus impossible.")))
    return RedirectResponse(f"/flux-financiers/rapprochement?{cle}={quote_plus(texte)}",
                            status_code=303)


@router.post("/flux-financiers/rapprochement/{lettrage_id}/annuler")
async def flux_annuler(request: Request, lettrage_id: str):
    form = await request.form()
    res = lettrage.annuler(lettrage_id, motif=str(form.get("motif", "") or ""),
                           acteur=str(form.get("acteur", "") or ""))
    cle, texte = (("message", "Rapprochement annulé : écritures contrepassées, rien n'est effacé.")
                  if res.get("ok") else ("erreur", res.get("message", "Annulation impossible.")))
    retour = str(form.get("retour", "") or "/flux-financiers/rapprochement")
    if not retour.startswith("/flux-financiers/"):
        retour = "/flux-financiers/rapprochement"
    sep = "&" if "?" in retour else "?"
    return RedirectResponse(f"{retour}{sep}{cle}={quote_plus(texte)}", status_code=303)


# ══ Fournisseur créé à la volée ═══════════════════════════════════════════════════════════════

@router.post("/flux-financiers/fournisseurs")
async def flux_creer_fournisseur(request: Request):
    form = await request.form()
    res = lettrage.creer_fournisseur(str(form.get("nom", "") or ""),
                                     siren=str(form.get("siren", "") or ""),
                                     siret=str(form.get("siret", "") or ""),
                                     acteur=str(form.get("acteur", "") or ""))
    if "application/json" in (request.headers.get("accept") or ""):
        return JSONResponse({k: v for k, v in res.items() if k != "erreurs"},
                            status_code=200 if res.get("ok") else 422)
    retour = str(form.get("retour", "") or "")
    cible = ("/flux-financiers/rapprochement/valider?" + retour) if retour else \
        "/flux-financiers/rapprochement"
    sep = "&" if "?" in cible else "?"
    texte = (f"Fournisseur « {res['nom']} » créé : sélectionnez-le comme auxiliaire."
             if res.get("ok") else res.get("message", "Création refusée."))
    return RedirectResponse(f"{cible}{sep}{'message' if res.get('ok') else 'erreur'}="
                            f"{quote_plus(texte)}", status_code=303)

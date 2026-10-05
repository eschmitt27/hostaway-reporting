from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates
from urllib.parse import quote

from app.readers.banques_reader import date_affichage, datetime_affichage
from app.services import clotures_service as clotures
from app.services import perimetre_gestion_service as perimetre
from app.services import reservations_hh_service as svc
from app.services import saisie_hh_service as saisie_svc
from app.services import reservations_hh_confirmation_service as confirmation
from app.services import hostaway_actualisation_service as hostaway_svc
from app.services import hostaway_depot_service as depot_svc
from app.services import hostaway_cleaning_tasks_actualisation_service as cleaning_svc
from app.services import ordonnanceur_service as ordo
from app.services import regularisation_hh_service as regul_svc

router = APIRouter()
templates = get_templates()
# Formatage des dates, partagé avec les autres écrans. Une date non convertible est affichée telle
# quelle, précédée d'une mention : jamais une date inventée pour combler un champ vide.

# Séparateurs typographiques possibles dans une valeur texte en entrée (espaces à retirer avant conversion)
_SPACES = (" ", " ", " ")


def format_eur(value) -> str:
    """Formatage d'AFFICHAGE au format francais (2 343,48 EUR). Aucun recalcul, aucun arrondi metier.

    - Milliers = espace ASCII, decimale = virgule, suffixe = espace + symbole euro.
    - Valeur vide -> 'Non renseigne'. Texte non convertible -> renvoye tel quel (aucune invention).
    """
    if value in (None, ""):
        return "Non renseigné"
    raw = str(value)
    for sp in _SPACES:
        raw = raw.replace(sp, "")
    raw = raw.replace(",", ".")
    try:
        num = float(raw)
    except (TypeError, ValueError):
        return str(value)
    us = f"{num:,.2f}"                      # ex. "2,343.48"
    fr = us.replace(",", " ").replace(".", ",")  # milliers = espace ASCII, decimale = virgule
    return fr + " €"              # + " €"


templates.env.filters["eur"] = format_eur


@router.get("/reservations", response_class=HTMLResponse)
def reservations_list(
    request: Request,
    q: str = "",
    mois: str = "",
    logement_id: str = "",
    proprietaire_id: str = "",
    canal_id: str = "",
    source_financiere: str = "",
    statut_controle: str = "",
    code_impact: str = "",
    comptabilisation: str = "",
):
    data = svc.load_list(
        q=q,
        mois=mois,
        logement_id=logement_id,
        proprietaire_id=proprietaire_id,
        canal_id=canal_id,
        source_financiere=source_financiere,
        statut_controle=statut_controle,
        code_impact=code_impact,
        comptabilisation=comptabilisation,
    )
    return templates.TemplateResponse(request, "reservations_list.html", {
        "active_menu": "reservations",
        "data": data,
        "nb_hors_gestion": len(perimetre.a_trancher()),
    })


@router.get("/reservations/nouvelle", response_class=HTMLResponse)
def reservation_nouvelle_form(request: Request):
    refs = saisie_svc.load_form_refs()
    return templates.TemplateResponse(request, "reservation_nouvelle_form.html", {
        "active_menu": "reservations",
        "refs": refs,
        "form": {"source_financiere": "SAISIE_MANUELLE"},
        "erreurs": [],
    })


@router.post("/reservations/nouvelle/verifier", response_class=HTMLResponse)
async def reservation_nouvelle_verifier(request: Request):
    form_data = await request.form()
    data = dict(form_data)
    result = saisie_svc.valider(data)
    if not result["ok"]:
        refs = saisie_svc.load_form_refs()
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_form.html",
            {
                "active_menu": "reservations",
                "refs": refs,
                "form": data,
                "erreurs": result["erreurs"],
            },
            status_code=422,
        )
    return templates.TemplateResponse(request, "reservation_nouvelle_verif.html", {
        "active_menu": "reservations",
        "preview": result["preview"],
        "pk": result["pk"],
        "form_data": data,
        "resultat_ecriture": None,
    })


@router.post("/reservations/nouvelle/previsualiser", response_class=HTMLResponse)
async def reservation_nouvelle_previsualiser(request: Request):
    form_data = await request.form()
    data = dict(form_data)
    result = confirmation.previsualiser(data)
    if not result["ok"] and result["manifest"].get("status") == "VALIDATION_REFUSEE":
        refs = saisie_svc.load_form_refs()
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_form.html",
            {
                "active_menu": "reservations",
                "refs": refs,
                "form": data,
                "erreurs": result["manifest"].get("errors", []),
            },
            status_code=422,
        )
    return RedirectResponse(
        url=f"/reservations/nouvelle/previsualisation/{result['token']}",
        status_code=303,
    )


@router.get("/reservations/nouvelle/previsualisation/{token}", response_class=HTMLResponse)
def reservation_nouvelle_previsualisation(request: Request, token: str):
    try:
        data = confirmation.load_previsualisation(token)
    except confirmation.DryRunError:
        return templates.TemplateResponse(
            request,
            "reservation_nouvelle_previsualisation.html",
            {"active_menu": "reservations", "manifest": None, "token": token},
            status_code=404,
        )
    return templates.TemplateResponse(request, "reservation_nouvelle_previsualisation.html", {
        "active_menu": "reservations",
        "manifest": data["manifest"],
        "token": token,
        "deja_confirme": confirmation.resultat_existe(token),
        "resultat": None,
    })


@router.post("/reservations/nouvelle/previsualisation/{token}/enregistrer", response_class=HTMLResponse)
async def reservation_nouvelle_ecriture_reelle(request: Request, token: str):
    if confirmation.resultat_existe(token):
        return RedirectResponse(
            url=f"/reservations/nouvelle/previsualisation/{token}", status_code=303)
    resultat = confirmation.confirmer(token, acteur="local")
    try:
        data = confirmation.load_previsualisation(token)
        manifest = data["manifest"]
    except confirmation.DryRunError:
        manifest = None
    return templates.TemplateResponse(request, "reservation_nouvelle_previsualisation.html", {
        "active_menu": "reservations",
        "manifest": manifest,
        "token": token,
        "deja_confirme": False,
        "resultat": resultat.as_dict(),
    })


# ── Séjours hors périmètre de gestion (décision explicite) ──────────────────────────────────────────
# Là où mène « Traiter » depuis la clôture : un séjour Hostaway sans période de gestion qui le couvre y est
# expliqué, et l'on peut y TRANCHER — prolonger la gestion (depuis la fiche du logement) ou l'exclure du
# périmètre, avec justification. Déclarées AVANT `/reservations/{reservation_hh_id}` (qui attraperait le chemin).

def _contexte_page_hors_gestion(mois: str, logement_id: str) -> dict:
    mois = (mois or "").strip()[:7]
    tous = perimetre.sejours_hors_gestion("")
    sejours = [s for s in tous if (not mois or s["mois"] == mois)
               and (not logement_id or s["logement_id"] == logement_id)]
    for s in sejours:
        s["historique"] = perimetre.historique(s["reservation_id"]) if s["reservation_id"] else []
    return {
        "active_menu": "reservations", "mois": mois,
        "mois_fr": clotures.mois_fr(mois) if len(mois) == 7 else "",
        "du_mois": clotures.du_mois(mois) if len(mois) == 7 else "",
        "mois_options": [{"id": m, "libelle": clotures.mois_fr(m).capitalize()}
                         for m in sorted({s["mois"] for s in tous} | ({mois} if len(mois) == 7 else set()))],
        "logement_id": logement_id,
        "nom_logement": perimetre.nom_logement(logement_id) if logement_id else "",
        "a_trancher": [s for s in sejours if s["etat"] == perimetre.ETAT_A_TRANCHER],
        "exclus": [s for s in sejours if s["etat"] == perimetre.ETAT_EXCLU],
    }


@router.get("/reservations/hors-gestion", response_class=HTMLResponse)
def reservations_hors_gestion(request: Request, mois: str = "", logement_id: str = "",
                              message: str = "", erreur: str = ""):
    contexte = _contexte_page_hors_gestion(mois, logement_id)
    contexte.update({"message": message, "erreur": erreur})
    return templates.TemplateResponse(request, "reservations_hors_gestion.html", contexte)


def _contexte_decision(mode: str, cle: str) -> dict:
    sejour = perimetre.sejour(cle)
    return {"active_menu": "reservations", "mode": mode, "sejour": sejour, "cle": cle, "erreur": "",
            "justification": "", "retour": perimetre.lien_page(sejour["mois"] if sejour else "")}


@router.get("/reservations/hors-gestion/{cle}/exclure", response_class=HTMLResponse)
def reservation_exclure_form(request: Request, cle: str):
    contexte = _contexte_decision("exclure", cle)
    return templates.TemplateResponse(request, "reservation_perimetre_decision.html", contexte,
                                      status_code=200 if contexte["sejour"] else 404)


@router.post("/reservations/hors-gestion/{cle}/exclure")
async def reservation_exclure(request: Request, cle: str):
    form = await request.form()
    justification = str(form.get("justification", "") or "")
    if str(form.get("confirmation", "") or "") != "oui":
        res = {"ok": False, "message": "Cochez la case pour confirmer l'exclusion de ce séjour."}
    else:
        res = perimetre.exclure(cle, justification=justification, acteur="local")
    if not res.get("ok"):
        contexte = _contexte_decision("exclure", cle)
        contexte.update({"erreur": res.get("message", "Exclusion refusée."), "justification": justification})
        return templates.TemplateResponse(request, "reservation_perimetre_decision.html", contexte,
                                          status_code=422)
    return RedirectResponse(
        url=perimetre.lien_page(res["mois"]) + "&message=" + quote(
            "Séjour exclu du périmètre de gestion : la décision est enregistrée, il ne bloque plus la clôture."),
        status_code=303)


@router.get("/reservations/hors-gestion/{cle}/reintegrer", response_class=HTMLResponse)
def reservation_reintegrer_form(request: Request, cle: str):
    contexte = _contexte_decision("reintegrer", cle)
    return templates.TemplateResponse(request, "reservation_perimetre_decision.html", contexte,
                                      status_code=200 if contexte["sejour"] else 404)


@router.post("/reservations/hors-gestion/{cle}/reintegrer")
async def reservation_reintegrer(request: Request, cle: str):
    form = await request.form()
    justification = str(form.get("justification", "") or "")
    if str(form.get("confirmation", "") or "") != "oui":
        res = {"ok": False, "message": "Cochez la case pour confirmer la réintégration de ce séjour."}
    else:
        res = perimetre.reintegrer(cle, justification=justification, acteur="local")
    if not res.get("ok"):
        contexte = _contexte_decision("reintegrer", cle)
        contexte.update({"erreur": res.get("message", "Réintégration refusée."), "justification": justification})
        return templates.TemplateResponse(request, "reservation_perimetre_decision.html", contexte,
                                          status_code=422)
    return RedirectResponse(
        url=perimetre.lien_page(res["mois"]) + "&message=" + quote(
            "Séjour réintégré : il redevient à trancher. La décision d'exclusion reste dans l'historique."),
        status_code=303)


@router.get("/reservations/{reservation_hh_id}", response_class=HTMLResponse)
def reservation_detail(request: Request, reservation_hh_id: str):
    detail = svc.load_detail(reservation_hh_id)
    if detail is None:
        return templates.TemplateResponse(
            request,
            "reservations_detail.html",
            {"active_menu": "reservations", "detail": None, "reservation_hh_id": reservation_hh_id},
            status_code=404,
        )
    return templates.TemplateResponse(request, "reservations_detail.html", {
        "active_menu": "reservations",
        "detail": detail,
        "reservation_hh_id": reservation_hh_id,
    })


# ── Régularisation DIRECT_SANS_SAISIE_HH / VRBO_MONTANT_NON_RENSEIGNE (Mission 18b) ──────────────
# Réutilise reservations_hh_saisie_service (aucun second moteur de saisie). Voir
# regularisation_hh_service pour la RÈGLE ABSOLUE (jamais total_price copié dans montant_percu).

@router.get("/reservations/regulariser/{ctrl_opaque}", response_class=HTMLResponse)
def reservation_regulariser_form(request: Request, ctrl_opaque: str):
    prep = regul_svc.preparer_formulaire(ctrl_opaque)
    if prep is None:
        return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
            "active_menu": "controles", "prep": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
        "active_menu": "controles", "prep": prep, "ctrl_opaque": ctrl_opaque,
        "recap": None, "erreur": "",
    })


@router.post("/reservations/regulariser/{ctrl_opaque}/previsualiser", response_class=HTMLResponse)
async def reservation_regulariser_previsualiser(request: Request, ctrl_opaque: str):
    form = await request.form()
    recap = regul_svc.recap(
        ctrl_opaque,
        montant_percu=(form.get("montant_percu") or "").strip(),
        menage=(form.get("menage") or "").strip(),
        code_impact=(form.get("code_impact") or "").strip(),
        commentaire=(form.get("commentaire") or "").strip(),
        canal_id=(form.get("canal_id") or "").strip(),
        source_financiere=(form.get("source_financiere") or "").strip(),
    )
    if recap is None:
        return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
            "active_menu": "controles", "prep": None, "ctrl_opaque": ctrl_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
        "active_menu": "controles", "prep": recap, "ctrl_opaque": ctrl_opaque,
        "recap": recap, "erreur": "",
    })


@router.post("/reservations/regulariser/{ctrl_opaque}/confirmer", response_class=HTMLResponse)
async def reservation_regulariser_confirmer(request: Request, ctrl_opaque: str):
    form = await request.form()
    resultat = regul_svc.regulariser(
        ctrl_opaque,
        montant_percu=(form.get("montant_percu") or "").strip(),
        menage=(form.get("menage") or "").strip(),
        code_impact=(form.get("code_impact") or "").strip(),
        commentaire=(form.get("commentaire") or "").strip(),
        canal_id=(form.get("canal_id") or "").strip(),
        source_financiere=(form.get("source_financiere") or "").strip(),
        acteur="local",
    )
    if not resultat.get("ok"):
        prep = regul_svc.preparer_formulaire(ctrl_opaque)
        return templates.TemplateResponse(request, "reservation_regulariser_form.html", {
            "active_menu": "controles", "prep": prep, "ctrl_opaque": ctrl_opaque,
            "recap": None, "erreur": resultat.get("message", "Régularisation refusée."),
        }, status_code=422)
    return templates.TemplateResponse(request, "reservation_regulariser_confirmation.html", {
        "active_menu": "controles", "ctrl_opaque": ctrl_opaque, "resultat": resultat,
        "recalcul": None,
    })


@router.post("/reservations/regulariser/{ctrl_opaque}/recalculer", response_class=HTMLResponse)
def reservation_regulariser_recalculer(request: Request, ctrl_opaque: str):
    recalcul = regul_svc.recalculer()
    return templates.TemplateResponse(request, "reservation_regulariser_confirmation.html", {
        "active_menu": "controles", "ctrl_opaque": ctrl_opaque, "resultat": {"ok": True},
        "recalcul": recalcul,
    })


# ── Actualisation Hostaway (APP-6A) ──────────────────────────────────────────
# Le bouton appelle `hostaway_actualisation_service.actualiser()`, qui est aussi le point d'entrée
# prévu pour un déclenchement automatique : un seul chemin métier, donc un seul comportement.
# La route ne fait qu'appeler et rendre l'état — aucune logique d'extraction ici.

@router.get("/hostaway", response_class=HTMLResponse)
def hostaway_actualisation(request: Request, message: str = "", message_type: str = ""):
    return templates.TemplateResponse(request, "hostaway_actualisation.html", {
        "active_menu": "reservations",
        "etat": hostaway_svc.etat(),
        "historique": hostaway_svc.historique(limite=10),
        # État du scheduler, en LECTURE SEULE : l'utilisateur voit si l'actualisation automatique
        # est active et quand elle repartira, sans quitter l'écran manuel. Aucune action ici — le
        # scheduler s'active par variable d'environnement, jamais depuis l'interface.
        "ordonnanceur": ordo.etat(),
        # Diagnostic de configuration : présence du fichier et des variables, JAMAIS leurs valeurs.
        # Sans lui, l'écran annonce « identifiants absents » sans dire où les déposer — et le
        # fichier `.env` étant ignoré par Git, il manque par construction dans un worktree neuf.
        "config_hostaway": cleaning_svc.diagnostic_configuration(),
        # Fraîcheur du dépôt publié : sans réseau (`rafraichir=False`), un affichage d'écran ne
        # doit pas dépendre d'un `git fetch`. Le bouton, lui, rafraîchit réellement.
        "fraicheur_depot": depot_svc.fraicheur(rafraichir=False),
        "message": message,
        "message_type": message_type,
    })


@router.post("/hostaway/actualiser")
def hostaway_actualiser(request: Request):
    """Synchronise le dernier jeu publié par le pipeline GitHub vers SQLite.

    CE BOUTON N'APPELLE PLUS HOSTAWAY DEPUIS CE POSTE.
    Il l'a fait tant que l'installation portait des identifiants ; elle n'en porte plus, et n'en a
    plus besoin : le pipeline GitHub interroge la plateforme trois fois par jour avec les secrets
    qu'il détient, et publie le résultat dans le dépôt. Conserver ici un second appel direct
    ferait coexister deux chaînes d'ingestion pour la même donnée — celle qui fonctionne et celle
    qui échoue faute de secret, sans que l'écran dise laquelle a produit ce qu'il affiche.

    Le moteur d'extraction, lui, est rigoureusement le même : seul le transport change.

    ATTENDUE, contrairement à l'ancien appel : l'import depuis le dépôt prend quelques secondes,
    là où l'appel API prenait des minutes. Rendre la main avant la fin obligerait l'utilisateur à
    deviner quand recharger, pour une attente qui ne se voit pas.
    """
    from app.services import hostaway_depot_service as depot

    resultat = depot.synchroniser(declencheur=hostaway_svc.DECLENCHEUR_MANUEL, attendre=True)
    if not resultat.get("ok"):
        message = resultat.get("message", "Synchronisation impossible.")
        type_message = "error"
    elif not resultat.get("importe"):
        # Déjà à jour : ce n'est ni un échec ni un import. Le dire évite qu'un utilisateur relance
        # indéfiniment en croyant que rien ne se passe.
        publie = (resultat.get("publie") or {}).get("source_horodatage", "")
        message = ("Données déjà à jour" + (f" (jeu publié le {publie})." if publie else ".")
                   + " Rien de nouveau à importer.")
        type_message = "info"
    else:
        publie = (resultat.get("publie") or {}).get("source_horodatage", "")
        message = ("Synchronisation terminée"
                   + (f" — données Hostaway produites le {publie}." if publie else "."))
        type_message = "info"
    return RedirectResponse(
        url=f"/hostaway?message={quote(message)}&message_type={type_message}", status_code=303)

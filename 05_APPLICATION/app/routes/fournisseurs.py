from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates
from app.moteurs.charges_engine import impact_charge
from app.services import charges_confirmation_service as confirmation
from app.services import charges_perimetre_service as perim
from app.services import charges_refacturation_service as refac
from app.services import charges_saisie_service as saisie
from app.services import charges_service as svc
from app.services.charges_preview_service import (
    ChargesPreviewError,
    load_form_refs,
    load_previsualisation,
    previsualiser,
)

router = APIRouter()
templates = get_templates()


@router.get("/charges")
def charges_alias():
    """Recette n°3 §107 — `/charges` est l'adresse qu'on tape pour l'entrée « Charges ». La liste
    vit désormais dans le module Flux financiers : redirection permanente plutôt qu'un 404."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/flux-financiers/charges", status_code=308)


@router.get("/fournisseurs")
def fournisseurs_list(request: Request):
    """Ancienne adresse de la liste des charges : elle mène à l'onglet Charges du module Flux
    financiers, filtres conservés. Les fiches, la saisie et les actions restent sous
    `/fournisseurs/...` — seul l'écran de liste a déménagé."""
    requete = request.url.query
    return RedirectResponse("/flux-financiers/charges" + (f"?{requete}" if requete else ""),
                            status_code=307)


def _prefill_depuis_mouvement(origine: str) -> tuple[dict, str]:
    """Préremplissage du formulaire HABITUEL depuis un mouvement. Rien n'est créé : l'utilisateur
    complète (catégorie, logement, refacturable, justificatif…) puis confirme comme toujours."""
    from app.services import flux_financiers_service as flux
    source, _, identifiant = str(origine or "").partition(":")
    mvt = flux.mouvement(source, identifiant) if source in flux.SOURCES else None
    if mvt is None:
        return {}, "Mouvement d'origine introuvable."
    if mvt["sens"] != flux.SORTIE or not mvt["lettrable"]:
        return {}, ("Ce mouvement ne peut pas recevoir de charge : il n'est pas une dépense "
                    "disponible (déjà rapproché, en attente chez la banque ou encaissement).")
    libelle = mvt["libelle"] + (f" ({mvt['reference']})" if mvt.get("reference") else "")
    return {
        "date_charge": mvt["date"],
        "montant": f"{mvt['restant']:.2f}",
        "mode_paiement_id": flux.MODE_PAR_SOURCE[source],
        "code_impact": "IC",
        "commentaire": (f"{'Paiement bancaire' if source == flux.BANQUE else 'Dépense en espèces'}"
                        f" du {mvt['date_fr']} — {libelle}")[:250],
        "mouvement_origine": f"{source}:{identifiant}",
        "mouvement_libelle": f"{mvt['libelle']} — {mvt['date_fr']} — {mvt['restant']:.2f} €",
    }, ""


@router.get("/fournisseurs/nouvelle", response_class=HTMLResponse)
def fournisseurs_nouvelle_form(request: Request, mouvement: str = ""):
    refs = load_form_refs()
    from datetime import date as _date
    form, erreur = _prefill_depuis_mouvement(mouvement) if mouvement else ({}, "")
    return templates.TemplateResponse(request, "fournisseurs_nouvelle.html", {
        "active_menu": "flux",
        "refs": refs,
        "form": form,
        "erreurs": [{"code": "MOUVEMENT", "message": erreur}] if erreur else [],
        "annee_justificatif": _date.today().strftime("%Y"),
    })


@router.post("/fournisseurs/nouvelle/previsualiser", response_class=HTMLResponse)
async def fournisseurs_nouvelle_previsualiser(request: Request):
    form_raw = await request.form()
    # Champs multi-valeurs (cases à cocher / multi-select) transportés en listes.
    MULTI = {"logements", "proprietaires", "menage_intervenants",
             "menage_logements", "menage_proprietaires"}
    form_data: dict = {}
    for k in form_raw.keys():
        if k in MULTI:
            form_data[k] = [str(v) for v in form_raw.getlist(k)]
        else:
            form_data[k] = str(form_raw[k])
    result = previsualiser(form_data)
    if not result["ok"]:
        refs = load_form_refs()
        from datetime import date as _date
        return templates.TemplateResponse(request, "fournisseurs_nouvelle.html", {
            "active_menu": "flux",
            "refs": refs,
            "form": form_data,
            "erreurs": result["manifest"]["errors"],
            "annee_justificatif": _date.today().strftime("%Y"),
        })
    return RedirectResponse(
        url=f"/fournisseurs/nouvelle/previsualisation/{result['token']}",
        status_code=303,
    )


@router.get("/fournisseurs/nouvelle/previsualisation/{token}", response_class=HTMLResponse)
def fournisseurs_previsualisation(request: Request, token: str):
    try:
        data = load_previsualisation(token)
    except ChargesPreviewError:
        return templates.TemplateResponse(
            request,
            "fournisseurs_previsualisation.html",
            {"active_menu": "flux", "token": token, "manifest": None, "not_found": True},
            status_code=404,
        )
    return templates.TemplateResponse(request, "fournisseurs_previsualisation.html", {
        "active_menu": "flux",
        "token": token,
        "manifest": data["manifest"],
        "not_found": False,
        "ecriture_activee": True,
        "deja_confirme": confirmation.resultat_existe(token),
    })


@router.post("/fournisseurs/nouvelle/confirmer/{token}")
def fournisseurs_confirmer(request: Request, token: str):
    """Confirme l'écriture réelle. **Ne reçoit AUCUNE donnée métier du navigateur** : seul le token
    compte, tout le reste est relu du manifest serveur.

    Protection contre la double soumission : si un résultat existe déjà pour ce token, on redirige
    sans rien réexécuter. Et comme on répond par une redirection (POST-Redirect-Get), rafraîchir la
    page de résultat est un simple GET — l'écriture n'est jamais rejouée.
    """
    if confirmation.resultat_existe(token):
        return RedirectResponse(url=f"/fournisseurs/nouvelle/resultat/{token}", status_code=303)

    resultat = confirmation.confirmer(token)   # les flags sont gardés en aval, avant toute écriture

    if not confirmation.resultat_existe(token):
        # Refus qui ne peut pas être persisté (token inconnu, manifest illisible) : aucun dossier de
        # prévisualisation où déposer un résultat. On rend le refus directement plutôt que de
        # rediriger vers une page vide. Sans écriture, rejouer ce POST est sans conséquence.
        return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
            "active_menu": "flux",
            "token": token,
            "resultat": resultat.as_dict(),
        }, status_code=404)

    return RedirectResponse(url=f"/fournisseurs/nouvelle/resultat/{token}", status_code=303)


@router.get("/fournisseurs/nouvelle/resultat/{token}", response_class=HTMLResponse)
def fournisseurs_resultat(request: Request, token: str):
    """Affiche le résultat d'une confirmation. Lecture seule : n'écrit jamais."""
    resultat = confirmation.charger_resultat(token)
    if resultat is None:
        return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
            "active_menu": "flux",
            "token": token,
            "resultat": None,
        }, status_code=404)
    # Charge née d'un mouvement : la suite logique est de la rapprocher et de la comptabiliser.
    rapprocher = ""
    if resultat.get("statut") == "SUCCES" and resultat.get("charge_id"):
        ligne = saisie.lire(resultat["charge_id"]) or {}
        lien = str(ligne.get("lien_virement_banque") or "")
        if lien.startswith(("QMV-", "CAI-")):
            source = "BANQUE" if lien.startswith("QMV-") else "CAISSE"
            rapprocher = (f"/flux-financiers/rapprochement/valider?m={source}:{lien}"
                          f"&o=CHARGE:{resultat['charge_id']}")
    return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
        "active_menu": "flux",
        "token": token,
        "resultat": resultat,
        "lien_rapprocher": rapprocher,
    })


@router.post("/fournisseurs/{charge_id}/valider")
async def charge_valider(request: Request, charge_id: str):
    """« Valider la charge » : `A_CONTROLER` → `VALIDE` (vocabulaire unifié, migration 0080).

    Réponse en REDIRECTION (POST-Redirect-Get) : rafraîchir la fiche ne rejoue jamais la
    validation, et le service est de toute façon idempotent.
    """
    form = await request.form()
    res = saisie.valider_controle(charge_id, acteur="interface",
                                  motif=str(form.get("motif", "") or ""))
    cible = f"/fournisseurs/{charge_id}"
    if not res.get("ok"):
        from urllib.parse import quote as _q
        return RedirectResponse(
            url=f"{cible}?erreur={_q(res.get('message') or res.get('code', 'REFUS'))}",
            status_code=303)
    return RedirectResponse(url=cible, status_code=303)


@router.post("/fournisseurs/{charge_id}/rouvrir-controle")
async def charge_rouvrir_controle(request: Request, charge_id: str):
    """§38 — « Repasser à contrôler » : `VALIDE` → `A_CONTROLER`, motif obligatoire.

    Le service refuse de lui-même si le mois est clôturé ou si une facture propriétaire émise
    porte déjà la refacturation : la route se contente de transmettre le motif du refus.
    """
    form = await request.form()
    res = saisie.rouvrir_controle(charge_id, acteur="interface",
                                  motif=str(form.get("motif", "") or ""))
    cible = f"/fournisseurs/{charge_id}"
    if not res.get("ok"):
        from urllib.parse import quote as _q
        return RedirectResponse(
            url=f"{cible}?erreur={_q(res.get('message') or res.get('code', 'REFUS'))}",
            status_code=303)
    return RedirectResponse(url=f"{cible}?message=Charge+repassee+a+controler", status_code=303)


@router.post("/fournisseurs/{charge_id}/anomalie")
async def charge_anomalie(request: Request, charge_id: str):
    """Contrepartie de la validation : signaler une anomalie sur la charge."""
    form = await request.form()
    res = saisie.signaler_anomalie(charge_id, acteur="interface",
                                   motif=str(form.get("motif", "") or ""))
    cible = f"/fournisseurs/{charge_id}"
    if not res.get("ok"):
        return RedirectResponse(url=f"{cible}?erreur={res.get('code', 'REFUS')}", status_code=303)
    return RedirectResponse(url=cible, status_code=303)


def _logements_disponibles() -> list[dict]:
    """Parc actif, pour proposer un périmètre. Liste vide si le référentiel n'est pas disponible —
    l'écran affiche alors l'explication plutôt qu'un menu vide inexplicable."""
    try:
        return [l for l in load_form_refs().get("logements", []) if l.get("logement_id")]
    except Exception:      # noqa: BLE001
        return []


@router.post("/fournisseurs/{charge_id}/perimetre")
async def charge_perimetre(request: Request, charge_id: str):
    """Renseigne le périmètre d'une charge qui n'en a pas (créée avant la migration 0074).

    Aucune déduction automatique : les logements viennent de la saisie. L'application ne peut pas
    les retrouver — ils n'ont jamais été écrits.
    """
    form = await request.form()
    res = saisie.definir_perimetre(charge_id, form.getlist("logements"), acteur="interface",
                                   motif=str(form.get("motif", "") or ""))
    cible = f"/fournisseurs/{charge_id}"
    if not res.get("ok"):
        return RedirectResponse(url=f"{cible}?erreur={res.get('code', 'REFUS')}", status_code=303)
    return RedirectResponse(url=cible, status_code=303)


@router.get("/fournisseurs/{charge_id}", response_class=HTMLResponse)
def fournisseur_detail(request: Request, charge_id: str, erreur: str = ""):
    detail = svc.load_detail(charge_id)
    if detail is None:
        return templates.TemplateResponse(
            request,
            "fournisseurs_detail.html",
            {
                "active_menu": "flux",
                "detail": None,
                "charge_id": charge_id,
            },
            status_code=404,
        )
    charge = detail.get("charge") or {}
    # Périmètre analytique (0074) : les N logements réellement concernés et leur quote-part. Sans
    # lui, une charge commune affichait « logement : — » alors que la saisie en désignait deux.
    perimetre = perim.resume(charge_id, charge.get("montant"))
    position = refac.position_de_charge(charge_id)
    # Le cycle de vie (`statut`) vient du service de saisie : le lecteur moteur ne le projette pas.
    ligne = saisie.lire(charge_id) or {}
    active = str(ligne.get("statut") or "") == saisie.STATUT_ACTIVE
    # Normalisé : une colonne vide signifie « pas encore contrôlé », pas « état inconnu ».
    controle = saisie.statut_controle(ligne or charge)
    # L'impact affiché DÉCOULE de `code_impact` (source unique) au lieu d'être relu dans deux
    # colonnes dérivées, supprimées par la migration 0078. Concrètement l'écran s'améliore : ces
    # colonnes étaient vides sur 4 charges réelles sur 5, qui affichaient « — » alors que leur
    # code d'impact était parfaitement renseigné. `None` reste possible — une charge sans
    # `code_impact` est un état réel — et se lit « non renseigné ».
    impact = impact_charge(charge.get("code_impact"))
    return templates.TemplateResponse(request, "fournisseurs_detail.html", {
        "active_menu": "flux",
        "detail": detail,
        "charge_id": charge_id,
        "impact": impact,
        "perimetre": perimetre,
        "position_refac": position,
        "statut_cycle": ligne.get("statut"),
        "peut_valider": active and controle != saisie.CONTROLE_VALIDE,
        "peut_signaler": active and controle != saisie.CONTROLE_ANOMALIE,
        # Périmètre à compléter : charge active, sans aucun logement, dont la position de
        # refacturation attend un périmètre pour devenir proposable (cas des charges antérieures
        # à la migration 0074). On offre la saisie ; on ne devine rien.
        "perimetre_a_completer": (
            active and not perimetre["nb_logements"] and not ligne.get("logement_id")
            and ((position or {}).get("statut") == refac.STATUT_A_TRAITER
                 or str(ligne.get("refacturable") or "").upper() == "OUI")),
        "logements_disponibles": _logements_disponibles(),
        "erreur": erreur,
    })

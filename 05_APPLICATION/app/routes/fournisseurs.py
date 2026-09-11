from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates
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


@router.get("/fournisseurs", response_class=HTMLResponse)
def fournisseurs_list(
    request: Request,
    mois: str = "",
    logement_id: str = "",
    categorie_charge_id: str = "",
    code_impact: str = "",
    statut_controle: str = "",
    associe_id: str = "",
):
    data = svc.load_list(
        mois=mois,
        logement_id=logement_id,
        categorie_charge_id=categorie_charge_id,
        code_impact=code_impact,
        statut_controle=statut_controle,
        associe_id=associe_id,
    )
    return templates.TemplateResponse(request, "fournisseurs_list.html", {
        "active_menu": "fournisseurs",
        "data": data,
    })


@router.get("/fournisseurs/nouvelle", response_class=HTMLResponse)
def fournisseurs_nouvelle_form(request: Request):
    refs = load_form_refs()
    return templates.TemplateResponse(request, "fournisseurs_nouvelle.html", {
        "active_menu": "fournisseurs",
        "refs": refs,
        "form": {},
        "erreurs": [],
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
        return templates.TemplateResponse(request, "fournisseurs_nouvelle.html", {
            "active_menu": "fournisseurs",
            "refs": refs,
            "form": form_data,
            "erreurs": result["manifest"]["errors"],
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
            {"active_menu": "fournisseurs", "token": token, "manifest": None, "not_found": True},
            status_code=404,
        )
    return templates.TemplateResponse(request, "fournisseurs_previsualisation.html", {
        "active_menu": "fournisseurs",
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
            "active_menu": "fournisseurs",
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
            "active_menu": "fournisseurs",
            "token": token,
            "resultat": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "fournisseurs_resultat.html", {
        "active_menu": "fournisseurs",
        "token": token,
        "resultat": resultat,
    })


@router.post("/fournisseurs/{charge_id}/valider")
async def charge_valider(request: Request, charge_id: str):
    """« Valider la charge » : `A_CONTROLER` → `CONFORME` (vocabulaire de la migration 0011).

    Réponse en REDIRECTION (POST-Redirect-Get) : rafraîchir la fiche ne rejoue jamais la
    validation, et le service est de toute façon idempotent.
    """
    form = await request.form()
    res = saisie.valider_controle(charge_id, acteur="interface",
                                  motif=str(form.get("motif", "") or ""))
    cible = f"/fournisseurs/{charge_id}"
    if not res.get("ok"):
        return RedirectResponse(url=f"{cible}?erreur={res.get('code', 'REFUS')}", status_code=303)
    return RedirectResponse(url=cible, status_code=303)


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
                "active_menu": "fournisseurs",
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
    return templates.TemplateResponse(request, "fournisseurs_detail.html", {
        "active_menu": "fournisseurs",
        "detail": detail,
        "charge_id": charge_id,
        "perimetre": perimetre,
        "position_refac": position,
        "statut_cycle": ligne.get("statut"),
        "peut_valider": active and controle != saisie.CONTROLE_CONFORME,
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

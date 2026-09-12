"""Écran « Correspondances logement » — le parcours dédié qui remplace l'édition brute (§18).

Une correspondance n'est pas une ligne de table qu'on saisit : c'est une décision qu'on prend sur
un libellé venu de l'extérieur. L'écran pose la question dans ces termes, montre la proposition du
moteur, et trace le choix. `ref_mapping_logements` reste consultable en administration, en lecture
seule, avec un renvoi vers ici.
"""
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.services import correspondances_logement_service as svc
from app.template_env import get_templates

router = APIRouter()
templates = get_templates()

ACTEUR = "ui:correspondances"


@router.get("/correspondances-logement", response_class=HTMLResponse)
def correspondances(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "correspondances_logement.html", {
        "active_menu": "administration",
        "a_traiter": svc.a_traiter(),
        "declarees": svc.declarees(),
        "logements": svc.logements_selectionnables(),
        "historique": svc.historique(limite=25),
        "message": message,
        "erreur": erreur,
    })


@router.post("/correspondances-logement/rattacher-annonce")
def rattacher_annonce(listing_map_id: str = Form(...), logement_id: str = Form(...)):
    """Rattache une annonce Hostaway au parc — fiche logement ET correspondance."""
    resultat = svc.rattacher_listing_au_parc(
        listing_map_id=listing_map_id, logement_id=logement_id, acteur=ACTEUR)
    return _retour(resultat, succes=(
        f"Annonce {listing_map_id} rattachée à {logement_id}. Relancez le calcul des réservations "
        "pour que ses séjours entrent dans le pilotage."))


@router.post("/correspondances-logement/enregistrer")
def enregistrer(source: str = Form(""), champ_source: str = Form(""),
                valeur_source: str = Form(...), logement_id: str = Form(...),
                motif: str = Form("")):
    resultat = svc.enregistrer(source=source, champ_source=champ_source,
                               valeur_source=valeur_source, logement_id=logement_id,
                               acteur=ACTEUR, motif=motif)
    if resultat.get("inchange"):
        return _retour(resultat, succes=resultat.get("message", ""))
    return _retour(resultat, succes=f"« {valeur_source} » est rattaché à {logement_id}.")


@router.post("/correspondances-logement/{mapping_logement_id}/desactiver")
def desactiver(mapping_logement_id: str, motif: str = Form("")):
    resultat = svc.desactiver(mapping_logement_id, acteur=ACTEUR, motif=motif)
    return _retour(resultat, succes="Correspondance retirée de l'usage (conservée en historique).")


def _retour(resultat: dict, *, succes: str):
    """Une seule forme de retour : le message vient du service quand il refuse, de la route quand
    elle réussit. L'écran n'a jamais à deviner ce qui s'est passé."""
    from urllib.parse import urlencode

    if resultat.get("ok"):
        params = urlencode({"message": succes})
    else:
        params = urlencode({"erreur": resultat.get("message") or resultat.get("code", "")})
    return RedirectResponse(f"/correspondances-logement?{params}", status_code=303)

"""Créances & Dettes › Associés — suivi consolidé, IK (indemnités kilométriques), compte courant.

Lecture consolidée de données saisies ailleurs (charges, rapprochements, écritures), plus la fiche
analytique d'une IK : trajets et part engagée pour l'activité. Aucune écriture comptable ici.
"""
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import associes_service as svc

router = APIRouter()
templates = get_templates()

_MENU = "creances"


def _retour(url: str, res: dict, succes: str) -> RedirectResponse:
    cle, texte = ("message", succes) if res.get("ok") else ("erreur", res.get("message") or "Refusé.")
    sep = "&" if "?" in url else "?"
    return RedirectResponse(f"{url}{sep}{cle}={quote(texte)}", status_code=303)


@router.get("/associes", response_class=HTMLResponse)
def associes(request: Request, associe: str = "", debut: str = "", fin: str = "",
             message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "associes.html", {
        "active_menu": _MENU,
        "suivi": svc.suivi(associe_id=associe, mois_debut=debut, mois_fin=fin),
        "ik": svc.lister_ik(associe_id=associe),
        "filtres": {"associe": associe, "debut": debut, "fin": fin},
        "message": message, "erreur": erreur,
    })


@router.get("/associes/mois/{mois}", response_class=HTMLResponse)
def detail_mois(request: Request, mois: str, associe: str = ""):
    return templates.TemplateResponse(request, "associes_mois.html", {
        "active_menu": _MENU, "d": svc.detail_mois(mois, associe_id=associe),
        "motifs": svc.MOTIFS_TRAJET,
    })


@router.get("/associes/ik/nouvelle", response_class=HTMLResponse)
def ik_nouvelle(request: Request, erreur: str = ""):
    return templates.TemplateResponse(request, "associes_ik_nouvelle.html", {
        "active_menu": _MENU, "candidates": svc.charges_candidates_ik(),
        "associes": svc.associes(), "erreur": erreur,
    })


@router.post("/associes/ik/nouvelle")
async def ik_creer(request: Request):
    f = await request.form()
    res = svc.creer_ik(str(f.get("charge_id", "")), str(f.get("associe_id", "")),
                       str(f.get("date_debut", "")), str(f.get("date_fin", "")),
                       commentaire=str(f.get("commentaire", "")), acteur="interface")
    if not res.get("ok"):
        return RedirectResponse(f"/associes/ik/nouvelle?erreur={quote(res['message'])}",
                                status_code=303)
    return RedirectResponse(f"/associes/ik/{res['ik_id_opaque']}?message="
                            f"{quote('IK créée : ajoutez les trajets qui la justifient.')}",
                            status_code=303)


@router.get("/associes/ik/{ik_id}", response_class=HTMLResponse)
def ik_fiche(request: Request, ik_id: str, message: str = "", erreur: str = ""):
    ik = svc.charger_ik(ik_id)
    return templates.TemplateResponse(request, "associes_ik.html", {
        "active_menu": _MENU, "ik": ik, "motifs": svc.MOTIFS_TRAJET,
        "natures": svc.NATURES_DEPENSE, "statuts": svc.LIBELLES_STATUT_IK,
        "transitions": svc.TRANSITIONS_IK.get(ik["statut"], ()) if ik else (),
        "nom_associe": svc.nom_associe(ik["associe_id"]) if ik else "",
        "message": message, "erreur": erreur,
    }, status_code=200 if ik else 404)


@router.post("/associes/ik/{ik_id}/trajets")
async def ik_trajets(request: Request, ik_id: str):
    f = await request.form()
    champs = ("date_trajet", "motif", "depart", "destination", "km", "vehicule", "commentaire")
    colonnes = {c: f.getlist(c) for c in champs}
    n = max((len(v) for v in colonnes.values()), default=0)
    trajets = [{c: (colonnes[c][i] if i < len(colonnes[c]) else "") for c in champs}
               for i in range(n)]
    res = svc.ajouter_trajets(ik_id, trajets, acteur="interface")
    return _retour(f"/associes/ik/{ik_id}", res, f"{res.get('nb', 0)} trajet(s) ajouté(s).")


@router.post("/associes/ik/{ik_id}/depenses")
async def ik_depense(request: Request, ik_id: str):
    f = await request.form()
    res = svc.ajouter_depense(
        ik_id, f.get("montant", ""), str(f.get("date_debit", "")), str(f.get("nature", "")),
        commentaire=str(f.get("commentaire", "")), intervenant_id=str(f.get("intervenant_id", "")),
        prestation_ref=str(f.get("prestation_ref", "")),
        justificatif_reference=str(f.get("justificatif_reference", "")), acteur="interface")
    return _retour(f"/associes/ik/{ik_id}", res, "Dépense liée à l'activité ajoutée.")


@router.post("/associes/ik/{ik_id}/retirer")
async def ik_retirer(request: Request, ik_id: str):
    f = await request.form()
    try:
        ligne = int(str(f.get("ligne_id", "0")))
    except ValueError:
        ligne = 0
    res = svc.retirer_ligne(ik_id, str(f.get("table", "")), ligne, acteur="interface")
    return _retour(f"/associes/ik/{ik_id}", res, "Ligne retirée.")


@router.post("/associes/ik/{ik_id}/statut")
async def ik_statut(request: Request, ik_id: str):
    f = await request.form()
    statut = str(f.get("statut", ""))
    res = svc.changer_statut(ik_id, statut, acteur="interface")
    return _retour(f"/associes/ik/{ik_id}", res,
                   f"IK : {svc.LIBELLES_STATUT_IK.get(statut, statut).lower()}.")

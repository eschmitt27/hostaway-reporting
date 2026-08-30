"""Page « Éléments à refacturer » (mission 15) — vue opérationnelle des positions produites
automatiquement par les charges refacturable='OUI' (`charges_refacturation_service`). Aucune
création ici : cette page ne fait qu'afficher/décider sur des positions déjà nées d'une charge.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import charges_refacturation_service as refac
from app.services import referentiel_service as ref_svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["nom_proprietaire"] = lambda pid: ref_svc.libelle_proprietaire(pid)
templates.env.filters["nom_logement"] = lambda lid: ref_svc.libelle_logement(lid)


@router.get("/charges-refacturation", response_class=HTMLResponse)
def liste(request: Request, statut: str = "", proprietaire_id: str = "", logement_id: str = "",
         message: str = "", erreur: str = ""):
    positions = refac.lister(statut=statut or None, proprietaire_id=proprietaire_id or None,
                             logement_id=logement_id or None)
    compteurs: dict[str, int] = {}
    for p in positions:
        compteurs[p["statut"]] = compteurs.get(p["statut"], 0) + 1
    return templates.TemplateResponse(request, "charges_refacturation.html", {
        "active_menu": "charges_refacturation", "positions": positions, "compteurs": compteurs,
        "message": message, "erreur": erreur,
        "applied": {"statut": statut, "proprietaire_id": proprietaire_id,
                    "logement_id": logement_id},
    })


@router.post("/charges-refacturation/{position_id}/reporter")
def reporter(position_id: str, request: Request):
    r = refac.reporter(position_id, acteur="UI")
    if r.get("ok"):
        return RedirectResponse("/charges-refacturation?message=Position+reportee", status_code=303)
    return RedirectResponse(f"/charges-refacturation?erreur={r.get('message')}", status_code=303)


@router.post("/charges-refacturation/{position_id}/ne-pas-refacturer")
def ne_pas_refacturer(position_id: str, request: Request, justification: str):
    r = refac.ne_pas_refacturer(position_id, acteur="UI", justification=justification)
    if r.get("ok"):
        return RedirectResponse("/charges-refacturation?message=Position+cloturee",
                                status_code=303)
    return RedirectResponse(f"/charges-refacturation?erreur={r.get('message')}", status_code=303)

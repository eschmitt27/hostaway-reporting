"""Routes Créances propriétaires, Dettes fournisseurs et Échéancier.

Vues de consultation uniquement : aucune écriture, aucun calcul métier. Elles répondent aux trois
questions du quotidien — qui me doit quoi, à qui dois-je quoi, et à quelle échéance.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import creances_dettes_service as svc

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/creances", response_class=HTMLResponse)
def creances(request: Request, proprietaire: str = "", logement: str = "", mois: str = "",
             statut: str = "", echues: str = ""):
    lignes = svc.creances(proprietaire_id=proprietaire, logement_id=logement, mois=mois,
                          statut=statut, echues_seulement=bool(echues))
    return templates.TemplateResponse(request, "creances_list.html", {
        "active_menu": "creances", "lignes": lignes,
        "total": round(sum(l["solde"] for l in lignes), 2),
        "total_echu": round(sum(l["solde"] for l in lignes if l["echue"]), 2),
        "filtres": {"proprietaire": proprietaire, "logement": logement, "mois": mois,
                    "statut": statut, "echues": echues},
        "statuts": (svc.ST_NON_REGLEE, svc.ST_PARTIELLE, svc.ST_REGLEE, svc.ST_TROP_PERCU),
        "par_tiers": svc.par_tiers()["proprietaires"],
    })


@router.get("/dettes", response_class=HTMLResponse)
def dettes(request: Request, fournisseur: str = "", statut: str = "", echues: str = ""):
    lignes = svc.dettes(fournisseur=fournisseur, statut=statut, echues_seulement=bool(echues))
    return templates.TemplateResponse(request, "dettes_list.html", {
        "active_menu": "creances", "lignes": lignes,
        "total": round(sum(l["solde"] for l in lignes), 2),
        "total_echu": round(sum(l["solde"] for l in lignes if l["echue"]), 2),
        "filtres": {"fournisseur": fournisseur, "statut": statut, "echues": echues},
        "par_tiers": svc.par_tiers()["fournisseurs"],
    })


@router.get("/echeancier", response_class=HTMLResponse)
def echeancier(request: Request):
    return templates.TemplateResponse(request, "echeancier.html", {
        "active_menu": "creances", "data": svc.echeancier(),
    })

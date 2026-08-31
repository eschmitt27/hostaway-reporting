"""Écrans du compte global propriétaire — position, allocations FIFO, historique.

Préfixe distinct de `/proprietaires/...` volontairement : ce module y déclare déjà
`/proprietaires/{prop_id}/{mois}`, qui capturerait n'importe quel second segment. Un chemin
`/proprietaires/{id}/compte` serait donc interprété comme le mois « compte ».

Aucune imputation manuelle n'est proposée : l'affectation d'un paiement à une facture n'est pas une
décision d'écran, c'est le résultat de la règle FIFO.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import compte_proprietaire_service as cpt

router = APIRouter()
templates = get_templates()

_MENU = "comptes_proprietaires"


@router.get("/comptes-proprietaires", response_class=HTMLResponse)
def liste(request: Request):
    positions = [cpt.position(pid) for pid in cpt.proprietaires_concernes()]
    positions.sort(key=lambda p: p["proprietaire_id"])
    return templates.TemplateResponse(request, "comptes_proprietaires_list.html", {
        "active_menu": _MENU,
        "positions": positions,
        "totaux": {
            "factures_a_recevoir": round(sum(p["factures_a_recevoir"] for p in positions), 2),
            "paiements_recus": round(sum(p["paiements_recus"] for p in positions), 2),
            "creance_restante": round(sum(p["creance_restante"] for p in positions), 2),
            "credit_disponible": round(sum(p["credit_disponible"] for p in positions), 2),
            "reversements_dus": round(sum(p["reversements_dus"] for p in positions), 2),
            "compensations": round(sum(p["compensations"] for p in positions), 2),
            "virement_net": round(sum(p["virement_net"] for p in positions), 2),
            "position_nette": round(sum(p["position_nette"] for p in positions), 2),
        },
    })


@router.get("/comptes-proprietaires/{proprietaire_id}", response_class=HTMLResponse)
def detail(request: Request, proprietaire_id: str, logement: str = "", message: str = ""):
    position = cpt.position(proprietaire_id)
    # Le filtre logement sert à ANALYSER, jamais à cloisonner le compte : les totaux affichés
    # restent ceux du propriétaire entier, seul le détail des factures est restreint.
    factures = [f for f in position["factures"]
                if not logement or f["logement_id"] == logement]
    return templates.TemplateResponse(request, "comptes_proprietaires_detail.html", {
        "active_menu": _MENU,
        "position": position,
        "factures_affichees": factures,
        "logements": sorted({f["logement_id"] for f in position["factures"] if f["logement_id"]}),
        "logement": logement,
        "historique": cpt.historique_recalculs(proprietaire_id),
        "message": message,
    })


@router.post("/comptes-proprietaires/{proprietaire_id}/recalculer")
def recalculer(proprietaire_id: str):
    r = cpt.recalculer(proprietaire_id, declencheur="MANUEL")
    return RedirectResponse(
        f"/comptes-proprietaires/{proprietaire_id}"
        f"?message=Recalcul {r['recalcul_id']} — {r['nb_allocations']} allocation(s)",
        status_code=303)

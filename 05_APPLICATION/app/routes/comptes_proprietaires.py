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


# ══ Crédits clients : reversements Airbnb et acomptes (Mission 37) ═══════════════════════════

def _retour_credits(proprietaire_id: str, res: dict, succes: str) -> RedirectResponse:
    from urllib.parse import quote
    cle, texte = ("message", succes) if res.get("ok") else ("erreur", res.get("message") or "Refusé.")
    return RedirectResponse(f"/comptes-proprietaires/{quote(proprietaire_id)}/credits"
                            f"?{cle}={quote(texte)}", status_code=303)


@router.get("/comptes-proprietaires/{proprietaire_id}/credits", response_class=HTMLResponse)
def credits(request: Request, proprietaire_id: str, message: str = "", erreur: str = ""):
    """Crédit disponible du client : origine, montant, utilisé, reste, factures — lecture seule."""
    from datetime import date as _date
    from app.services import comptabilite_plan_service as plan
    from app.services import credits_clients_service as cr
    from app.services import factures_proprietaires_service as fpr
    vue = cr.vue(proprietaire_id)
    factures = []
    for f in fpr.lister(proprietaire_id=proprietaire_id):
        if f["type_document"] != fpr.TYPE_FACTURE or f["statut"] == fpr.ST_ANNULE:
            continue
        _, solde = cr._facture_et_solde(f["facture_id_opaque"])
        if solde > cr.EPS:
            factures.append({"facture_id": f["facture_id_opaque"], "solde": solde,
                             "numero": f.get("numero_facture") or f"brouillon {f['mois']}",
                             "statut": f["statut"]})
    comptes_source = [c for c in plan.lister(statut="ACTIF")
                      if not c["compte"].startswith(cr.COMPTES_SOURCE_INTERDITS)]
    return templates.TemplateResponse(request, "comptes_proprietaires_credits.html", {
        "active_menu": _MENU, "proprietaire_id": proprietaire_id, "vue": vue,
        "factures": factures, "comptes_source": comptes_source,
        "aujourdhui": _date.today().isoformat(), "message": message, "erreur": erreur,
    })


@router.post("/comptes-proprietaires/{proprietaire_id}/credits")
async def credits_creer(request: Request, proprietaire_id: str):
    from app.services import credits_clients_service as cr
    f = await request.form()
    res = cr.creer_reversement_airbnb(
        proprietaire_id, f.get("montant", ""), str(f.get("date_origine", "") or ""),
        reference=str(f.get("reference", "") or ""), mode=str(f.get("mode", "") or "BANQUE"),
        compte_source=str(f.get("compte_source", "") or ""),
        auxiliaire_source=str(f.get("auxiliaire_source", "") or ""),
        justification=str(f.get("justification", "") or ""), acteur=str(f.get("acteur", "") or ""))
    succes = ("Reversement Airbnb déclaré : rapprochez-le de son virement dans Flux › Rapprochement."
              if res.get("statut") == cr.ST_EN_ATTENTE else
              "Reversement Airbnb enregistré, origine justifiée : disponible.")
    return _retour_credits(proprietaire_id, res, succes)


@router.post("/comptes-proprietaires/{proprietaire_id}/credits/imputer")
async def credits_imputer(request: Request, proprietaire_id: str):
    from app.services import credits_clients_service as cr
    f = await request.form()
    res = cr.imputer(str(f.get("credit_id", "") or ""), str(f.get("facture_id", "") or ""),
                     f.get("montant", ""), acteur=str(f.get("acteur", "") or ""))
    return _retour_credits(proprietaire_id, res, "Crédit imputé sur la facture.")


@router.post("/comptes-proprietaires/{proprietaire_id}/credits/regulariser")
async def credits_regulariser(request: Request, proprietaire_id: str):
    from app.services import credits_clients_service as cr
    f = await request.form()
    res = cr.regulariser(str(f.get("imputation_id", "") or ""), str(f.get("credit_id", "") or ""),
                         acteur=str(f.get("acteur", "") or ""))
    return _retour_credits(proprietaire_id, res, "Reversement rattaché à son crédit d'origine.")


@router.post("/comptes-proprietaires/{proprietaire_id}/credits/acompte-origine")
async def credits_acompte_origine(request: Request, proprietaire_id: str):
    from app.services import credits_clients_service as cr
    f = await request.form()
    res = cr.comptabiliser_encaissement_acompte(str(f.get("mouvement", "") or ""),
                                                acteur=str(f.get("acteur", "") or ""))
    return _retour_credits(proprietaire_id, res, "Encaissement de l'acompte comptabilisé (512 / 419100).")

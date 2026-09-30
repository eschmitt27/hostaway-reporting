"""Créances & Dettes — le point d'entrée du pilotage financier.

Quatre vues : Créances propriétaires, Dettes fournisseurs, Échéancier, Associés (route voisine).
Les vues sont des lectures. La seule action d'écriture, « Régler », délègue au rapprochement
canonique de Flux financiers (`creances_reglement_service`) : aucune donnée parallèle.
"""
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import creances_dettes_service as svc
from app.services import creances_reglement_service as reglement

router = APIRouter()
templates = get_templates()


@router.get("/creances", response_class=HTMLResponse)
def creances(request: Request, proprietaire: str = "", logement: str = "", mois: str = "",
             statut: str = "", echues: str = "", soldes: str = "", message: str = ""):
    lignes = svc.creances(proprietaire_id=proprietaire, logement_id=logement, mois=mois,
                          statut=statut, echues_seulement=bool(echues))
    return templates.TemplateResponse(request, "creances_list.html", {
        "active_menu": "creances", "lignes": lignes,
        # Pilotage par propriétaire : c'est l'entrée naturelle vers le compte et le règlement.
        "positions": reglement.positions(inclure_soldes=bool(soldes)),
        "soldes": soldes,
        "message": message,
        "total": round(sum(l["solde"] for l in lignes), 2),
        "total_echu": round(sum(l["solde"] for l in lignes if l["echue"]), 2),
        # §52 — les créances sans échéance contractuelle qu'il est temps de relancer.
        "total_a_relancer": round(sum(l["solde"] for l in lignes if l.get("a_relancer")), 2),
        "seuil_relance": svc.SEUIL_RELANCE_JOURS,
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


# ── Préparer le règlement → Régler ───────────────────────────────────────────────────────────────

def _page_reglement(request: Request, pid: str, *, mouvement: str = "", traitement_ecart: str = "",
                    acteur: str = "", erreurs: list | None = None, status_code: int = 200):
    prep = reglement.preparer(pid)
    apercu = reglement.apercu(pid, mouvement, traitement_ecart=traitement_ecart) if mouvement else None
    from app.services import flux_lettrage_service as lettrage
    return templates.TemplateResponse(request, "creances_reglement.html", {
        "active_menu": "creances", "prep": prep, "apercu": apercu, "mouvement": mouvement,
        "traitement_ecart": traitement_ecart, "acteur": acteur, "erreurs": erreurs or [],
        "traitements": lettrage.LIBELLES_TRAITEMENT_ECART,
    }, status_code=status_code)


@router.get("/creances/proprietaires/{proprietaire_id}/reglement", response_class=HTMLResponse)
def preparer_reglement(request: Request, proprietaire_id: str, mouvement: str = "",
                       traitement_ecart: str = ""):
    """Préparer le règlement : ce qui compose le montant, puis le choix du mouvement reçu.
    LECTURE SEULE : l'aperçu de l'écriture est calculé, jamais enregistré."""
    return _page_reglement(request, proprietaire_id, mouvement=mouvement,
                           traitement_ecart=traitement_ecart)


@router.post("/creances/proprietaires/{proprietaire_id}/regler")
async def regler(request: Request, proprietaire_id: str):
    form = await request.form()
    mouvement = str(form.get("mouvement", "") or "")
    traitement = str(form.get("traitement_ecart", "") or "")
    acteur = str(form.get("acteur", "") or "").strip()
    res = reglement.regler(proprietaire_id, mouvement, acteur=acteur, traitement_ecart=traitement)
    if not res.get("ok"):
        erreurs = res.get("erreurs") or [{"message": res.get("message") or "Règlement refusé."}]
        return _page_reglement(request, proprietaire_id, mouvement=mouvement,
                               traitement_ecart=traitement, acteur=acteur, erreurs=erreurs)
    texte = ("Ce règlement était déjà enregistré : rien n'a été rejoué." if res.get("deja_valide")
             else "Règlement enregistré : le compte, les factures et les créances sont à jour.")
    # Retour au hub : le solde qu'on vient de régler y est déjà à jour.
    return RedirectResponse(f"/creances?message={quote(texte)}", status_code=303)


def _page_versement(request: Request, pid: str, *, mouvement: str = "", acteur: str = "",
                    erreurs: list | None = None):
    prep = reglement.preparer_versement(pid)
    apercu = reglement.apercu_versement(pid, mouvement) if mouvement else None
    return templates.TemplateResponse(request, "creances_versement.html", {
        "active_menu": "creances", "prep": prep, "apercu": apercu, "mouvement": mouvement,
        "acteur": acteur, "erreurs": erreurs or [],
    })


@router.get("/creances/proprietaires/{proprietaire_id}/versement", response_class=HTMLResponse)
def preparer_versement(request: Request, proprietaire_id: str, mouvement: str = ""):
    """Préparer le versement propriétaire : ce qui reste à virer, puis le débit bancaire.
    LECTURE SEULE : l'aperçu de l'écriture est calculé, jamais enregistré."""
    return _page_versement(request, proprietaire_id, mouvement=mouvement)


@router.post("/creances/proprietaires/{proprietaire_id}/verser")
async def verser(request: Request, proprietaire_id: str):
    form = await request.form()
    mouvement = str(form.get("mouvement", "") or "")
    acteur = str(form.get("acteur", "") or "").strip()
    res = reglement.verser(proprietaire_id, mouvement, acteur=acteur,
                           traitement_ecart=str(form.get("traitement_ecart", "") or ""))
    if not res.get("ok"):
        erreurs = res.get("erreurs") or [{"message": res.get("message") or "Versement refusé."}]
        return _page_versement(request, proprietaire_id, mouvement=mouvement, acteur=acteur,
                               erreurs=erreurs)
    texte = ("Ce versement était déjà enregistré : rien n'a été rejoué." if res.get("deja_valide")
             else "Versement propriétaire enregistré : le compte et les créances sont à jour.")
    return RedirectResponse(f"/creances?message={quote(texte)}", status_code=303)

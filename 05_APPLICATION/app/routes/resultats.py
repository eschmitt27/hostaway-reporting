"""Écrans Résultats (Phase 3, mission Analytique/Résultats).

Lit exclusivement `comptabilite_analytique_service` / `comptabilite_reconciliations_service`
(Phase 2) et les services Comptabilité déjà livrés (`49`) : aucun calcul n'est refait ici, cet
écran n'est qu'une présentation. Jamais de calendrier : les mois viennent de
`ana.mois_disponibles()` (ceux réellement présents dans Lot10).
"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import comptabilite_analytique_service as ana
from app.services import comptabilite_auxiliaires_service as aux
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_periodes_service as per
from app.services import comptabilite_reconciliations_service as recon

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _mois_defaut(mois: str) -> str:
    if mois:
        return mois
    dispo = ana.mois_disponibles()
    return dispo[-1] if dispo else ""


@router.get("/resultats", response_class=HTMLResponse)
def resultats_dashboard(request: Request, mois: str = "", vision: str = "REEL"):
    mois = _mois_defaut(mois)
    globales = ana.mesures_globales()
    par_logement = ana.mesures_par_logement(mois=mois, vision=vision) if mois else {"statut": ana.NON_DISPONIBLE, "lignes": []}
    mois_prec = ana.mois_precedent(mois) if mois else ""
    par_logement_prec = ana.mesures_par_logement(mois=mois_prec, vision=vision) if mois_prec else {"statut": ana.NON_DISPONIBLE, "lignes": []}
    cumule = ana.mesures_cumulees(vision=vision, annee=mois[:4] if mois else "")

    resultat_prec = round(sum(l["resultat"] for l in par_logement_prec.get("lignes", [])), 2) \
        if par_logement_prec["statut"] == "OK" else None
    resultat_courant = round(sum(l["resultat"] for l in par_logement.get("lignes", [])), 2) \
        if par_logement["statut"] == "OK" else None

    periode = per.charger(mois) if mois else None
    return templates.TemplateResponse(request, "resultats_dashboard.html", {
        "active_menu": "resultats", "mois": mois, "vision": vision, "visions": ana.VISIONS,
        "mois_disponibles": ana.mois_disponibles(), "globales": globales,
        "resultat_courant": resultat_courant, "resultat_precedent": resultat_prec,
        "mois_precedent": mois_prec, "cumule": cumule, "periode": periode,
    })


@router.get("/resultats/mensuel", response_class=HTMLResponse)
def resultats_mensuel(request: Request, mois: str = "", vision: str = "REEL"):
    mois = _mois_defaut(mois)
    par_logement = ana.mesures_par_logement(mois=mois, vision=vision)
    par_proprietaire = ana.mesures_par_proprietaire(mois=mois, vision=vision)
    return templates.TemplateResponse(request, "resultats_mensuel.html", {
        "active_menu": "resultats", "mois": mois, "vision": vision, "visions": ana.VISIONS,
        "mois_disponibles": ana.mois_disponibles(),
        "par_logement": par_logement, "par_proprietaire": par_proprietaire,
    })


@router.get("/resultats/cumule", response_class=HTMLResponse)
def resultats_cumule(request: Request, vision: str = "REEL", annee: str = ""):
    cumule = ana.mesures_cumulees(vision=vision, annee=annee)
    return templates.TemplateResponse(request, "resultats_cumule.html", {
        "active_menu": "resultats", "vision": vision, "visions": ana.VISIONS, "annee": annee,
        "cumule": cumule,
    })


@router.get("/resultats/logements", response_class=HTMLResponse)
def resultats_logements(request: Request, mois: str = "", vision: str = "REEL"):
    mois = _mois_defaut(mois)
    par_logement = ana.mesures_par_logement(mois=mois, vision=vision)
    return templates.TemplateResponse(request, "resultats_logements.html", {
        "active_menu": "resultats", "mois": mois, "vision": vision, "visions": ana.VISIONS,
        "mois_disponibles": ana.mois_disponibles(), "resultats": par_logement,
    })


@router.get("/resultats/logements/{logement_id}", response_class=HTMLResponse)
def resultats_logement_detail(request: Request, logement_id: str, mois: str = ""):
    mois = _mois_defaut(mois)
    fiche = ana.fiche_logement(logement_id, mois=mois)
    mouvements = ana.drill_down_logement(logement_id, mois=mois)
    return templates.TemplateResponse(request, "resultats_logement_detail.html", {
        "active_menu": "resultats", "logement_id": logement_id, "mois": mois,
        "fiche": fiche, "mouvements": mouvements,
    })


@router.get("/resultats/proprietaires", response_class=HTMLResponse)
def resultats_proprietaires(request: Request, mois: str = "", vision: str = "REEL"):
    mois = _mois_defaut(mois)
    par_proprietaire = ana.mesures_par_proprietaire(mois=mois, vision=vision)
    return templates.TemplateResponse(request, "resultats_proprietaires.html", {
        "active_menu": "resultats", "mois": mois, "vision": vision, "visions": ana.VISIONS,
        "mois_disponibles": ana.mois_disponibles(), "resultats": par_proprietaire,
    })


@router.get("/resultats/proprietaires/{proprietaire_id}", response_class=HTMLResponse)
def resultats_proprietaire_detail(request: Request, proprietaire_id: str, mois: str = ""):
    mois = _mois_defaut(mois)
    fiche = ana.fiche_proprietaire(proprietaire_id, mois=mois)
    auxiliaire = aux.fiche_auxiliaire(aux.FAMILLE_PROPRIETAIRE, proprietaire_id)
    return templates.TemplateResponse(request, "resultats_proprietaire_detail.html", {
        "active_menu": "resultats", "proprietaire_id": proprietaire_id, "mois": mois,
        "fiche": fiche, "auxiliaire": auxiliaire,
    })


@router.get("/resultats/plateformes", response_class=HTMLResponse)
def resultats_plateformes(request: Request):
    """Aucune dimension plateforme n'est peuplée à ce stade (ni Lot10 lu ici, ni les écritures) —
    écran honnête : NON_DISPONIBLE affiché explicitement, jamais un tableau de zéros."""
    return templates.TemplateResponse(request, "resultats_plateformes.html", {
        "active_menu": "resultats", "statut": ana.NON_DISPONIBLE,
    })


@router.get("/resultats/fournisseurs", response_class=HTMLResponse)
def resultats_fournisseurs(request: Request):
    synthese = aux.synthese()
    return templates.TemplateResponse(request, "resultats_fournisseurs.html", {
        "active_menu": "resultats", "fournisseurs": synthese.get(aux.FAMILLE_FOURNISSEUR, []),
    })


@router.get("/resultats/charges", response_class=HTMLResponse)
def resultats_charges(request: Request):
    from app.readers import charges_reader
    try:
        charges = charges_reader.read_charges()
    except Exception:
        charges = []
    par_categorie: dict[str, dict] = {}
    for c in charges:
        cat = str(c.get("categorie_charge_id") or "A_CONTROLER")
        d = par_categorie.setdefault(cat, {"categorie": cat, "montant": 0.0, "nb": 0})
        try:
            d["montant"] += float(c.get("montant") or 0)
        except (TypeError, ValueError):
            pass
        d["nb"] += 1
    return templates.TemplateResponse(request, "resultats_charges.html", {
        "active_menu": "resultats",
        "statut": "OK" if charges else ana.NON_DISPONIBLE,
        "par_categorie": sorted(par_categorie.values(), key=lambda d: -d["montant"]),
    })


@router.get("/resultats/menages", response_class=HTMLResponse)
def resultats_menages(request: Request, mois: str = ""):
    from app.services import menages_cycle_service as men
    mois = _mois_defaut(mois)
    try:
        menages = men.lister(mois=mois) if mois else []
    except Exception:
        menages = []
    par_type: dict[str, dict] = {}
    for m in menages:
        t = m.get("type_menage") or "A_CONTROLER"
        d = par_type.setdefault(t, {"type_menage": t, "nb": 0})
        d["nb"] += 1
    return templates.TemplateResponse(request, "resultats_menages.html", {
        "active_menu": "resultats", "mois": mois, "mois_disponibles": ana.mois_disponibles(),
        "statut": "OK" if menages else ana.NON_DISPONIBLE, "par_type": list(par_type.values()),
        "nb_total": len(menages),
    })


@router.get("/resultats/comptabilite", response_class=HTMLResponse)
def resultats_comptabilite(request: Request, mois: str = ""):
    mois = _mois_defaut(mois)
    ecritures = compta.lister(periode=mois) if mois else compta.lister()
    par_journal: dict[str, dict] = {}
    for e in ecritures:
        d = par_journal.setdefault(e["journal"], {"journal": e["journal"], "nb": 0,
                                                   "total_debit": 0.0, "total_credit": 0.0})
        d["nb"] += 1
        d["total_debit"] += e["total_debit"] or 0
        d["total_credit"] += e["total_credit"] or 0
    periode = per.charger(mois) if mois else None
    return templates.TemplateResponse(request, "resultats_comptabilite.html", {
        "active_menu": "resultats", "mois": mois, "mois_disponibles": ana.mois_disponibles(),
        "par_journal": list(par_journal.values()), "periode": periode,
    })


@router.get("/resultats/reconciliation", response_class=HTMLResponse)
def resultats_reconciliation(request: Request, mois: str = ""):
    mois = _mois_defaut(mois)
    lignes = {
        "A — Lot9 ↔ Lot10": recon.lot9_vs_lot10(mois=mois),
        "B — Lot10 ↔ Analytique": recon.lot10_vs_analytique(mois=mois),
        "C — Analytique ↔ Comptabilité": recon.analytique_vs_comptabilite(mois=mois),
        "D — Banque ↔ journal BANQUE": recon.banque_vs_journal_banque(mois=mois),
        "E — Factures ↔ auxiliaires": recon.factures_vs_auxiliaires(),
        "F — Ménages ↔ charges": recon.menages_vs_charges(mois=mois),
        "G — Commissions ↔ VENTES": recon.commissions_vs_ventes(mois) if mois else
            {"statut": recon.ST_NON_DISPONIBLE, "libelle_gauche": "Lot12", "libelle_droit": "VENTES",
             "montant_gauche": None, "montant_droit": None, "ecart": None, "tolerance": 0.01, "detail": []},
        "H — Total analytique ↔ résultat global": recon.total_analytique_vs_resultat_global(),
    }
    return templates.TemplateResponse(request, "resultats_reconciliation.html", {
        "active_menu": "resultats", "mois": mois, "mois_disponibles": ana.mois_disponibles(),
        "lignes": lignes,
    })


@router.get("/resultats/lignes/{ecriture_id_opaque}")
def resultats_ligne_detail(ecriture_id_opaque: str):
    """Descend jusqu'à la pièce : la fiche écriture existante porte déjà lignes + ventilation
    analytique (Phase 1/2) — pas de second écran dupliqué."""
    return RedirectResponse(url=f"/comptabilite/ecritures/{ecriture_id_opaque}", status_code=303)


@router.get("/resultats/export.csv")
def resultats_export_csv(mois: str = "", vision: str = "REEL"):
    mois = mois or (ana.mois_disponibles()[-1] if ana.mois_disponibles() else "")
    par_logement = ana.mesures_par_logement(mois=mois, vision=vision)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["mois", "logement_id", "proprietaire_id", "vision", "total_produits",
               "total_charges", "resultat", "nb_flux"])
    for l in par_logement.get("lignes", []):
        w.writerow([l["mois"], l["logement_id"], l["proprietaire_id"], l["vision"],
                   l["total_produits"], l["total_charges"], l["resultat"], l["nb_flux"]])
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv", headers={
        "Content-Disposition": f'attachment; filename="resultats_{mois or "aucun_mois"}_{vision}.csv"'})

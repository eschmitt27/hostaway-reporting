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
from app.services import comptabilite_axes_service as axes
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


@router.get("/resultats/proprietaires/export.csv")
def resultats_proprietaires_export_csv(mois: str = "", vision: str = "REEL"):
    mois = _mois_defaut(mois)
    par_proprietaire = ana.mesures_par_proprietaire(mois=mois, vision=vision)
    lignes = [[l["mois"], l["proprietaire_id"], l["vision"], l["total_produits"],
              l["total_charges"], l["resultat"], l["nb_flux"]]
             for l in par_proprietaire.get("lignes", [])]
    return _csv_response(f"resultats_proprietaires_{mois or 'aucun_mois'}_{vision}",
        ["mois", "proprietaire_id", "vision", "total_produits", "total_charges", "resultat", "nb_flux"],
        lignes)


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
    """Aucune dimension plateforme fiable n'est peuplée à ce stade — écran honnête :
    NON_DISPONIBLE affiché explicitement avec sa raison, jamais un tableau de zéros."""
    res = axes.plateformes()
    return templates.TemplateResponse(request, "resultats_plateformes.html", {
        "active_menu": "resultats", "statut": res["statut"], "raison": res.get("raison"),
    })


@router.get("/resultats/plateformes/{plateforme_id}", response_class=HTMLResponse)
def resultats_plateforme_detail(request: Request, plateforme_id: str):
    res = axes.plateformes()
    return templates.TemplateResponse(request, "resultats_plateformes.html", {
        "active_menu": "resultats", "statut": res["statut"], "raison": res.get("raison"),
        "plateforme_id": plateforme_id,
    })


@router.get("/resultats/fournisseurs", response_class=HTMLResponse)
def resultats_fournisseurs(request: Request):
    res = axes.fournisseurs()
    return templates.TemplateResponse(request, "resultats_fournisseurs.html", {
        "active_menu": "resultats", "fournisseurs": res.get("lignes", []),
    })


@router.get("/resultats/fournisseurs/export.csv")
def resultats_fournisseurs_export_csv():
    res = axes.fournisseurs()
    lignes = [[f["auxiliaire"], f["debit"], f["credit"], f["solde"]]
             for f in res.get("lignes", [])]
    return _csv_response("resultats_fournisseurs",
        ["fournisseur_id_opaque", "debit", "credit", "solde"], lignes)


@router.get("/resultats/fournisseurs/{fournisseur_id_opaque}", response_class=HTMLResponse)
def resultats_fournisseur_detail(request: Request, fournisseur_id_opaque: str):
    res = axes.fournisseur_detail(fournisseur_id_opaque)
    return templates.TemplateResponse(request, "resultats_fournisseur_detail.html", {
        "active_menu": "resultats", "fournisseur_id_opaque": fournisseur_id_opaque, "resultat": res,
    })


@router.get("/resultats/categories", response_class=HTMLResponse)
def resultats_categories(request: Request):
    res = axes.categories()
    return templates.TemplateResponse(request, "resultats_categories.html", {
        "active_menu": "resultats", "resultat": res,
    })


@router.get("/resultats/categories/export.csv")
def resultats_categories_export_csv():
    res = axes.categories()
    lignes = [[c["categorie"], c["montant"], c["nb"]] for c in res.get("lignes", [])]
    return _csv_response("resultats_categories", ["categorie", "montant", "nb"], lignes)


@router.get("/resultats/categories/{categorie}", response_class=HTMLResponse)
def resultats_categorie_detail(request: Request, categorie: str):
    res = axes.categorie_detail(categorie)
    return templates.TemplateResponse(request, "resultats_categorie_detail.html", {
        "active_menu": "resultats", "categorie": categorie, "resultat": res,
    })


@router.get("/resultats/prestataires", response_class=HTMLResponse)
def resultats_prestataires(request: Request, mois: str = ""):
    mois = _mois_defaut(mois) or mois
    res = axes.prestataires(mois=mois)
    return templates.TemplateResponse(request, "resultats_prestataires.html", {
        "active_menu": "resultats", "mois": mois, "resultat": res,
    })


@router.get("/resultats/prestataires/export.csv")
def resultats_prestataires_export_csv(mois: str = ""):
    mois = _mois_defaut(mois) or mois
    res = axes.prestataires(mois=mois)
    lignes = [[p["prestataire_id"], p["nb_menages"], p["cout_prevu"], p["cout_reel"],
              p["ecart"], p["nb_logements"]] for p in res.get("lignes", [])]
    return _csv_response(f"resultats_prestataires_{mois or 'tous_mois'}",
        ["prestataire_id", "nb_menages", "cout_prevu", "cout_reel", "ecart", "nb_logements"], lignes)


@router.get("/resultats/prestataires/{prestataire_id}", response_class=HTMLResponse)
def resultats_prestataire_detail(request: Request, prestataire_id: str, mois: str = ""):
    res = axes.prestataire_detail(prestataire_id, mois=mois)
    return templates.TemplateResponse(request, "resultats_prestataire_detail.html", {
        "active_menu": "resultats", "prestataire_id": prestataire_id, "mois": mois, "resultat": res,
    })


@router.get("/resultats/activites", response_class=HTMLResponse)
def resultats_activites(request: Request):
    """Aucune taxonomie d'activité documentée n'existe (`type_flux_id` est une classification
    technique fine, pas un regroupement métier) — NON_DISPONIBLE assumé, jamais inventé."""
    res = axes.activites()
    return templates.TemplateResponse(request, "resultats_activites.html", {
        "active_menu": "resultats", "statut": res["statut"], "raison": res.get("raison"),
    })


@router.get("/resultats/activites/{activite_id}", response_class=HTMLResponse)
def resultats_activite_detail(request: Request, activite_id: str):
    res = axes.activites()
    return templates.TemplateResponse(request, "resultats_activites.html", {
        "active_menu": "resultats", "statut": res["statut"], "raison": res.get("raison"),
        "activite_id": activite_id,
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
        "B — Lot10 ↔ Analytique": recon.lot10_vs_analytique(),
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


def _csv_response(nom: str, entetes: list[str], lignes: list[list]) -> StreamingResponse:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(entetes)
    for l in lignes:
        w.writerow(l)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{nom}.csv"'})


@router.get("/resultats/export.csv")
def resultats_export_csv(mois: str = "", vision: str = "REEL"):
    """Export logement — grain `PAR_MOIS_LOGEMENT`, filtré mois/vision."""
    mois = mois or (ana.mois_disponibles()[-1] if ana.mois_disponibles() else "")
    par_logement = ana.mesures_par_logement(mois=mois, vision=vision)
    lignes = [[l["mois"], l["logement_id"], l["proprietaire_id"], l["vision"],
              l["total_produits"], l["total_charges"], l["resultat"], l["nb_flux"]]
             for l in par_logement.get("lignes", [])]
    return _csv_response(f"resultats_logements_{mois or 'aucun_mois'}_{vision}",
        ["mois", "logement_id", "proprietaire_id", "vision", "total_produits",
         "total_charges", "resultat", "nb_flux"], lignes)


@router.get("/resultats/dashboard/export.csv")
def resultats_dashboard_export_csv():
    """Export dashboard — grain global par vision (`GLOBAL`), sans recalcul."""
    globales = ana.mesures_globales()
    lignes = [[v, d["total_produits"], d["total_charges"], d["resultat"], d["commentaire"]]
             for v, d in globales.get("visions", {}).items()]
    return _csv_response("resultats_dashboard_global",
        ["vision", "total_produits", "total_charges", "resultat", "commentaire"], lignes)


@router.get("/resultats/reconciliation/export.csv")
def resultats_reconciliation_export_csv(mois: str = ""):
    mois = _mois_defaut(mois)
    reconciliations = {
        "A_Lot9_Lot10": recon.lot9_vs_lot10(mois=mois),
        "B_Lot10_Analytique": recon.lot10_vs_analytique(),
        "C_Analytique_Comptabilite": recon.analytique_vs_comptabilite(mois=mois),
        "D_Banque_JournalBanque": recon.banque_vs_journal_banque(mois=mois),
        "E_Factures_Auxiliaires": recon.factures_vs_auxiliaires(),
        "F_Menages_Charges": recon.menages_vs_charges(mois=mois),
        "H_Total_analytique_resultat_global": recon.total_analytique_vs_resultat_global(),
    }
    lignes = [[nom, r.get("statut"), r.get("montant_gauche"), r.get("montant_droit"),
              r.get("ecart"), r.get("tolerance")] for nom, r in reconciliations.items()]
    return _csv_response(f"resultats_reconciliation_{mois or 'aucun_mois'}",
        ["reconciliation", "statut", "montant_gauche", "montant_droit", "ecart", "tolerance"], lignes)

"""Routes Factures fournisseurs et règlements.

Les écritures passent par `factures_service` / `reglements_fournisseurs_service`, tous deux gardés
par `FACTURES_REAL_WRITE_*` (double verrou RECETTE_MODE). Le rapprochement bancaire n'est pas
réimplémenté ici : il réutilise `banques_rapprochement_service` (service unique et partagé).
"""
import app.config as cfg
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from app.template_env import get_templates

from app.services import factures_service as svc
from app.services import reglements_fournisseurs_service as reg
from app.services import fournisseurs_referentiel_service as frs
from app.services import factures_banque_service as pont
from app.services import factures_controles_service as controles
from app.services import factures_import_service as imp

router = APIRouter()
templates = get_templates()


def _ecriture_active() -> bool:
    return bool(cfg.FACTURES_REAL_WRITE_ENABLED and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED)


def _fournisseurs_actifs() -> list[dict]:
    try:
        return [f for f in frs.lister(actif_seul=True)]
    except Exception:
        return []


def _libelles_fournisseurs() -> dict[str, str]:
    try:
        return {f["fournisseur_id_opaque"]: f.get("nom", "") for f in frs.lister()}
    except Exception:
        return {}


@router.get("/factures", response_class=HTMLResponse)
def factures_list(request: Request, statut: str = "", fournisseur: str = "",
                  echues: str = "", message: str = "", erreur: str = ""):
    factures = svc.lister(statut=statut, fournisseur=fournisseur,
                          echues_seulement=bool(echues))
    return templates.TemplateResponse(request, "factures_list.html", {
        "active_menu": "factures", "factures": factures, "statuts": svc.STATUTS,
        "fournisseurs": _fournisseurs_actifs(), "libelles": _libelles_fournisseurs(),
        "applied": {"statut": statut, "fournisseur": fournisseur, "echues": echues},
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.get("/factures/a-payer", response_class=HTMLResponse)
def factures_a_payer(request: Request):
    toutes = svc.lister()
    a_payer = [f for f in toutes
               if f["statut"] in svc.STATUTS_OUVERTS and f["solde_restant"] > 0.005]
    return templates.TemplateResponse(request, "factures_a_payer.html", {
        "active_menu": "factures", "factures": a_payer, "libelles": _libelles_fournisseurs(),
        "total_du": round(sum(f["solde_restant"] for f in a_payer), 2),
        "nb_echues": sum(1 for f in a_payer if f["echue"]),
    })


@router.get("/factures/controles", response_class=HTMLResponse)
def factures_controles(request: Request, severite: str = ""):
    data = controles.controler()
    anomalies = data["anomalies"]
    if severite:
        anomalies = [a for a in anomalies if a["severite"] == severite]
    return templates.TemplateResponse(request, "factures_controles.html", {
        "active_menu": "factures", "data": data, "anomalies": anomalies,
        "niveaux": controles.NIVEAUX, "applied": {"severite": severite},
    })


@router.get("/factures/importer", response_class=HTMLResponse)
def facture_importer_form(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "factures_importer.html", {
        "active_menu": "factures", "fournisseurs": _fournisseurs_actifs(),
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/factures/importer/previsualiser")
async def facture_importer_previsualiser(request: Request):
    form = await request.form()
    fichier = form.get("fichier")
    if fichier is None or not getattr(fichier, "filename", ""):
        return RedirectResponse(url="/factures/importer?erreur=Aucun fichier sélectionné.",
                                status_code=303)
    contenu = await fichier.read()
    res = imp.previsualiser(contenu, fichier.filename,
                            fournisseur_id_opaque=str(form.get("fournisseur_id_opaque", "") or ""))
    if not res.get("ok"):
        return RedirectResponse(url=f"/factures/importer?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(url=f"/factures/importer/previsualisation/{res['token']}",
                            status_code=303)


@router.get("/factures/importer/previsualisation/{token}", response_class=HTMLResponse)
def facture_importer_previsualisation(request: Request, token: str, erreur: str = ""):
    manifest = imp.charger_manifest(token)
    if manifest is None:
        return templates.TemplateResponse(request, "factures_importer_previsualisation.html", {
            "active_menu": "factures", "manifest": None, "token": token,
        }, status_code=404)
    return templates.TemplateResponse(request, "factures_importer_previsualisation.html", {
        "active_menu": "factures", "manifest": manifest, "token": token,
        "fournisseurs": _fournisseurs_actifs(), "ecriture_active": _ecriture_active(),
        "erreur": erreur,
    })


@router.post("/factures/importer/confirmer/{token}")
async def facture_importer_confirmer(request: Request, token: str):
    form = dict(await request.form())
    corrections = {k: v for k, v in form.items() if k != "acteur"}
    res = imp.confirmer(token, corrections, acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        msgs = "; ".join(e["message"] for e in res.get("erreurs", [])) or res.get("message", "")
        return RedirectResponse(
            url=f"/factures/importer/previsualisation/{token}?erreur={msgs}", status_code=303)
    return RedirectResponse(
        url=f"/factures/{res['facture_id_opaque']}?message=Facture importée depuis le PDF.",
        status_code=303)


@router.get("/factures/nouvelle", response_class=HTMLResponse)
def facture_nouvelle(request: Request, erreur: str = ""):
    return templates.TemplateResponse(request, "factures_nouvelle.html", {
        "active_menu": "factures", "fournisseurs": _fournisseurs_actifs(),
        "ecriture_active": _ecriture_active(), "erreur": erreur,
    })


@router.post("/factures")
async def facture_creer(request: Request):
    form = dict(await request.form())
    fid = str(form.get("fournisseur_id_opaque", "") or "")
    actif = any(f["fournisseur_id_opaque"] == fid for f in _fournisseurs_actifs()) if fid else None
    res = svc.creer(form, acteur=str(form.get("acteur", "") or "local"), fournisseur_actif=actif)
    if not res.get("ok"):
        msgs = "; ".join(e["message"] for e in res.get("erreurs", [])) or res.get("message", "")
        return RedirectResponse(url=f"/factures/nouvelle?erreur={msgs}", status_code=303)
    return RedirectResponse(url=f"/factures/{res['facture_id_opaque']}?message=Facture enregistrée.",
                            status_code=303)


@router.get("/factures/{opaque}", response_class=HTMLResponse)
def facture_detail(request: Request, opaque: str, message: str = "", erreur: str = ""):
    facture = svc.charger(opaque)
    if facture is None:
        return templates.TemplateResponse(request, "factures_detail.html", {
            "active_menu": "factures", "facture": None, "opaque": opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "factures_detail.html", {
        "active_menu": "factures", "facture": facture,
        "libelles": _libelles_fournisseurs(),
        "reglements": reg.reglements_de_facture(opaque),
        "historique": svc.historique(opaque),
        "statuts": svc.STATUTS, "moyens": reg.MOYENS,
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/factures/{opaque}/statut")
async def facture_statut(request: Request, opaque: str):
    form = await request.form()
    res = svc.changer_statut(opaque, str(form.get("statut", "") or ""),
                             commentaire=str(form.get("commentaire", "") or ""),
                             acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Statut mis à jour." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


@router.post("/factures/{opaque}/lier-charge")
async def facture_lier_charge(request: Request, opaque: str):
    form = await request.form()
    res = svc.lier_charge(opaque, str(form.get("charge_id", "") or ""),
                          acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Charge rattachée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


@router.post("/factures/{opaque}/lignes")
async def facture_ajouter_ligne(request: Request, opaque: str):
    form = await request.form()
    res = svc.ajouter_ligne(
        opaque, str(form.get("charge_id", "") or ""),
        logement_id=str(form.get("logement_id", "") or ""),
        montant_ttc=form.get("montant_ttc"), montant_ht=form.get("montant_ht"),
        montant_tva=form.get("montant_tva"),
        commentaire=str(form.get("commentaire", "") or ""),
        acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Ligne ajoutée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


@router.post("/factures/{opaque}/reglement")
async def facture_reglement(request: Request, opaque: str):
    form = await request.form()
    facture = svc.charger(opaque)
    if facture is None:
        return RedirectResponse(url=f"/factures/{opaque}?erreur=Facture introuvable.", status_code=303)
    montant_txt = str(form.get("montant", "") or "").strip()
    res = reg.enregistrer(
        facture["fournisseur_id_opaque"],
        [{"facture_id_opaque": opaque, "montant": montant_txt}],
        date_reglement=str(form.get("date_reglement", "") or ""),
        moyen=str(form.get("moyen", "") or ""), compte=str(form.get("compte", "") or ""),
        commentaire=str(form.get("commentaire", "") or ""),
        acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Règlement enregistré." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/factures/{opaque}?{msg}", status_code=303)


# ── Règlements ──────────────────────────────────────────────────────────────

@router.get("/reglements", response_class=HTMLResponse)
def reglements_list(request: Request, non_rapproches: str = ""):
    reglements = reg.lister(non_rapproches_seulement=bool(non_rapproches))
    return templates.TemplateResponse(request, "reglements_fournisseurs_list.html", {
        "active_menu": "factures", "reglements": reglements,
        "libelles": _libelles_fournisseurs(),
        "applied": {"non_rapproches": non_rapproches},
    })


@router.get("/fournisseurs-soldes/{opaque}", response_class=HTMLResponse)
def fournisseur_solde(request: Request, opaque: str, message: str = "", erreur: str = ""):
    """Fiche fournisseur orientée dette : identité, solde, factures, règlements, anomalies.
    Complète `/referentiel-fournisseurs/{id}` (identité seule) sans la remplacer."""
    fournisseur = frs.charger_par_opaque(opaque)
    if fournisseur is None:
        return templates.TemplateResponse(request, "fournisseur_solde.html", {
            "active_menu": "factures", "fournisseur": None, "opaque": opaque,
        }, status_code=404)
    solde = svc.solde_fournisseur(opaque)
    reglements = reg.lister(fournisseur=opaque)
    etats = {r["reglement_id_opaque"]: pont.etat_reglement(r["reglement_id_opaque"])
             for r in reglements}
    anomalies = [a for a in controles.controler()["anomalies"]
                 if a["identifiant"] in {f["facture_ref"] for f in solde["factures"]}
                 or a["identifiant"] in etats]
    return templates.TemplateResponse(request, "fournisseur_solde.html", {
        "active_menu": "factures", "fournisseur": fournisseur, "solde": solde,
        "reglements": reglements, "etats_rapprochement": etats, "anomalies": anomalies,
        "historique": frs.historique(opaque),
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


# ── Rapprochement bancaire (réutilise le moteur Banque, aucun second moteur) ──

@router.get("/reglements/{opaque}/rapprocher", response_class=HTMLResponse)
def reglement_rapprocher_form(request: Request, opaque: str, message: str = "", erreur: str = ""):
    reglement = reg.charger(opaque)
    if reglement is None:
        return templates.TemplateResponse(request, "reglement_rapprocher.html", {
            "active_menu": "factures", "reglement": None, "opaque": opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "reglement_rapprocher.html", {
        "active_menu": "factures", "reglement": reglement,
        "etat": pont.etat_reglement(opaque),
        "candidats": pont.candidats_pour_reglement(opaque),
        "liens": pont.liens_du_reglement(opaque),
        "libelles": _libelles_fournisseurs(),
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/reglements/{opaque}/rapprocher")
async def reglement_rapprocher(request: Request, opaque: str):
    form = await request.form()
    montant_txt = str(form.get("montant", "") or "").strip()
    res = pont.rapprocher(opaque, str(form.get("mouvement_id_opaque", "") or ""),
                          float(montant_txt) if montant_txt else 0,
                          acteur=str(form.get("acteur", "") or "local"),
                          commentaire=str(form.get("commentaire", "") or ""))
    msg = ("message=Rapprochement proposé — à confirmer." if res.get("ok")
           else f"erreur={res.get('message')}")
    return RedirectResponse(url=f"/reglements/{opaque}/rapprocher?{msg}", status_code=303)


@router.post("/reglements/{opaque}/rapprochements/{rap}/confirmer")
async def reglement_rapprochement_confirmer(request: Request, opaque: str, rap: str):
    form = await request.form()
    res = pont.confirmer(rap, opaque, acteur=str(form.get("acteur", "") or "local"),
                         commentaire=str(form.get("commentaire", "") or ""))
    msg = "message=Rapprochement confirmé." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/reglements/{opaque}/rapprocher?{msg}", status_code=303)


@router.post("/reglements/{opaque}/rapprochements/{rap}/annuler")
async def reglement_rapprochement_annuler(request: Request, opaque: str, rap: str):
    form = await request.form()
    res = pont.annuler(rap, opaque, acteur=str(form.get("acteur", "") or "local"),
                       commentaire=str(form.get("commentaire", "") or ""))
    msg = "message=Rapprochement annulé." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/reglements/{opaque}/rapprocher?{msg}", status_code=303)


@router.post("/reglements/{opaque}/annuler")
async def reglement_annuler(request: Request, opaque: str):
    form = await request.form()
    res = reg.annuler(opaque, commentaire=str(form.get("commentaire", "") or ""),
                      acteur=str(form.get("acteur", "") or "local"))
    retour = str(form.get("retour", "") or "/reglements")
    msg = "message=Règlement annulé." if res.get("ok") else f"erreur={res.get('message')}"
    sep = "&" if "?" in retour else "?"
    return RedirectResponse(url=f"{retour}{sep}{msg}", status_code=303)

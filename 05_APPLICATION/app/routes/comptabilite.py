"""Routes du premier socle Comptabilité (migration `0021`).

Première verticale : consultation des journaux/écritures/plan comptable/auxiliaires, génération
d'une écriture ACHATS depuis une facture validée, validation, contrepassation. Aucun écran
Résultats ni tableau analytique — hors périmètre de cette mission (cadrage `43`).
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

import app.config as cfg
from app.config import TEMPLATES_DIR
from app.services import comptabilite_auxiliaires_service as aux
from app.services import comptabilite_controles_service as ctrl
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_periodes_service as per
from app.services import operations_caisse_service as caisse
from app.services import operations_diverses_service as od
from app.services import reglements_fournisseurs_service as regl
from app.services import ventes_lot12_adapter_service as ventes_adapter

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _ecriture_active() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


def _plan_comptable(db_path=None):
    from app.db.connection import get_db
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY compte")]
    finally:
        conn.close()


@router.get("/comptabilite", response_class=HTMLResponse)
def comptabilite_accueil(request: Request):
    ecritures = compta.lister()
    return templates.TemplateResponse(request, "comptabilite_accueil.html", {
        "active_menu": "comptabilite", "journaux": compta.JOURNAUX,
        "nb_ecritures": len(ecritures),
        "nb_proposees": sum(1 for e in ecritures if e["statut"] == compta.ST_PROPOSEE),
        "ecriture_active": _ecriture_active(),
    })


@router.get("/comptabilite/journaux", response_class=HTMLResponse)
def comptabilite_journaux(request: Request):
    par_journal = {j: compta.lister(journal=j) for j in compta.JOURNAUX}
    return templates.TemplateResponse(request, "comptabilite_journaux.html", {
        "active_menu": "comptabilite", "par_journal": par_journal,
    })


@router.get("/comptabilite/ecritures", response_class=HTMLResponse)
def comptabilite_ecritures(request: Request, journal: str = "", periode: str = "", statut: str = ""):
    ecritures = compta.lister(journal=journal, periode=periode, statut=statut)
    return templates.TemplateResponse(request, "comptabilite_ecritures.html", {
        "active_menu": "comptabilite", "ecritures": ecritures, "journaux": compta.JOURNAUX,
        "statuts": compta.STATUTS,
        "applied": {"journal": journal, "periode": periode, "statut": statut},
    })


@router.get("/comptabilite/a-controler", response_class=HTMLResponse)
def comptabilite_a_controler(request: Request):
    proposees = compta.lister(statut=compta.ST_PROPOSEE)
    return templates.TemplateResponse(request, "comptabilite_ecritures.html", {
        "active_menu": "comptabilite", "ecritures": proposees, "journaux": compta.JOURNAUX,
        "statuts": compta.STATUTS, "applied": {}, "titre": "Écritures à contrôler",
    })


@router.get("/comptabilite/plan-comptable", response_class=HTMLResponse)
def comptabilite_plan_comptable(request: Request):
    return templates.TemplateResponse(request, "comptabilite_plan_comptable.html", {
        "active_menu": "comptabilite", "comptes": _plan_comptable(),
    })


@router.get("/comptabilite/auxiliaires", response_class=HTMLResponse)
def comptabilite_auxiliaires(request: Request, auxiliaire: str = ""):
    solde = compta.solde_auxiliaire(auxiliaire) if auxiliaire else None
    return templates.TemplateResponse(request, "comptabilite_auxiliaires.html", {
        "active_menu": "comptabilite", "auxiliaire": auxiliaire, "solde": solde,
        "synthese": aux.synthese(),
    })


@router.get("/comptabilite/auxiliaires/{opaque}", response_class=HTMLResponse)
def comptabilite_auxiliaire_detail(request: Request, opaque: str, famille: str = aux.FAMILLE_FOURNISSEUR):
    if famille not in aux.FAMILLES:
        famille = aux.FAMILLE_FOURNISSEUR
    return templates.TemplateResponse(request, "comptabilite_auxiliaire_detail.html", {
        "active_menu": "comptabilite", "opaque": opaque, "fiche": aux.fiche_auxiliaire(famille, opaque),
    })


@router.get("/comptabilite/ecritures/{opaque}", response_class=HTMLResponse)
def comptabilite_ecriture_detail(request: Request, opaque: str, message: str = "", erreur: str = ""):
    e = compta.charger(opaque)
    if e is None:
        return templates.TemplateResponse(request, "comptabilite_ecriture_detail.html", {
            "active_menu": "comptabilite", "ecriture": None, "opaque": opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "comptabilite_ecriture_detail.html", {
        "active_menu": "comptabilite", "ecriture": e, "opaque": opaque,
        "lignes": compta.lignes(opaque), "ecriture_active": _ecriture_active(),
        "message": message, "erreur": erreur,
    })


@router.post("/comptabilite/ecritures/{opaque}/valider")
async def comptabilite_valider(request: Request, opaque: str):
    form = await request.form()
    res = compta.valider(opaque, acteur=str(form.get("acteur", "") or "local"))
    suffixe = "" if res.get("ok") else f"&erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/ecritures/{opaque}?message=Validée.{suffixe}",
                            status_code=303)


@router.post("/comptabilite/ecritures/{opaque}/contrepasser")
async def comptabilite_contrepasser(request: Request, opaque: str):
    form = await request.form()
    res = compta.contrepasser(opaque, commentaire=str(form.get("commentaire", "") or ""),
                              acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/comptabilite/ecritures/{opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(
        url=f"/comptabilite/ecritures/{res['miroir_id_opaque']}?message=Contrepassation générée.",
        status_code=303)


@router.post("/factures/{facture_opaque}/generer-ecriture-achat")
async def comptabilite_generer_depuis_facture(request: Request, facture_opaque: str):
    """Point d'entrée depuis la fiche facture — cf. brief §22 : « générer l'écriture ACHATS »."""
    form = await request.form()
    res = compta.generer_ecriture_achat(facture_opaque, acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/factures/{facture_opaque}?erreur={res.get('message')}",
                                status_code=303)
    return RedirectResponse(url=f"/comptabilite/ecritures/{res['ecriture_id_opaque']}", status_code=303)


# ── Journal VENTES (adaptateur Lot12) ────────────────────────────────────────

@router.get("/comptabilite/journaux/ventes", response_class=HTMLResponse)
def comptabilite_journal_ventes(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "comptabilite_journal_ventes.html", {
        "active_menu": "comptabilite", "ecritures": compta.lister(journal="VENTES"),
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur, "resultats": None,
    })


@router.post("/comptabilite/journaux/ventes/generer")
async def comptabilite_ventes_generer(request: Request):
    form = await request.form()
    mois = str(form.get("mois", "") or "")
    resultats = ventes_adapter.generer_ecritures_du_mois(
        mois, acteur=str(form.get("acteur", "") or "local"))
    return templates.TemplateResponse(request, "comptabilite_journal_ventes.html", {
        "active_menu": "comptabilite", "ecritures": compta.lister(journal="VENTES"),
        "ecriture_active": _ecriture_active(), "message": "", "erreur": "", "resultats": resultats,
    })


# ── Journal CAISSE ────────────────────────────────────────────────────────────

@router.get("/comptabilite/journaux/caisse", response_class=HTMLResponse)
def comptabilite_journal_caisse(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "comptabilite_journal_caisse.html", {
        "active_menu": "comptabilite", "ecritures": compta.lister(journal="CAISSE"),
        "operations": caisse.lister(), "types": caisse.TYPES,
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/comptabilite/journaux/caisse/operations")
async def comptabilite_caisse_creer_operation(request: Request):
    form = await request.form()
    res = caisse.creer(
        str(form.get("type_operation", "") or ""), form.get("montant"),
        date_operation=str(form.get("date_operation", "") or ""),
        tiers_type=str(form.get("tiers_type", "") or ""), tiers_id=str(form.get("tiers_id", "") or ""),
        piece=str(form.get("piece", "") or ""), commentaire=str(form.get("commentaire", "") or ""),
        acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Opération enregistrée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/journaux/caisse?{msg}", status_code=303)


@router.post("/comptabilite/journaux/caisse/operations/{opaque}/generer-ecriture")
async def comptabilite_caisse_generer_ecriture(request: Request, opaque: str):
    form = await request.form()
    res = compta.generer_ecriture_caisse_operation(opaque, acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Écriture CAISSE générée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/journaux/caisse?{msg}", status_code=303)


@router.post("/reglements/{opaque}/generer-ecriture-caisse")
async def comptabilite_generer_caisse_depuis_reglement(request: Request, opaque: str):
    form = await request.form()
    res = compta.generer_ecriture_caisse_reglement(opaque, acteur=str(form.get("acteur", "") or "local"))
    if not res.get("ok"):
        return RedirectResponse(url=f"/reglements/{opaque}?erreur={res.get('message')}", status_code=303)
    return RedirectResponse(url=f"/comptabilite/ecritures/{res['ecriture_id_opaque']}", status_code=303)


# ── Journal ODIVERSES ─────────────────────────────────────────────────────────

@router.get("/comptabilite/journaux/od", response_class=HTMLResponse)
def comptabilite_journal_od(request: Request, message: str = "", erreur: str = ""):
    return templates.TemplateResponse(request, "comptabilite_journal_od.html", {
        "active_menu": "comptabilite", "ecritures": compta.lister(journal="ODIVERSES"),
        "ods": od.lister(), "types": od.TYPES,
        "ecriture_active": _ecriture_active(), "message": message, "erreur": erreur,
    })


@router.post("/comptabilite/journaux/od")
async def comptabilite_od_creer(request: Request):
    form = await request.form()
    lignes = [
        {"compte": form.get("compte_1"), "auxiliaire": form.get("auxiliaire_1"),
         "debit": form.get("debit_1"), "credit": form.get("credit_1")},
        {"compte": form.get("compte_2"), "auxiliaire": form.get("auxiliaire_2"),
         "debit": form.get("debit_2"), "credit": form.get("credit_2")},
    ]
    res = od.creer(str(form.get("type_operation", "") or ""), str(form.get("libelle", "") or ""),
                   lignes, date_operation=str(form.get("date_operation", "") or ""),
                   justification=str(form.get("justification", "") or ""),
                   acteur=str(form.get("acteur", "") or "local"))
    msg = "message=OD créée (BROUILLON)." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/journaux/od?{msg}", status_code=303)


@router.post("/comptabilite/journaux/od/{opaque}/valider")
async def comptabilite_od_valider(request: Request, opaque: str):
    form = await request.form()
    res = od.valider(opaque, acteur=str(form.get("acteur", "") or "local"))
    msg = "message=OD validée, écriture ODIVERSES générée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/journaux/od?{msg}", status_code=303)


# ── Périodes comptables ───────────────────────────────────────────────────────

@router.get("/comptabilite/periodes", response_class=HTMLResponse)
def comptabilite_periodes(request: Request, periode: str = ""):
    if periode:
        per.obtenir_ou_creer(periode)
    return templates.TemplateResponse(request, "comptabilite_periodes.html", {
        "active_menu": "comptabilite", "periodes": per.lister(),
    })


@router.get("/comptabilite/periodes/{periode}", response_class=HTMLResponse)
def comptabilite_periode_detail(request: Request, periode: str, message: str = "", erreur: str = ""):
    p = per.obtenir_ou_creer(periode)
    return templates.TemplateResponse(request, "comptabilite_periode_detail.html", {
        "active_menu": "comptabilite", "periode": p, "historique": per.historique(periode),
        "rapport": ctrl.controler(periode=periode), "ecriture_active": _ecriture_active(),
        "message": message, "erreur": erreur,
    })


@router.post("/comptabilite/periodes/{periode}/passer-en-controle")
async def comptabilite_periode_passer_en_controle(request: Request, periode: str):
    form = await request.form()
    res = per.passer_en_controle(periode, acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Période EN_CONTROLE." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/periodes/{periode}?{msg}", status_code=303)


@router.post("/comptabilite/periodes/{periode}/rouvrir-controle")
async def comptabilite_periode_rouvrir_controle(request: Request, periode: str):
    form = await request.form()
    res = per.rouvrir_pour_controle(periode, acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Retour en contrôle." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/periodes/{periode}?{msg}", status_code=303)


@router.post("/comptabilite/periodes/{periode}/valider")
async def comptabilite_periode_valider(request: Request, periode: str):
    form = await request.form()
    res = per.valider(periode, acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Période validée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/periodes/{periode}?{msg}", status_code=303)


@router.post("/comptabilite/periodes/{periode}/cloturer")
async def comptabilite_periode_cloturer(request: Request, periode: str):
    form = await request.form()
    res = per.cloturer(periode, acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Période clôturée." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/periodes/{periode}?{msg}", status_code=303)


@router.post("/comptabilite/periodes/{periode}/rouvrir")
async def comptabilite_periode_rouvrir(request: Request, periode: str):
    form = await request.form()
    res = per.rouvrir(periode, justification=str(form.get("justification", "") or ""),
                      acteur=str(form.get("acteur", "") or "local"))
    msg = "message=Période rouverte." if res.get("ok") else f"erreur={res.get('message')}"
    return RedirectResponse(url=f"/comptabilite/periodes/{periode}?{msg}", status_code=303)


# ── Rapprochement comptable ───────────────────────────────────────────────────

@router.get("/comptabilite/rapprochement", response_class=HTMLResponse)
def comptabilite_rapprochement(request: Request):
    from app.db.connection import get_db
    conn = get_db()
    try:
        raps = conn.execute(
            "SELECT * FROM banque_rapprochements WHERE type_objet='REGLEMENT_CHARGE' "
            "AND statut='CONFIRME' ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    ecritures_banque = {e["origine_id_opaque"]: e["ecriture_id_opaque"]
                       for e in compta.lister(journal="BANQUE") if e["origine_id_opaque"]}
    lignes = []
    for rap in raps:
        reglement = regl.charger(rap["objet_id"]) if rap["objet_id"] else None
        facture_id = (reglement["repartitions"][0]["facture_id_opaque"]
                     if reglement and reglement.get("repartitions") else None)
        lignes.append({
            "rapprochement_id_opaque": rap["rapprochement_id_opaque"],
            "mouvement_id_opaque": rap["mouvement_id_opaque"],
            "montant_rapproche": rap["montant_rapproche"],
            "reglement_id_opaque": rap["objet_id"],
            "facture_id_opaque": facture_id,
            "fournisseur_id_opaque": (reglement or {}).get("fournisseur_id_opaque"),
            "ecriture_id_opaque": ecritures_banque.get(rap["rapprochement_id_opaque"]),
        })
    return templates.TemplateResponse(request, "comptabilite_rapprochement.html", {
        "active_menu": "comptabilite", "lignes": lignes,
    })

"""Routes trésorerie propriétaires (MOUVEMENT_TRESORERIE_PROPRIETAIRE, migration 0025).

Aucune règle métier ici — tout délégué à `proprietaires_tresorerie_service`. Aucune écriture
Excel, aucune écriture comptable automatique, aucune confirmation de rapprochement automatique.
"""
from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
# Import du MODULE, pas du nom : `from ... import find_proprietaire` fige la
# fonction au chargement de la route, et toute redirection ultérieure du
# référentiel reste sans effet. Même raison que les chemins lus à l'appel.
from app.readers import proprietaires_reader
from app.services import proprietaires_tresorerie_service as svc
from app.services import tresorerie_controles_service as ctrl

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

PAGE_SIZE = 20


def _paginer(lignes: list[dict], page: int) -> tuple[list[dict], int]:
    page = max(1, page)
    total = len(lignes)
    debut = (page - 1) * PAGE_SIZE
    return lignes[debut:debut + PAGE_SIZE], total


def _enrichir(m: dict) -> dict:
    return {**m, "montant_rapproche": svc.montant_rapproche(m["mouvement_opaque"]),
            "reste_a_rapprocher": svc.reste_a_rapprocher(m["mouvement_opaque"])}


def _filtrer(lignes: list[dict], *, sens: str = "", nature: str = "", statut: str = "",
            logement_id: str = "", reference: str = "", montant_min: float | None = None,
            montant_max: float | None = None, avec_reste: bool = False,
            totalement_rapproche: bool = False) -> list[dict]:
    out = lignes
    if sens:
        out = [m for m in out if m["sens"] == sens]
    if nature:
        out = [m for m in out if m["nature"] == nature]
    if statut:
        out = [m for m in out if m["statut"] == statut]
    if logement_id:
        out = [m for m in out if m.get("logement_id") == logement_id]
    if reference:
        ref_low = reference.lower()
        out = [m for m in out if ref_low in (m.get("reference_metier") or "").lower()]
    if montant_min is not None:
        out = [m for m in out if m["montant"] >= montant_min]
    if montant_max is not None:
        out = [m for m in out if m["montant"] <= montant_max]
    if avec_reste or totalement_rapproche:
        enrichies = [_enrichir(m) for m in out]
        if avec_reste:
            enrichies = [m for m in enrichies if m["reste_a_rapprocher"] > 0.005]
        if totalement_rapproche:
            enrichies = [m for m in enrichies if m["reste_a_rapprocher"] <= 0.005]
        out = enrichies
    return out


@router.get("/proprietaires/tresorerie", response_class=HTMLResponse)
def tresorerie_liste_globale(
    request: Request, proprietaire_id: str = "", logement_id: str = "", sens: str = "",
    nature: str = "", statut: str = "", reference: str = "", montant_min: float | None = None,
    montant_max: float | None = None, avec_reste: bool = False, totalement_rapproche: bool = False,
    page: int = 1,
):
    lignes = svc.lister(proprietaire_id=proprietaire_id, statut=statut)
    lignes = _filtrer(lignes, sens=sens, nature=nature, logement_id=logement_id,
                      reference=reference, montant_min=montant_min, montant_max=montant_max,
                      avec_reste=avec_reste, totalement_rapproche=totalement_rapproche)
    if not avec_reste and not totalement_rapproche:
        lignes = [_enrichir(m) for m in lignes]
    page_lignes, total = _paginer(lignes, page)
    return templates.TemplateResponse(request, "tresorerie_list.html", {
        "active_menu": "proprietaires", "lignes": page_lignes, "total": total, "page": page,
        "page_size": PAGE_SIZE, "proprietaire_id": proprietaire_id, "logement_id": logement_id,
        "sens": sens, "nature": nature, "statut": statut, "reference": reference,
        "montant_min": montant_min, "montant_max": montant_max, "avec_reste": avec_reste,
        "totalement_rapproche": totalement_rapproche, "natures": svc.NATURES, "sens_valeurs": svc.SENS,
        "statuts": svc.STATUTS, "proprietaire_filtre": None,
        # Les dix contrôles Lot 5, calculés sur les mouvements et non sur la page affichée : un
        # constat qui n'apparaît qu'en page 3 doit être visible dès la page 1.
        "controles": ctrl.controler(proprietaire_id=proprietaire_id),
    })


@router.get("/proprietaires/{proprietaire_id}/tresorerie", response_class=HTMLResponse)
def tresorerie_liste_proprietaire(request: Request, proprietaire_id: str, page: int = 1,
                                  statut: str = ""):
    prop = proprietaires_reader.find_proprietaire(proprietaire_id)
    if prop is None:
        return templates.TemplateResponse(request, "tresorerie_list.html", {
            "active_menu": "proprietaires", "lignes": [], "total": 0, "page": 1,
            "page_size": PAGE_SIZE, "proprietaire_id": proprietaire_id, "proprietaire_filtre": None,
            "logement_id": "", "sens": "", "nature": "", "statut": statut, "reference": "",
            "montant_min": None, "montant_max": None, "avec_reste": False,
            "totalement_rapproche": False, "natures": svc.NATURES, "sens_valeurs": svc.SENS,
            "statuts": svc.STATUTS,
        }, status_code=404)
    lignes = [_enrichir(m) for m in svc.lister(proprietaire_id=proprietaire_id, statut=statut)]
    page_lignes, total = _paginer(lignes, page)
    return templates.TemplateResponse(request, "tresorerie_list.html", {
        "active_menu": "proprietaires", "lignes": page_lignes, "total": total, "page": page,
        "page_size": PAGE_SIZE, "proprietaire_id": proprietaire_id, "proprietaire_filtre": prop,
        "logement_id": "", "sens": "", "nature": "", "statut": statut, "reference": "",
        "montant_min": None, "montant_max": None, "avec_reste": False,
        "totalement_rapproche": False, "natures": svc.NATURES, "sens_valeurs": svc.SENS,
        "statuts": svc.STATUTS, "solde": svc.solde(proprietaire_id),
    })


@router.get("/proprietaires/{proprietaire_id}/tresorerie/nouveau", response_class=HTMLResponse)
def tresorerie_nouveau_form(request: Request, proprietaire_id: str, erreur: str = ""):
    prop = proprietaires_reader.find_proprietaire(proprietaire_id)
    if prop is None:
        return templates.TemplateResponse(request, "tresorerie_nouveau.html", {
            "active_menu": "proprietaires", "proprietaire_id": proprietaire_id,
            "proprietaire_filtre": None, "natures": svc.NATURES, "sens_valeurs": svc.SENS,
        }, status_code=404)
    return templates.TemplateResponse(request, "tresorerie_nouveau.html", {
        "active_menu": "proprietaires", "proprietaire_id": proprietaire_id,
        "proprietaire_filtre": prop, "natures": svc.NATURES, "sens_valeurs": svc.SENS,
        "erreur": erreur,
    })


@router.post("/proprietaires/{proprietaire_id}/tresorerie/previsualiser", response_class=HTMLResponse)
async def tresorerie_previsualiser(request: Request, proprietaire_id: str):
    form = await request.form()
    champs = {
        "sens": (form.get("sens") or "").strip(), "nature": (form.get("nature") or "").strip(),
        "montant": (form.get("montant") or "").strip(),
        "date_mouvement": (form.get("date_mouvement") or "").strip(),
        "logement_id": (form.get("logement_id") or "").strip(),
        "mode_reglement": (form.get("mode_reglement") or "").strip(),
        "reference_metier": (form.get("reference_metier") or "").strip(),
        "justification": (form.get("justification") or "").strip(),
    }
    res = svc.previsualiser(proprietaire_id, champs["sens"], champs["nature"], champs["montant"],
                            champs["date_mouvement"], logement_id=champs["logement_id"],
                            mode_reglement=champs["mode_reglement"],
                            reference_metier=champs["reference_metier"],
                            justification=champs["justification"])
    if not res["ok"]:
        return RedirectResponse(
            url=f"/proprietaires/{proprietaire_id}/tresorerie/nouveau?erreur={quote(res['message'])}",
            status_code=303)
    return templates.TemplateResponse(request, "tresorerie_previsualisation.html", {
        "active_menu": "proprietaires", "proprietaire_id": proprietaire_id,
        "apercu": res["apercu"], "champs": champs,
    })


@router.post("/proprietaires/{proprietaire_id}/tresorerie/confirmer")
async def tresorerie_confirmer(request: Request, proprietaire_id: str):
    form = await request.form()
    res = svc.creer(
        proprietaire_id, (form.get("sens") or "").strip(), (form.get("nature") or "").strip(),
        (form.get("montant") or "").strip(), (form.get("date_mouvement") or "").strip(),
        logement_id=(form.get("logement_id") or "").strip(),
        mode_reglement=(form.get("mode_reglement") or "").strip(),
        reference_metier=(form.get("reference_metier") or "").strip(),
        justification=(form.get("justification") or "").strip(), acteur="local")
    if not res["ok"]:
        return RedirectResponse(
            url=f"/proprietaires/{proprietaire_id}/tresorerie/nouveau?erreur={quote(res['message'])}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires/tresorerie/{res['mouvement_opaque']}",
                            status_code=303)


def _detail_ctx(mouvement_opaque: str) -> dict | None:
    m = svc.charger(mouvement_opaque)
    if m is None:
        return None
    prop = proprietaires_reader.find_proprietaire(m["proprietaire_id"])
    return {
        "mouvement": m, "proprietaire": prop,
        "montant_rapproche": svc.montant_rapproche(mouvement_opaque),
        "reste_a_rapprocher": svc.reste_a_rapprocher(mouvement_opaque),
        "historique": svc.historique(mouvement_opaque),
        "modifiable": m["statut"] == svc.ST_BROUILLON,
        "validable": m["statut"] in (svc.ST_BROUILLON, svc.ST_A_CONTROLER),
        "annulable": m["statut"] != svc.ST_ANNULE,
    }


@router.get("/proprietaires/tresorerie/{mouvement_opaque}", response_class=HTMLResponse)
def tresorerie_detail(request: Request, mouvement_opaque: str, erreur: str = ""):
    ctx = _detail_ctx(mouvement_opaque)
    if ctx is None:
        return templates.TemplateResponse(request, "tresorerie_detail.html", {
            "active_menu": "proprietaires", "mouvement": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "tresorerie_detail.html", {
        "active_menu": "proprietaires", "erreur": erreur, **ctx})


@router.post("/proprietaires/tresorerie/{mouvement_opaque}/modifier")
async def tresorerie_modifier(request: Request, mouvement_opaque: str):
    form = await request.form()
    champs = {k: (form.get(k) or "").strip() for k in
             ("sens", "nature", "montant", "date_mouvement", "logement_id", "mode_reglement",
              "reference_metier", "justification")
             if form.get(k) is not None and str(form.get(k)).strip() != ""}
    res = svc.modifier_brouillon(mouvement_opaque, acteur="local", **champs)
    if not res["ok"]:
        return RedirectResponse(
            url=f"/proprietaires/tresorerie/{mouvement_opaque}?erreur={quote(res['message'])}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires/tresorerie/{mouvement_opaque}", status_code=303)


@router.post("/proprietaires/tresorerie/{mouvement_opaque}/valider")
async def tresorerie_valider(request: Request, mouvement_opaque: str):
    res = svc.valider(mouvement_opaque, acteur="local")
    if not res["ok"]:
        return RedirectResponse(
            url=f"/proprietaires/tresorerie/{mouvement_opaque}?erreur={quote(res['message'])}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires/tresorerie/{mouvement_opaque}", status_code=303)


@router.post("/proprietaires/tresorerie/{mouvement_opaque}/annuler")
async def tresorerie_annuler(request: Request, mouvement_opaque: str):
    form = await request.form()
    justification = (form.get("justification") or "").strip()
    if not justification:
        return RedirectResponse(
            url=f"/proprietaires/tresorerie/{mouvement_opaque}?erreur={quote('Justification obligatoire pour annuler.')}",
            status_code=303)
    res = svc.annuler(mouvement_opaque, commentaire=justification, acteur="local")
    if not res["ok"]:
        return RedirectResponse(
            url=f"/proprietaires/tresorerie/{mouvement_opaque}?erreur={quote(res['message'])}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires/tresorerie/{mouvement_opaque}", status_code=303)


@router.get("/proprietaires/tresorerie/{mouvement_opaque}/historique", response_class=HTMLResponse)
def tresorerie_historique(request: Request, mouvement_opaque: str):
    m = svc.charger(mouvement_opaque)
    if m is None:
        return templates.TemplateResponse(request, "tresorerie_historique.html", {
            "active_menu": "proprietaires", "mouvement": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "tresorerie_historique.html", {
        "active_menu": "proprietaires", "mouvement": m,
        "historique": svc.historique(mouvement_opaque)})

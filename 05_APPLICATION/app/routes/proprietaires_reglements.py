"""Routes Propriétaires & règlements (APP-3C + APP-3D) — pilotage en lecture seule.

Aucun virement, aucune génération de facture, aucun déclenchement Lot12, aucune écriture réelle.
Les statuts et montants viennent du moteur (Lot10 / Lot12) — jamais recalculés.

Point d'entrée UNIQUE (`/proprietaires-reglements`) pour la consultation (APP-3C, tableau de bord) et
la préparation humaine de relevé/préfacture (APP-3D, suivi de facturation, identifiants opaques
`REG-<hash>`). `/proprietaires-reglements/{identifiant}` accepte à la fois un `proprietaire_id` brut
(comportement historique inchangé, compatibilité ascendante) et un identifiant opaque `REG-` (fiche
de suivi APP-3D) — jamais deux modules parallèles dans la navigation.
"""
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.readers import controles_cloture_reader as ref_reader
from app.readers import rapprochement_bancaire_reader as banque_contrat
from app.services import charges_affectations_service as charges_aff
from app.services import clotures_service as cs
from app.services import fournisseurs_referentiel_service as frs
from app.services import proprietaires_blocages_service as blocages
from app.services import proprietaires_paiement_service as pay
from app.services import proprietaires_releve_cycle_service as cycle_svc
from app.services import proprietaires_releve_export_service as export_svc
from app.services import proprietaires_reglements_service as svc
from app.services import proprietaires_service as legacy_svc
from app.services import proprietaires_suivi_service as suivi
from app.services import rapprochement_candidats_service as rap_cand
from app.services import rapprochement_reglements_service as rap

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _statut_moteur_mois(mois: str) -> str:
    ref = ref_reader.cloture_ref()
    if not ref.etat.disponible:
        return "SOURCE_INDISPONIBLE"
    for r in ref.lignes:
        if ref_reader.to_mois(r.get("mois")) == mois:
            return ref_reader.to_texte(r.get("statut_mois")).upper() or "INCONNU"
    return "INCONNU"


def _statut_cloture_humain(mois: str) -> str | None:
    c = cs.charger_par_mois(mois)
    return cs.STATUTS_LIBELLES.get(c["statut"], c["statut"]) if c else None


# ── APP-3C — Tableau de bord (inchangé) ───────────────────────────────────────

@router.get("/proprietaires-reglements", response_class=HTMLResponse)
def reglements_dashboard(
    request: Request,
    mois: str = "",
    proprietaire_id: str = "",
    logement_id: str = "",
    statut: str = "",
    avec_anomalie: bool = False,
    avec_reste: bool = False,
    facture: str = "",
    regle: str = "",
    tri: str = "anomalie",
    page: int = 1,
):
    data = svc.load_dashboard(
        mois=mois, proprietaire_id=proprietaire_id, logement_id=logement_id, statut=statut,
        avec_anomalie=avec_anomalie, avec_reste=avec_reste, facture=facture, regle=regle,
        tri=tri, page=page,
    )
    suivis = {}
    if mois:
        suivis = {r["proprietaire_id"]: r for r in suivi.lister(mois=mois)}
    return templates.TemplateResponse(request, "reglements_list.html", {
        "active_menu": "proprietaires", "data": data, "suivis_facturation": suivis,
    })


@router.get("/proprietaires-reglements/a-controler", response_class=HTMLResponse)
def reglements_a_controler(request: Request, mois: str = ""):
    data = svc.load_to_control(mois)
    return templates.TemplateResponse(request, "reglements_a_controler.html", {
        "active_menu": "proprietaires", "data": data,
    })


@router.get("/proprietaires-reglements/export.csv")
def reglements_export_csv(
    request: Request,
    mois: str = "",
    proprietaire_id: str = "",
    logement_id: str = "",
    statut: str = "",
    avec_anomalie: bool = False,
    avec_reste: bool = False,
    facture: str = "",
    regle: str = "",
    tri: str = "anomalie",
):
    contenu = svc.export_csv(
        mois=mois, proprietaire_id=proprietaire_id, logement_id=logement_id, statut=statut,
        avec_anomalie=avec_anomalie, avec_reste=avec_reste, facture=facture, regle=regle, tri=tri,
    )
    nom = f"proprietaires_reglements_{mois or 'tous'}.csv"
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})


# ── APP-3E — Préparation des règlements, SANS virement (route statique, avant {identifiant}) ──
# Aucun virement, aucune API bancaire, aucun IBAN. MARQUE_COMME_PAYE est une déclaration humaine.

@router.get("/proprietaires-reglements/a-payer", response_class=HTMLResponse)
def a_payer_liste(request: Request):
    lignes = []
    for p in pay.lister_a_payer():
        r = suivi.charger_par_opaque(p["releve_id_opaque"])
        if r is None:
            continue
        c = cycle_svc.charger(p["releve_id_opaque"])
        detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
        montant = None
        if detail and detail.get("status") == "OK" and detail.get("vue"):
            montant = detail["vue"].get("net")
        rapp = rap.charger_par_releve(r["releve_id_opaque"])
        lignes.append({
            "proprietaire_id": r["proprietaire_id"], "mois": r["mois"],
            "releve_id_opaque": r["releve_id_opaque"],
            "statut_releve": r["statut_facturation"], "etat_cycle": c["etat_cycle"] if c else "NON_DEMARRE",
            "statut_moteur": _statut_moteur_mois(r["mois"]),
            "montant_moteur": montant if montant is not None else "DONNEE_MOTEUR_INDISPONIBLE",
            "statut_paiement": p["statut_paiement"],
            "statut_rapprochement": rapp["statut"] if rapp else rap.ST_NON_RAPPROCHE,
            "nb_candidats": rapp["score_explicable"] if rapp and rapp.get("mouvement_id_opaque") else None,
            "date_modification": p["date_modification"],
        })
    return templates.TemplateResponse(request, "proprietaires_a_payer.html", {
        "active_menu": "proprietaires", "lignes": lignes, "mention_rapprochement": rap.MENTION})


@router.get("/proprietaires-reglements/a-payer/export.csv")
def a_payer_export_csv():
    import csv
    import io
    from app.services.proprietaires_releve_export_service import _cellule_sure
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow(["# Fichier préparatoire interne — ne constitue pas un ordre bancaire."])
    w.writerow(["releve_id_opaque", "proprietaire_id", "mois", "statut_paiement", "reference_interne",
               "date_preparation"])
    for p in pay.lister_a_payer():
        r = suivi.charger_par_opaque(p["releve_id_opaque"])
        if r is None:
            continue
        w.writerow([_cellule_sure(r["releve_id_opaque"]), _cellule_sure(r["proprietaire_id"]),
                   _cellule_sure(r["mois"]), _cellule_sure(p["statut_paiement"]),
                   _cellule_sure(p.get("reference_interne_paiement") or ""),
                   _cellule_sure(p["date_modification"])])
    return Response(content=buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="a_payer_preparatoire.csv"'})


# ── APP-3D — Démarrer un suivi de facturation (route statique, avant {identifiant}) ──

@router.post("/proprietaires-reglements/demarrer")
async def releve_demarrer(request: Request):
    form = await request.form()
    proprietaire_id = (form.get("proprietaire_id") or "").strip()
    mois = (form.get("mois") or "").strip()
    try:
        r = suivi.creer_ou_charger(proprietaire_id, mois, acteur="local")
    except suivi.ReleveRefuse as exc:
        return RedirectResponse(url=f"/proprietaires-reglements?mois={mois}&erreur={quote(str(exc))}",
                                status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{r['releve_id_opaque']}", status_code=303)


# ── Fiche : accepte un proprietaire_id brut (APP-3C, inchangé) OU un REG- opaque (APP-3D) ───

def _fiche_releve_ctx(releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return None
    detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
    eval_blocages = blocages.evaluer(r["proprietaire_id"], r["mois"])
    actions = []
    suivants = suivi.TRANSITIONS.get(r["statut_facturation"], set())
    if suivi.ST_A_FACTURER in suivants and r["statut_facturation"] != suivi.ST_FACTURE:
        actions.append("marquer_a_facturer")
    if suivi.ST_FACTURE in suivants:
        actions.append("marquer_facture")
    if suivi.ST_AVOIR_A_EMETTRE in suivants:
        actions.append("marquer_avoir_a_emettre")
    if suivi.ST_AVOIR_EMIS in suivants:
        actions.append("marquer_avoir_emis")
    return {
        "releve": r, "detail": detail, "blocages": eval_blocages, "actions": actions,
        "statut_moteur": _statut_moteur_mois(r["mois"]),
        "statut_cloture_humain": _statut_cloture_humain(r["mois"]),
    }


@router.get("/proprietaires-reglements/{identifiant}", response_class=HTMLResponse)
def reglements_detail(request: Request, identifiant: str, mois: str = "", erreur: str = ""):
    if identifiant.startswith("REG-"):
        ctx = _fiche_releve_ctx(identifiant)
        if ctx is None:
            return templates.TemplateResponse(request, "proprietaire_releve_fiche.html", {
                "active_menu": "proprietaires", "releve": None,
            }, status_code=404)
        return templates.TemplateResponse(request, "proprietaire_releve_fiche.html", {
            "active_menu": "proprietaires", "erreur": erreur, **ctx})

    # Comportement historique APP-3C inchangé : proprietaire_id brut.
    detail = svc.load_owner_detail(identifiant, mois)
    if detail is None:
        return templates.TemplateResponse(request, "reglements_detail.html", {
            "active_menu": "proprietaires", "detail": None, "proprietaire_id": identifiant,
        }, status_code=404)
    charges_affectees = []
    if mois:
        for a in charges_aff.lister_par_proprietaire_mois(identifiant, mois):
            fournisseur = frs.charger_par_opaque(a["fournisseur_id_opaque"]) if a.get("fournisseur_id_opaque") else None
            charges_affectees.append({**a, "fournisseur_nom": fournisseur["nom"] if fournisseur else None})
    return templates.TemplateResponse(request, "reglements_detail.html", {
        "active_menu": "proprietaires", "detail": detail, "proprietaire_id": identifiant,
        "charges_affectees": charges_affectees,
    })


def _snapshot_donnees(r: dict, data: dict) -> dict:
    return {**data, "proprietaire_id": r["proprietaire_id"], "mois": r["mois"]}


@router.get("/proprietaires-reglements/{releve_opaque}/releve", response_class=HTMLResponse)
def releve_detail(request: Request, releve_opaque: str, erreur: str = ""):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return templates.TemplateResponse(request, "proprietaire_releve_detail.html", {
            "active_menu": "proprietaires", "releve": None,
        }, status_code=404)
    data = legacy_svc.load_releve(r["proprietaire_id"], r["mois"])
    cycle = cycle_svc.creer_ou_charger(releve_opaque)
    derive = cycle_svc.detecter_derive(cycle, _snapshot_donnees(r, data))
    eval_blocages = blocages.evaluer(r["proprietaire_id"], r["mois"])
    paiement = pay.creer_ou_charger(releve_opaque)
    return templates.TemplateResponse(request, "proprietaire_releve_detail.html", {
        "active_menu": "proprietaires", "releve": r, "data": data, "cycle": cycle,
        "derive": derive, "blocages": eval_blocages, "erreur": erreur, "paiement": paiement})


@router.post("/proprietaires-reglements/{releve_opaque}/cycle/demarrer")
async def releve_cycle_demarrer(request: Request, releve_opaque: str):
    cycle = cycle_svc.creer_ou_charger(releve_opaque)
    try:
        cycle_svc.demarrer(cycle, acteur="local", version_attendue=cycle["version"])
    except cycle_svc.CycleRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/cycle/valider")
async def releve_cycle_valider(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    cycle = cycle_svc.creer_ou_charger(releve_opaque)
    eval_blocages = blocages.evaluer(r["proprietaire_id"], r["mois"])
    data = legacy_svc.load_releve(r["proprietaire_id"], r["mois"])
    try:
        if cycle["etat_cycle"] not in (cycle_svc.ETAT_EN_PREPARATION, cycle_svc.ETAT_A_VALIDER):
            cycle = cycle_svc.demarrer(cycle, acteur="local", version_attendue=cycle["version"])
        if cycle["etat_cycle"] == cycle_svc.ETAT_EN_PREPARATION:
            cycle = cycle_svc.marquer_a_valider(cycle, acteur="local", version_attendue=cycle["version"])
        cycle_svc.valider(cycle, _snapshot_donnees(r, data), bloquants=eval_blocages["bloquants"],
                          acteur="local", version_attendue=cycle["version"])
    except cycle_svc.CycleRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/cycle/reouvrir")
async def releve_cycle_reouvrir(request: Request, releve_opaque: str):
    cycle = cycle_svc.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        cycle_svc.rouvrir(cycle, motif, acteur="local", version_attendue=cycle["version"])
    except cycle_svc.CycleRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.get("/proprietaires-reglements/{releve_opaque}/prefacture", response_class=HTMLResponse)
def releve_prefacture(request: Request, releve_opaque: str, erreur: str = ""):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return templates.TemplateResponse(request, "proprietaire_releve_prefacture.html", {
            "active_menu": "proprietaires", "releve": None,
        }, status_code=404)
    data = legacy_svc.load_prefacture(r["proprietaire_id"], r["mois"])
    eval_blocages = blocages.evaluer(r["proprietaire_id"], r["mois"])
    return templates.TemplateResponse(request, "proprietaire_releve_prefacture.html", {
        "active_menu": "proprietaires", "releve": r, "data": data, "erreur": erreur,
        "blocages": eval_blocages})


@router.post("/proprietaires-reglements/{releve_opaque}/marquer-a-facturer")
async def releve_marquer_a_facturer(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    eval_blocages = blocages.evaluer(r["proprietaire_id"], r["mois"])
    if not eval_blocages["preparable"]:
        codes = ", ".join(eval_blocages["bloquants"])
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}?erreur={quote('Bloqueur(s) : ' + codes)}",
            status_code=303)
    try:
        suivi.marquer_a_facturer(r, acteur="local", version_attendue=r["version"])
    except suivi.ReleveRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/marquer-facture")
async def releve_marquer_facture(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    form = await request.form()
    commentaire = (form.get("commentaire") or "").strip()
    try:
        suivi.marquer_facture(r, acteur="local", commentaire=commentaire, version_attendue=r["version"])
    except suivi.ReleveRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/prefacture?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/demander-avoir")
async def releve_demander_avoir(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        suivi.marquer_avoir_a_emettre(r, acteur="local", motif=motif, version_attendue=r["version"])
    except suivi.ReleveRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/emettre-avoir")
async def releve_emettre_avoir(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    try:
        suivi.marquer_avoir_emis(r, acteur="local", version_attendue=r["version"])
    except suivi.ReleveRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}", status_code=303)


@router.get("/proprietaires-reglements/{releve_opaque}/reouvrir", response_class=HTMLResponse)
def releve_reouvrir_form(request: Request, releve_opaque: str, erreur: str = ""):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return templates.TemplateResponse(request, "proprietaire_releve_reouvrir.html", {
            "active_menu": "proprietaires", "releve": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "proprietaire_releve_reouvrir.html", {
        "active_menu": "proprietaires", "releve": r, "erreur": erreur})


@router.post("/proprietaires-reglements/{releve_opaque}/reouvrir")
async def releve_reouvrir(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    form = await request.form()
    justification = (form.get("justification") or "").strip()
    try:
        suivi.rouvrir(r, acteur="local", justification=justification, version_attendue=r["version"])
    except suivi.ReleveRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/reouvrir?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}", status_code=303)


# ── APP-3E — Préparation du règlement (actions), SANS virement ───────────────

@router.post("/proprietaires-reglements/{releve_opaque}/paiement/controler")
async def paiement_controler(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    p = pay.creer_ou_charger(releve_opaque)
    try:
        pay.demarrer_controle(p, acteur="local", version_attendue=p["version"])
    except pay.PaiementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/paiement/pret-a-payer")
async def paiement_pret_a_payer(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements", status_code=303)
    p = pay.creer_ou_charger(releve_opaque)
    cycle = cycle_svc.creer_ou_charger(releve_opaque)
    data = legacy_svc.load_releve(r["proprietaire_id"], r["mois"])
    derive = cycle_svc.detecter_derive(cycle, _snapshot_donnees(r, data))
    detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
    montant_dispo = bool(detail and detail.get("status") == "OK" and detail.get("vue")
                        and detail["vue"].get("net") is not None)
    codes = pay.controler_passage_pret_a_payer(
        cycle_etat=cycle["etat_cycle"], derive=derive["derive"], statut_app5c_compatible=True,
        statut_moteur_compatible=_statut_moteur_mois(r["mois"]) == "CLOTURE",
        montant_moteur_disponible=montant_dispo, proprietaire_connu=detail is not None,
        source_obligatoire_disponible=detail is not None and detail.get("status") == "OK")
    try:
        pay.marquer_pret_a_payer(p, bloquants=codes, acteur="local", version_attendue=p["version"])
    except pay.PaiementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/paiement/marquer-paye")
async def paiement_marquer_paye(request: Request, releve_opaque: str):
    p = pay.creer_ou_charger(releve_opaque)
    form = await request.form()
    reference = (form.get("reference_interne") or "").strip()
    commentaire = (form.get("commentaire") or "").strip()
    try:
        pay.marquer_paye(p, reference_interne=reference, commentaire=commentaire, acteur="local",
                         version_attendue=p["version"])
    except pay.PaiementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/paiement/reouvrir")
async def paiement_reouvrir(request: Request, releve_opaque: str):
    p = pay.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        pay.rouvrir(p, motif, acteur="local", version_attendue=p["version"])
    except pay.PaiementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/paiement/annuler")
async def paiement_annuler(request: Request, releve_opaque: str):
    p = pay.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        pay.annuler(p, motif, acteur="local", version_attendue=p["version"])
    except pay.PaiementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/releve?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/releve", status_code=303)


# ── APP-3F — Rapprochement déclaratif règlement ↔ mouvement bancaire (lecture seule) ──
# Aucun paiement, aucun virement, aucune écriture bancaire, aucune API bancaire, aucun IBAN.

def _rapprochement_ctx(releve_opaque: str) -> dict | None:
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return None
    paiement = pay.creer_ou_charger(releve_opaque)
    rapp = rap.creer_ou_charger(releve_opaque)
    detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
    montant_declare = None
    logements = []
    if detail and detail.get("status") == "OK" and detail.get("vue"):
        montant_declare = detail["vue"].get("net")
        logements = [l.get("logement_id") for l in detail.get("logements", [])]
    reglement_paye = paiement["statut_paiement"] == pay.ST_MARQUE_COMME_PAYE
    reglement_annule = paiement["statut_paiement"] == pay.ST_ANNULE
    # Candidat sélectionné (le cas échéant) résolu depuis la source courante (détection disparition).
    candidat_courant = None
    mouvement_present = None
    if rapp.get("mouvement_id_opaque"):
        candidat_courant = banque_contrat.charger_mouvement(rapp["mouvement_id_opaque"])
        mouvement_present = candidat_courant is not None
    controles = rap_cand.evaluer_controles(
        source_etat_code=None, reglement_paye=reglement_paye, reglement_annule=reglement_annule,
        nb_candidats=1 if rapp.get("mouvement_id_opaque") else 0, mouvement_present=mouvement_present,
        empreinte_snapshot=rapp.get("mouvement_empreinte") or "",
        empreinte_courante=candidat_courant.empreinte if candidat_courant else "",
        ecart_montant=rapp.get("ecart_montant"),
        sens_sortant=(candidat_courant.sens == "DEBIT") if candidat_courant else None)
    import json as _json
    criteres = _json.loads(rapp["criteres_json"]) if rapp.get("criteres_json") else []
    return {
        "releve": r, "paiement": paiement, "rapprochement": rapp, "montant_declare": montant_declare,
        "logements": logements, "date_declaration": paiement.get("date_paiement"),
        "reference_interne": paiement.get("reference_interne_paiement"),
        "candidat": candidat_courant, "mouvement_present": mouvement_present,
        "criteres": criteres, "controles": controles,
        "historique": rap.historique(rapp["rapprochement_id_opaque"]),
        "mention_rapprochement": rap.MENTION,
    }


@router.get("/proprietaires-reglements/{releve_opaque}/rapprochement", response_class=HTMLResponse)
def rapprochement_fiche(request: Request, releve_opaque: str, erreur: str = ""):
    ctx = _rapprochement_ctx(releve_opaque)
    if ctx is None:
        return templates.TemplateResponse(request, "rapprochement_fiche.html", {
            "active_menu": "proprietaires", "releve": None}, status_code=404)
    return templates.TemplateResponse(request, "rapprochement_fiche.html", {
        "active_menu": "proprietaires", "erreur": erreur, **ctx})


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/rechercher")
async def rapprochement_rechercher(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return RedirectResponse(url="/proprietaires-reglements/a-payer", status_code=303)
    paiement = pay.creer_ou_charger(releve_opaque)
    rapp = rap.creer_ou_charger(releve_opaque)
    detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
    montant = detail["vue"].get("net") if detail and detail.get("status") == "OK" and detail.get("vue") else None
    deja = {x["mouvement_id_opaque"] for x in rap.lister()
            if x["statut"] == rap.ST_RAPPROCHE and x["mouvement_id_opaque"]}
    res = rap_cand.chercher_candidats(
        montant_declare=montant, date_declaree=paiement.get("date_paiement") or "", mois=r["mois"],
        reference_interne=paiement.get("reference_interne_paiement") or "",
        mouvements_deja_rapproches=deja)
    candidats = res["candidats"]
    try:
        if candidats:
            best = candidats[0]
            m = best["mouvement"]
            ecart_j = best["ecart_jours"]
            rap.enregistrer_proposition(
                rapp, m.mouvement_opaque, criteres=best["criteres"], score=best["score"],
                mouvement_empreinte=m.empreinte,
                ecart_montant=best["ecart_montant"], ecart_jours=ecart_j,
                acteur="local", version_attendue=rapp["version"])
        else:
            rap.signaler_anomalie(rapp, "Aucun mouvement candidat trouvé", acteur="local",
                                  version_attendue=rapp["version"]) if False else None
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/controler")
async def rapprochement_controler(request: Request, releve_opaque: str):
    rapp = rap.creer_ou_charger(releve_opaque)
    try:
        rap.passer_a_controler(rapp, acteur="local", version_attendue=rapp["version"])
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/confirmer")
async def rapprochement_confirmer(request: Request, releve_opaque: str):
    rapp = rap.creer_ou_charger(releve_opaque)
    paiement = pay.creer_ou_charger(releve_opaque)
    form = await request.form()
    commentaire = (form.get("commentaire") or "").strip()
    candidat = banque_contrat.charger_mouvement(rapp["mouvement_id_opaque"]) if rapp.get("mouvement_id_opaque") else None
    try:
        rap.confirmer(
            rapp, reglement_paye=(paiement["statut_paiement"] == pay.ST_MARQUE_COMME_PAYE),
            mouvement_present=candidat is not None,
            sens_sortant=(candidat.sens == "DEBIT") if candidat else False,
            acteur="local", commentaire=commentaire, version_attendue=rapp["version"])
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/ecarter")
async def rapprochement_ecarter(request: Request, releve_opaque: str):
    rapp = rap.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        rap.ecarter(rapp, motif, acteur="local", version_attendue=rapp["version"])
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/anomalie")
async def rapprochement_anomalie(request: Request, releve_opaque: str):
    rapp = rap.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        rap.signaler_anomalie(rapp, motif, acteur="local", version_attendue=rapp["version"])
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/rouvrir")
async def rapprochement_rouvrir(request: Request, releve_opaque: str):
    rapp = rap.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        rap.rouvrir(rapp, motif, acteur="local", version_attendue=rapp["version"])
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.post("/proprietaires-reglements/{releve_opaque}/rapprochement/annuler")
async def rapprochement_annuler(request: Request, releve_opaque: str):
    rapp = rap.creer_ou_charger(releve_opaque)
    form = await request.form()
    motif = (form.get("motif") or "").strip()
    try:
        rap.annuler(rapp, motif, acteur="local", version_attendue=rapp["version"])
    except rap.RapprochementRefuse as exc:
        return RedirectResponse(
            url=f"/proprietaires-reglements/{releve_opaque}/rapprochement?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/proprietaires-reglements/{releve_opaque}/rapprochement", status_code=303)


@router.get("/proprietaires-reglements/a-payer/rapprochement-export.csv")
def rapprochement_export_csv():
    import csv
    import io
    from app.services.proprietaires_releve_export_service import _cellule_sure
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow([f"# {rap.MENTION}"])
    w.writerow(["rapprochement_opaque", "releve_opaque", "proprietaire", "mois", "montant_declare",
               "montant_candidat", "ecart_montant", "date_candidat", "statut", "commentaire",
               "date_controle"])
    for x in rap.lister():
        r = suivi.charger_par_opaque(x["releve_id_opaque"])
        if r is None:
            continue
        detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
        montant = detail["vue"].get("net") if detail and detail.get("status") == "OK" and detail.get("vue") else ""
        cand_mvt = banque_contrat.charger_mouvement(x["mouvement_id_opaque"]) if x.get("mouvement_id_opaque") else None
        w.writerow([
            _cellule_sure(x["rapprochement_id_opaque"]), _cellule_sure(x["releve_id_opaque"]),
            _cellule_sure(r["proprietaire_id"]), _cellule_sure(r["mois"]), _cellule_sure(montant),
            _cellule_sure(cand_mvt.montant if cand_mvt else ""),
            _cellule_sure(x.get("ecart_montant") if x.get("ecart_montant") is not None else ""),
            _cellule_sure(cand_mvt.date if cand_mvt else ""), _cellule_sure(x["statut"]),
            _cellule_sure(x.get("commentaire") or ""), _cellule_sure(x["date_modification"])])
    return Response(content=buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="rapprochement_controle.csv"'})


@router.get("/proprietaires-reglements/{releve_opaque}/historique", response_class=HTMLResponse)
def releve_historique(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return templates.TemplateResponse(request, "proprietaire_releve_historique.html", {
            "active_menu": "proprietaires", "releve": None,
        }, status_code=404)
    return templates.TemplateResponse(request, "proprietaire_releve_historique.html", {
        "active_menu": "proprietaires", "releve": r, "historique": suivi.historique(releve_opaque)})


@router.get("/proprietaires-reglements/{releve_opaque}/export-releve.csv")
def releve_export(releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return Response(content="Relevé introuvable.", status_code=404)
    detail = svc.load_owner_detail(r["proprietaire_id"], r["mois"])
    contenu = export_svc.exporter(r, detail, blocages.evaluer(r["proprietaire_id"], r["mois"]))
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f'attachment; filename="{export_svc.nom_fichier(r["mois"])}"'})

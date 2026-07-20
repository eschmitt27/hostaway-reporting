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
from app.services import clotures_service as cs
from app.services import proprietaires_blocages_service as blocages
from app.services import proprietaires_releve_export_service as export_svc
from app.services import proprietaires_reglements_service as svc
from app.services import proprietaires_service as legacy_svc
from app.services import proprietaires_suivi_service as suivi

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
    return templates.TemplateResponse(request, "reglements_detail.html", {
        "active_menu": "proprietaires", "detail": detail, "proprietaire_id": identifiant,
    })


@router.get("/proprietaires-reglements/{releve_opaque}/releve", response_class=HTMLResponse)
def releve_detail(request: Request, releve_opaque: str):
    r = suivi.charger_par_opaque(releve_opaque)
    if r is None:
        return templates.TemplateResponse(request, "proprietaire_releve_detail.html", {
            "active_menu": "proprietaires", "releve": None,
        }, status_code=404)
    data = legacy_svc.load_releve(r["proprietaire_id"], r["mois"])
    return templates.TemplateResponse(request, "proprietaire_releve_detail.html", {
        "active_menu": "proprietaires", "releve": r, "data": data})


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

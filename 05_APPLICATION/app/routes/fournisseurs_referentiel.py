"""Routes référentiel fournisseur minimal (APP-3D) — SQLite isolée uniquement.

Distinct de `/fournisseurs` (existant, gère des CHARGES). Ce référentiel gère des FICHES
fournisseur (nom, type, statut) — aucune écriture réelle, aucune donnée bancaire.
"""
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import fournisseur_rattachements_service as fl
from app.services import fournisseurs_referentiel_service as frs

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@router.get("/referentiel-fournisseurs", response_class=HTMLResponse)
def fournisseurs_liste(request: Request, actif_seul: bool = False, erreur: str = ""):
    lignes = frs.lister(actif_seul=actif_seul)
    return templates.TemplateResponse(request, "fournisseurs_referentiel_list.html", {
        "active_menu": "fournisseurs", "lignes": lignes, "actif_seul": actif_seul,
        "types": sorted(frs.TYPES), "erreur": erreur,
    })


@router.post("/referentiel-fournisseurs/creer")
async def fournisseurs_creer(request: Request):
    form = await request.form()
    nom = (form.get("nom") or "").strip()
    type_fournisseur = (form.get("type") or "").strip()
    commentaire = (form.get("commentaire") or "").strip()
    try:
        frs.creer(nom, type_fournisseur, commentaire=commentaire, acteur="local")
    except frs.FournisseurRefuse as exc:
        return RedirectResponse(url=f"/referentiel-fournisseurs?erreur={quote(str(exc))}",
                                status_code=303)
    return RedirectResponse(url="/referentiel-fournisseurs", status_code=303)


@router.get("/referentiel-fournisseurs/{fournisseur_opaque}", response_class=HTMLResponse)
def fournisseur_fiche(request: Request, fournisseur_opaque: str, erreur: str = ""):
    f = frs.charger_par_opaque(fournisseur_opaque)
    if f is None:
        return templates.TemplateResponse(request, "fournisseur_referentiel_fiche.html", {
            "active_menu": "fournisseurs", "fournisseur": None,
        }, status_code=404)
    associations = [a for a in _associations_du_fournisseur(fournisseur_opaque)]
    return templates.TemplateResponse(request, "fournisseur_referentiel_fiche.html", {
        "active_menu": "fournisseurs", "fournisseur": f, "erreur": erreur,
        "historique": frs.historique(fournisseur_opaque),
        "associations": associations, "types_prestation": sorted(fl.TYPES_PRESTATION),
    })


def _associations_du_fournisseur(fournisseur_opaque: str):
    from app.db.connection import get_db
    import app.config as cfg
    conn = get_db(cfg.DB_PATH)
    try:
        rows = conn.execute(
            "SELECT * FROM fournisseur_rattachements WHERE fournisseur_id_opaque=? ORDER BY date_debut DESC",
            (fournisseur_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.post("/referentiel-fournisseurs/{fournisseur_opaque}/associer-logement")
async def fournisseur_associer_logement(request: Request, fournisseur_opaque: str):
    form = await request.form()
    logement_id = (form.get("logement_id") or "").strip()
    type_prestation = (form.get("type_prestation") or "AUTRE").strip()
    date_debut = (form.get("date_debut") or "").strip()
    try:
        fl.associer(fournisseur_opaque, logement_id, type_prestation=type_prestation,
                    date_debut=date_debut, acteur="local")
    except fl.AssociationRefusee as exc:
        return RedirectResponse(
            url=f"/referentiel-fournisseurs/{fournisseur_opaque}?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/referentiel-fournisseurs/{fournisseur_opaque}", status_code=303)


@router.post("/referentiel-fournisseurs/{fournisseur_opaque}/fermer-association")
async def fournisseur_fermer_association(request: Request, fournisseur_opaque: str):
    form = await request.form()
    association_opaque = (form.get("association_opaque") or "").strip()
    date_fin = (form.get("date_fin") or "").strip()
    a = fl.charger_par_opaque(association_opaque)
    if a is None:
        return RedirectResponse(url=f"/referentiel-fournisseurs/{fournisseur_opaque}", status_code=303)
    try:
        fl.fermer(a, date_fin, acteur="local", version_attendue=a["version"])
    except fl.AssociationRefusee as exc:
        return RedirectResponse(
            url=f"/referentiel-fournisseurs/{fournisseur_opaque}?erreur={quote(str(exc))}", status_code=303)
    return RedirectResponse(url=f"/referentiel-fournisseurs/{fournisseur_opaque}", status_code=303)


@router.post("/referentiel-fournisseurs/{fournisseur_opaque}/desactiver")
async def fournisseur_desactiver(request: Request, fournisseur_opaque: str):
    f = frs.charger_par_opaque(fournisseur_opaque)
    if f is None:
        return RedirectResponse(url="/referentiel-fournisseurs", status_code=303)
    try:
        frs.desactiver(f, acteur="local", version_attendue=f["version"])
    except frs.FournisseurRefuse as exc:
        return RedirectResponse(
            url=f"/referentiel-fournisseurs/{fournisseur_opaque}?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/referentiel-fournisseurs/{fournisseur_opaque}", status_code=303)


@router.post("/referentiel-fournisseurs/{fournisseur_opaque}/reactiver")
async def fournisseur_reactiver(request: Request, fournisseur_opaque: str):
    f = frs.charger_par_opaque(fournisseur_opaque)
    if f is None:
        return RedirectResponse(url="/referentiel-fournisseurs", status_code=303)
    try:
        frs.reactiver(f, acteur="local", version_attendue=f["version"])
    except frs.FournisseurRefuse as exc:
        return RedirectResponse(
            url=f"/referentiel-fournisseurs/{fournisseur_opaque}?erreur={quote(str(exc))}",
            status_code=303)
    return RedirectResponse(url=f"/referentiel-fournisseurs/{fournisseur_opaque}", status_code=303)

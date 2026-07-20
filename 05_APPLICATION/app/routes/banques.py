"""Routes Banques & Caisse (APP-4A) — lecture seule.

Aucune route n'écrit, ne déclenche d'import, ni ne rapproche : l'application lit les sorties du
pipeline banque (lot8a/8b/8c). Aucune connexion bancaire, aucun virement.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from app.config import TEMPLATES_DIR
from app.services import banques_service as svc
from app.services import banques_controle_service as ctrl
from app.services import banques_controle_writer as writer
from app.readers.banques_reader import date_affichage, datetime_affichage

router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["date_fr"] = date_affichage
templates.env.filters["datetime_fr"] = datetime_affichage
# Identifiant mouvement opaque pour tout lien généré (jamais le mouvement_id brut, qui contient le
# compte, dans un href/option/HTML visible). Les nouvelles pages ne génèrent que des MVT-<hash>.
templates.env.filters["mvt_opaque"] = ctrl.id_opaque


def _form_to_decision(form) -> dict:
    """Extrait les champs de décision d'un formulaire (valeurs vides -> None)."""
    def g(k):
        v = str(form.get(k, "")).strip()
        return v or None
    return {
        "categorie": g("categorie"), "type_flux_id": g("type_flux_id"),
        "proprietaire_id": g("proprietaire_id"), "logement_id": g("logement_id"),
        "reservation_id": g("reservation_id"), "facture_id": g("facture_id"),
        "statut_controle": g("statut_controle"), "commentaire": g("commentaire"),
        "justification": g("justification"),
    }


@router.get("/banques-caisse", response_class=HTMLResponse)
def banques_dashboard(
    request: Request,
    mois: str = "",
    compte_id: str = "",
    sens: str = "",
    statut: str = "",
    non_rapproche: bool = False,
    montant_min: str = "",
    montant_max: str = "",
    recherche: str = "",
    tri: str = "anomalie",
    page: int = 1,
):
    data = svc.load_dashboard(
        mois=mois, compte_id=compte_id, sens=sens, statut=statut, non_rapproche=non_rapproche,
        montant_min=montant_min, montant_max=montant_max, recherche=recherche, tri=tri, page=page,
    )
    return templates.TemplateResponse(request, "banques_list.html", {
        "active_menu": "banques", "data": data, "nb_a_controler": ctrl.compter_a_controler(),
    })


@router.get("/banques-caisse/a-rapprocher", response_class=HTMLResponse)
def banques_a_rapprocher(request: Request, mois: str = ""):
    if not mois:
        mois = svc.periode_par_defaut()
    data = svc.load_unmatched(mois)
    return templates.TemplateResponse(request, "banques_a_rapprocher.html", {
        "active_menu": "banques", "data": data,
    })


@router.get("/banques-caisse/export.csv")
def banques_export_csv(
    request: Request,
    mois: str = "",
    compte_id: str = "",
    sens: str = "",
    statut: str = "",
    non_rapproche: bool = False,
    montant_min: str = "",
    montant_max: str = "",
    recherche: str = "",
    tri: str = "anomalie",
):
    contenu = svc.export_movements_csv(
        mois=mois, compte_id=compte_id, sens=sens, statut=statut, non_rapproche=non_rapproche,
        montant_min=montant_min, montant_max=montant_max, recherche=recherche, tri=tri,
    )
    nom = f"banques_mouvements_{mois or 'tous'}.csv"
    return Response(content=contenu, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{nom}"'})


# ── APP-4B — Contrôle & catégorisation (fiche actionnable, ID opaque, writer copies) ──
# Déclaré AVANT « /mouvements/{stable_id} » : « /mouvements/{id}/modifier » etc. ont un segment
# supplémentaire ; la fiche opaque est servie par banque_detail (délégation ci-dessous).

@router.get("/banques-caisse/mouvements/{id_opaque}/modifier", response_class=HTMLResponse)
def banque_mouvement_modifier(request: Request, id_opaque: str):
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return templates.TemplateResponse(request, "banques_mouvement.html", {
            "active_menu": "banques", "fiche": None, "id_opaque": id_opaque,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_mouvement_modifier.html", {
        "active_menu": "banques", "fiche": fiche, "options": ctrl.options_reference(),
    })


@router.post("/banques-caisse/mouvements/{id_opaque}/previsualiser", response_class=HTMLResponse)
async def banque_mouvement_previsualiser(request: Request, id_opaque: str):
    form = await request.form()
    fiche = ctrl.load_fiche(id_opaque)
    if fiche is None:
        return templates.TemplateResponse(request, "banques_mouvement.html", {
            "active_menu": "banques", "fiche": None, "id_opaque": id_opaque,
        }, status_code=404)
    decision = _form_to_decision(form)
    # Prévisualisation : ne journalise rien, montre la décision proposée à côté de la proposition moteur.
    return templates.TemplateResponse(request, "banques_mouvement_modifier.html", {
        "active_menu": "banques", "fiche": fiche, "options": ctrl.options_reference(),
        "previsualisation": decision,
    })


@router.post("/banques-caisse/mouvements/{id_opaque}/enregistrer-copie")
async def banque_mouvement_enregistrer(request: Request, id_opaque: str):
    form = await request.form()
    decision = _form_to_decision(form)
    try:
        ctrl.enregistrer_decision(id_opaque, version_attendue=None, **decision)
    except ctrl.DecisionRefusee as exc:
        fiche = ctrl.load_fiche(id_opaque)
        return templates.TemplateResponse(request, "banques_mouvement_modifier.html", {
            "active_menu": "banques", "fiche": fiche, "options": ctrl.options_reference(),
            "erreur": str(exc), "previsualisation": decision,
        }, status_code=200)
    # Applique les décisions actives sur une COPIE (jamais le réel), puis affiche le run.
    resultat = writer.enregistrer_sur_copie()
    run_id = resultat.get("run_id")
    if run_id is None:
        return templates.TemplateResponse(request, "banques_action_run.html", {
            "active_menu": "banques", "run": None, "erreur": resultat.get("message"),
        }, status_code=200)
    return RedirectResponse(url=f"/banques-caisse/actions/{run_id}?mvt={id_opaque}", status_code=303)


@router.get("/banques-caisse/actions/{run_id}", response_class=HTMLResponse)
def banque_action_run(request: Request, run_id: int, mvt: str = ""):
    run = writer.load_run(run_id)
    if run is None:
        return templates.TemplateResponse(request, "banques_action_run.html", {
            "active_menu": "banques", "run": None, "run_id": run_id,
        }, status_code=404)
    fiche = ctrl.load_fiche(mvt) if mvt else None
    return templates.TemplateResponse(request, "banques_action_run.html", {
        "active_menu": "banques", "run": run, "fiche": fiche,
    })


@router.get("/banques-caisse/mouvements/{stable_id}", response_class=HTMLResponse)
def banque_detail(request: Request, stable_id: str):
    # Délégation APP-4B : un identifiant OPAQUE (MVT-<hash12>) ouvre la fiche actionnable.
    if ctrl.resoudre_opaque(stable_id) is not None:
        fiche = ctrl.load_fiche(stable_id)
        return templates.TemplateResponse(request, "banques_mouvement.html", {
            "active_menu": "banques", "fiche": fiche,
        })
    # Sinon : ancienne fiche APP-4A (lecture seule) — non utilisée dans la nouvelle interface.
    detail = svc.load_detail(stable_id)
    if detail is None:
        return templates.TemplateResponse(request, "banques_detail.html", {
            "active_menu": "banques", "detail": None, "stable_id": stable_id,
        }, status_code=404)
    return templates.TemplateResponse(request, "banques_detail.html", {
        "active_menu": "banques", "detail": detail, "stable_id": stable_id,
    })


@router.get("/banques-caisse/controle", response_class=HTMLResponse)
def banque_controle_liste(request: Request, statut: str = "", categorie: str = "",
                          proprietaire_id: str = "", logement_id: str = "",
                          anomalie: str = "", mois: str = ""):
    data = ctrl.load_liste(statut=statut, categorie=categorie, proprietaire_id=proprietaire_id,
                           logement_id=logement_id, anomalie=anomalie, mois=mois)
    return templates.TemplateResponse(request, "banques_controle_liste.html", {
        "active_menu": "banques", "data": data,
        "applied": {"statut": statut, "categorie": categorie, "proprietaire_id": proprietaire_id,
                    "logement_id": logement_id, "anomalie": anomalie, "mois": mois},
    })

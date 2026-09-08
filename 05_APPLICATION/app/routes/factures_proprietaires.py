"""Routes Factures propriétaires ÉMISES.

Distinctes des routes `factures.py`, qui traitent les factures fournisseurs REÇUES : ce sont deux
objets différents (l'une est créée par nous, l'autre nous est envoyée).

Aucune écriture avant confirmation explicite : la prévisualisation est une lecture pure.
"""
from pathlib import Path

import app.config as cfg
from fastapi import APIRouter, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from app.template_env import get_templates
from app.readers import proprietaires_reader as prop_reader
from app.services import comptabilite_ecritures_service as compta
from app.services import facturation_config_service as fconf
from app.services import factures_proprietaires_conformite_service as conformite
from app.services import factures_proprietaires_pdf as pdf
from app.services import factures_proprietaires_edition_service as edition
from app.services import factures_proprietaires_service as svc
from app.services import factures_proprietaires_source as source_svc

router = APIRouter()
templates = get_templates()


def _repertoire_documents() -> Path:
    """Emplacement de stockage des PDF, résolu **à chaque appel**.

    `cfg.DATA_DIR` est lu au moment de l'appel et non figé : sans cela, une instance de recette ou
    un test qui redirige DATA_DIR écrirait quand même dans le vrai dossier `data/`. Seule une
    surcharge explicite par variable d'environnement prend le pas.
    """
    surcharge = getattr(cfg, "FACTURES_PROPRIETAIRES_DIR", None)
    return Path(surcharge) if surcharge else Path(cfg.DATA_DIR) / "factures_proprietaires"


def _emetteur() -> dict:
    """Identité de la société émettrice. Absente en recette : la facture reste alors BROUILLON."""
    return {
        "nom": getattr(cfg, "SOCIETE_NOM", ""),
        "adresse": getattr(cfg, "SOCIETE_ADRESSE", ""),
        "siret": getattr(cfg, "SOCIETE_SIRET", ""),
    }


def _destinataire(proprietaire_id: str) -> dict:
    p = prop_reader.find_proprietaire(proprietaire_id) or {}
    nom = " ".join(x for x in (p.get("prenom_proprietaire"), p.get("nom_proprietaire")) if x)
    return {"nom": nom or proprietaire_id,
            "adresse": p.get("adresse_facturation") or "",
            "proprietaire_id": proprietaire_id}


def _comptabilite(facture: dict) -> dict:
    """État de l'écriture VENTES liée à la facture, pour affichage sur la fiche.

    Une facture non émise n'a pas d'écriture : ce n'est pas une anomalie, c'est le contrat —
    la vente naît à l'émission.
    """
    etat = {"statut": "ABSENTE", "ecriture": None, "lignes": [], "conflit": None}
    if facture["statut"] != svc.ST_EMIS:
        etat["statut"] = "SANS_OBJET"
        etat["detail"] = "la vente est constatée à l'émission, pas avant"
        return etat

    conflits = compta._ventes_lot12_du_mois(facture["proprietaire_id"], facture["mois"])
    if conflits:
        etat["conflit"] = (f"{compta.E_DOUBLE_SOURCE} : la vente de ce mois a déjà été "
                           f"comptabilisée par l'ancien mécanisme ({', '.join(conflits)})")

    try:
        ecr = compta.charger_par_origine(compta.ORIGINE_FACTURE, facture["facture_id_opaque"])
    except Exception:
        ecr = None
    if ecr:
        etat["statut"] = ecr["statut"]
        etat["ecriture"] = ecr
        etat["lignes"] = compta.lignes(ecr["ecriture_id_opaque"])
    return etat


def _ids_proprietaires() -> list[str]:
    try:
        return [p["proprietaire_id"] for p in prop_reader.read_proprietaires()
                if p.get("proprietaire_id")]
    except Exception:
        return []


@router.get("/factures-proprietaires", response_class=HTMLResponse)
def liste(request: Request, mois: str = "", statut: str = ""):
    factures = svc.lister(mois=mois or None, statut=statut or None)
    for f in factures:
        f["solde"] = svc.solde(f["facture_id_opaque"])["solde"]
    return templates.TemplateResponse(request, "factures_proprietaires_list.html", {
        "active_menu": "factures", "factures": factures, "mois": mois, "statut": statut,
        "statuts": svc.STATUTS,
    })


@router.get("/factures-proprietaires/proposer", response_class=HTMLResponse)
def proposer(request: Request, mois: str = ""):
    """Prévisualisation du mois complet. Lecture pure : aucune écriture."""
    propositions = source_svc.propositions_du_mois(mois, _ids_proprietaires()) if mois else []
    resume = {s: sum(1 for p in propositions if p["statut_proposition"] == s)
              for s in ("PRETE", "A_CONTROLER", "NON_CONCERNE")}
    return templates.TemplateResponse(request, "factures_proprietaires_proposer.html", {
        "active_menu": "factures", "mois": mois, "propositions": propositions, "resume": resume,
        "total_pret": round(sum(p["montant_total"] for p in propositions
                                if p["statut_proposition"] == "PRETE"), 2),
    })


@router.post("/factures-proprietaires/generer", response_class=HTMLResponse)
def generer(request: Request, mois: str = Form(...)):
    """Crée les BROUILLON des seules propositions PRETE, après confirmation de l'utilisateur."""
    propositions = source_svc.propositions_du_mois(mois, _ids_proprietaires())
    resultat = source_svc.creer_lot(propositions, acteur="interface")
    return templates.TemplateResponse(request, "factures_proprietaires_resultat.html", {
        "active_menu": "factures", "mois": mois, "resultat": resultat,
    })


def _contexte_fiche(facture_id: str, erreur: str | None = None) -> dict:
    facture = svc.lire(facture_id)
    return {
        "active_menu": "factures", "facture": facture,
        "solde": svc.solde(facture_id),
        # Édition du BROUILLON : tout est calculé ICI. Le gabarit n'effectue aucune arithmétique
        # et ne dérive aucun droit — il affiche.
        "editable": facture["statut"] == svc.ST_BROUILLON,
        "types_ligne": svc.TYPES_LIGNE_SAISISSABLES,
        "choix_ligne_charge": svc.CHOIX_LIGNE_CHARGE,
        "avertissement_ligne_calculee": svc.AVERTISSEMENT_LIGNE_CALCULEE,
        "avertissement_charge": edition.AVERTISSEMENT_CHARGE,
        "modes_charge": edition.modes_charge(),
        "categories_charges": edition.categories_charges(),
        "reservations": svc.reservations(facture_id),
        "emetteur": _emetteur(),
        "destinataire": _destinataire(facture["proprietaire_id"]),
        "peut_valider": facture["statut"] == svc.ST_BROUILLON,
        "peut_emettre": facture["statut"] == svc.ST_VALIDE,
        "peut_avoir": facture["statut"] == svc.ST_EMIS
                      and facture["type_document"] == svc.TYPE_FACTURE,
        "comptabilite": _comptabilite(facture),
        "conformite": conformite.verifier(facture, db_path=None),
        "conformite_figee": conformite.charger(facture["facture_id_opaque"]),
        "erreur_validation": erreur,
    }


@router.get("/factures-proprietaires/{facture_id}", response_class=HTMLResponse)
def fiche(request: Request, facture_id: str):
    return templates.TemplateResponse(request, "factures_proprietaires_fiche.html",
                                       _contexte_fiche(facture_id))


@router.post("/factures-proprietaires/{facture_id}/valider")
def valider(request: Request, facture_id: str):
    facture = svc.lire(facture_id)
    try:
        svc.valider(facture_id, emetteur=_emetteur(),
                    destinataire=_destinataire(facture["proprietaire_id"]), acteur="interface")
    except svc.FactureProprietaireError as exc:
        return templates.TemplateResponse(
            request, "factures_proprietaires_fiche.html",
            _contexte_fiche(facture_id,
                            erreur="Impossible de valider cette facture : des informations "
                                   f"obligatoires restent à compléter ({exc})."),
            status_code=422)
    return RedirectResponse(f"/factures-proprietaires/{facture_id}", status_code=303)


@router.post("/factures-proprietaires/{facture_id}/emettre")
def emettre(request: Request, facture_id: str, date_facture: str = Form(...)):
    facture = svc.lire(facture_id)
    # Série laissée à la configuration : F-AAAA-NNNNNN pour les factures, A-AAAA-NNNNNN pour les
    # avoirs. La conformité est exigée dès que l'émission réelle est ouverte ; en recette elle est
    # seulement affichée, pour pouvoir exercer le parcours avec une configuration incomplète.
    try:
        emise = svc.emettre(facture_id, emetteur=_emetteur(),
                            destinataire=_destinataire(facture["proprietaire_id"]),
                            date_facture=date_facture,
                            generer_pdf=pdf.fabrique(_repertoire_documents()), acteur="interface",
                            exiger_conformite=fconf.emission_reelle_autorisee())
    except svc.FactureProprietaireError as exc:
        return templates.TemplateResponse(
            request, "factures_proprietaires_fiche.html",
            _contexte_fiche(facture_id,
                            erreur=f"Impossible d'émettre cette facture : {exc}."),
            status_code=422)
    # L'émission constate la vente : c'est ici, et nulle part ailleurs, que naît l'écriture VENTES.
    # Un refus (flags désactivés, mapping, double source) n'annule pas l'émission — la facture est
    # émise et le conflit reste visible sur la fiche, jamais résolu en silence.
    compta.generer_ecriture_vente_facture(emise, acteur="interface")
    return RedirectResponse(f"/factures-proprietaires/{facture_id}", status_code=303)


@router.post("/factures-proprietaires/{facture_id}/avoir")
def avoir(request: Request, facture_id: str, motif: str = Form(...)):
    """Un avoir est un NOUVEAU document : la redirection vers l'avoir est délibérée.

    C'est la seule action de cette fiche qui n'y ramène pas — parce qu'elle ne modifie pas cette
    facture, elle en crée une autre, et c'est celle-là que l'utilisateur doit voir. Un refus, lui,
    ramène bien sur la facture d'origine, jamais sur une page morte.
    """
    try:
        a = svc.creer_avoir(facture_id, motif=motif, acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Impossible de créer un avoir : {exc}.")
    return RedirectResponse(f"/factures-proprietaires/{a['facture_id_opaque']}", status_code=303)


# ── Édition d'un BROUILLON ──────────────────────────────────────────────────────────────────────
# Contrat commun à TOUTES les actions ci-dessous : quel que soit le résultat, l'utilisateur revient
# sur CETTE facture — succès par redirection ancrée, refus par re-rendu 422 de la même fiche.
# Jamais un JSON brut, jamais une page générique, jamais un 500.

ANCRE_LIGNES = "#lignes-facturees"
ANCRE_REGLEMENT = "#reglement"


def _refus_fiche(request: Request, facture_id: str, message: str):
    return templates.TemplateResponse(request, "factures_proprietaires_fiche.html",
                                      _contexte_fiche(facture_id, erreur=message),
                                      status_code=422)


def _retour(facture_id: str, ancre: str = ANCRE_LIGNES):
    return RedirectResponse(f"/factures-proprietaires/{facture_id}{ancre}", status_code=303)


@router.post("/factures-proprietaires/{facture_id}/lignes/ajouter")
async def ligne_ajouter(request: Request, facture_id: str):
    """Ajoute une ligne. Si le type est une CHARGE, la charge réelle est créée par le service
    canonique (`charges_saisie_service.creer`) — jamais par un INSERT depuis cette route."""
    form = await request.form()
    type_ligne = str(form.get("type_ligne", "") or "").strip()
    try:
        if type_ligne == svc.CHOIX_LIGNE_CHARGE:
            edition.ajouter_ligne_charge(
                facture_id, libelle=str(form.get("libelle", "") or ""),
                montant=form.get("montant"),
                code_impact=str(form.get("code_impact", "") or ""),
                categorie_charge_id=str(form.get("categorie_charge_id", "") or ""),
                date_charge=str(form.get("date_charge", "") or ""),
                refacturable=str(form.get("refacturable", "NON") or "NON"),
                commentaire=str(form.get("commentaire", "") or ""), acteur="interface")
        else:
            svc.ajouter_ligne(facture_id, type_ligne=type_ligne,
                              libelle=str(form.get("libelle", "") or ""),
                              montant=form.get("montant"), acteur="interface",
                              commentaire=str(form.get("commentaire", "") or ""))
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Ligne non ajoutée : {exc}")
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/lignes/{ligne_id}/modifier")
async def ligne_modifier(request: Request, facture_id: str, ligne_id: str):
    form = await request.form()
    try:
        svc.modifier_ligne(facture_id, ligne_id,
                           libelle=str(form.get("libelle", "") or ""),
                           montant=form.get("montant"), acteur="interface",
                           commentaire=str(form.get("commentaire", "") or ""))
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Ligne non modifiée : {exc}")
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/lignes/{ligne_id}/supprimer")
async def ligne_supprimer(request: Request, facture_id: str, ligne_id: str):
    """Retire la ligne du DOCUMENT. La source de calcul Lot12 n'est jamais touchée."""
    form = await request.form()
    try:
        edition.supprimer_ligne_charge(facture_id, ligne_id, acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Ligne non supprimée : {exc}")
    _ = form
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/reversement-airbnb")
async def reversement_airbnb(request: Request, facture_id: str):
    form = await request.form()
    try:
        edition.ajouter_reversement_airbnb(
            facture_id, montant=form.get("montant"),
            date_imputation=str(form.get("date_imputation", "") or ""),
            reference_airbnb=str(form.get("reference_airbnb", "") or ""),
            commentaire=str(form.get("commentaire", "") or ""), acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Reversement non enregistré : {exc}")
    return _retour(facture_id, ANCRE_REGLEMENT)


@router.post("/factures-proprietaires/{facture_id}/acompte")
async def acompte(request: Request, facture_id: str):
    form = await request.form()
    try:
        edition.ajouter_acompte(
            facture_id, montant=form.get("montant"),
            date_mouvement=str(form.get("date_mouvement", "") or ""),
            mode_reglement=str(form.get("mode_reglement", "") or ""),
            commentaire=str(form.get("commentaire", "") or ""), acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Acompte non enregistré : {exc}")
    return _retour(facture_id, ANCRE_REGLEMENT)


@router.get("/factures-proprietaires/{facture_id}/document")
def document(facture_id: str):
    """Sert le fichier figé correspondant au hash enregistré — jamais un PDF reconstruit."""
    facture = svc.lire(facture_id)
    if facture["statut"] != svc.ST_EMIS or not facture["document_nom"]:
        return HTMLResponse(f"{svc.C_PDF_ABSENT}: aucun document emis", status_code=404)
    annee, _, mm = str(facture["mois"] or "0000-00").partition("-")
    chemin = _repertoire_documents() / annee / (mm or "00") / facture["document_nom"]
    if not chemin.exists():
        return HTMLResponse(f"{svc.C_PDF_ABSENT}: fichier introuvable", status_code=404)
    return FileResponse(chemin, media_type="application/pdf",
                        filename=facture["document_nom"])

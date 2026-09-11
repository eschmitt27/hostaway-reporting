"""Routes Factures propriétaires ÉMISES.

Distinctes des routes `factures.py`, qui traitent les factures fournisseurs REÇUES : ce sont deux
objets différents (l'une est créée par nous, l'autre nous est envoyée).

Aucune écriture avant confirmation explicite : la prévisualisation est une lecture pure.
"""
import calendar
import re
from datetime import date
from pathlib import Path

import app.config as cfg
from fastapi import APIRouter, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from app.template_env import get_templates
from app.readers import proprietaires_reader as prop_reader
from app.services import comptabilite_ecritures_service as compta
from app.services import facturation_config_service as fconf
from app.services import factures_proprietaires_composition_service as compo
from app.services import factures_proprietaires_conformite_service as conformite
from app.services import factures_proprietaires_pdf as pdf
from app.services import factures_proprietaires_edition_service as edition
from app.services import factures_proprietaires_service as svc
from app.services import factures_proprietaires_source as source_svc
from app.services import proprietaires_facturation_service as classement

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
    """Identité de la société émettrice. Absente en recette : la facture reste alors BROUILLON.

    `siren` et `siret` sont DEUX champs distincts, jamais interchangeables : le SIREN identifie
    l'entreprise (9 chiffres), le SIRET un établissement (14 = SIREN + NIC). Chacun n'est imprimé
    que sous sa propre étiquette, et aucun n'est déduit de l'autre.
    """
    return {
        "nom": getattr(cfg, "SOCIETE_NOM", ""),
        "adresse": getattr(cfg, "SOCIETE_ADRESSE", ""),
        "siret": getattr(cfg, "SOCIETE_SIRET", ""),
        "siren": getattr(cfg, "SOCIETE_SIREN", ""),
        # Même règle de présentation que le document : le numéro est stocké brut et formaté à
        # l'affichage. Deux formatages différents à l'écran et sur le PDF feraient douter du numéro.
        "siren_lisible": pdf._siren_lisible(getattr(cfg, "SOCIETE_SIREN", "")),
        "forme_juridique": getattr(cfg, "SOCIETE_FORME_JURIDIQUE", ""),
        "capital": getattr(cfg, "SOCIETE_CAPITAL", ""),
        "rcs": getattr(cfg, "SOCIETE_RCS", ""),
        "tva_intra": getattr(cfg, "SOCIETE_TVA_INTRA", ""),
        "contact": getattr(cfg, "SOCIETE_CONTACT", ""),
        "coordonnees_paiement": getattr(cfg, "SOCIETE_COORDONNEES_PAIEMENT", ""),
    }


def _fin_de_mois(mois: str) -> str:
    """`2026-08` → `2026-08-31`. Date technique DÉRIVÉE, jamais demandée à l'utilisateur (§19).

    Les acomptes et reversements se raisonnent au mois : plusieurs encaissements Airbnb d'un même
    mois sont agrégés, et aucun jour précis ne les représente. Le dernier jour du mois place le
    mouvement dans la bonne période sans prétendre à une précision qu'on n'a pas.
    """
    mois = str(mois or "").strip()
    if not re.fullmatch(r"\d{4}-(?:0[1-9]|1[0-2])", mois):
        return ""
    annee, m = int(mois[:4]), int(mois[5:7])
    return f"{mois}-{calendar.monthrange(annee, m)[1]:02d}"


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
def liste(request: Request, mois: str = "", statut: str = "", comptabilisee: str = ""):
    factures = svc.lister(mois=mois or None, statut=statut or None)
    for f in factures:
        f["solde"] = svc.solde(f["facture_id_opaque"])["solde"]
        # « Comptabilisée » se LIT dans les écritures, jamais dans un drapeau porté par la facture :
        # un drapeau se désynchroniserait de la comptabilité au premier incident, et c'est
        # précisément l'écart qu'on veut rendre visible.
        f["comptabilisee"] = _comptabilite(f)["statut"] == "PRESENTE"
    if comptabilisee in ("oui", "non"):
        factures = [f for f in factures if f["comptabilisee"] == (comptabilisee == "oui")]
    return templates.TemplateResponse(request, "factures_proprietaires_list.html", {
        "active_menu": "factures_proprietaires", "factures": factures, "mois": mois, "statut": statut,
        "statuts": svc.STATUTS, "comptabilisee": comptabilisee,
    })


@router.get("/factures-proprietaires/proposer", response_class=HTMLResponse)
def proposer(request: Request, mois: str = ""):
    """Prévisualisation du mois complet. Lecture pure : aucune écriture."""
    propositions = source_svc.propositions_du_mois(mois, _ids_proprietaires()) if mois else []
    resume = {s: sum(1 for p in propositions if p["statut_proposition"] == s)
              for s in ("PRETE", "A_CONTROLER", "NON_CONCERNE")}
    return templates.TemplateResponse(request, "factures_proprietaires_proposer.html", {
        "active_menu": "factures_proprietaires", "mois": mois, "propositions": propositions, "resume": resume,
        "total_pret": round(sum(p["montant_total"] for p in propositions
                                if p["statut_proposition"] == "PRETE"), 2),
    })


@router.post("/factures-proprietaires/generer", response_class=HTMLResponse)
def generer(request: Request, mois: str = Form(...)):
    """Crée les BROUILLON des seules propositions PRETE, après confirmation de l'utilisateur."""
    propositions = source_svc.propositions_du_mois(mois, _ids_proprietaires())
    resultat = source_svc.creer_lot(propositions, acteur="interface")
    return templates.TemplateResponse(request, "factures_proprietaires_resultat.html", {
        "active_menu": "factures_proprietaires", "mois": mois, "resultat": resultat,
    })


def _contexte_fiche(facture_id: str, erreur: str | None = None) -> dict:
    facture = svc.lire(facture_id)
    return {
        # `factures_proprietaires`, PAS `factures` : c'est l'onglet « Factures fournisseurs » qui
        # s'allumait alors qu'on éditait une facture PROPRIÉTAIRE. La liste posait déjà la bonne
        # clé ; seules les pages enfants héritaient de la mauvaise.
        "active_menu": "factures_proprietaires", "facture": facture,
        "aujourdhui": date.today().isoformat(),
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
        # Décomposition et charges éligibles : calculées CÔTÉ SERVEUR, à chaque affichage. Le total
        # affiché ne peut donc pas diverger du total réel — il n'existe pas de seconde addition,
        # ni en JavaScript ni dans le gabarit.
        "decomposition": compo.decomposition(facture_id),
        "periode": compo.periode(facture["mois"]),
        "charges_eligibles": (compo.charges_eligibles(facture_id)
                              if facture["statut"] == svc.ST_BROUILLON else []),
        "emetteur": _emetteur(),
        "destinataire": _destinataire(facture["proprietaire_id"]),
        # Classement du propriétaire : `A_CONTROLER` tant qu'il n'a pas été saisi. Jamais déduit.
        "type_client_actuel": classement.type_client(facture["proprietaire_id"]),
        "peut_valider": facture["statut"] == svc.ST_BROUILLON,
        "peut_emettre": facture["statut"] == svc.ST_VALIDE,
        # Rouvrir n'est offert que sur une facture VALIDE et non numérotée : une facture ÉMISE se
        # corrige par annulation ou avoir, jamais par un retour discret à l'état modifiable.
        "peut_repasser_en_brouillon": (facture["statut"] == svc.ST_VALIDE
                                       and not facture.get("numero_facture")),
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


@router.post("/factures-proprietaires/{facture_id}/repasser-en-brouillon")
async def repasser_en_brouillon(request: Request, facture_id: str):
    """Rouvre une facture VALIDE pour correction (§21).

    Refusé sur une facture ÉMISE : elle porte un numéro de la série légale et a constaté une vente.
    Le service pose ce refus ; la route se contente de le rendre lisible.
    """
    form = await request.form()
    try:
        svc.repasser_en_brouillon(facture_id, acteur="interface",
                                  motif=str(form.get("motif", "") or ""))
    except svc.FactureProprietaireError as exc:
        return templates.TemplateResponse(
            request, "factures_proprietaires_fiche.html",
            _contexte_fiche(facture_id, erreur=f"Retour en brouillon impossible : {exc}"),
            status_code=422)
    return _retour(facture_id)


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
    # Série dérivée du MOIS DE PRESTATION : `2026-08-001` pour une facture, `A-2026-08-001` pour un
    # avoir (§22). La conformité est exigée dès que l'émission réelle est ouverte ; en recette elle
    # est seulement affichée, pour pouvoir exercer le parcours avec une configuration incomplète.
    #
    # CE QUE FAIT « ÉMETTRE », EXHAUSTIVEMENT : attribuer le numéro, figer le snapshot, générer le
    # PDF sur le disque local et le hacher. AUCUN e-mail n'est envoyé, AUCUNE API externe n'est
    # appelée — le projet ne contient aucun module d'envoi (vérifié : pas de `smtplib`, pas de
    # client HTTP sortant dans cette chaîne). La comptabilisation reste une action distincte.
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


@router.post("/factures-proprietaires/{facture_id}/extra")
async def ajouter_extra(request: Request, facture_id: str):
    """Ajoute un EXTRA (prestation ponctuelle). Montant strictement positif."""
    form = await request.form()
    try:
        compo.ajouter_extra(facture_id,
                            libelle=str(form.get("libelle", "") or ""),
                            montant=form.get("montant"),
                            commentaire=str(form.get("commentaire", "") or ""),
                            acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Extra non ajouté : {exc}")
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/reduction")
async def ajouter_reduction(request: Request, facture_id: str):
    """Ajoute une RÉDUCTION commerciale. Saisie en positif, stockée en négatif.

    Distincte d'un acompte : elle diminue ce qui est FACTURÉ, pas ce qui reste à payer.
    """
    form = await request.form()
    try:
        compo.ajouter_reduction(facture_id,
                                libelle=str(form.get("libelle", "") or "Remise commerciale"),
                                montant=form.get("montant"),
                                motif=str(form.get("motif", "") or ""),
                                acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Réduction non ajoutée : {exc}")
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/charge-existante")
async def rattacher_charge(request: Request, facture_id: str):
    """Ajoute un ÉLÉMENT À REFACTURER à la facture, désigné par sa position de refacturation.

    Le montant est LIBRE dans la limite du solde restant : c'est la décision commerciale de
    l'utilisateur (700 d'un coup, ou 350 ici et 350 sur une autre facture). Vide = tout le solde.
    La ventilation analytique de la charge n'impose rien ici — ce sont deux axes distincts.
    """
    form = await request.form()
    montant_saisi = str(form.get("montant", "") or "").strip().replace(",", ".")
    try:
        compo.rattacher_charge(facture_id, str(form.get("position_id", "") or "").strip(),
                               libelle=str(form.get("libelle", "") or ""),
                               montant=montant_saisi or None,
                               # Obligatoire seulement si le montant est partiel — le service
                               # tranche, la route se contente de transmettre ce qui a été saisi.
                               justification=str(form.get("justification", "") or ""),
                               acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Charge non rattachée : {exc}")
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/lignes/{ligne_id}/detacher-charge")
def detacher_charge(request: Request, facture_id: str, ligne_id: str):
    """Retire une charge RATTACHÉE. La charge redevient immédiatement sélectionnable."""
    try:
        compo.detacher_charge(facture_id, ligne_id, acteur="interface")
    except svc.FactureProprietaireError as exc:
        return _refus_fiche(request, facture_id, f"Charge non détachée : {exc}")
    return _retour(facture_id)


@router.post("/factures-proprietaires/{facture_id}/type-client")
async def definir_type_client(request: Request, facture_id: str):
    """Classe le PROPRIÉTAIRE (pas la facture) en particulier ou professionnel.

    Le classement est porté par le propriétaire parce qu'il ne change pas d'une facture à l'autre :
    le saisir sur chaque facture inviterait à des réponses divergentes pour un même client, et donc
    à des mentions légales incohérentes d'un mois sur l'autre. Les factures DÉJÀ ÉMISES ne sont pas
    affectées — leur bloc de conformité est figé dans leur snapshot.
    """
    form = await request.form()
    facture = svc.lire(facture_id)
    try:
        classement.definir(
            facture["proprietaire_id"], str(form.get("type_client", "") or ""),
            siren_client=str(form.get("siren_client", "") or ""),
            tva_intra_client=str(form.get("tva_intra_client", "") or ""),
            motif=str(form.get("motif", "") or ""), acteur="interface")
    except classement.TypeClientError as exc:
        return _refus_fiche(request, facture_id, f"Type de client non enregistré : {exc}")
    return _retour(facture_id)


@router.get("/factures-proprietaires/{facture_id}/previsualiser")
def previsualiser_pdf(facture_id: str):
    """Prévisualisation : le PDF RÉEL, rendu par le même moteur que le document final.

    Volontairement pas un second gabarit HTML « qui ressemble » : deux rendus divergeraient, et
    l'utilisateur validerait un aperçu différent de ce que recevra le propriétaire. Un BROUILLON
    est rendu à chaud (il change à chaque modification) ; une facture ÉMISE rend son snapshot figé.
    """
    document = compo.document(facture_id, emetteur=_emetteur(),
                              destinataire=_destinataire(svc.lire(facture_id)["proprietaire_id"]))
    if not document.get("conformite"):
        document["conformite"] = conformite.construire(
            svc.lire(facture_id),
            date_facture=document.get("date_facture") or f"{document.get('mois')}-01")
    octets = pdf.rendre(document)
    entetes = {"Content-Disposition":
               f'inline; filename="apercu-{facture_id}.pdf"'}
    return Response(content=octets, media_type="application/pdf", headers=entetes)


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
    """Acompte propriétaire, saisi au MOIS (§19).

    Le schéma de trésorerie exige une date ; on la dérive du dernier jour du mois de référence
    plutôt que de la demander. Un acompte se raisonne par période — exiger un jour précis obligeait
    l'utilisateur à en inventer un, et plusieurs encaissements d'un même mois n'en ont de toute
    façon pas un seul.
    """
    form = await request.form()
    mois = str(form.get("mois_reference", "") or "").strip()
    date_mouvement = str(form.get("date_mouvement", "") or "").strip()
    try:
        edition.ajouter_acompte(
            facture_id, montant=form.get("montant"),
            date_mouvement=date_mouvement or _fin_de_mois(mois),
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

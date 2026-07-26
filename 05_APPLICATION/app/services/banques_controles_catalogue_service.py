"""Catalogue de contrôles du module Banque — import, mouvements, rapprochements, métier, cohérence
avec les lots.

Ce service NE remplace PAS les contrôles moteur (lot8a/8b/8c/lot11) : il les complète côté
applicatif, sur ce que l'application est seule à connaître (rapprochements journalisés en SQLite,
imports applicatifs) ou sur ce que le moteur ne recalcule pas entre deux runs. Les anomalies moteur
déjà présentes dans `NORM_Banque.codes_anomalie` sont **reprises telles quelles**, jamais réécrites
ni masquées.

Niveaux : BLOQUANT > CRITIQUE > AVERTISSEMENT > INFO. Un contrôle justifié/résolu reste visible
(jamais supprimé) — la justification est une information de plus, pas un effacement.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers import banques_reader as reader
from app.readers.banques_reader import to_texte, to_nombre, to_date
from app.services import banques_controle_service as ctrl
from app.services import banques_rapprochement_service as rappro

BLOQUANT = "BLOQUANT"
CRITIQUE = "CRITIQUE"
AVERTISSEMENT = "AVERTISSEMENT"
INFO = "INFO"

_ORDRE = {BLOQUANT: 0, CRITIQUE: 1, AVERTISSEMENT: 2, INFO: 3}

# Codes — un par ligne, jamais un message libre non identifiable.
C_MONTANT_NUL = "CTRL_BQ_MONTANT_NUL"
C_DEVISE_INATTENDUE = "CTRL_BQ_DEVISE_INATTENDUE"
C_DATE_FUTURE = "CTRL_BQ_DATE_FUTURE_ANORMALE"
C_DATE_INVALIDE = "CTRL_BQ_DATE_INVALIDE"
C_ID_INSTABLE = "CTRL_BQ_IDENTIFIANT_STABLE_ABSENT"
C_SENS_INCOHERENT = "CTRL_BQ_SENS_INCOHERENT"
C_STATUT_INVALIDE = "CTRL_BQ_STATUT_INVALIDE"
C_DOUBLON_CERTAIN = "CTRL_BQ_DOUBLON_CERTAIN"
C_COMPTE_INCONNU = "CTRL_BQ_COMPTE_INCONNU"

C_RAPPRO_DEPASSEMENT = "CTRL_BQ_RAPPRO_SUPERIEUR_MOUVEMENT"
C_RAPPRO_CUMUL = "CTRL_BQ_RAPPRO_CUMUL_SUPERIEUR"
C_RAPPRO_SANS_ACTEUR = "CTRL_BQ_RAPPRO_CONFIRME_SANS_ACTEUR"
C_RAPPRO_TYPE_INCOHERENT = "CTRL_BQ_RAPPRO_TYPE_OBJET_INCOHERENT"
C_RAPPRO_OBJET_ABSENT = "CTRL_BQ_RAPPRO_OBJET_NON_IDENTIFIE"
C_RAPPROCHE_AVEC_SOLDE = "CTRL_BQ_RAPPROCHE_AVEC_SOLDE_RESTANT"
C_RAPPROCHE_NON_CATEGORISE = "CTRL_BQ_RAPPROCHE_NON_CATEGORISE"

C_PROP_SANS_PROPRIETAIRE = "CTRL_BQ_PAIEMENT_PROPRIETAIRE_SANS_PROPRIETAIRE"
C_FOURN_SANS_FOURNISSEUR = "CTRL_BQ_PAIEMENT_FOURNISSEUR_SANS_FOURNISSEUR"
C_PAYOUT_SANS_PLATEFORME = "CTRL_BQ_PAYOUT_SANS_PLATEFORME"
C_ASSOCIE_SANS_ASSOCIE = "CTRL_BQ_REMBOURSEMENT_ASSOCIE_SANS_ASSOCIE"
C_FRAIS_MAUVAIS_TYPE = "CTRL_BQ_FRAIS_BANCAIRES_MAUVAIS_TYPE_FLUX"

C_LOT9_NON_VALIDE = "CTRL_BQ_LOT9_MOUVEMENT_NON_VALIDE_ELIGIBLE"
C_LOT9_REJETE = "CTRL_BQ_LOT9_MOUVEMENT_REJETE_ELIGIBLE"

MESSAGES = {
    C_MONTANT_NUL: "Mouvement au montant nul ou absent.",
    C_DEVISE_INATTENDUE: "Devise différente de l'euro.",
    C_DATE_FUTURE: "Date d'opération dans le futur au-delà du délai raisonnable.",
    C_DATE_INVALIDE: "Date d'opération illisible ou absente.",
    C_ID_INSTABLE: "Identifiant stable (ROW_HASH) absent : anti-doublon impossible.",
    C_SENS_INCOHERENT: "Sens du mouvement absent ou différent de DEBIT/CREDIT.",
    C_STATUT_INVALIDE: "Statut de contrôle inconnu du référentiel moteur.",
    C_DOUBLON_CERTAIN: "Doublon bancaire signalé par le moteur d'import.",
    C_COMPTE_INCONNU: "Mouvement sans compte bancaire identifié.",
    C_RAPPRO_DEPASSEMENT: "Un rapprochement dépasse à lui seul le montant du mouvement.",
    C_RAPPRO_CUMUL: "Les rapprochements actifs cumulés dépassent le montant du mouvement.",
    C_RAPPRO_SANS_ACTEUR: "Rapprochement confirmé sans acteur identifié.",
    C_RAPPRO_TYPE_INCOHERENT: "Type d'objet du rapprochement incohérent avec la catégorie du mouvement.",
    C_RAPPRO_OBJET_ABSENT: "Rapprochement actif sans identifiant d'objet (hors NON_IDENTIFIE).",
    C_RAPPROCHE_AVEC_SOLDE: "Mouvement présenté comme rapproché alors qu'un solde reste à couvrir.",
    C_RAPPROCHE_NON_CATEGORISE: "Mouvement rapproché mais toujours sans catégorie.",
    C_PROP_SANS_PROPRIETAIRE: "Reversement propriétaire sans propriétaire identifié.",
    C_FOURN_SANS_FOURNISSEUR: "Paiement fournisseur sans fournisseur identifié.",
    C_PAYOUT_SANS_PLATEFORME: "Versement de plateforme sans plateforme identifiable.",
    C_ASSOCIE_SANS_ASSOCIE: "Remboursement d'associé sans associé identifié.",
    C_FRAIS_MAUVAIS_TYPE: "Frais bancaires portant un type de flux autre que TYPE_FLUX_016.",
    C_LOT9_NON_VALIDE: "Mouvement éligible à Lot9 alors qu'il n'est pas VALIDE.",
    C_LOT9_REJETE: "Mouvement rejeté/bloquant pourtant éligible au filtre Lot9.",
}

STATUTS_MOTEUR_CONNUS = {"BLOQUANT", "A_CONTROLER", "EN_ATTENTE_CLASSIFICATION", "VALIDE", "REJETE"}
TYPE_FLUX_FRAIS_BANCAIRES = "TYPE_FLUX_016"

# Cohérence catégorie moteur <-> type d'objet du rapprochement (seulement les cas non ambigus).
COHERENCE_CATEGORIE_TYPE = {
    "PAYOUT_PLATEFORME": {"RESERVATION", "PAYOUT_PLATEFORME", "NON_IDENTIFIE"},
    "VIREMENT_PROPRIETAIRE_A_RAPPROCHER": {"REVERSEMENT_PROPRIETAIRE", "NON_IDENTIFIE"},
    "FACTURE_PRESTATAIRE": {"CHARGE_FOURNISSEUR", "REGLEMENT_CHARGE", "NON_IDENTIFIE"},
    "VIR_ASSOCIE": {"REMBOURSEMENT_ASSOCIE", "NON_IDENTIFIE"},
    "FRAIS_BANCAIRES": {"MOUVEMENT_INTERNE", "NON_IDENTIFIE"},
}


def _anomalie(code: str, severite: str, mouvement_opaque: str, detail: str = "",
              **extra: Any) -> dict[str, Any]:
    return {"code": code, "severite": severite, "message": MESSAGES.get(code, code),
            "mouvement_id_opaque": mouvement_opaque, "detail": detail, **extra}


def _controler_mouvement(row: dict[str, Any], opaque: str) -> list[dict[str, Any]]:
    """Contrôles portant sur la ligne NORM_Banque elle-même."""
    out: list[dict[str, Any]] = []
    montant = to_nombre(row.get("montant"))
    sens = to_texte(row.get("sens")).upper()
    devise = to_texte(row.get("devise")).upper()
    statut = to_texte(row.get("statut_controle"))
    date_op = to_date(row.get("date_operation"))
    categorie = to_texte(row.get("categorie"))
    type_flux = to_texte(row.get("type_flux_id"))

    if montant is None or abs(montant) < 0.005:
        out.append(_anomalie(C_MONTANT_NUL, CRITIQUE, opaque, f"montant={montant}"))
    if devise and devise != "EUR":
        out.append(_anomalie(C_DEVISE_INATTENDUE, CRITIQUE, opaque, devise))
    if not to_texte(row.get("ROW_HASH")):
        out.append(_anomalie(C_ID_INSTABLE, BLOQUANT, opaque))
    if sens not in ("DEBIT", "CREDIT"):
        out.append(_anomalie(C_SENS_INCOHERENT, BLOQUANT, opaque, sens or "(vide)"))
    if statut and statut not in STATUTS_MOTEUR_CONNUS:
        out.append(_anomalie(C_STATUT_INVALIDE, CRITIQUE, opaque, statut))
    if not to_texte(row.get("compte_id")):
        out.append(_anomalie(C_COMPTE_INCONNU, CRITIQUE, opaque))

    if not date_op:
        out.append(_anomalie(C_DATE_INVALIDE, BLOQUANT, opaque))
    else:
        try:
            d = date.fromisoformat(date_op[:10])
            if d > date.today() + timedelta(days=5):
                out.append(_anomalie(C_DATE_FUTURE, AVERTISSEMENT, opaque, date_op))
        except ValueError:
            out.append(_anomalie(C_DATE_INVALIDE, BLOQUANT, opaque, date_op))

    # Anomalies moteur reprises telles quelles (jamais réécrites).
    codes = to_texte(row.get("codes_anomalie"))
    if "DOUBLON_BANCAIRE_POTENTIEL" in codes:
        out.append(_anomalie(C_DOUBLON_CERTAIN, AVERTISSEMENT, opaque, codes))

    # Frais bancaires portant un autre type de flux : entrerait mal (ou pas) dans Lot9.
    if categorie == "FRAIS_BANCAIRES" and type_flux and type_flux != TYPE_FLUX_FRAIS_BANCAIRES:
        out.append(_anomalie(C_FRAIS_MAUVAIS_TYPE, CRITIQUE, opaque, type_flux))

    # Métier : catégorie qui exige un tiers, sans tiers identifié.
    tiers = to_texte(row.get("tiers_detecte"))
    if categorie == "VIREMENT_PROPRIETAIRE_A_RAPPROCHER" and not tiers:
        out.append(_anomalie(C_PROP_SANS_PROPRIETAIRE, CRITIQUE, opaque))
    if categorie == "FACTURE_PRESTATAIRE" and not tiers:
        out.append(_anomalie(C_FOURN_SANS_FOURNISSEUR, AVERTISSEMENT, opaque))
    if categorie == "PAYOUT_PLATEFORME" and not tiers:
        out.append(_anomalie(C_PAYOUT_SANS_PLATEFORME, AVERTISSEMENT, opaque))
    if categorie == "VIR_ASSOCIE" and not tiers:
        out.append(_anomalie(C_ASSOCIE_SANS_ASSOCIE, AVERTISSEMENT, opaque))

    # Cohérence Lot9 : le filtre réel est (TYPE_FLUX_016 ET statut VALIDE).
    if type_flux == TYPE_FLUX_FRAIS_BANCAIRES and statut != "VALIDE":
        severite = CRITIQUE if statut in ("BLOQUANT", "REJETE") else INFO
        code = C_LOT9_REJETE if statut in ("BLOQUANT", "REJETE") else C_LOT9_NON_VALIDE
        out.append(_anomalie(code, severite, opaque, f"statut={statut or '(vide)'}"))

    return out


def _controler_rapprochements(row: dict[str, Any], opaque: str, db_path=None) -> list[dict[str, Any]]:
    """Contrôles portant sur les rapprochements applicatifs de ce mouvement."""
    out: list[dict[str, Any]] = []
    montant = abs(to_nombre(row.get("montant")) or 0)
    categorie = to_texte(row.get("categorie"))
    liens = rappro.lister(opaque, db_path=db_path)
    actifs = [l for l in liens if l["statut"] in rappro.STATUTS_ACTIFS]
    if not actifs:
        return out

    cumul = sum(l["montant_rapproche"] for l in actifs)
    for l in actifs:
        if l["montant_rapproche"] > montant + 1e-9:
            out.append(_anomalie(C_RAPPRO_DEPASSEMENT, BLOQUANT, opaque,
                                 f"{l['montant_rapproche']:.2f} € > {montant:.2f} €",
                                 rapprochement_id_opaque=l["rapprochement_id_opaque"]))
        if l["statut"] == rappro.ST_CONFIRME and not to_texte(l.get("acteur")):
            out.append(_anomalie(C_RAPPRO_SANS_ACTEUR, AVERTISSEMENT, opaque,
                                 rapprochement_id_opaque=l["rapprochement_id_opaque"]))
        if l["type_objet"] != "NON_IDENTIFIE" and not to_texte(l.get("objet_id")):
            out.append(_anomalie(C_RAPPRO_OBJET_ABSENT, CRITIQUE, opaque, l["type_objet"],
                                 rapprochement_id_opaque=l["rapprochement_id_opaque"]))
        attendus = COHERENCE_CATEGORIE_TYPE.get(categorie)
        if attendus and l["type_objet"] not in attendus:
            out.append(_anomalie(C_RAPPRO_TYPE_INCOHERENT, AVERTISSEMENT, opaque,
                                 f"catégorie {categorie} ↔ objet {l['type_objet']}",
                                 rapprochement_id_opaque=l["rapprochement_id_opaque"]))

    if cumul > montant + 1e-9:
        out.append(_anomalie(C_RAPPRO_CUMUL, BLOQUANT, opaque,
                             f"cumul {cumul:.2f} € > mouvement {montant:.2f} €"))

    # Mouvement rapproché mais toujours sans catégorie : impossible de savoir ce qu'il finance.
    if cumul >= montant - 1e-9 and not categorie:
        out.append(_anomalie(C_RAPPROCHE_NON_CATEGORISE, AVERTISSEMENT, opaque))

    return out


def controler(db_path=None) -> dict[str, Any]:
    """Contrôle l'ensemble des mouvements disponibles. Ne lève jamais : une source absente donne un
    résultat vide explicite, jamais une page en erreur."""
    src = reader.mouvements()
    if not src.etat.disponible:
        return {"statut": "SOURCE_INDISPONIBLE", "etat_source": src.etat, "anomalies": [],
                "compteurs": {BLOQUANT: 0, CRITIQUE: 0, AVERTISSEMENT: 0, INFO: 0},
                "nb_bloquants": 0, "recalcul_fiable": False}

    anomalies: list[dict[str, Any]] = []
    for row in src.lignes:
        mid = to_texte(row.get("mouvement_id"))
        if not mid:
            continue
        opaque = ctrl.id_opaque(mid)
        anomalies.extend(_controler_mouvement(row, opaque))
        anomalies.extend(_controler_rapprochements(row, opaque, db_path=db_path))

    anomalies.sort(key=lambda a: (_ORDRE.get(a["severite"], 9), a["code"]))
    compteurs = {niv: sum(1 for a in anomalies if a["severite"] == niv)
                 for niv in (BLOQUANT, CRITIQUE, AVERTISSEMENT, INFO)}
    return {
        "statut": "OK", "etat_source": src.etat, "anomalies": anomalies, "compteurs": compteurs,
        "nb_bloquants": compteurs[BLOQUANT],
        "recalcul_fiable": compteurs[BLOQUANT] == 0,
        "genere_le": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def controler_import(compteurs: dict[str, Any], nom_fichier: str = "",
                     deja_importe: bool = False) -> list[dict[str, Any]]:
    """Contrôles d'un import à partir des compteurs de prévisualisation — appelés avant confirmation,
    jamais après coup."""
    out: list[dict[str, Any]] = []

    def add(code: str, severite: str, message: str, detail: str = ""):
        out.append({"code": code, "severite": severite, "message": message, "detail": detail,
                    "mouvement_id_opaque": ""})

    if not compteurs.get("lignes_lues"):
        add("CTRL_IMP_FICHIER_VIDE", BLOQUANT, "Fichier sans aucune ligne de données.", nom_fichier)
    if deja_importe:
        add("CTRL_IMP_DEJA_IMPORTE", AVERTISSEMENT,
            "Un import portant ce nom de fichier existe déjà.", nom_fichier)
    if compteurs.get("invalides"):
        add("CTRL_IMP_LIGNES_INVALIDES", CRITIQUE,
            f"{compteurs['invalides']} ligne(s) invalide(s) ne seront pas importées.", "")
    if compteurs.get("doublons_probables"):
        add("CTRL_IMP_DOUBLON_PROBABLE", AVERTISSEMENT,
            f"{compteurs['doublons_probables']} doublon(s) probable(s) exclus sauf justification.", "")
    if compteurs.get("doublons_certains"):
        add("CTRL_IMP_DOUBLON_CERTAIN", INFO,
            f"{compteurs['doublons_certains']} doublon(s) certain(s) automatiquement exclus.", "")
    # Import partiel : des lignes lues ne seront ni importées ni expliquées comme doublon/invalide.
    lues = compteurs.get("lignes_lues", 0)
    expliquees = (compteurs.get("valides", 0) + compteurs.get("doublons_certains", 0)
                  + compteurs.get("doublons_probables", 0) + compteurs.get("invalides", 0))
    if lues and expliquees < lues:
        add("CTRL_IMP_LIGNES_NON_EXPLIQUEES", BLOQUANT,
            f"{lues - expliquees} ligne(s) lue(s) ne sont ni importées ni expliquées.", "")
    return out

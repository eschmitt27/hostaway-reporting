"""Règles de blocage — préparation d'un relevé/préfacture propriétaire (APP-3D).

Détection PURE (booléens/textes) sur des champs déjà calculés par le moteur — jamais un nouveau
calcul financier. Compose `proprietaires_reglements_service.load_owner_detail()` (moteur) +
`proprietaires_extras_reader` (acomptes/AirCover/imputations/ajustements) +
`clotures_service`/`controles_cloture_reader` (statut réel/humain du mois, APP-5C/5D) +
`proprietaires_suivi_service` (garde anti-doublon de facturation).
"""
from __future__ import annotations

import app.config as cfg
from app.readers import controles_cloture_reader as ref_reader
from app.readers import proprietaires_extras_reader as extras
from app.services import clotures_service as cs
from app.services import proprietaires_reglements_service as regl_svc
from app.services import proprietaires_suivi_service as suivi

BLOQUANT = "BLOQUANT"
INFO = "INFO"

_LIBELLES = {
    "PROPRIETAIRE_INCONNU": "Propriétaire inconnu du référentiel.",
    "TAUX_COMMISSION_ABSENT": "Aucun taux de commission historique trouvé pour ce mois.",
    "PAYOUT_ABSENT": "Aucun montant de payout retenu pour ce mois.",
    "NET_ABSENT": "Net propriétaire non calculé par le moteur pour ce mois.",
    "SOURCE_OBLIGATOIRE_ABSENTE": "Une source obligatoire (net, commissions ou factures) est indisponible.",
    "MOIS_INCOHERENT": "Format de mois invalide.",
    "CLOTURE_MOTEUR_INCOMPATIBLE": "Le mois n'est pas encore clôturé par le moteur (statut réel non CLOTURE).",
    "DOUBLON_FACTURATION": "Un relevé de ce propriétaire pour ce mois est déjà au statut Facturé.",
    "AJUSTEMENT_SANS_MOTIF": "Un ajustement post-clôture est présent sans motif renseigné.",
    "ANOMALIE_MOTEUR_OUVERTE": "Au moins une anomalie de facturation moteur reste ouverte.",
    "PREFACTURE_DEJA_PREPAREE": "Une préfacture est déjà en préparation pour ce propriétaire/mois.",
    "SOURCE_SCHEMA_INVALIDE": "Une source complémentaire (acomptes, AirCover, imputations, ajustements) est illisible.",
}

_INFO_CODES = {"ANOMALIE_MOTEUR_OUVERTE"}


def libelle(code: str) -> str:
    return _LIBELLES.get(code, code)


def evaluer(proprietaire_id: str, mois: str, db_path=None) -> dict:
    """Retourne {'bloquants': [...], 'informatifs': [...], 'cloturable': bool} — jamais un nouveau
    montant, uniquement des codes de blocage/information composés depuis des données déjà lues."""
    codes: list[str] = []

    if not cs.mois_valide(mois):
        codes.append("MOIS_INCOHERENT")
        return {"bloquants": ["MOIS_INCOHERENT"], "informatifs": [], "preparable": False}

    detail = regl_svc.load_owner_detail(proprietaire_id, mois)
    if detail is None:
        return {"bloquants": ["PROPRIETAIRE_INCONNU"], "informatifs": [], "preparable": False}
    if detail.get("status") != "OK":
        return {"bloquants": ["SOURCE_OBLIGATOIRE_ABSENTE"], "informatifs": [], "preparable": False}

    vue = detail.get("vue")
    if vue is None:
        codes.append("NET_ABSENT")
    else:
        if vue.get("ca_retenu") is None:
            codes.append("PAYOUT_ABSENT")
        if vue.get("net") is None:
            codes.append("NET_ABSENT")

    if not detail.get("taux_historiques"):
        codes.append("TAUX_COMMISSION_ABSENT")

    ref = ref_reader.cloture_ref()
    statut_moteur = "INCONNU"
    if ref.etat.disponible:
        for r in ref.lignes:
            if ref_reader.to_mois(r.get("mois")) == mois:
                statut_moteur = ref_reader.to_texte(r.get("statut_mois")).upper() or "INCONNU"
                break
    if statut_moteur != "CLOTURE":
        codes.append("CLOTURE_MOTEUR_INCOMPATIBLE")

    for aj in extras.ajustements_prop_mois(proprietaire_id, mois):
        if not extras.to_texte(aj.get("motif")):
            codes.append("AJUSTEMENT_SANS_MOTIF")
            break

    informatifs: list[str] = []
    if any(a.get("severite", "").upper() != "INFO" for a in detail.get("anomalies", [])):
        informatifs.append("ANOMALIE_MOTEUR_OUVERTE")

    existant = suivi.charger_par_prop_mois(proprietaire_id, mois, db_path)
    if existant and existant["statut_facturation"] == suivi.ST_FACTURE:
        codes.append("DOUBLON_FACTURATION")
    elif existant and existant["statut_facturation"] == suivi.ST_A_FACTURER:
        codes.append("PREFACTURE_DEJA_PREPAREE")

    if any(s.etat.etat == extras.ETAT_ILLISIBLE for s in (
        extras.acomptes(), extras.aircover(), extras.imputations_airbnb(), extras.ajustements_post_cloture())):
        codes.append("SOURCE_SCHEMA_INVALIDE")

    bloquants = [c for c in dict.fromkeys(codes) if c not in _INFO_CODES]
    return {"bloquants": bloquants, "informatifs": informatifs, "preparable": len(bloquants) == 0}

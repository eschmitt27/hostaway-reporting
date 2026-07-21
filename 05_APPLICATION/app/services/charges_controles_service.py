"""Règles de contrôle des charges et de leur affectation (APP-3E).

Détection PURE (booléens/textes) — jamais un nouveau calcul financier. Compose une charge (dict
issu du moteur/source réelle), son affectation logique (`charges_affectations_service`, optionnelle)
et le référentiel fournisseur (`fournisseurs_referentiel_service`). Même pattern que
`proprietaires_blocages_service.py`.
"""
from __future__ import annotations

from typing import Any

from app.services import fournisseurs_referentiel_service as frs

BLOQUANT = "BLOQUANT"
INFO = "INFO"

_LIBELLES = {
    "CHARGE_SOURCE_ABSENTE": "La source de charges est absente.",
    "CHARGE_SOURCE_VIDE": "La source de charges est vide.",
    "CHARGE_SCHEMA_INVALIDE": "Le schéma de la source de charges est invalide.",
    "CHARGE_MONTANT_INVALIDE": "Montant de charge invalide ou absent.",
    "CHARGE_MOIS_ABSENT": "Mois de charge absent ou invalide.",
    "CHARGE_LOGEMENT_ABSENT": "Logement non renseigné pour cette charge.",
    "CHARGE_PROPRIETAIRE_ABSENT": "Propriétaire non renseigné pour cette charge.",
    "CHARGE_FOURNISSEUR_ABSENT": "Aucun fournisseur affecté à cette charge.",
    "CHARGE_FOURNISSEUR_INACTIF": "Le fournisseur affecté est inactif.",
    "CHARGE_DOUBLON": "Charge en doublon détectée.",
    "CHARGE_REFACTURABLE_NON_JUSTIFIEE": "Charge marquée refacturable sans justificatif logique.",
    "CHARGE_AFFECTATION_INCOMPLETE": "Affectation incomplète (fournisseur, logement ou nature manquant).",
    "CHARGE_SOURCE_MODIFIEE": "La source a été modifiée depuis l'affectation (empreinte différente).",
    "CHARGE_APRES_CLOTURE": "Charge ajoutée après la clôture du mois.",
    "CHARGE_DIVERGENCE_MOTEUR": "Divergence entre le montant affiché et celui produit par le moteur.",
    "AJUSTEMENT_SANS_MOTIF": "Ajustement post-clôture sans motif renseigné.",
}

_INFO_CODES = {"CHARGE_REFACTURABLE_NON_JUSTIFIEE", "CHARGE_DIVERGENCE_MOTEUR"}


def libelle(code: str) -> str:
    return _LIBELLES.get(code, code)


def evaluer(charge: dict[str, Any], affectation: dict[str, Any] | None, *,
            source_etat: str = "OK", mois_cloture: bool = False, doublon: bool = False,
            db_path=None) -> dict[str, Any]:
    """Retourne {'bloquants': [...], 'informatifs': [...], 'conforme': bool} — jamais un nouveau
    montant, uniquement des codes composés depuis des données déjà connues."""
    codes: list[str] = []
    informatifs: list[str] = []

    if source_etat == "ABSENTE":
        return {"bloquants": ["CHARGE_SOURCE_ABSENTE"], "informatifs": [], "conforme": False}
    if source_etat == "VIDE":
        return {"bloquants": ["CHARGE_SOURCE_VIDE"], "informatifs": [], "conforme": False}
    if source_etat == "ILLISIBLE":
        return {"bloquants": ["CHARGE_SCHEMA_INVALIDE"], "informatifs": [], "conforme": False}

    montant = charge.get("montant")
    if montant is None or not isinstance(montant, (int, float)):
        codes.append("CHARGE_MONTANT_INVALIDE")
    if not charge.get("mois"):
        codes.append("CHARGE_MOIS_ABSENT")
    if not charge.get("logement_id") and not affectation:
        codes.append("CHARGE_LOGEMENT_ABSENT")
    if not charge.get("proprietaire_id") and not (affectation and affectation.get("proprietaire_id")):
        codes.append("CHARGE_PROPRIETAIRE_ABSENT")

    if doublon:
        codes.append("CHARGE_DOUBLON")

    if mois_cloture and charge.get("ajoutee_apres_cloture"):
        codes.append("CHARGE_APRES_CLOTURE")

    if charge.get("est_ajustement") and not (charge.get("motif") or "").strip():
        codes.append("AJUSTEMENT_SANS_MOTIF")

    if affectation is None:
        codes.append("CHARGE_AFFECTATION_INCOMPLETE")
    else:
        fournisseur_opaque = affectation.get("fournisseur_id_opaque")
        if not fournisseur_opaque:
            codes.append("CHARGE_FOURNISSEUR_ABSENT")
        else:
            fournisseur = frs.charger_par_opaque(fournisseur_opaque, db_path)
            if fournisseur is None or fournisseur.get("statut") != "ACTIF":
                codes.append("CHARGE_FOURNISSEUR_INACTIF")

        if not (affectation.get("logement_id") and affectation.get("nature")):
            codes.append("CHARGE_AFFECTATION_INCOMPLETE")

        if affectation.get("refacturable") and not (affectation.get("justificatif_logique") or "").strip():
            informatifs.append("CHARGE_REFACTURABLE_NON_JUSTIFIEE")

        empreinte_actuelle = charge.get("source_empreinte")
        if empreinte_actuelle and affectation.get("source_empreinte") and \
                empreinte_actuelle != affectation.get("source_empreinte"):
            codes.append("CHARGE_SOURCE_MODIFIEE")

    if charge.get("divergence_moteur"):
        informatifs.append("CHARGE_DIVERGENCE_MOTEUR")

    bloquants = [c for c in dict.fromkeys(codes) if c not in _INFO_CODES]
    informatifs = list(dict.fromkeys(informatifs))
    return {"bloquants": bloquants, "informatifs": informatifs, "conforme": len(bloquants) == 0}

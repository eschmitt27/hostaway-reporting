"""Collecte des objets métier réellement rapprochables avec un mouvement bancaire.

Aucun objet fictif n'est fabriqué ici : chaque candidat vient d'une source existante du dépôt
(charges Lot3, trésorerie propriétaires...). Quand une source est absente ou illisible, elle est
simplement ignorée — jamais remplacée par des données inventées, jamais une exception qui ferait
échouer la page.

RÈGLE MÉTIER (2026-08-08, définitive) : les réservations (Hostaway ou hors Hostaway) ne sont
JAMAIS des candidats de rapprochement bancaire — un virement entrant de plateforme ne correspond
pas de façon fiable à une réservation individuelle. Voir `candidats_pour()` ci-dessous.

Ce service ne score rien : il fournit la matière à `banques_suggestions_service.evaluer()`.
"""
from __future__ import annotations

from typing import Any

import app.config as cfg
from app.readers import charges_reader
from app.readers.banques_reader import to_texte, to_nombre, to_date


def _charges() -> list[dict[str, Any]]:
    """Charges disponibles (table `charges`, 0052) → candidats CHARGE_FOURNISSEUR /
    REGLEMENT_CHARGE."""
    try:
        if not charges_reader.master_available():
            return []
        lignes = charges_reader.read_charges()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in lignes:
        cid = to_texte(r.get("charge_id"))
        montant = to_nombre(r.get("montant"))
        if not cid or montant is None:
            continue
        out.append({
            "type_objet": "CHARGE_FOURNISSEUR",
            "objet_id": cid,
            "montant": abs(montant),
            "date": to_date(r.get("date_charge")),
            "reference": to_texte(r.get("facture_ref")) or "",
            "fournisseur": to_texte(r.get("fournisseur")) or to_texte(r.get("tiers")),
            "proprietaire_id": to_texte(r.get("proprietaire_id")),
            "libelle": f"Charge {cid} — {to_texte(r.get('categorie_charge_id'))}",
        })
    return out


def _reversements_proprietaires() -> list[dict[str, Any]]:
    """Mouvements de trésorerie propriétaires VALIDE avec un reste à rapprocher > 0 →
    candidats REVERSEMENT_PROPRIETAIRE. Ne duplique jamais le calcul de solde/reste, délègue à
    `proprietaires_tresorerie_service` (source unique du reste à rapprocher)."""
    try:
        from app.services import proprietaires_tresorerie_service as tresorerie
        mouvements = tresorerie.objets_rapprochables()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for m in mouvements:
        out.append({
            "type_objet": "REVERSEMENT_PROPRIETAIRE",
            "objet_id": m["mouvement_opaque"],
            "montant": m["reste_a_rapprocher"],
            "date": to_date(m.get("date_mouvement")),
            "reference": to_texte(m.get("reference_metier")) or "",
            "sens_objet": to_texte(m.get("sens")),
            "nature": to_texte(m.get("nature")),
            "proprietaire_id": to_texte(m.get("proprietaire_id")),
            "libelle": f"Trésorerie propriétaire {m['mouvement_opaque']} — {m.get('nature')}",
        })
    return out


def candidats_pour(mouvement: dict[str, Any]) -> list[dict[str, Any]]:
    """Candidats pertinents selon le SENS du mouvement — un débit ne peut pas être un encaissement
    de réservation, un crédit ne peut pas être un paiement de charge : filtrer ici évite d'exposer
    des suggestions absurdes à l'utilisateur.

    RÈGLE MÉTIER (2026-08-08, définitive) : un virement entrant de plateforme (Airbnb ou autre)
    n'est JAMAIS rapproché d'une réservation individuelle — le montant reçu en banque n'a pas de
    correspondance fiable avec une réservation (commissions/frais agrégés, versements groupés,
    plateformes multiples). La Banque catégorise l'origine du flux (cf. `banques_service`,
    catégorie moteur `PAYOUT_PLATEFORME`) ; elle ne génère jamais de candidat RESERVATION. Aucun
    générateur de ce type n'existe dans ce module — ne pas en réintroduire un.

    Trésorerie propriétaire : le sens du mouvement bancaire doit être cohérent avec le sens
    déclaré de l'objet (un CREDIT bancaire ne peut candidater que sur un mouvement propriétaire
    PROPRIETAIRE_VERS_SOCIETE ; un DEBIT bancaire, que sur SOCIETE_VERS_PROPRIETAIRE) — jamais
    déduit automatiquement, seulement filtré sur ce qui est déjà déclaré."""
    sens = to_texte(mouvement.get("sens")).upper()
    tresorerie = _reversements_proprietaires()
    if sens == "CREDIT":
        return [c for c in tresorerie if c["sens_objet"] == "PROPRIETAIRE_VERS_SOCIETE"]
    if sens == "DEBIT":
        return _charges() + [c for c in tresorerie
                             if c["sens_objet"] == "SOCIETE_VERS_PROPRIETAIRE"]
    return _charges() + tresorerie


def compter_sources() -> dict[str, int]:
    """Diagnostic honnête pour l'interface : combien de candidats sont réellement disponibles."""
    return {"charges": len(_charges()),
            "tresorerie_proprietaires": len(_reversements_proprietaires())}

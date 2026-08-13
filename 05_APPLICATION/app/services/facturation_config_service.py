"""Configuration unique de la facturation propriétaire.

Tout ce qui rend une facture opposable est regroupé ici : numérotation, identité de l'émetteur,
régime de TVA, mentions, conditions de règlement, pénalités, préparation de la facturation
électronique. Aucun hardcode dispersé ailleurs dans le code.

**Rien n'est inventé.** Les valeurs viennent de l'environnement (ou d'un fichier de configuration
non versionné) et sont **vides par défaut**. Une valeur réglementaire absente ne reçoit pas de
valeur « raisonnable » de repli : elle bloque l'émission réelle, et le contrôle dit laquelle
manque. C'est particulièrement vrai pour le régime de TVA, le taux de pénalités de retard et
l'indemnité forfaitaire de recouvrement — trois données juridiques que ce module ne choisit pas.

`DECISIONS_METIER.md` acte que les coordonnées définitives de la société seront fournies plus tard :
cette configuration est donc conçue pour fonctionner à vide en recette et refuser en réel.
"""
from __future__ import annotations

import os
from typing import Any

import app.config as cfg

# ── Régimes de TVA ──────────────────────────────────────────────────────────────────────────────
# Vocabulaire aligné sur la décision D083, déjà en vigueur pour les prestataires de ménage.
TVA_FRANCHISE = "FRANCHISE_TVA"
TVA_ASSUJETTI = "ASSUJETTI_TVA"
TVA_EXONERATION_AUTRE = "EXONERATION_AUTRE"
TVA_A_CONTROLER = "A_CONTROLER"
REGIMES_TVA = (TVA_FRANCHISE, TVA_ASSUJETTI, TVA_EXONERATION_AUTRE, TVA_A_CONTROLER)

# ── Types de client ─────────────────────────────────────────────────────────────────────────────
CLIENT_PARTICULIER = "PARTICULIER"
CLIENT_PROFESSIONNEL = "PROFESSIONNEL"
CLIENT_A_CONTROLER = "A_CONTROLER"
TYPES_CLIENT = (CLIENT_PARTICULIER, CLIENT_PROFESSIONNEL, CLIENT_A_CONTROLER)

NATURE_PRESTATION = "PRESTATION_DE_SERVICES"
ADRESSE_LIVRAISON_NA = "NON_APPLICABLE"

# ── Numérotation ────────────────────────────────────────────────────────────────────────────────
# Séries annuelles explicites : le compteur est porté par `factures_proprietaires_sequence`, dont
# la clé est la série. `F-2026-000001` et `A-2026-000001` sont donc deux compteurs indépendants,
# et le passage à 2027 ouvre une nouvelle série sans jamais réécrire l'ancienne.
SERIE_FACTURE_PREFIXE = "F"
SERIE_AVOIR_PREFIXE = "A"
PADDING_NUMERO = 6


def _env(nom: str, defaut: str = "") -> str:
    return str(os.environ.get(nom, getattr(cfg, nom, defaut)) or "").strip()


def serie(type_document: str, annee: int | str) -> str:
    """Série de numérotation pour un type de document et une année.

    Une série par année et par type : la continuité chronologique est garantie à l'intérieur d'une
    série, et l'année reste lisible dans le numéro (`F-2026-000001`).
    """
    prefixe = SERIE_AVOIR_PREFIXE if str(type_document).upper() == "AVOIR" else SERIE_FACTURE_PREFIXE
    return f"{prefixe}-{annee}"


def formater_numero(serie_nom: str, sequence: int) -> str:
    return f"{serie_nom}-{int(sequence):0{PADDING_NUMERO}d}"


# ── Identité de l'émetteur ──────────────────────────────────────────────────────────────────────

def emetteur() -> dict[str, Any]:
    """Identité de la société émettrice. Champs vides tant qu'ils ne sont pas renseignés."""
    return {
        "denomination": _env("SOCIETE_NOM"),
        "forme_juridique": _env("SOCIETE_FORME_JURIDIQUE"),
        "capital": _env("SOCIETE_CAPITAL"),
        "siren": _env("SOCIETE_SIREN"),
        "siret": _env("SOCIETE_SIRET"),
        "rcs": _env("SOCIETE_RCS"),
        "adresse_siege": _env("SOCIETE_ADRESSE"),
        "tva_intra": _env("SOCIETE_TVA_INTRA"),
        "contact": _env("SOCIETE_CONTACT"),
        "coordonnees_paiement": _env("SOCIETE_COORDONNEES_PAIEMENT"),
    }


# Champs sans lesquels un document ne peut pas être émis réellement. Volontairement restreint à ce
# qui identifie l'émetteur de façon non ambiguë ; le reste est facultatif ou dépend du régime.
EMETTEUR_REQUIS = ("denomination", "adresse_siege", "siren")


# ── Régime de TVA ───────────────────────────────────────────────────────────────────────────────

def regime_tva() -> str:
    """Régime déclaré. `A_CONTROLER` par défaut — l'absence de TVA facturée ne suffit pas à
    déterminer son fondement juridique, et ce module ne le devine pas."""
    valeur = _env("FACTURATION_REGIME_TVA", TVA_A_CONTROLER).upper()
    return valeur if valeur in REGIMES_TVA else TVA_A_CONTROLER


def mention_tva(regime: str | None = None) -> str:
    """Mention à imprimer. Elle vient de la configuration : aucune mention légale n'est rédigée
    ici, seul l'emplacement est prévu."""
    regime = regime or regime_tva()
    if regime == TVA_FRANCHISE:
        return _env("FACTURATION_MENTION_FRANCHISE_TVA")
    if regime == TVA_EXONERATION_AUTRE:
        return _env("FACTURATION_MENTION_EXONERATION")
    if regime == TVA_ASSUJETTI:
        return _env("FACTURATION_MENTION_ASSUJETTI")
    return ""


def taux_tva_applicable(regime: str | None = None) -> float:
    """Taux appliqué aux lignes. Hors assujettissement, 0 — et HT = TTC."""
    regime = regime or regime_tva()
    if regime != TVA_ASSUJETTI:
        return 0.0
    try:
        return float(_env("FACTURATION_TAUX_TVA", "0") or 0)
    except ValueError:
        return 0.0


# ── Conditions de règlement ─────────────────────────────────────────────────────────────────────

def delai_paiement_jours() -> int | None:
    valeur = _env("FACTURATION_DELAI_PAIEMENT_JOURS")
    try:
        return int(valeur) if valeur else None
    except ValueError:
        return None


def conditions_escompte() -> str:
    return _env("FACTURATION_CONDITIONS_ESCOMPTE")


def taux_penalites_retard() -> str:
    """Taux de pénalités de retard — chaîne libre, jamais un défaut choisi par le code.
    Obligatoire pour un client professionnel (voir le contrôle de pré-émission)."""
    return _env("FACTURATION_TAUX_PENALITES_RETARD")


def indemnite_recouvrement() -> str:
    """Indemnité forfaitaire de recouvrement (B2B). Montant légal non écrit en dur : il doit être
    renseigné explicitement, faute de quoi l'émission professionnelle est bloquée."""
    return _env("FACTURATION_INDEMNITE_RECOUVREMENT")


# ── Facturation électronique ────────────────────────────────────────────────────────────────────

def facturation_electronique() -> dict[str, Any]:
    """Préparation seulement : aucun envoi, aucune plateforme choisie. Les valeurs restent neutres
    tant qu'un fournisseur agréé n'a pas été retenu."""
    return {
        "status": _env("FACTURATION_ELECTRONIQUE_STATUT", "NON_APPLICABLE") or "NON_APPLICABLE",
        "provider": _env("FACTURATION_ELECTRONIQUE_PROVIDER") or None,
        "format": _env("FACTURATION_ELECTRONIQUE_FORMAT") or None,
    }


def emission_reelle_autorisee() -> bool:
    """Verrou distinct des flags techniques : même configuration complète, l'émission réelle reste
    fermée tant qu'elle n'est pas explicitement ouverte."""
    return bool(cfg.RECETTE_MODE) is False and _env("FACTURATION_EMISSION_REELLE_ENABLED") in (
        "1", "true", "True")


def resume() -> dict[str, Any]:
    """Vue complète de la configuration, pour l'interface et les contrôles."""
    r = regime_tva()
    return {
        "emetteur": emetteur(),
        "emetteur_requis": EMETTEUR_REQUIS,
        "regime_tva": r,
        "mention_tva": mention_tva(r),
        "taux_tva": taux_tva_applicable(r),
        "delai_paiement_jours": delai_paiement_jours(),
        "conditions_escompte": conditions_escompte(),
        "taux_penalites_retard": taux_penalites_retard(),
        "indemnite_recouvrement": indemnite_recouvrement(),
        "facturation_electronique": facturation_electronique(),
        "numerotation": {"facture": f"{SERIE_FACTURE_PREFIXE}-AAAA-" + "N" * PADDING_NUMERO,
                         "avoir": f"{SERIE_AVOIR_PREFIXE}-AAAA-" + "N" * PADDING_NUMERO},
    }

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
import re
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

# Code STOCKÉ (stable, machine) et libellé IMPRIMÉ (lisible, métier) : deux choses distinctes.
# Le code sert aux contrôles et aux archives ; « PRESTATION_DE_SERVICES » sur un document adressé à
# un propriétaire ne décrit rien de ce qu'il a réellement acheté.
NATURE_PRESTATION = "PRESTATION_DE_SERVICES"
NATURE_PRESTATION_LIBELLE = "Gestion de location courte durée"
ADRESSE_LIVRAISON_NA = "NON_APPLICABLE"

# ── Numérotation ────────────────────────────────────────────────────────────────────────────────
# Séries annuelles explicites : le compteur est porté par `factures_proprietaires_sequence`, dont
# la clé est la série. `F-2026-000001` et `A-2026-000001` sont donc deux compteurs indépendants,
# et le passage à 2027 ouvre une nouvelle série sans jamais réécrire l'ancienne.
SERIE_FACTURE_PREFIXE = "F"
SERIE_AVOIR_PREFIXE = "A"
PADDING_NUMERO = 6          # séries annuelles historiques : F-2026-000001
PADDING_NUMERO_MENSUEL = 3  # séries mensuelles (§22)     : 2026-08-001

# `2026-08` ou `A-2026-08` — sert à reconnaître une série mensuelle pour choisir son padding.
_EST_SERIE_MENSUELLE = re.compile(r"^(?:A-)?\d{4}-(?:0[1-9]|1[0-2])$")
_MOIS_VALIDE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")


def _env(nom: str, defaut: str = "") -> str:
    return str(os.environ.get(nom, getattr(cfg, nom, defaut)) or "").strip()


def serie(type_document: str, annee: int | str) -> str:
    """Série ANNUELLE historique (`F-2026`, `A-2026`).

    Conservée pour les documents déjà émis sous ce format : leur numéro est définitif et ne se
    réécrit pas. Les nouvelles émissions passent par `serie_mois()` (recette utilisateur n°2, §22).
    """
    prefixe = SERIE_AVOIR_PREFIXE if str(type_document).upper() == "AVOIR" else SERIE_FACTURE_PREFIXE
    return f"{prefixe}-{annee}"


def serie_mois(type_document: str, mois: str) -> str:
    """Série MENSUELLE, indexée sur le mois de PRESTATION : `2026-08`, `A-2026-08` pour un avoir.

    Pourquoi le mois de prestation et non celui d'émission (§22) : une facture d'août émise début
    septembre appartient au mois d'août pour le propriétaire comme pour le rapprochement. Numéroter
    sur la date d'émission mélangerait deux mois de prestation dans une même série.

    Le mois est lu sur la facture (`AAAA-MM`, champ canonique), jamais dérivé d'une date
    d'affichage. La version précédente faisait `str(date_facture)[:4]` : une date saisie
    « 11/09/2026 » au lieu de « 2026-09-11 » produisait silencieusement la série « F-11/0 » et le
    numéro « F-11/0-000001 » — un numéro légalement inexploitable, obtenu sans le moindre refus.
    Partir du mois supprime la classe entière de ce défaut.

    Les avoirs gardent un préfixe distinct : deux séries séparées sont une exigence de forme.
    """
    mois = str(mois or "").strip()
    if not _MOIS_VALIDE.match(mois):
        raise ValueError(f"mois de prestation invalide pour la numérotation : {mois!r} "
                         f"(attendu AAAA-MM)")
    return f"{SERIE_AVOIR_PREFIXE}-{mois}" if str(type_document).upper() == "AVOIR" else mois


def formater_numero(serie_nom: str, sequence: int) -> str:
    """`2026-08` + 1 → `2026-08-001`. Les séries annuelles historiques gardent leur padding à 6.

    Le padding dépend de la série pour ne pas réécrire l'apparence des numéros déjà émis : une
    facture porte son numéro à vie, y compris quand la règle change après elle.
    """
    padding = PADDING_NUMERO_MENSUEL if _EST_SERIE_MENSUELLE.match(str(serie_nom)) else PADDING_NUMERO
    return f"{serie_nom}-{int(sequence):0{padding}d}"


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
    """Délai en jours. `0` signifie **paiement à réception**, `None` que rien n'est configuré.

    `if valeur else None` traitait « 0 » comme une absence : la chaîne « 0 » est vraie, mais un
    délai de 0 jour devenait ensuite indistinguable d'un délai manquant plus bas dans la chaîne.
    Le test porte donc sur la présence de la valeur, pas sur sa vérité.
    """
    valeur = _env("FACTURATION_DELAI_PAIEMENT_JOURS")
    if valeur == "":
        return None
    try:
        return int(valeur)
    except ValueError:
        return None


def conditions_paiement(delai: int | None = None) -> str:
    """Libellé des conditions de règlement, dérivé du délai — jamais saisi deux fois.

    Un libellé indépendant du délai finirait par le contredire : « 30 jours » écrit à la main
    au-dessus d'une échéance calculée à réception.
    """
    delai = delai_paiement_jours() if delai is None else delai
    if delai is None:
        return ""
    if delai == 0:
        return "Paiement à réception"
    return f"Paiement à {delai} jours"


def representants() -> str:
    """Personnes représentant la société sur le document (« Représentée par … »).

    Paramètre pur, sans défaut : aucun nom n'est déduit du référentiel des associés, et AUCUN titre
    juridique n'est ajouté. Le Kbis fourni n'en documente pas, et en imprimer un engagerait la
    société sur une qualité non vérifiée.
    """
    return _env("SOCIETE_REPRESENTANTS")


def conditions_escompte() -> str:
    """Escompte pour paiement anticipé. Mention obligatoire : quand aucun escompte n'est consenti,
    la facture doit le dire — d'où un défaut explicite plutôt qu'un silence."""
    return _env("FACTURATION_CONDITIONS_ESCOMPTE") or "Escompte pour paiement anticipé : néant"


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

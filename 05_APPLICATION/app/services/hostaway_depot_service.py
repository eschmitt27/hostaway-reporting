"""Dépôt de données Hostaway — ce que le pipeline GitHub publie, et où en est la synchronisation.

L'ARCHITECTURE, EN UNE PHRASE
Les identifiants Hostaway vivent dans les Secrets GitHub. Un workflow (`Hostaway Pipeline`,
`.github/workflows/pipeline.yml` sur la branche `main`) interroge l'API trois fois par jour et
**commite** le résultat dans le dépôt. Cette installation ne parle donc jamais à Hostaway : elle
synchronise le dernier état publié vers SQLite, et les moteurs travaillent sur SQLite.

    GitHub Actions + Secrets → API Hostaway → fichiers publiés dans le dépôt
                                                       ↓  (ce service)
                                          couche RAW SQLite → moteurs → application

CE SERVICE NE LIT PAS LES DONNÉES
Il ne sait ni ouvrir un TSV ni normaliser un canal. Il fait deux choses : dire ce que le dépôt
publie aujourd'hui, et lancer la synchronisation par le MÊME moteur d'extraction que le chemin API
(`lot1_hostaway_extract.py --source DEPOT_GITHUB`). C'est ce qui garantit qu'il n'existe pas deux
extracteurs Hostaway : il n'y a qu'un moteur, et deux façons pour les faits d'y entrer.

DEUX DATES, JAMAIS UNE SEULE
« Données produites le » n'est pas « données synchronisées le ». Le pipeline peut avoir tourné à
11h40 sans que rien n'ait été importé ici. Afficher une seule date ferait passer pour à jour une
base qui ne l'est pas — c'est précisément le mensonge que cette séparation interdit.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import app.config as cfg

# La lecture du dépôt appartient à la couche moteur, avec le reste de l'extraction Hostaway.
DOSSIER_MOTEUR = "02_TRAVAIL"
MODULE_DEPOT = "lib_hostaway_depot"

REMOTE_DEFAUT = os.environ.get("HOSTAWAY_DEPOT_REMOTE", "origin")
BRANCHE_DEFAUT = os.environ.get("HOSTAWAY_DEPOT_BRANCHE", "main")

E_DEPOT_INDISPONIBLE = "HOSTAWAY_DEPOT_INDISPONIBLE"
E_DEJA_SYNCHRONISE = "HOSTAWAY_DEPOT_DEJA_SYNCHRONISE"

MESSAGES = {
    E_DEPOT_INDISPONIBLE: ("Le dépôt de données Hostaway n'a pas pu être lu : les réservations "
                           "affichées restent celles de la dernière synchronisation."),
    E_DEJA_SYNCHRONISE: "Les données publiées sont déjà synchronisées : rien de nouveau à importer.",
}


def _lib_depot():
    """Charge le lecteur de dépôt, qui vit avec les moteurs.

    Import différé et localisé : l'application ne dépend pas du dossier des scripts au chargement,
    et une installation dépourvue de ce dossier reste démarrable — l'écran dira simplement que le
    dépôt est illisible, ce qui est vrai.
    """
    racine = Path(cfg.PROJECT_ROOT) / DOSSIER_MOTEUR
    if str(racine) not in sys.path:
        sys.path.insert(0, str(racine))
    import importlib

    return importlib.import_module(MODULE_DEPOT)


# ── Ce que le dépôt publie ──────────────────────────────────────────────────────────────────────

def etat_publie(*, rafraichir: bool = True, remote: str = "", branche: str = "") -> dict[str, Any]:
    """Dernier état publié par le pipeline : commit, date de production, disponibilité.

    `rafraichir=False` lit l'état déjà connu du poste — utile pour un affichage qui ne doit pas
    attendre le réseau, à condition de ne pas présenter ce résultat comme « le dernier publié ».
    """
    try:
        lib = _lib_depot()
    except Exception as exc:  # noqa: BLE001 — un dossier moteur absent est un état, pas un bug
        return {"disponible": False, "code": E_DEPOT_INDISPONIBLE,
                "message": MESSAGES[E_DEPOT_INDISPONIBLE],
                "erreur": f"{type(exc).__name__}: {exc}"}
    try:
        resultat = lib.etat(cfg.PROJECT_ROOT, remote=remote or REMOTE_DEFAUT,
                            branche=branche or BRANCHE_DEFAUT, rafraichir=rafraichir)
    except Exception as exc:  # noqa: BLE001
        return {"disponible": False, "code": E_DEPOT_INDISPONIBLE,
                "message": MESSAGES[E_DEPOT_INDISPONIBLE],
                "erreur": f"{type(exc).__name__}: {exc}"}
    if not resultat.get("disponible"):
        resultat.setdefault("code", E_DEPOT_INDISPONIBLE)
        resultat.setdefault("message", MESSAGES[E_DEPOT_INDISPONIBLE])
    return resultat


# ── Où en est cette installation ────────────────────────────────────────────────────────────────

def fraicheur(*, rafraichir: bool = True, db_path=None) -> dict[str, Any]:
    """Confronte ce qui est PUBLIÉ à ce qui est IMPORTÉ. C'est la seule vérité utile à l'écran.

    Trois états, et aucun ne se déduit d'une date seule :
      — `a_jour`        : la version publiée est celle qui est en base ;
      — `retard`        : une version plus récente existe, jamais importée ;
      — `jamais_importe`: aucune extraction en base.

    Dire « à jour » parce que la donnée locale est récente serait faux dès que le pipeline a
    republié entre-temps. C'est pourquoi la comparaison porte sur l'IDENTITÉ de la version
    (`source_ref`), jamais sur l'ancienneté.
    """
    from app.services import hostaway_raw_service as raw

    publie = etat_publie(rafraichir=rafraichir)
    importe = raw.fraicheur(db_path=db_path)

    reference_publiee = publie.get("commit") or ""
    reference_importee = (importe.get("source_ref") or "") if importe.get("disponible") else ""

    if not importe.get("disponible"):
        etat = "JAMAIS_IMPORTE"
    elif not reference_publiee:
        # Le dépôt n'a pas pu être lu : on ne peut pas affirmer un retard, ni le contraire.
        etat = "INDETERMINE"
    elif reference_publiee == reference_importee:
        etat = "A_JOUR"
    else:
        etat = "RETARD"

    return {
        "etat": etat,
        "publie": publie,
        "importe": importe,
        # Les deux dates, nommées sans ambiguïté possible.
        "source_produite_le": (importe.get("source_horodatage")
                               if importe.get("disponible") else None),
        "synchronise_le": importe.get("importe_le") if importe.get("disponible") else None,
        "publie_le": publie.get("source_horodatage"),
        "a_jour": etat == "A_JOUR",
        "nb_reservations": importe.get("nb_reservations") if importe.get("disponible") else None,
        "mode": importe.get("mode") if importe.get("disponible") else None,
    }


# ── Synchronisation ─────────────────────────────────────────────────────────────────────────────

def synchroniser(*, declencheur: str = "MANUEL", force: bool = False, attendre: bool = True,
                 db_path=None, timeout_s: int = 1800) -> dict[str, Any]:
    """Importe le dernier état publié dans la couche RAW SQLite.

    IDEMPOTENTE PAR CONSTRUCTION. Si la version publiée est déjà en base, rien n'est relancé :
    réimporter un état identique fabriquerait une extraction de plus, indiscernable de la
    précédente, et la comparaison d'une extraction à l'autre — qui sert à repérer les mois
    impactés — perdrait tout sens. `force=True` passe outre, pour rejouer délibérément.

    Synchrone par défaut : l'import depuis le dépôt prend quelques secondes, et l'appelant doit
    pouvoir afficher l'état réel au retour plutôt qu'un état encore périmé.
    """
    from app.services import hostaway_actualisation_service as moteur
    from app.services import hostaway_raw_service as raw

    publie = etat_publie(rafraichir=True)
    if not publie.get("disponible"):
        return {"ok": False, "code": E_DEPOT_INDISPONIBLE,
                "message": publie.get("erreur") or MESSAGES[E_DEPOT_INDISPONIBLE],
                "publie": publie}

    reference = publie.get("commit") or ""
    if not force:
        deja = raw.extraction_de_source(reference, db_path=db_path)
        if deja is not None:
            return {"ok": True, "importe": False, "code": E_DEJA_SYNCHRONISE,
                    "message": MESSAGES[E_DEJA_SYNCHRONISE], "publie": publie,
                    "extraction": deja, "fraicheur": fraicheur(rafraichir=False, db_path=db_path)}

    resultat = moteur.actualiser(declencheur=declencheur, arguments=moteur.ARGUMENTS_DEPOT,
                                 db_path=db_path, attendre=attendre, timeout_s=timeout_s)
    resultat["publie"] = publie
    resultat["importe"] = bool(resultat.get("ok"))
    if resultat.get("ok"):
        resultat["fraicheur"] = fraicheur(rafraichir=False, db_path=db_path)
    return resultat

"""Identité de l'environnement et base active — LA source de vérité unique.

POURQUOI CE MODULE
Le 2026-10-04, un diagnostic s.est trompé de base : il a lu `05_APPLICATION/data/app.db` (ancienne
base de développement du worktree) au lieu de la vraie base, désignée par `APP_DATA_DIR` dans le
`.env`. Rien ne le signalait : les deux s'appellent `app.db`, et sans `APP_DATA_DIR` la
configuration retombe silencieusement sur la première. Avec `MODE_REEL_ECRITURES=1`, l'erreur
inverse — un test ou une recette qui écrirait dans la vraie base — serait bien plus grave.

CE QUE CE MODULE DIT, ET RIEN D'AUTRE
  - quelle est la base RÉELLE : celle du `APP_DATA_DIR` DÉCLARÉ dans le `.env` du projet. C'est la
    configuration explicite de l'exploitant ; elle est lue dans le fichier même quand le processus
    ne charge pas le `.env` (la suite de tests le neutralise), pour qu'un processus isolé sache
    toujours ce qu'il ne doit pas toucher ;
  - quelle est la base ACTIVE : `cfg.DB_PATH`, lu à chaud (les tests le redirigent) ;
  - dans quel ENVIRONNEMENT on est : REAL, DEV, RECETTE ou TEST.

RÈGLE D'IDENTITÉ — JAMAIS DEVINÉE DEPUIS UN NOM DE DOSSIER
  1. `PILOTAGE_ENVIRONNEMENT` explicite (TEST posé par la suite de tests, RECETTE par les lanceurs
     de recette, DEV au besoin). Il ne peut PAS valoir REAL : être réel ne se déclare pas, cela se
     constate (sinon une copie étiquetée « REAL » se ferait passer pour la vraie base) ;
  2. `RECETTE_MODE=1` (mode recette historique) → RECETTE ;
  3. dossier de données = celui déclaré réel dans le `.env` → REAL ;
  4. aucun `APP_DATA_DIR` → DEV : repli sur `05_APPLICATION/data/`, BASE_DEV_OBSOLETE ;
  5. un autre `APP_DATA_DIR` → RECETTE (une copie isolée).

LA GARDE
Tout environnement autre que REAL qui ouvrirait la base réelle est REFUSÉ (`BaseReelleInterdite`),
au démarrage de l'application, au lancement de la suite de tests, et à chaque `get_db()`. Pas
d'avertissement : un refus. Aucune donnée n'est lue ni écrite pour établir l'identité.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import app.config as cfg

REAL = "REAL"
DEV = "DEV"
RECETTE = "RECETTE"
TEST = "TEST"
#: Valeurs acceptées pour `PILOTAGE_ENVIRONNEMENT` — REAL n'en fait volontairement pas partie.
DECLARABLES = (DEV, RECETTE, TEST)

VARIABLE = "PILOTAGE_ENVIRONNEMENT"

LIBELLES = {REAL: "RÉEL", DEV: "DÉVELOPPEMENT", RECETTE: "RECETTE", TEST: "TEST"}


class BaseReelleInterdite(RuntimeError):
    """Un environnement non réel s'apprête à ouvrir la base réelle."""


def _resolu(chemin) -> Path | None:
    try:
        return Path(chemin).expanduser().resolve()
    except (OSError, ValueError, TypeError):
        return None


@lru_cache(maxsize=4)
def _data_dir_reel_depuis(fichier_env: str) -> Path | None:
    fichier = Path(fichier_env)
    if not fichier.is_file():
        return None
    try:
        from dotenv import dotenv_values
    except ImportError:  # pragma: no cover - python-dotenv est une dépendance du projet
        return None
    valeur = (dotenv_values(fichier).get("APP_DATA_DIR") or "").strip()
    return _resolu(valeur) if valeur else None


def data_dir_reel() -> Path | None:
    """Dossier de données RÉEL : `APP_DATA_DIR` déclaré dans le `.env` du projet. None si le `.env`
    est absent ou n'en déclare pas — l'application ne peut alors rien qualifier de réel."""
    return _data_dir_reel_depuis(str(cfg.ENV_FILE))


def base_reelle() -> Path | None:
    reel = data_dir_reel()
    return reel / "app.db" if reel is not None else None


def est_dans_donnees_reelles(chemin) -> bool:
    """Vrai si `chemin` est le dossier de données réel ou se trouve dessous (base, sauvegardes…)."""
    reel = data_dir_reel()
    cible = _resolu(chemin)
    if reel is None or cible is None:
        return False
    return cible == reel or reel in cible.parents


def environnement() -> str:
    """REAL / DEV / RECETTE / TEST — voir la règle d'identité en tête de module."""
    declare = os.environ.get(VARIABLE, "").strip().upper()
    if declare in DECLARABLES:
        return declare
    if cfg.RECETTE_MODE:
        return RECETTE
    if est_dans_donnees_reelles(cfg.DB_PATH):
        return REAL
    if not os.environ.get("APP_DATA_DIR", "").strip():
        return DEV
    return RECETTE


def refuser_base_reelle(chemin=None) -> None:
    """Lève `BaseReelleInterdite` si un environnement non réel vise la base réelle.

    `chemin` : la base qu'on s'apprête à ouvrir (défaut : `cfg.DB_PATH`, lu à chaud). Un
    environnement REAL passe toujours. Une `PILOTAGE_ENVIRONNEMENT` invalide est refusée aussi :
    une faute de frappe ne doit pas faire basculer silencieusement vers la déduction automatique.
    """
    declare = os.environ.get(VARIABLE, "").strip().upper()
    if declare and declare not in DECLARABLES:
        raise BaseReelleInterdite(
            f"{VARIABLE}={declare!r} invalide : valeurs admises {', '.join(DECLARABLES)} "
            "(REAL ne se déclare pas, il se constate).")
    env = environnement()
    if env == REAL:
        return
    cible = chemin if chemin is not None else cfg.DB_PATH
    if est_dans_donnees_reelles(cible):
        raise BaseReelleInterdite(
            f"Environnement {env} : ouverture de la BASE RÉELLE refusée. Utiliser une copie "
            "(APP_DATA_DIR temporaire), une base de test ou une fixture.")


def identite() -> dict[str, Any]:
    """Ce qu'affiche Administration › Observabilité. Aucun chemin brut, aucun secret."""
    env = environnement()
    app_data_dir = os.environ.get("APP_DATA_DIR", "").strip()
    reel_connu = data_dir_reel() is not None
    if env == REAL:
        dossier = "Dossier de données réel (APP_DATA_DIR du .env)"
        base = "app.db — base réelle"
    elif not app_data_dir:
        dossier = "APP_DATA_DIR non défini — repli sur 05_APPLICATION/data"
        base = "app.db — BASE_DEV_OBSOLETE (développement, jamais la base réelle)"
    else:
        dossier = "Dossier isolé (APP_DATA_DIR ≠ dossier réel)"
        base = "app.db — copie isolée"
    try:
        isolation_ok = True
        refuser_base_reelle()
    except BaseReelleInterdite:
        isolation_ok = False
    return {
        "environnement": env,
        "libelle": LIBELLES[env],
        "est_reel": env == REAL,
        "declare": os.environ.get(VARIABLE, "").strip().upper() or None,
        "base_active": base,
        "dossier_donnees": dossier,
        "base_reelle_connue": reel_connu,
        "ecritures_actives": bool(cfg.MODE_REEL_ECRITURES or cfg.RECETTE_MODE),
        # Hors REAL, la base réelle est inaccessible par construction (garde ci-dessus) : les
        # écritures, si elles sont actives, ne portent que sur la copie.
        "ecritures_reelles_bloquees": env != REAL,
        "isolation_ok": isolation_ok,
    }

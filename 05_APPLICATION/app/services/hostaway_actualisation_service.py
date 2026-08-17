"""Actualisation Hostaway — point d'entrée unique.

    actualiser() → sous-processus Lot 1 → API → couche RAW SQLite

UN SEUL CHEMIN MÉTIER
Le bouton de l'interface et un futur déclenchement automatique appellent la MÊME fonction. Deux
implémentations divergeraient : l'une aurait un garde-fou, l'autre pas, et personne ne saurait
laquelle s'est exécutée. `actualiser()` ne prend aucun objet HTTP en paramètre, précisément pour
qu'un ordonnanceur puisse l'appeler tel quel.

L'API N'EST PAS RÉIMPLÉMENTÉE ICI
Ce service lance `lot1_hostaway_extract.py`, qui parle à Hostaway et écrit lui-même la couche RAW.
Réécrire l'appel API dans l'application créerait un second client, avec ses propres règles de
pagination et de nouvelle tentative — deux clients pour une seule API finissent toujours par se
comporter différemment sous limitation de débit.

PAS DE REQUÊTE WEB BLOQUÉE
Le sous-processus est lancé sans attendre sa fin. L'état se lit ensuite dans `moteur_runs` et
`hostaway_extractions`, qui sont déjà la source de vérité : aucun second système de tâches n'est
introduit, le journal de run EST le suivi.

CE QUE « FRAIS » VEUT DIRE
La fraîcheur vient du journal, jamais d'un fichier. Un classeur récent ne prouve pas une extraction
récente, et une extraction récente n'écrit plus forcément de classeur.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db

SCRIPT = "lot1_hostaway_extract.py"

DECLENCHEUR_MANUEL = "MANUEL"
DECLENCHEUR_AUTO = "AUTO"

ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_PARTIEL = "PARTIEL"
ST_ECHEC = "ECHEC"
ST_INTERROMPU = "INTERROMPU"

E_SCRIPT_ABSENT = "HOSTAWAY_SCRIPT_ABSENT"
E_INTERPRETEUR = "HOSTAWAY_INTERPRETEUR_ABSENT"
E_DEJA_EN_COURS = "HOSTAWAY_ACTUALISATION_EN_COURS"
E_LANCEMENT = "HOSTAWAY_LANCEMENT_IMPOSSIBLE"

MESSAGES = {
    E_SCRIPT_ABSENT: "Le moteur d'extraction Hostaway est introuvable sur cette installation.",
    E_INTERPRETEUR: ("Aucun interpréteur Python avec pandas n'est disponible : le moteur ne peut "
                     "pas être lancé."),
    E_DEJA_EN_COURS: ("Une actualisation Hostaway est déjà en cours. Attendez qu'elle se termine "
                      "avant d'en lancer une autre."),
    E_LANCEMENT: "Le moteur n'a pas pu être lancé.",
}

# Les tâches de ménage sont extraites par la même commande mais relèvent d'une autre chaîne, et leur
# endpoint limite le débit bien plus tôt. Les inclure ferait échouer une actualisation de
# réservations pour une raison qui ne la concerne pas.
ARGUMENTS_DEFAUT = ("--skip-cleaning-tasks",)

_COLS_RUN = ("run_id", "lot", "started_at", "ended_at", "statut", "declencheur", "pid",
             "nb_etapes", "nb_etapes_ok", "nb_etapes_ko", "duree_s", "erreur_resume")
_COLS_ETAPE = ("etape", "ordre", "started_at", "ended_at", "statut", "nb_lus", "nb_ecrits",
               "position", "tentatives", "http_status", "erreur")


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _table_presente(nom: str, *, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone())
    finally:
        conn.close()


def _racine_moteur() -> Path:
    return Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


def _interpreteur() -> str | None:
    """Interpréteur portant pandas. Le moteur en dépend ; l'application peut en être dépourvue.

    Résolu à l'appel, jamais figé : une installation peut changer d'interpréteur sans redémarrer.
    """
    candidats = []
    if os.environ.get("PILOTAGE_ENGINE_PYTHON"):
        candidats.append(os.environ["PILOTAGE_ENGINE_PYTHON"])
    candidats += [sys.executable, r"C:\Program Files\Python312\python.exe"]
    for chemin in candidats:
        if not chemin or not Path(chemin).exists():
            continue
        try:
            r = subprocess.run([chemin, "-c", "import pandas"], capture_output=True, timeout=30)
        except Exception:
            continue
        if r.returncode == 0:
            return chemin
    return None


# ── Lancement ───────────────────────────────────────────────────────────────────────────────────

def actualisation_en_cours(*, db_path=None) -> dict[str, Any] | None:
    """Run Hostaway encore ouvert, s'il y en a un.

    Empêche deux extractions simultanées : elles écriraient deux extractions concurrentes et la
    dernière close deviendrait « la » courante, quel que soit son contenu.
    """
    if not _table_presente("moteur_runs", db_path=db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            f"SELECT {', '.join(_COLS_RUN)} FROM moteur_runs "
            "WHERE lot = 'lot1_hostaway_extract' AND statut = ? "
            "ORDER BY started_at DESC LIMIT 1", (ST_EN_COURS,)).fetchone()
    finally:
        conn.close()
    return dict(zip(_COLS_RUN, r)) if r else None


def actualiser(*, declencheur: str = DECLENCHEUR_MANUEL, arguments: tuple[str, ...] = (),
               db_path=None, attendre: bool = False, timeout_s: int = 3600) -> dict[str, Any]:
    """Lance une actualisation Hostaway. Rend immédiatement, sauf `attendre=True`.

    Appelable sans contexte HTTP : c'est ce qui permettra à un ordonnanceur d'emprunter exactement ce
    chemin, sans seconde implémentation.

    `attendre` n'existe que pour les tests et un usage en ligne de commande : une requête web ne doit
    jamais rester bloquée sur une extraction, qui dure des minutes.
    """
    script = _racine_moteur() / SCRIPT
    if not script.exists():
        return {"ok": False, "code": E_SCRIPT_ABSENT, "message": MESSAGES[E_SCRIPT_ABSENT]}

    en_cours = actualisation_en_cours(db_path=db_path)
    if en_cours:
        return {"ok": False, "code": E_DEJA_EN_COURS, "message": MESSAGES[E_DEJA_EN_COURS],
                "run": en_cours}

    interpreteur = _interpreteur()
    if interpreteur is None:
        return {"ok": False, "code": E_INTERPRETEUR, "message": MESSAGES[E_INTERPRETEUR]}

    base = Path(db_path or cfg.DB_PATH)
    commande = [interpreteur, str(script), "--db", str(base),
                *(arguments or ARGUMENTS_DEFAUT)]

    # Environnement DÉRIVÉ : celui du serveur n'est jamais modifié. `PILOTAGE_DB_PATH` double
    # l'argument `--db` pour que les lots appelés en cascade visent la même base.
    env = dict(os.environ)
    env["PROJECT_ROOT"] = str(cfg.PROJECT_ROOT)
    env["PILOTAGE_DB_PATH"] = str(base)
    env["PYTHONIOENCODING"] = "utf-8"

    debut = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        if attendre:
            proc = subprocess.run(commande, cwd=str(cfg.PROJECT_ROOT), env=env,
                                  capture_output=True, text=True, timeout=timeout_s)
            code = proc.returncode
        else:
            proc = subprocess.Popen(commande, cwd=str(cfg.PROJECT_ROOT), env=env,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            code = None
    except Exception as exc:
        return {"ok": False, "code": E_LANCEMENT,
                "message": f"{MESSAGES[E_LANCEMENT]} ({type(exc).__name__})"}

    return {"ok": True, "lance_le": debut, "declencheur": declencheur, "pid": proc.pid,
            "code_retour": code, "attendu": attendre, "etat": etat(db_path=db_path)}


# ── État ────────────────────────────────────────────────────────────────────────────────────────

def dernier_run(*, db_path=None) -> dict[str, Any] | None:
    """Dernier run d'extraction Hostaway, quel que soit son statut."""
    if not _table_presente("moteur_runs", db_path=db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            f"SELECT {', '.join(_COLS_RUN)} FROM moteur_runs "
            "WHERE lot = 'lot1_hostaway_extract' ORDER BY started_at DESC, id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    return dict(zip(_COLS_RUN, r)) if r else None


def etapes(run_id: str, *, db_path=None) -> list[dict[str, Any]]:
    """Étapes d'un run. C'est ce qui permet de dire « réservations oui, ménages non »."""
    if not run_id or not _table_presente("moteur_run_etapes", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(_COLS_ETAPE, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS_ETAPE)} FROM moteur_run_etapes "
            "WHERE run_id = ? ORDER BY ordre, id", (run_id,))]
    finally:
        conn.close()


def etat(*, db_path=None) -> dict[str, Any]:
    """État complet pour l'affichage : dernier run, ses étapes, la donnée qui en découle.

    Un run PARTIEL est rendu comme tel, avec le détail par étape. Résumer un run partiel en « à jour »
    laisserait croire que tout a été rafraîchi — c'est exactement la fraîcheur mensongère qu'on
    cherche à éviter.
    """
    from app.services import hostaway_raw_service as raw
    from app.services import reservations_dataset_service as ds

    run = dernier_run(db_path=db_path)
    resultat: dict[str, Any] = {
        "run": run,
        "etapes": etapes(_txt(run and run.get("run_id")), db_path=db_path) if run else [],
        "en_cours": bool(run and run["statut"] == ST_EN_COURS),
        "extraction": raw.fraicheur(db_path=db_path),
        "reservations": ds.fraicheur(db_path=db_path),
    }
    resultat["complet"] = bool(run and run["statut"] == ST_SUCCES)
    resultat["partiel"] = bool(run and run["statut"] == ST_PARTIEL)
    return resultat


def historique(*, limite: int = 10, db_path=None) -> list[dict[str, Any]]:
    if not _table_presente("moteur_runs", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(_COLS_RUN, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS_RUN)} FROM moteur_runs WHERE lot = 'lot1_hostaway_extract' "
            "ORDER BY started_at DESC, id DESC LIMIT ?", (limite,))]
    finally:
        conn.close()

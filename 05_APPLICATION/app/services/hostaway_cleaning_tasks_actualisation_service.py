"""Actualisation Hostaway CleaningTasks — point d'entrée unique, réel.

    actualiser() → lot1_hostaway_extract.py --only-cleaning-tasks → Excel Discovery → SQLite

MÊME CLIENT, MÊME SCRIPT QUE HOSTAWAY_RAW
`--only-cleaning-tasks` est une branche dédiée de `lot1_hostaway_extract.py` (le MÊME script que
`hostaway_actualisation_service`) : elle saute listings/réservations/payouts, ne fait qu'appeler
`/v1/tasks` par logement (segmentation D065, déjà sûre), et écrit toujours
`MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`. Aucun second client Hostaway n'est créé ici.

POURQUOI UN PONT EXCEL INTERMÉDIAIRE
`lot1_hostaway_extract.py` n'écrit jamais directement `hostaway_cleaning_tasks` en SQLite (seul
`hostaway_cleaning_tasks_adaptateur.py::depuis_master_excel` sait lire ce fichier). Cette fonction
appelle donc le script en sous-processus (mêmes garde-fous que `hostaway_actualisation_service` :
jamais de `.pid` sur le chemin synchrone), puis relit le fichier Excel qu'il vient de produire et
l'enregistre en SQLite versionné via `hostaway_cleaning_tasks_raw_service` — avec `mode=MODE_API`
(donnée réellement fraîche, pas une reprise legacy).

CADENCE SÉPARÉE, JAMAIS AUTOMATIQUE
Ce service est appelé UNIQUEMENT par une demande explicite (`inclure_imports_externes=True`,
cf. `orchestrateur_dag.py` — nœud HOSTAWAY_CLEANING_TASKS marqué `externe=True`). H6 a déjà essuyé
des 429 sévères par le passé ; le déclencher à chaque « Actualiser toute l'activité » serait la même
erreur.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.services import hostaway_cleaning_tasks_adaptateur as adaptateur
from app.services import hostaway_cleaning_tasks_raw_service as raw
from app.services import run_history_service as history

SCRIPT = "lot1_hostaway_extract.py"

DECLENCHEUR_MANUEL = "MANUEL"
DECLENCHEUR_AUTO = "AUTO"

E_SCRIPT_ABSENT = "HOSTAWAY_CLEANING_TASKS_SCRIPT_ABSENT"
E_INTERPRETEUR = "HOSTAWAY_CLEANING_TASKS_INTERPRETEUR_ABSENT"
E_LANCEMENT = "HOSTAWAY_CLEANING_TASKS_LANCEMENT_IMPOSSIBLE"
E_CODE_RETOUR = "HOSTAWAY_CLEANING_TASKS_CODE_RETOUR"
E_MASTER_ABSENT_APRES_RUN = "HOSTAWAY_CLEANING_TASKS_MASTER_ABSENT_APRES_RUN"

MESSAGES = {
    E_SCRIPT_ABSENT: "Le moteur d'extraction Hostaway est introuvable sur cette installation.",
    E_INTERPRETEUR: ("Aucun interpréteur Python avec pandas n'est disponible : le moteur ne peut "
                     "pas être lancé."),
    E_LANCEMENT: "Le moteur n'a pas pu être lancé.",
}


def _racine_moteur() -> Path:
    return Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


def _interpreteur() -> str | None:
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


def actualiser(*, declencheur: str = DECLENCHEUR_MANUEL, db_path=None,
               timeout_s: int = 1800) -> dict[str, Any]:
    """Lance une extraction RÉELLE des tâches ménage Hostaway, jusqu'à son terme.

    Toujours synchrone (`--only-cleaning-tasks` est rapide : un appel par logement, pas par
    réservation) — appelable directement par l'orchestrateur, comme `importer_hostaway`.
    """
    script = _racine_moteur() / SCRIPT
    if not script.exists():
        return {"ok": False, "code": E_SCRIPT_ABSENT, "message": MESSAGES[E_SCRIPT_ABSENT]}

    interpreteur = _interpreteur()
    if interpreteur is None:
        return {"ok": False, "code": E_INTERPRETEUR, "message": MESSAGES[E_INTERPRETEUR]}

    base = Path(db_path or cfg.DB_PATH)
    commande = [interpreteur, str(script), "--db", str(base), "--only-cleaning-tasks"]

    env = dict(os.environ)
    env["PROJECT_ROOT"] = str(cfg.PROJECT_ROOT)
    env["PILOTAGE_DB_PATH"] = str(base)
    env["PYTHONIOENCODING"] = "utf-8"

    debut = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    history_run_id = history.demarrer("HOSTAWAY_CLEANING_TASKS", acteur=declencheur, db_path=db_path)

    try:
        # Même garde que `hostaway_actualisation_service.actualiser` : `subprocess.run()` rend un
        # `CompletedProcess`, jamais de `.pid` lu ici (mission 14b).
        proc = subprocess.run(commande, cwd=str(cfg.PROJECT_ROOT), env=env,
                              capture_output=True, text=True, timeout=timeout_s)
        code = proc.returncode
    except Exception as exc:
        history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}", db_path=db_path)
        return {"ok": False, "code": E_LANCEMENT,
                "message": f"{MESSAGES[E_LANCEMENT]} ({type(exc).__name__})"}

    if code != 0:
        history.marquer_echec(history_run_id, erreur=f"code_retour={code}", db_path=db_path)
        return {"ok": False, "code": E_CODE_RETOUR,
                "message": f"lot1_hostaway_extract.py --only-cleaning-tasks rc={code}"}

    # Le script a réussi et a écrit MASTER_FACT_HA_CleaningTasks_Discovery.xlsx : le relire et
    # l'enregistrer en SQLite versionné — même mécanisme que la reprise legacy, mode API car la
    # donnée est réellement fraîche.
    if not adaptateur.master_disponible():
        history.marquer_echec(history_run_id, erreur=MESSAGES.get(E_MASTER_ABSENT_APRES_RUN, ""),
                              db_path=db_path)
        return {"ok": False, "code": E_MASTER_ABSENT_APRES_RUN,
                "message": "Le script a réussi mais n'a produit aucun fichier Discovery."}

    lignes = adaptateur.depuis_master_excel()
    extraction_id = raw.ouvrir(mode=raw.MODE_API, run_id=history_run_id or "", db_path=db_path)
    if not extraction_id:
        history.marquer_echec(history_run_id, erreur="migration 0035 absente", db_path=db_path)
        return {"ok": False, "code": "MIGRATION_ABSENTE",
                "message": "Table hostaway_cleaning_tasks_extractions absente (migration 0035 non "
                           "appliquée)."}
    try:
        raw.enregistrer(extraction_id, taches=lignes, db_path=db_path)
    except Exception as exc:
        raw.cloturer(extraction_id, statut=raw.ST_ECHEC, message=f"{type(exc).__name__}: {exc}",
                     db_path=db_path)
        history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}", db_path=db_path)
        raise

    resultat_cloture = raw.cloturer(extraction_id, statut=raw.ST_SUCCES, db_path=db_path)
    history.marquer_succes(history_run_id, db_path=db_path)

    return {"ok": True, "lance_le": debut, "declencheur": declencheur, "code_retour": code,
            "history_run_id": history_run_id, **resultat_cloture}

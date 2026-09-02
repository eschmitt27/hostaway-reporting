"""Actualisation Hostaway CleaningTasks — point d'entrée unique, réel.

    actualiser() → HostawayClient.get_tasks() (API) → normalisation Python → SQLite versionné

ZÉRO EXCEL. L'ancienne version passait par un pont `lot1_hostaway_extract.py --only-cleaning-tasks`
(sous-processus) qui écrivait `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`, puis relisait ce
fichier pour l'enregistrer en SQLite — un effet de bord sur un fichier réel du dépôt à chaque
actualisation (mission « supprimer le dernier effet de bord Excel »). Ce module appelle
`HostawayAuth`/`HostawayClient`/`_extract_cleaning_tasks` (02_TRAVAIL/lot1_hostaway_extract.py,
MÊME client, MÊME transformation de champs, MÊME correctif de pagination — rien réimplémenté) EN
PROCESS, sans sous-processus ni fichier intermédiaire, et transmet directement le résultat à
`hostaway_cleaning_tasks_raw_service` (versioning déjà existant, réutilisé tel quel).

Un export Excel explicite reste possible via `exporter_cleaning_tasks_excel()` (§3 mission) — jamais
appelé par ce module ni par aucun parcours automatique (Actualiser Hostaway/les ménages/toute
l'activité, scheduler).

CADENCE SÉPARÉE, JAMAIS AUTOMATIQUE
Ce service est appelé UNIQUEMENT par une demande explicite (`inclure_imports_externes=True`,
cf. `orchestrateur_dag.py` — nœud HOSTAWAY_CLEANING_TASKS marqué `externe=True`). H6 a déjà essuyé
des 429 sévères par le passé ; le déclencher à chaque « Actualiser toute l'activité » serait la même
erreur.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.services import hostaway_cleaning_tasks_raw_service as raw
from app.services import run_history_service as history

_TRAVAIL_DIR = str(Path(cfg.PROJECT_ROOT) / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

DECLENCHEUR_MANUEL = "MANUEL"
DECLENCHEUR_AUTO = "AUTO"

E_CREDENTIALS_ABSENTES = "HOSTAWAY_CLEANING_TASKS_CREDENTIALS_ABSENTES"
E_API_ECHOUEE = "HOSTAWAY_CLEANING_TASKS_API_ECHOUEE"
E_MIGRATION_ABSENTE = "MIGRATION_ABSENTE"

MESSAGES = {
    E_CREDENTIALS_ABSENTES: ("Identifiants Hostaway absents (HOSTAWAY_CLIENT_ID/"
                              "HOSTAWAY_CLIENT_SECRET/HOSTAWAY_ACCOUNT_ID) : appel API impossible."),
}


def _credentials() -> tuple[str, str, str, str] | None:
    from dotenv import load_dotenv

    load_dotenv(Path(cfg.PROJECT_ROOT) / ".env")
    client_id = os.getenv("HOSTAWAY_CLIENT_ID", "")
    client_secret = os.getenv("HOSTAWAY_CLIENT_SECRET", "")
    account_id = os.getenv("HOSTAWAY_ACCOUNT_ID", "")
    base_url = os.getenv("HOSTAWAY_BASE_URL", "https://api.hostaway.com")
    if not (client_id and client_secret and account_id):
        return None
    return base_url, client_id, client_secret, account_id


def actualiser(*, declencheur: str = DECLENCHEUR_MANUEL, date_from: str = "2026-01-01",
               db_path=None) -> dict[str, Any]:
    """Récupère les tâches ménage Hostaway (H6) et les enregistre en SQLite versionné.

    Aucun fichier Excel créé ni lu — API → SQLite direct. `date_from` : mêmes tâches que
    `lot1_hostaway_extract.py --only-cleaning-tasks` (défaut identique).
    """
    creds = _credentials()
    if creds is None:
        return {"ok": False, "code": E_CREDENTIALS_ABSENTES,
                "message": MESSAGES[E_CREDENTIALS_ABSENTES]}
    base_url, client_id, client_secret, account_id = creds

    import lot1_hostaway_extract as lot1

    debut = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    history_run_id = history.demarrer("HOSTAWAY_CLEANING_TASKS", acteur=declencheur, db_path=db_path)

    log = _LogRelais()
    auth = lot1.HostawayAuth(base_url, client_id, client_secret)
    client = lot1.HostawayClient(auth, account_id, log)
    detector = lot1.AnomalyDetector(set())

    try:
        lignes, statut_extraction = lot1._extract_cleaning_tasks(client, date_from, detector, log)
    except Exception as exc:
        history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}", db_path=db_path)
        return {"ok": False, "code": E_API_ECHOUEE,
                "message": f"{MESSAGES.get(E_API_ECHOUEE, 'Appel API échoué')} ({type(exc).__name__})"}

    if statut_extraction == "FAILED":
        history.marquer_echec(history_run_id, erreur="extraction FAILED", db_path=db_path)
        return {"ok": False, "code": E_API_ECHOUEE,
                "message": "L'extraction des tâches ménage a échoué (voir logs)."}

    extraction_id = raw.ouvrir(mode=raw.MODE_API, run_id=history_run_id or "", db_path=db_path)
    if not extraction_id:
        history.marquer_echec(history_run_id, erreur="migration 0035 absente", db_path=db_path)
        return {"ok": False, "code": E_MIGRATION_ABSENTE,
                "message": "Table hostaway_cleaning_tasks_extractions absente (migration 0035 non "
                           "appliquée)."}
    try:
        raw.enregistrer(extraction_id, taches=lignes, db_path=db_path)
    except Exception as exc:
        raw.cloturer(extraction_id, statut=raw.ST_ECHEC, message=f"{type(exc).__name__}: {exc}",
                     db_path=db_path)
        history.marquer_echec(history_run_id, erreur=f"{type(exc).__name__}: {exc}", db_path=db_path)
        raise

    statut_cloture = raw.ST_SUCCES if statut_extraction == "OK" else raw.ST_PARTIEL
    resultat_cloture = raw.cloturer(extraction_id, statut=statut_cloture, db_path=db_path)
    history.marquer_succes(history_run_id, db_path=db_path)

    return {"ok": True, "lance_le": debut, "declencheur": declencheur, "code_retour": 0,
            "history_run_id": history_run_id, "extraction_id": extraction_id,
            "statut": statut_cloture, "nb_taches": len(lignes), **resultat_cloture}


class _LogRelais:
    """`_extract_cleaning_tasks` attend un logger (`.info`/`.warning`) — relais minimal vers le
    logger applicatif standard, jamais un `print` silencieux ni un logger inventé."""

    def __init__(self) -> None:
        import logging
        self._log = logging.getLogger("hostaway_cleaning_tasks_actualisation")

    def info(self, msg: str) -> None:
        self._log.info(msg)

    def warning(self, msg: str) -> None:
        self._log.warning(msg)

    def error(self, msg: str) -> None:
        self._log.error(msg)


def exporter_cleaning_tasks_excel(*, db_path=None, racine: Path | None = None) -> dict[str, Any]:
    """Export EXPLICITE (diagnostic/comparaison legacy uniquement) — jamais appelé par
    `actualiser()` ni par aucun parcours automatique (§3 mission). Réutilise l'extraction active
    déjà en SQLite ; n'appelle pas l'API."""
    import pandas as pd

    from app.db.connection import get_db

    extraction_id = raw.derniere_extraction_utilisable(db_path=db_path)
    if not extraction_id:
        return {"ok": False, "code": "AUCUNE_EXTRACTION_UTILISABLE",
                "message": "Aucune extraction Cleaning Tasks disponible à exporter."}

    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT task_id, reservation_id, listing_map_id, title, status, can_start_from, "
            "assignee_user_id, extrait_le, row_hash FROM hostaway_cleaning_tasks "
            "WHERE extraction_id = ?", (extraction_id,)
        ).fetchall()
    finally:
        conn.close()

    chemin = (Path(racine) if racine else Path(cfg.PROJECT_ROOT)).joinpath(
        "02_TRAVAIL", "Lot1_Hostaway", "MASTER_FACT_HA_CleaningTasks_Discovery.xlsx")
    df = pd.DataFrame([dict(r) for r in rows])
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(chemin, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="data", index=False)
    return {"ok": True, "extraction_id": extraction_id, "nb_lignes": len(rows),
            "chemin": str(chemin)}

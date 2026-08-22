"""Migration de schéma protégée — mission industrialisation socle technique.

`app.db.connection.apply_migrations()` reste inchangée et rapide (appelée par chaque test via le
fixture `tmp_db` — des centaines de fois par campagne) : lui ajouter une sauvegarde automatique
ralentirait toute la suite pour un risque nul sur une base de test jetable.

`migrer_avec_sauvegarde()` est le point d'entrée à utiliser pour une VRAIE migration de schéma
(cf. `85_RUNBOOK_MIGRATION_APP_DB_REELLE.md`) : sauvegarde → migration → vérification d'intégrité
→ validation, ou rollback automatique vers la sauvegarde si la migration échoue ou si la base
résultante est corrompue.

    Nouvelle opération → Sauvegarde → Traitement → Contrôles → Validation → Activation
    Si erreur → Rejet → Retour dernière version valide
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import backup_service, run_history_service as history


def _integrity_ok(path: Path) -> bool:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def _echec_et_rollback(db_path: Path | None, sauvegarde: dict[str, Any], run_id: str,
                       message: str, code: str) -> dict[str, Any]:
    """Restaure, puis journalise la reprise SUR LA BASE RESTAURÉE.

    `restaurer()` remplace tout le fichier cible — y compris la ligne `run_history` du run en
    échec, écrite AVANT la restauration. On rejournalise donc l'issue APRÈS coup, sur le fichier
    qui subsiste réellement : sinon la trace du rollback disparaîtrait avec le fichier remplacé.
    """
    restauration = backup_service.restaurer(
        sauvegarde["sauvegarde_id"], confirmer=True, cible=db_path, db_path=db_path)
    if restauration["ok"]:
        run_id_final = history.demarrer(
            "MIGRATION", sauvegarde_id=sauvegarde["sauvegarde_id"], db_path=db_path)
        history.marquer_rollback(
            run_id_final, erreur=f"run {run_id} : {message}", db_path=db_path)
    return {"ok": False, "code": code, "message": message,
            "sauvegarde_id": sauvegarde["sauvegarde_id"], "rollback": restauration}


def migrer_avec_sauvegarde(db_path: Path | None = None, *, acteur: str = "") -> dict[str, Any]:
    sauvegarde = backup_service.sauvegarder("MIGRATION", db_path=db_path)
    if not sauvegarde["ok"]:
        return {"ok": False, "code": "E_SAUVEGARDE_ECHOUEE",
                "message": "Sauvegarde préalable impossible ou corrompue — migration refusée."}

    run_id = history.demarrer(
        "MIGRATION", acteur=acteur, sauvegarde_id=sauvegarde["sauvegarde_id"], db_path=db_path)
    history.marquer_validating(run_id, db_path=db_path)

    try:
        apply_migrations(db_path)
    except Exception as exc:
        return _echec_et_rollback(db_path, sauvegarde, run_id, f"{type(exc).__name__}: {exc}",
                                  "E_MIGRATION_ECHOUEE")

    cible = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if not _integrity_ok(cible):
        return _echec_et_rollback(
            db_path, sauvegarde, run_id, "integrity_check a échoué après migration",
            "E_INTEGRITE_ECHOUEE")

    history.marquer_succes(run_id, db_path=db_path)
    return {"ok": True, "run_id": run_id, "sauvegarde_id": sauvegarde["sauvegarde_id"]}

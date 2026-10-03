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
from app.db.connection import apply_migrations, migrations_en_attente
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
        try:
            run_id_final = history.demarrer(
                "MIGRATION", sauvegarde_id=sauvegarde["sauvegarde_id"], db_path=db_path)
            history.marquer_rollback(
                run_id_final, erreur=f"run {run_id} : {message}", db_path=db_path)
        except sqlite3.OperationalError:
            pass    # schéma restauré antérieur à `run_history` (0057) : la trace reste le sidecar
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


# ── Migration au démarrage (mission « Fiabiliser les sauvegardes », 2026-10-03) ────────────────
#
#   Application démarre → migrations en attente ?
#     NON → démarrage normal, AUCUNE sauvegarde (un redémarrage n'en produit jamais).
#     OUI → sauvegarde BEFORE_MIGRATION vérifiée → migrations → integrity_check +
#           foreign_key_check → succès, ou restauration contrôlée de la sauvegarde et refus de
#           démarrer.
#   Sauvegarde impossible → la migration N'EST PAS lancée.
#   Base neuve (aucune version) → rien à protéger : migrations directes.

E_SAUVEGARDE_ECHOUEE = "E_SAUVEGARDE_ECHOUEE"
E_FK_ECHOUEE = "E_FK_ECHOUEE"


def migrer_au_demarrage(db_path: Path | None = None, *, acteur: str = "demarrage"
                        ) -> dict[str, Any]:
    etat = migrations_en_attente(db_path)
    if not etat["a_jouer"]:
        return {"ok": True, "migrations": [], "sauvegarde_id": None}
    if etat["base_neuve"]:
        apply_migrations(db_path)
        return {"ok": True, "base_neuve": True, "migrations": etat["a_jouer"],
                "sauvegarde_id": None}

    sauvegarde = backup_service.sauvegarder(
        backup_service.OPERATION_MIGRATION, db_path=db_path,
        categorie=backup_service.CAT_AVANT_MIGRATION, raison="AVANT_MIGRATION",
        migration_cible=etat["version_cible"], attente_s=300)
    if not sauvegarde["ok"]:
        return {"ok": False, "code": E_SAUVEGARDE_ECHOUEE, "migrations": etat["a_jouer"],
                "message": "Sauvegarde préalable impossible ou non valide : migration NON lancée."}

    try:
        run_id = history.demarrer("MIGRATION", acteur=acteur,
                                  sauvegarde_id=sauvegarde["sauvegarde_id"], db_path=db_path)
        history.marquer_validating(run_id, db_path=db_path)
    except sqlite3.OperationalError:
        run_id = None

    try:
        apply_migrations(db_path)
    except Exception as exc:   # noqa: BLE001
        return {**_echec_et_rollback(db_path, sauvegarde, run_id or "-",
                                     f"{type(exc).__name__}: {exc}", "E_MIGRATION_ECHOUEE"),
                "migrations": etat["a_jouer"]}

    cible = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    insp = backup_service.inspecter(cible)
    if not insp.get("lisible") or insp.get("integrity_check") != "ok":
        return {**_echec_et_rollback(db_path, sauvegarde, run_id or "-",
                                     "integrity_check a échoué après migration",
                                     "E_INTEGRITE_ECHOUEE"), "migrations": etat["a_jouer"]}
    if insp.get("foreign_key_check", 0) > (sauvegarde.get("foreign_key_check") or 0):
        return {**_echec_et_rollback(db_path, sauvegarde, run_id or "-",
                                     "foreign_key_check : nouvelles violations après migration",
                                     E_FK_ECHOUEE), "migrations": etat["a_jouer"]}

    if run_id:
        history.marquer_succes(run_id, db_path=db_path)
    return {"ok": True, "run_id": run_id, "sauvegarde_id": sauvegarde["sauvegarde_id"],
            "migrations": etat["a_jouer"], "version_avant": etat["version_actuelle"],
            "version_apres": insp.get("schema_version")}

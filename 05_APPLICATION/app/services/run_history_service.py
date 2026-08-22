"""Historique centralisé des runs — mission industrialisation socle technique.

NE REMPLACE AUCUNE table de run existante : `calculs_runs`, `lot10_runs`, `lot12_runs`,
`hostaway_extractions`, `banque_imports` restent la source de vérité DÉTAILLÉE de leur propre lot
(mission : ne pas tout refactorer). `run_history` est une vue d'ensemble légère, alimentée
volontairement par les opérations qui choisissent de l'utiliser — pas rétroactivement.

Statuts (mission) : STARTED → VALIDATING → SUCCESS, ou → FAILED, ou → ROLLED_BACK.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.db.connection import get_db

STATUTS = ("STARTED", "VALIDATING", "SUCCESS", "FAILED", "ROLLED_BACK")


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def demarrer(operation: str, *, acteur: str = "", sauvegarde_id: str | None = None,
            db_path: Path | None = None) -> str:
    """Ouvre un run. Retourne son identifiant opaque (RUNH-xxxx)."""
    run_id = "RUNH-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO run_history (run_id_opaque, operation, statut, sauvegarde_id_opaque, "
            "acteur) VALUES (?,?,?,?,?)",
            (run_id, operation, "STARTED", sauvegarde_id, acteur or None))
        conn.commit()
    finally:
        conn.close()
    return run_id


def _transition(run_id: str, statut: str, *, erreur: str = "", cloturer: bool = False,
                db_path: Path | None = None) -> dict[str, Any]:
    if statut not in STATUTS:
        raise ValueError(f"statut inconnu : {statut!r} (attendu parmi {STATUTS}).")
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT date_debut FROM run_history WHERE run_id_opaque=?", (run_id,)).fetchone()
        if row is None:
            return {"ok": False, "code": "E_RUN_INTROUVABLE", "message": f"Run {run_id} introuvable."}

        duree = None
        date_fin = None
        if cloturer:
            date_fin = _maintenant()
            debut = datetime.strptime(row["date_debut"], "%Y-%m-%dT%H:%M:%SZ")
            fin = datetime.strptime(date_fin, "%Y-%m-%dT%H:%M:%SZ")
            duree = (fin - debut).total_seconds()

        conn.execute(
            "UPDATE run_history SET statut=?, erreur=?, date_fin=COALESCE(?, date_fin), "
            "duree_s=COALESCE(?, duree_s) WHERE run_id_opaque=?",
            (statut, erreur or None, date_fin, duree, run_id))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "run_id": run_id, "statut": statut}


def marquer_validating(run_id: str, *, db_path: Path | None = None) -> dict[str, Any]:
    return _transition(run_id, "VALIDATING", db_path=db_path)


def marquer_succes(run_id: str, *, db_path: Path | None = None) -> dict[str, Any]:
    return _transition(run_id, "SUCCESS", cloturer=True, db_path=db_path)


def marquer_echec(run_id: str, *, erreur: str = "", db_path: Path | None = None) -> dict[str, Any]:
    return _transition(run_id, "FAILED", erreur=erreur, cloturer=True, db_path=db_path)


def marquer_rollback(run_id: str, *, erreur: str = "", db_path: Path | None = None) -> dict[str, Any]:
    return _transition(run_id, "ROLLED_BACK", erreur=erreur, cloturer=True, db_path=db_path)


def derniers(limit: int = 20, *, db_path: Path | None = None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM run_history ORDER BY date_debut DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

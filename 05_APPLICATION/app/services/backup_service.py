"""Sauvegarde/restauration de `app.db` elle-même — mission industrialisation socle technique.

DISTINCT de `snapshot_service.py` (sauvegarde des fichiers Excel/masters moteur, jamais `app.db`).
Distinct de `calculs_pipeline_service.py` (sauvegarde des fichiers écrits par un run de calcul).

Avant toute opération critique sur le SCHÉMA ou le CONTENU de la base entière (migration de
schéma, recalcul global, import massif), copier `app.db` avec `sauvegarder()` avant d'agir. En cas
de problème, `restaurer()` remet la copie en place — jamais automatique, toujours un appel
explicite et journalisé.
"""
from __future__ import annotations

import hashlib
import shutil
import sqlite3
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> str | None:
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=str(cfg.PROJECT_ROOT),
            capture_output=True, text=True, timeout=10)
        return res.stdout.strip() or None if res.returncode == 0 else None
    except Exception:
        return None


def _integrity_ok(path: Path) -> bool:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def sauvegarder(operation: str, *, db_path: Path | None = None) -> dict[str, Any]:
    """Copie `app.db` (ou `db_path`) sous `BACKUPS_DIR`, vérifie son intégrité, journalise.

    `PRAGMA journal_mode=WAL` (actif partout dans ce projet, cf. `get_db`) implique un fichier
    `-wal` séparé pour les écritures non encore répercutées dans le fichier principal : un simple
    `shutil.copy2` du seul `.db` risquerait de figer un état incohérent. On force un checkpoint
    complet avant la copie pour que le fichier `.db` seul soit une image fidèle et autonome.
    """
    source = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if not source.exists():
        return {"ok": False, "code": "E_SOURCE_ABSENTE", "message": f"Base absente : {source}."}

    conn = sqlite3.connect(str(source))
    try:
        conn.execute("PRAGMA wal_checkpoint(FULL)")
    finally:
        conn.close()

    dossier = Path(cfg.BACKUPS_DIR)
    dossier.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sid = "BCK-" + uuid.uuid4().hex[:12].upper()
    dest = dossier / f"{ts}_{operation}_{sid}.db"
    shutil.copy2(source, dest)

    ok = _integrity_ok(dest)
    statut = "VALIDE" if ok else "CORROMPU"

    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO sauvegardes_base (sauvegarde_id_opaque, operation, chemin_fichier, "
            "git_commit, database_hash, taille_octets, validation_status) "
            "VALUES (?,?,?,?,?,?,?)",
            (sid, operation, str(dest), _git_commit(), _sha256(dest), dest.stat().st_size, statut))
        conn.commit()
    finally:
        conn.close()

    return {"ok": ok, "sauvegarde_id": sid, "chemin": str(dest), "validation_status": statut}


def verifier(sauvegarde_id: str, *, db_path: Path | None = None) -> dict[str, Any]:
    """Revérifie l'intégrité et le hash d'une sauvegarde existante ; met à jour son statut."""
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM sauvegardes_base WHERE sauvegarde_id_opaque=?", (sauvegarde_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return {"ok": False, "code": "E_INTROUVABLE", "message": "Sauvegarde introuvable."}

    chemin = Path(row["chemin_fichier"])
    if not chemin.exists():
        statut = "CORROMPU"
        details = "Fichier de sauvegarde manquant."
    elif _sha256(chemin) != row["database_hash"]:
        statut = "CORROMPU"
        details = "Empreinte différente de celle enregistrée à la création."
    elif not _integrity_ok(chemin):
        statut = "CORROMPU"
        details = "PRAGMA integrity_check a échoué."
    else:
        statut = "VALIDE"
        details = ""

    conn = get_db(db_path)
    try:
        conn.execute(
            "UPDATE sauvegardes_base SET validation_status=? WHERE sauvegarde_id_opaque=?",
            (statut, sauvegarde_id))
        conn.commit()
    finally:
        conn.close()

    return {"ok": statut == "VALIDE", "validation_status": statut, "details": details}


def lister(*, db_path: Path | None = None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM sauvegardes_base ORDER BY date_creation DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def restaurer(sauvegarde_id: str, *, cible: Path | None = None, confirmer: bool = False,
             db_path: Path | None = None) -> dict[str, Any]:
    """Restaure une sauvegarde EN PLACE sur `cible` (par défaut `cfg.DB_PATH`).

    Rollback explicite : refuse tant que `confirmer` n'est pas True — une restauration remplace
    entièrement le fichier cible, ce n'est jamais une opération anodine. Revérifie l'intégrité de
    la sauvegarde avant de l'appliquer : ne jamais restaurer un fichier connu corrompu.
    """
    if not confirmer:
        return {"ok": False, "code": "E_CONFIRMATION_REQUISE",
                "message": "Restauration refusée sans confirmation explicite (confirmer=True)."}

    verif = verifier(sauvegarde_id, db_path=db_path)
    if not verif["ok"]:
        return {"ok": False, "code": "E_SAUVEGARDE_INVALIDE",
                "message": f"Sauvegarde {sauvegarde_id} non restaurable : {verif['details']}"}

    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT chemin_fichier FROM sauvegardes_base WHERE sauvegarde_id_opaque=?",
            (sauvegarde_id,)).fetchone()
    finally:
        conn.close()

    cible_path = Path(cible) if cible is not None else Path(cfg.DB_PATH)
    shutil.copy2(Path(row["chemin_fichier"]), cible_path)

    if not _integrity_ok(cible_path):
        return {"ok": False, "code": "E_RESTAURATION_CORROMPUE",
                "message": "La restauration a produit un fichier illisible."}

    return {"ok": True, "sauvegarde_id": sauvegarde_id, "cible": str(cible_path)}


def purger(*, garder_n: int = 10, db_path: Path | None = None) -> dict[str, Any]:
    """Supprime les sauvegardes physiques au-delà des `garder_n` plus récentes VALIDÉES.

    Jamais automatique : appelé uniquement à la demande explicite (mission §2 : « aucune
    suppression automatique sans règle claire »). Ne supprime jamais une sauvegarde CORROMPUE
    sans l'avoir listée — elle reste la seule preuve du problème tant qu'elle n'est pas nettoyée
    à part.
    """
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT id, sauvegarde_id_opaque, chemin_fichier FROM sauvegardes_base "
            "WHERE validation_status='VALIDE' ORDER BY date_creation DESC").fetchall()
        a_supprimer = rows[garder_n:]
        supprimes = []
        for r in a_supprimer:
            p = Path(r["chemin_fichier"])
            if p.exists():
                p.unlink()
            conn.execute("DELETE FROM sauvegardes_base WHERE id=?", (r["id"],))
            supprimes.append(r["sauvegarde_id_opaque"])
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "supprimees": supprimes, "conservees": min(len(rows), garder_n)}

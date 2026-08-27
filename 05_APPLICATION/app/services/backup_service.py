"""Sauvegarde/restauration de `app.db` elle-même — mission industrialisation socle technique.

DISTINCT de `snapshot_service.py` (sauvegarde des fichiers Excel/masters moteur, jamais `app.db`).
Distinct de `calculs_pipeline_service.py` (sauvegarde des fichiers écrits par un run de calcul).

Avant toute opération critique sur le SCHÉMA ou le CONTENU de la base entière (migration de
schéma, recalcul global, import massif), copier `app.db` avec `sauvegarder()` avant d'agir. En cas
de problème, `restaurer()` remet la copie en place — jamais automatique, toujours un appel
explicite et journalisé.

INDÉPENDANT DU SCHÉMA DE LA SOURCE (mission 14 — activation réelle contrôlée)
La sauvegarde de bascule PRE_REAL_CUTOVER doit pouvoir être prise AVANT la migration de schéma
qu'elle protège — donc sur une base dont les tables `sauvegardes_base`/`sauvegardes_base_tracabilite`
elles-mêmes n'existent pas encore (migrations 0057/0061). Migrer la base juste pour pouvoir la
sauvegarder inverserait l'ordre de protection. La traçabilité est donc TOUJOURS écrite dans un
fichier sidecar JSON à côté de la copie physique (`<backup>.meta.json`, lisible quel que soit le
schéma) ; l'écriture en base (`sauvegardes_base`/`sauvegardes_base_tracabilite`, pour l'écran
applicatif) reste faite EN PLUS, seulement quand ces tables existent déjà dans la source.
"""
from __future__ import annotations

import hashlib
import json
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


def _schema_version(source: Path) -> str | None:
    conn = sqlite3.connect(str(source))
    try:
        row = conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def _table_existe(conn: sqlite3.Connection, nom: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone() is not None


def _table_existe_sans_modifier(chemin: Path, nom: str) -> bool:
    """Vérifie l'existence d'une table SANS jamais passer par `get_db()` : `get_db()` force
    `PRAGMA journal_mode=WAL` à la connexion, ce qui RÉÉCRIT l'en-tête d'une base qui n'était pas
    encore en WAL — un simple contrôle ne doit jamais modifier le fichier qu'il inspecte,
    a fortiori quand ce fichier est la SOURCE d'une sauvegarde de bascule (mission 14 §1/§3)."""
    try:
        conn = sqlite3.connect(str(chemin))
        try:
            return _table_existe(conn, nom)
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return False


def _sidecar_path(dest: Path) -> Path:
    return dest.with_suffix(dest.suffix + ".meta.json")


def _ecrire_sidecar(dest: Path, meta: dict[str, Any]) -> None:
    _sidecar_path(dest).write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")


def _lire_sidecar(chemin_backup: Path) -> dict[str, Any] | None:
    sidecar = _sidecar_path(chemin_backup)
    if not sidecar.exists():
        return None
    try:
        return json.loads(sidecar.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _trouver_sidecar_par_id(sauvegarde_id: str) -> dict[str, Any] | None:
    """Retrouve les métadonnées d'une sauvegarde par son identifiant, sans dépendre d'aucune table
    (utilisé quand `sauvegardes_base` n'existe pas encore dans le schéma courant, cf. docstring
    module)."""
    dossier = Path(cfg.BACKUPS_DIR)
    if not dossier.exists():
        return None
    for sidecar in dossier.glob("*.meta.json"):
        try:
            meta = json.loads(sidecar.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if meta.get("sauvegarde_id") == sauvegarde_id:
            return meta
    return None


def sauvegarder(operation: str, *, db_path: Path | None = None) -> dict[str, Any]:
    """Copie `app.db` (ou `db_path`) sous `BACKUPS_DIR` via l'API officielle SQLite, vérifie son
    intégrité, journalise (mission 14 — activation réelle contrôlée).

    Utilise `sqlite3.Connection.backup()` (déjà le patron employé ailleurs dans ce projet, cf.
    `menages_recalcul_service.py`, `controles_runner_service.py`) plutôt qu'un `PRAGMA
    wal_checkpoint(FULL)` + `shutil.copy2` : l'API de backup lit un état cohérent directement
    depuis la connexion source, y compris les écritures encore uniquement dans le fichier `-wal`
    (actif partout dans ce projet, cf. `get_db`), sans fenêtre de course entre un checkpoint et une
    copie de fichier séparée.
    """
    source = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if not source.exists():
        return {"ok": False, "code": "E_SOURCE_ABSENTE", "message": f"Base absente : {source}."}

    source_hash = _sha256(source)
    schema_version = _schema_version(source)

    dossier = Path(cfg.BACKUPS_DIR)
    dossier.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sid = "BCK-" + uuid.uuid4().hex[:12].upper()
    dest = dossier / f"{ts}_{operation}_{sid}.db"

    src_conn = sqlite3.connect(str(source))
    try:
        dst_conn = sqlite3.connect(str(dest))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()

    ok = _integrity_ok(dest)
    statut = "VALIDE" if ok else "CORROMPU"
    database_hash = _sha256(dest)
    date_creation = _maintenant()
    git_commit = _git_commit()
    taille = dest.stat().st_size

    # Traçabilité TOUJOURS écrite en sidecar : seule source garantie indépendante du schéma de la
    # base sauvegardée (cf. docstring module — cas PRE_REAL_CUTOVER sur schéma pré-migration).
    meta = {"sauvegarde_id": sid, "operation": operation, "chemin": str(dest),
            "git_commit": git_commit, "database_hash": database_hash, "source_hash": source_hash,
            "schema_version": schema_version, "taille_octets": taille,
            "validation_status": statut, "date_creation": date_creation}
    _ecrire_sidecar(dest, meta)

    # Journalisation en base EN PLUS, uniquement si le schéma la supporte déjà (opportuniste,
    # jamais bloquant : une base pré-migration n'a pas encore ces tables). Existence vérifiée SANS
    # `get_db()` d'abord : `get_db()` force `PRAGMA journal_mode=WAL`, ce qui réécrirait la SOURCE
    # rien que pour un contrôle si elle n'était pas déjà en WAL — inacceptable pour un backup de
    # bascule qui doit laisser la source strictement intacte.
    if _table_existe_sans_modifier(source, "sauvegardes_base"):
        conn = get_db(db_path)
        try:
            conn.execute(
                "INSERT INTO sauvegardes_base (sauvegarde_id_opaque, operation, chemin_fichier, "
                "git_commit, database_hash, taille_octets, validation_status) "
                "VALUES (?,?,?,?,?,?,?)",
                (sid, operation, str(dest), git_commit, database_hash, taille, statut))
            if _table_existe(conn, "sauvegardes_base_tracabilite"):
                conn.execute(
                    "INSERT INTO sauvegardes_base_tracabilite (sauvegarde_id_opaque, source_hash, "
                    "schema_version) VALUES (?,?,?)", (sid, source_hash, schema_version))
            conn.commit()
        finally:
            conn.close()

    return {"ok": ok, "sauvegarde_id": sid, "chemin": str(dest), "validation_status": statut,
            "source_hash": source_hash, "database_hash": database_hash,
            "schema_version": schema_version}


def verifier(sauvegarde_id: str, *, db_path: Path | None = None) -> dict[str, Any]:
    """Revérifie l'intégrité et le hash d'une sauvegarde existante ; met à jour son statut.

    Cherche d'abord en base (écran applicatif) ; si la table n'existe pas encore (schéma
    pré-migration) ou que la ligne est introuvable, retombe sur le sidecar JSON — toujours écrit
    par `sauvegarder()`, quel que soit le schéma de la source au moment de la sauvegarde.
    """
    cible_verif = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    row = None
    if _table_existe_sans_modifier(cible_verif, "sauvegardes_base"):
        conn = get_db(db_path)
        try:
            fetched = conn.execute(
                "SELECT * FROM sauvegardes_base WHERE sauvegarde_id_opaque=?",
                (sauvegarde_id,)).fetchone()
            row = dict(fetched) if fetched else None
        finally:
            conn.close()

    via_sidecar = row is None
    if row is None:
        row = _trouver_sidecar_par_id(sauvegarde_id)
        if row is None:
            return {"ok": False, "code": "E_INTROUVABLE", "message": "Sauvegarde introuvable."}
        row = {"chemin_fichier": row["chemin"], "database_hash": row["database_hash"]}

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

    if not via_sidecar:
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
    cible = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if not _table_existe_sans_modifier(cible, "sauvegardes_base"):
        return []
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT b.*, t.source_hash, t.schema_version FROM sauvegardes_base b "
            "LEFT JOIN sauvegardes_base_tracabilite t "
            "ON t.sauvegarde_id_opaque = b.sauvegarde_id_opaque "
            "ORDER BY b.date_creation DESC").fetchall()
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

    cible_verif = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    chemin_fichier = None
    if _table_existe_sans_modifier(cible_verif, "sauvegardes_base"):
        conn = get_db(db_path)
        try:
            row = conn.execute(
                "SELECT chemin_fichier FROM sauvegardes_base WHERE sauvegarde_id_opaque=?",
                (sauvegarde_id,)).fetchone()
            if row:
                chemin_fichier = row["chemin_fichier"]
        finally:
            conn.close()

    if chemin_fichier is None:
        meta = _trouver_sidecar_par_id(sauvegarde_id)
        if meta is None:
            return {"ok": False, "code": "E_INTROUVABLE", "message": "Sauvegarde introuvable."}
        chemin_fichier = meta["chemin"]

    cible_path = Path(cible) if cible is not None else Path(cfg.DB_PATH)
    shutil.copy2(Path(chemin_fichier), cible_path)

    if not _integrity_ok(cible_path):
        return {"ok": False, "code": "E_RESTAURATION_CORROMPUE",
                "message": "La restauration a produit un fichier illisible."}

    return {"ok": True, "sauvegarde_id": sauvegarde_id, "cible": str(cible_path)}


def purger(*, garder_n: int = 10, db_path: Path | None = None) -> dict[str, Any]:
    """Supprime les sauvegardes physiques au-delà des `garder_n` plus récentes VALIDÉES.

    Jamais automatique : appelé uniquement à la demande explicite (mission §2 : « aucune
    suppression automatique sans règle claire »). Ne supprime jamais une sauvegarde CORROMPUE
    sans l'avoir listée — elle reste la seule preuve du problème tant qu'elle n'est pas nettoyée
    à part. Nécessite le schéma courant (`sauvegardes_base`) : action d'exploitation post-bascule,
    jamais utilisée avant la migration elle-même.
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
            sidecar = _sidecar_path(p)
            if sidecar.exists():
                sidecar.unlink()
            conn.execute("DELETE FROM sauvegardes_base_tracabilite WHERE sauvegarde_id_opaque=?",
                        (r["sauvegarde_id_opaque"],))
            conn.execute("DELETE FROM sauvegardes_base WHERE id=?", (r["id"],))
            supprimes.append(r["sauvegarde_id_opaque"])
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "supprimees": supprimes, "conservees": min(len(rows), garder_n)}

"""Orchestrateur d'écriture SAISIE HH — guard, snapshot, SQLite, rollback (APP-2b).

Ce module ne fait aucune opération openpyxl.
Le writer (saisie_hh_writer.write_row) reçoit row_data et fait les opérations fichier.
L'orchestrateur gère tout ce qui entoure : guard, snapshot, lock SQLite, rollback.

Flux confirm_write :
1. Garde HH_REAL_WRITE_ENABLED → log GARDE_SECURITE si False.
2. assert_writable.
3. sha256 avant.
4. Snapshot (audit SQLite).
5. Copie rollback (même dossier = même volume → atomicité garantie).
6. Détection première ligne vide.
7. Appel write_row (opérations fichier pures).
8. Log SQLite OK + audit_event.
9. Si log échoue après write réussi → rollback atomique + log ROLLBACK_*.
10. Nettoyage copie rollback.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers.saisie_hh_reader import find_first_empty_data_row
from app.services.audit_service import log_event
from app.services.file_registry import assert_writable
from app.services.snapshot_service import create_snapshot
from app.writers import saisie_hh_writer as _writer

_ROLLBACK_SUFFIX = ".rollback.xlsx"


def _sha256(path: Path) -> str:
    return _writer._sha256(path)


def _log_write(
    db_path: Path,
    pk: str,
    mois: str,
    snapshot_id: int | None,
    sha256_avant: str | None,
    sha256_apres: str | None,
    ligne_cible: int | None,
    statut: str,
    details: str | None = None,
) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            """INSERT INTO saisie_hh_writes
               (pk, mois, snapshot_id, sha256_avant, sha256_apres, ligne_cible, statut, details)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (pk, mois, snapshot_id, sha256_avant, sha256_apres, ligne_cible, statut, details),
        )
        conn.commit()
    finally:
        conn.close()


def _log_write_safe(**kwargs: Any) -> None:
    try:
        _log_write(**kwargs)
    except Exception:
        pass


def _cleanup_rollback(path: Path, created: bool) -> None:
    if created:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass


def _do_rollback(
    saisie_path: Path,
    rollback_path: Path,
    sha256_avant: str,
    rollback_created: bool,
) -> tuple[str, str]:
    if not rollback_created or not rollback_path.exists():
        return "ROLLBACK_ECHEC", "copie rollback absente"
    try:
        os.replace(str(rollback_path), str(saisie_path))
        sha_restored = _sha256(saisie_path)
        if sha_restored == sha256_avant:
            return "ROLLBACK_REUSSI", f"sha256 restauré = {sha_restored[:12]}…"
        return "ROLLBACK_ECHEC", (
            f"sha256 post-rollback {sha_restored[:12]}… ≠ attendu {sha256_avant[:12]}…"
        )
    except Exception as exc:
        return "ROLLBACK_ECHEC", f"os.replace rollback échoué : {exc}"


def confirm_write(
    row_data: dict[str, Any],
    pk: str,
    mois: str,
    saisie_path: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """
    Orchestre l'écriture contrôlée d'une réservation HH.

    Retourne :
        {"statut": "GARDE_SECURITE"|"OK"|"ERREUR",
         "pk", "mois", "ligne_cible", "details"}
    """
    p_saisie = saisie_path or cfg.SAISIE_RESERVATIONS_HH
    p_db = db_path or cfg.DB_PATH

    # ── 1. Garde ──────────────────────────────────────────────────────────
    if not cfg.HH_REAL_WRITE_ENABLED:
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=None, sha256_avant=None, sha256_apres=None,
            ligne_cible=None, statut="GARDE_SECURITE",
            details="HH_REAL_WRITE_ENABLED = False",
        )
        log_event(
            "SAISIE_HH_GARDE_SECURITE",
            details={"pk": pk, "mois": mois, "raison": "HH_REAL_WRITE_ENABLED = False"},
            db_path=p_db,
        )
        return {
            "statut": "GARDE_SECURITE", "pk": pk, "mois": mois,
            "ligne_cible": None,
            "details": "HH_REAL_WRITE_ENABLED = False — aucune écriture effectuée",
        }

    # ── 2. assert_writable ────────────────────────────────────────────────
    try:
        assert_writable(p_saisie)
    except PermissionError as exc:
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=None, sha256_avant=None, sha256_apres=None,
            ligne_cible=None, statut="ERREUR", details=f"assert_writable : {exc}",
        )
        return {"statut": "ERREUR", "pk": pk, "mois": mois,
                "ligne_cible": None, "details": f"Chemin non autorisé : {exc}"}

    # ── 3. sha256 avant ───────────────────────────────────────────────────
    try:
        sha256_avant = _sha256(p_saisie)
    except Exception as exc:
        return {"statut": "ERREUR", "pk": pk, "mois": mois,
                "ligne_cible": None, "details": f"sha256 avant illisible : {exc}"}

    # ── 4. Snapshot ───────────────────────────────────────────────────────
    snapshot_id: int | None = None
    try:
        snap = create_snapshot("AVANT_SAISIE_HH", [p_saisie], db_path=p_db)
        snapshot_id = snap.get("id")
    except Exception as exc:
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=None, sha256_avant=sha256_avant, sha256_apres=None,
            ligne_cible=None, statut="ERREUR", details=f"snapshot : {exc}",
        )
        return {"statut": "ERREUR", "pk": pk, "mois": mois,
                "ligne_cible": None, "details": f"Snapshot impossible : {exc}"}

    # ── 5. Copie rollback ─────────────────────────────────────────────────
    rollback_path = p_saisie.parent / (p_saisie.stem + _ROLLBACK_SUFFIX)
    rollback_created = False
    try:
        shutil.copy2(str(p_saisie), str(rollback_path))
        rollback_created = True
    except Exception as exc:
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=snapshot_id, sha256_avant=sha256_avant, sha256_apres=None,
            ligne_cible=None, statut="ERREUR", details=f"copie rollback : {exc}",
        )
        return {"statut": "ERREUR", "pk": pk, "mois": mois,
                "ligne_cible": None, "details": f"Copie rollback impossible : {exc}"}

    # ── 6. Première ligne vide ────────────────────────────────────────────
    target_row = find_first_empty_data_row(p_saisie)
    if target_row is None:
        _cleanup_rollback(rollback_path, rollback_created)
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=snapshot_id, sha256_avant=sha256_avant, sha256_apres=None,
            ligne_cible=None, statut="ERREUR",
            details="Aucune ligne vide disponible (max 500 ?)",
        )
        return {"statut": "ERREUR", "pk": pk, "mois": mois,
                "ligne_cible": None, "details": "Aucune ligne vide disponible (max 500 ?)"}

    # ── 7. Écriture fichier (writer pur) ──────────────────────────────────
    write_result = _writer.write_row(row_data, p_saisie, target_row)

    if write_result["statut"] != "OK":
        _cleanup_rollback(rollback_path, rollback_created)
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=snapshot_id, sha256_avant=sha256_avant, sha256_apres=None,
            ligne_cible=target_row, statut="ERREUR", details=write_result.get("details"),
        )
        return {
            "statut": "ERREUR", "pk": pk, "mois": mois,
            "ligne_cible": target_row, "details": write_result.get("details"),
        }

    sha256_apres = write_result.get("sha256_apres")

    post_replace_v = _writer.check_post_replace_integrity(
        rollback_path, p_saisie, target_row
    )
    if post_replace_v:
        rb_statut, rb_details = _do_rollback(
            p_saisie, rollback_path, sha256_avant, rollback_created
        )
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=snapshot_id, sha256_avant=sha256_avant, sha256_apres=sha256_apres,
            ligne_cible=target_row, statut=rb_statut,
            details="post_replace=" + "; ".join(post_replace_v[:5]) + f" ; {rb_details}",
        )
        return {
            "statut": "ERREUR", "pk": pk, "mois": mois,
            "ligne_cible": target_row,
            "details": "Anomalie post-remplacement ; " + rb_statut,
        }

    # ── 8. Journalisation OK ──────────────────────────────────────────────
    try:
        _log_write(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=snapshot_id,
            sha256_avant=sha256_avant, sha256_apres=sha256_apres,
            ligne_cible=target_row, statut="OK",
        )
        log_event(
            "SAISIE_HH_ECRIT",
            details={"pk": pk, "mois": mois, "ligne": target_row,
                     "sha256_avant": sha256_avant, "sha256_apres": sha256_apres},
            db_path=p_db,
        )
        _cleanup_rollback(rollback_path, rollback_created)
        return {
            "statut": "OK", "pk": pk, "mois": mois,
            "ligne_cible": target_row, "details": None,
        }

    except Exception as log_exc:
        # ── 9. Rollback ───────────────────────────────────────────────────
        rb_statut, rb_details = _do_rollback(
            p_saisie, rollback_path, sha256_avant, rollback_created
        )
        _log_write_safe(
            db_path=p_db, pk=pk, mois=mois,
            snapshot_id=snapshot_id, sha256_avant=sha256_avant, sha256_apres=None,
            ligne_cible=target_row, statut=rb_statut,
            details=f"log_exc={log_exc} ; {rb_details}",
        )
        return {
            "statut": "ERREUR", "pk": pk, "mois": mois,
            "ligne_cible": target_row,
            "details": f"Journalisation échouée : {log_exc} ; {rb_statut}",
        }

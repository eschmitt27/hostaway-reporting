"""Couche RAW Hostaway CleaningTasks (Lot6a) — ce que /v1/tasks a retourné, tel quel, en SQLite.

    API Hostaway /v1/tasks → hostaway_cleaning_tasks_extractions + hostaway_cleaning_tasks

Même principe que `hostaway_raw_service` (migration 0034) : aucune interprétation métier ici — pas
de rattachement logement, pas de mois, pas de statut_menage. Ça reste le travail de Lot6a en aval
(résolution `REF_Logements`, `type_ligne_menage`, contrôles). H6 est irrévocable : `cost` n'est
jamais lu depuis l'API dans cette couche (comptage uniquement, jamais valorisation).

Registre d'extraction séparé de `hostaway_extractions` : endpoint différent, segmentation par
listing différente, échecs indépendants (cf D065 — un segment >= 500 tâches est un plafond probable,
signalé par `segments_plafonnes`, sans faire échouer les autres segments).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from app.db.connection import get_db

MODE_API = "API"
MODE_FIXTURE = "FIXTURE"
MODE_REPRISE_EXCEL = "REPRISE_EXCEL"

ST_EN_COURS = "EN_COURS"
ST_SUCCES = "SUCCES"
ST_PARTIEL = "PARTIEL"
ST_ECHEC = "ECHEC"


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_presente(nom: str, *, db_path=None) -> bool:
    """Les tables CleaningTasks n'existent qu'à partir de la migration 0035 — absence = état
    légitime, pas une erreur (base pas encore migrée)."""
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nom,)
        ).fetchone() is not None
    finally:
        conn.close()


def ouvrir(*, mode: str = MODE_API, run_id: str = "", db_path=None) -> str:
    if not table_presente("hostaway_cleaning_tasks_extractions", db_path=db_path):
        return ""
    extraction_id = "HCT-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO hostaway_cleaning_tasks_extractions "
            "(extraction_id, run_id, mode, date_debut, statut) VALUES (?,?,?,?,?)",
            (extraction_id, run_id or None, mode, _maintenant(), ST_EN_COURS))
        conn.commit()
    finally:
        conn.close()
    return extraction_id


def cloturer(extraction_id: str, *, statut: str, message: str = "",
            segments_plafonnes: Iterable[Any] = (), db_path=None) -> dict[str, Any]:
    if not extraction_id:
        return {}
    conn = get_db(db_path)
    try:
        nb_taches = conn.execute(
            "SELECT COUNT(*) FROM hostaway_cleaning_tasks WHERE extraction_id = ?",
            (extraction_id,)).fetchone()[0]
        conn.execute(
            "UPDATE hostaway_cleaning_tasks_extractions SET date_fin = ?, statut = ?, message = ?, "
            "nb_taches = ?, segments_plafonnes = ? WHERE extraction_id = ?",
            (_maintenant(), statut, message or None, nb_taches,
             ",".join(str(x) for x in segments_plafonnes) or None, extraction_id))
        conn.commit()
    finally:
        conn.close()
    return {"extraction_id": extraction_id, "statut": statut, "nb_taches": nb_taches}


_COLS = ("task_id", "reservation_id", "listing_map_id", "title", "status", "can_start_from",
         "assignee_user_id", "extrait_le", "row_hash")
_ALIAS = {"task_id": "id", "reservation_id": "reservationId", "listing_map_id": "listingMapId",
          "can_start_from": "canStartFrom", "assignee_user_id": "assigneeUserId",
          "row_hash": "ROW_HASH"}


def _valeur(ligne: dict[str, Any], colonne: str) -> Any:
    if colonne in ligne:
        return ligne[colonne]
    return ligne.get(_ALIAS.get(colonne, colonne))


def enregistrer(extraction_id: str, *, taches: Iterable[dict] = (), db_path=None) -> int:
    """Écrit les tâches d'une extraction. `task_id` n'est pas contraint unique ici : la
    déduplication par segment (D065) a déjà eu lieu côté Lot6a avant l'appel."""
    if not extraction_id:
        return 0
    taches = list(taches)
    if not taches:
        return 0
    conn = get_db(db_path)
    try:
        trous = ", ".join(["?"] * (len(_COLS) + 1))
        conn.executemany(
            f"INSERT INTO hostaway_cleaning_tasks (extraction_id, {', '.join(_COLS)}) "
            f"VALUES ({trous})",
            [(extraction_id, *(_valeur(t, c) for c in _COLS)) for t in taches])
        conn.commit()
    finally:
        conn.close()
    return len(taches)


def derniere_extraction_utilisable(*, db_path=None) -> str:
    if not table_presente("hostaway_cleaning_tasks_extractions", db_path=db_path):
        return ""
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT extraction_id FROM hostaway_cleaning_tasks_extractions "
            "WHERE statut IN (?, ?) ORDER BY date_debut DESC LIMIT 1",
            (ST_SUCCES, ST_PARTIEL)).fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def taches(*, extraction_id: str = "", db_path=None) -> list[dict[str, Any]]:
    """Tâches d'une extraction, ordre d'insertion. Par défaut la dernière utilisable."""
    eid = extraction_id or derniere_extraction_utilisable(db_path=db_path)
    if not eid or not table_presente("hostaway_cleaning_tasks", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        colonnes = ("task_id", "reservation_id", "listing_map_id", "title", "status",
                    "can_start_from", "assignee_user_id")
        return [dict(zip(colonnes, r)) for r in conn.execute(
            f"SELECT {', '.join(colonnes)} FROM hostaway_cleaning_tasks "
            "WHERE extraction_id = ? ORDER BY id", (eid,))]
    finally:
        conn.close()


def fraicheur(*, db_path=None) -> dict[str, Any] | None:
    if not table_presente("hostaway_cleaning_tasks_extractions", db_path=db_path):
        return None
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT extraction_id, mode, statut, date_debut, date_fin, nb_taches "
            "FROM hostaway_cleaning_tasks_extractions ORDER BY date_debut DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        colonnes = ("extraction_id", "mode", "statut", "date_debut", "date_fin", "nb_taches")
        return dict(zip(colonnes, row))
    finally:
        conn.close()

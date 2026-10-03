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

POLITIQUE DE SAUVEGARDE (mission « Fiabiliser les sauvegardes », 2026-10-03)
Le CATALOGUE qui fait foi est l'ensemble des sidecars `*.meta.json` sous `BACKUPS_DIR` — pas la
table `sauvegardes_base`. Cette table vit dans la base qu'elle décrit : une restauration la remplace
par celle de la copie, et elle ignore donc toutes les sauvegardes postérieures. Les sidecars, eux,
survivent à n'importe quelle restauration.

  · Catégories : DAILY (quotidienne), BEFORE_MIGRATION, BEFORE_GLOBAL_REFRESH, MANUAL, ARCHIVE.
    WEEKLY et MONTHLY ne sont PAS des fichiers : ce sont des RÔLES de rétention tenus par une
    sauvegarde quotidienne (la dernière de la semaine / du mois). Aucune copie de 70 Mo n'est
    refaite pour « promouvoir » une quotidienne.
  · Protection : `protegee=true` dans le sidecar, catégorie ARCHIVE, ou fichier rangé dans un dossier
    `archive_*` → jamais supprimée par la rotation, quelles que soient ses dates.
  · Sauvegarde VALIDE = copie ouverte, `integrity_check` = ok, et `foreign_key_check` identique à
    celui de la source au moment de la copie (une copie fidèle ne répare ni n'invente rien).
  · Rotation (`purger`) : seulement les sauvegardes créées par cette politique (`politique` dans le
    sidecar). Les sauvegardes HÉRITÉES (antérieures) sont listées mais ne sont supprimées que sur
    demande explicite (`inclure_heritees=True` + `confirmer=True`). Jamais supprimées : une
    protégée, une sauvegarde non valide (preuve d'un incident), la dernière sauvegarde saine, les
    sauvegardes avant migration récentes.
  · Concurrence : un seul `sauvegarder()` à la fois (verrou de processus + fichier verrou dans
    `BACKUPS_DIR` pour les outils lancés à côté) ; attente bornée, puis refus propre.
  · Destination secondaire : `BACKUP_SECONDARY_DIR`. Absente → rien ne change. Une erreur sur la
    copie secondaire est tracée et ne touche jamais la copie locale.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db

# ── Catégories ─────────────────────────────────────────────────────────────────────────────────
CAT_QUOTIDIENNE = "DAILY"
CAT_HEBDOMADAIRE = "WEEKLY"          # rôle de rétention d'une DAILY, jamais un fichier distinct
CAT_MENSUELLE = "MONTHLY"            # idem
CAT_AVANT_MIGRATION = "BEFORE_MIGRATION"
CAT_AVANT_ACTUALISATION = "BEFORE_GLOBAL_REFRESH"
CAT_MANUELLE = "MANUAL"
CAT_ARCHIVE = "ARCHIVE"
CATEGORIES = (CAT_QUOTIDIENNE, CAT_AVANT_MIGRATION, CAT_AVANT_ACTUALISATION, CAT_MANUELLE,
              CAT_ARCHIVE)

OPERATION_QUOTIDIENNE = "SAUVEGARDE_QUOTIDIENNE"
OPERATION_ACTUALISATION_GLOBALE = "ACTUALISATION_GLOBALE"
OPERATION_MIGRATION = "MIGRATION"
# Opération `run_history` de chaque sauvegarde (la catégorie est portée par `acteur`).
OPERATION_HISTORIQUE = "SAUVEGARDE"

#: Marque des sauvegardes gérées par la politique de rotation. Une sauvegarde sans cette marque est
#: HÉRITÉE : elle n'est jamais supprimée automatiquement.
POLITIQUE = "2026-10-03"

ST_VALIDE = "VALIDE"
ST_CORROMPU = "CORROMPU"

E_SAUVEGARDE_EN_COURS = "E_SAUVEGARDE_EN_COURS"
E_SAUVEGARDE_ECHOUEE = "E_SAUVEGARDE_ECHOUEE"

NOM_VERROU = ".sauvegarde.lock"
PREFIXE_DOSSIER_ARCHIVE = "archive_"
SUFFIXE_SIDECAR = ".meta.json"
# Au-delà de ce nombre d'échecs le même jour, la quotidienne n'est plus retentée avant le lendemain :
# une panne durable ne doit pas produire une copie illisible tous les quarts d'heure.
ESSAIS_QUOTIDIENS_MAX = 3


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


def _nb_violations_fk(source: Path) -> int | None:
    """`foreign_key_check` de la SOURCE, en lecture seule. `None` si illisible."""
    try:
        conn = sqlite3.connect(str(source))
    except sqlite3.DatabaseError:
        return None
    try:
        conn.execute("PRAGMA query_only=ON")
        return len(conn.execute("PRAGMA foreign_key_check").fetchall())
    except sqlite3.DatabaseError:
        return None
    finally:
        conn.close()


def inspecter(chemin: Path) -> dict[str, Any]:
    """Ouvre une base SANS l'écrire et relève ce qui prouve qu'elle est saine et complète :
    ouverture, `integrity_check`, `foreign_key_check`, tables, lignes, version de schéma."""
    chemin = Path(chemin)
    if not chemin.exists():
        return {"lisible": False, "erreur": "Fichier absent."}
    try:
        conn = sqlite3.connect(str(chemin))
    except sqlite3.DatabaseError as exc:
        return {"lisible": False, "erreur": type(exc).__name__}
    try:
        conn.execute("PRAGMA query_only=ON")
        integrite = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk = len(conn.execute("PRAGMA foreign_key_check").fetchall())
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name")]
        lignes = sum(conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables)
        try:
            row = conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").fetchone()
            version = row[0] if row else None
        except sqlite3.OperationalError:
            version = None
        return {"lisible": True, "integrity_check": integrite, "foreign_key_check": fk,
                "nb_tables": len(tables), "nb_lignes": lignes, "schema_version": version}
    except sqlite3.DatabaseError as exc:
        return {"lisible": False, "erreur": type(exc).__name__}
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
    return dest.with_suffix(dest.suffix + SUFFIXE_SIDECAR)


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
    module). Le chemin renvoyé est celui où le sidecar se trouve RÉELLEMENT."""
    for e in catalogue():
        if e["sauvegarde_id"] == sauvegarde_id:
            return {**e["meta"], "chemin": str(e["fichier"])}
    return None


# ── Catégories et catalogue ────────────────────────────────────────────────────────────────────

def categorie_pour(operation: str) -> str:
    """Catégorie normalisée d'une opération — y compris pour les sauvegardes héritées, dont le
    sidecar ne portait que le nom libre de l'opération."""
    op = (operation or "").upper()
    if op == OPERATION_QUOTIDIENNE:
        return CAT_QUOTIDIENNE
    if op == OPERATION_ACTUALISATION_GLOBALE:
        return CAT_AVANT_ACTUALISATION
    if "MIGRATION" in op:
        return CAT_AVANT_MIGRATION
    return CAT_MANUELLE


def _parse_utc(valeur: str | None) -> datetime | None:
    if not valeur:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            d = datetime.strptime(valeur, fmt)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        d = datetime.fromisoformat(valeur)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _local(d: datetime) -> datetime:
    """Heure locale de la machine, naïve — celle dans laquelle « aujourd'hui » et « 03:00 » ont un
    sens pour l'utilisateur."""
    return d.astimezone().replace(tzinfo=None)


def _dans_dossier_archive(fichier: Path, racine: Path) -> bool:
    try:
        parties = fichier.relative_to(racine).parts[:-1]
    except ValueError:
        parties = fichier.parts[:-1]
    return any(p.startswith(PREFIXE_DOSSIER_ARCHIVE) for p in parties)


def catalogue(*, dossier: Path | None = None) -> list[dict[str, Any]]:
    """Toutes les sauvegardes connues sous `BACKUPS_DIR` (sidecars), les plus récentes d'abord.

    Un fichier `.db` sans sidecar n'y figure pas : il n'a aucune métadonnée prouvant ce qu'il
    contient, et la rotation ne le touche donc jamais."""
    racine = Path(dossier or cfg.BACKUPS_DIR)
    if not racine.exists():
        return []
    entrees = []
    for sidecar in racine.rglob("*" + SUFFIXE_SIDECAR):
        try:
            meta = json.loads(sidecar.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(meta, dict) or not meta.get("sauvegarde_id"):
            continue
        fichier = sidecar.with_name(sidecar.name[:-len(SUFFIXE_SIDECAR)])
        categorie = meta.get("categorie") or categorie_pour(meta.get("operation", ""))
        date_utc = _parse_utc(meta.get("date_creation"))
        if date_utc is None:
            date_utc = datetime.fromtimestamp(sidecar.stat().st_mtime, tz=timezone.utc)
        protegee = (bool(meta.get("protegee")) or categorie == CAT_ARCHIVE
                    or _dans_dossier_archive(fichier, racine))
        existe = fichier.exists()
        entrees.append({
            "sauvegarde_id": meta["sauvegarde_id"],
            "fichier": fichier,
            "nom": fichier.name,
            "operation": meta.get("operation"),
            "categorie": categorie,
            "date_utc": date_utc,
            "date_locale": _local(date_utc),
            "statut": meta.get("validation_status") if existe else ST_CORROMPU,
            "protegee": protegee,
            "geree": meta.get("politique") is not None,
            "taille": meta.get("taille_octets") or (fichier.stat().st_size if existe else 0),
            "database_hash": meta.get("database_hash"),
            "schema_version": meta.get("schema_version"),
            "existe": existe,
            "meta": meta,
            # Départage de deux sauvegardes de la même seconde (dates à la seconde) : l'instant
            # d'écriture de la copie, puis du sidecar.
            "_ordre": (fichier.stat().st_mtime_ns if existe else 0, sidecar.stat().st_mtime_ns),
        })
    entrees.sort(key=lambda e: (e["date_utc"], e["_ordre"]), reverse=True)
    return entrees


# ── Verrou : une seule sauvegarde à la fois ────────────────────────────────────────────────────

_verrou_processus = threading.Lock()


def _chemin_verrou() -> Path:
    return Path(cfg.BACKUPS_DIR) / NOM_VERROU


def _verrou_perime(chemin: Path) -> bool:
    try:
        age = time.time() - chemin.stat().st_mtime
    except OSError:
        return True
    return age > cfg.BACKUP_LOCK_STALE_SECONDS


def sauvegarde_en_cours() -> bool:
    chemin = _chemin_verrou()
    return chemin.exists() and not _verrou_perime(chemin)


def _prendre_verrou(attente_s: float) -> Path | None:
    """Verrou de processus PUIS fichier verrou (création exclusive) — ce dernier couvre un outil
    lancé à côté de l'application. Attente BORNÉE : jamais d'attente infinie. `None` = refus."""
    fin = time.monotonic() + max(attente_s, 0)
    if attente_s > 0:
        if not _verrou_processus.acquire(timeout=attente_s):
            return None
    elif not _verrou_processus.acquire(blocking=False):
        return None
    chemin = _chemin_verrou()
    chemin.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            fd = os.open(str(chemin), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps({"pid": os.getpid(), "depuis": _maintenant()}))
            return chemin
        except FileExistsError:
            if _verrou_perime(chemin):
                try:
                    chemin.unlink()
                except OSError:
                    pass
                continue
            if time.monotonic() >= fin:
                _verrou_processus.release()
                return None
            time.sleep(0.2)


def _liberer_verrou(chemin: Path) -> None:
    try:
        chemin.unlink()
    except OSError:
        pass
    finally:
        _verrou_processus.release()


# ── Journalisation ─────────────────────────────────────────────────────────────────────────────

def _journaliser_run(source: Path, db_path: Path | None, *, sauvegarde_id: str | None,
                     categorie: str, statut: str, debut: str, fin: str, duree_s: float,
                     erreur: str = "") -> None:
    """Une ligne `run_history` par sauvegarde (observabilité), écrite APRÈS la copie : la copie
    ne contient donc jamais un run « en cours » qui n'aurait pas de fin."""
    if not _table_existe_sans_modifier(source, "run_history"):
        return
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO run_history (run_id_opaque, operation, statut, sauvegarde_id_opaque, "
            "duree_s, erreur, acteur, date_debut, date_fin) VALUES (?,?,?,?,?,?,?,?,?)",
            ("RUNH-" + uuid.uuid4().hex[:12].upper(), OPERATION_HISTORIQUE, statut,
             sauvegarde_id, round(duree_s, 3), erreur or None, categorie, debut, fin))
        conn.commit()
    finally:
        conn.close()


# ── Sauvegarde ─────────────────────────────────────────────────────────────────────────────────

_RAISONS = {CAT_AVANT_MIGRATION: "AVANT_MIGRATION",
            CAT_AVANT_ACTUALISATION: "AVANT_ACTUALISATION_GLOBALE",
            CAT_QUOTIDIENNE: "QUOTIDIENNE", CAT_MANUELLE: "MANUELLE", CAT_ARCHIVE: "ARCHIVE"}


def sauvegarder(operation: str, *, db_path: Path | None = None, categorie: str | None = None,
                protegee: bool = False, raison: str | None = None,
                migration_cible: str | None = None, attente_s: float = 60.0) -> dict[str, Any]:
    """Copie `app.db` (ou `db_path`) sous `BACKUPS_DIR` via l'API officielle SQLite, vérifie son
    intégrité, journalise (mission 14 — activation réelle contrôlée).

    Utilise `sqlite3.Connection.backup()` (déjà le patron employé ailleurs dans ce projet, cf.
    `menages_recalcul_service.py`, `controles_runner_service.py`) plutôt qu'un `PRAGMA
    wal_checkpoint(FULL)` + `shutil.copy2` : l'API de backup lit un état cohérent directement
    depuis la connexion source, y compris les écritures encore uniquement dans le fichier `-wal`
    (actif partout dans ce projet, cf. `get_db`), sans fenêtre de course entre un checkpoint et une
    copie de fichier séparée.

    La copie est écrite sous un nom provisoire puis renommée : un fichier `.db` visible dans le
    dossier est toujours une copie complète. Elle est passée en `journal_mode=DELETE` : un seul
    fichier autonome, dont l'empreinte ne bouge plus.
    """
    source = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if not source.exists():
        return {"ok": False, "code": "E_SOURCE_ABSENTE", "message": f"Base absente : {source}."}
    categorie = categorie or categorie_pour(operation)
    verrou = _prendre_verrou(attente_s)
    if verrou is None:
        return {"ok": False, "code": E_SAUVEGARDE_EN_COURS,
                "message": "Une sauvegarde est déjà en cours : nouvelle sauvegarde refusée."}
    try:
        return _sauvegarder_sous_verrou(operation, source, db_path, categorie=categorie,
                                        protegee=protegee, raison=raison,
                                        migration_cible=migration_cible)
    finally:
        _liberer_verrou(verrou)


def _sauvegarder_sous_verrou(operation: str, source: Path, db_path: Path | None, *,
                             categorie: str, protegee: bool, raison: str | None,
                             migration_cible: str | None) -> dict[str, Any]:
    debut = _maintenant()
    t0 = time.monotonic()
    source_hash = _sha256(source)
    schema_version = _schema_version(source)
    fk_source = _nb_violations_fk(source)

    dossier = Path(cfg.BACKUPS_DIR)
    dossier.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sid = "BCK-" + uuid.uuid4().hex[:12].upper()
    dest = dossier / f"{ts}_{operation}_{sid}.db"
    partiel = dest.with_name(dest.name + ".partiel")

    try:
        src_conn = sqlite3.connect(str(source))
        try:
            dst_conn = sqlite3.connect(str(partiel))
            try:
                src_conn.backup(dst_conn)
                dst_conn.execute("PRAGMA journal_mode=DELETE")
            finally:
                dst_conn.close()
        finally:
            src_conn.close()
        os.replace(partiel, dest)
    except Exception as exc:   # noqa: BLE001 — l'appelant reçoit un refus, jamais une exception
        for p in (partiel, partiel.with_name(partiel.name + "-journal")):
            try:
                p.unlink()
            except OSError:
                pass
        from app.services.path_sanitizer import sanitize_exception
        erreur = f"{type(exc).__name__}: {sanitize_exception(exc)}"
        try:
            _journaliser_run(source, db_path, sauvegarde_id=sid, categorie=categorie,
                             statut="FAILED", debut=debut, fin=_maintenant(),
                             duree_s=time.monotonic() - t0, erreur=erreur)
        except sqlite3.DatabaseError:
            pass
        return {"ok": False, "code": E_SAUVEGARDE_ECHOUEE, "categorie": categorie,
                "message": f"Sauvegarde impossible ({erreur})."}

    insp = inspecter(dest)
    ok = (insp.get("lisible") is True and insp.get("integrity_check") == "ok"
          and fk_source is not None and insp.get("foreign_key_check") == fk_source)
    statut = ST_VALIDE if ok else ST_CORROMPU
    database_hash = _sha256(dest)
    git_commit = _git_commit()
    taille = dest.stat().st_size
    fin = _maintenant()
    duree = time.monotonic() - t0

    # Traçabilité TOUJOURS écrite en sidecar : seule source garantie indépendante du schéma de la
    # base sauvegardée (cf. docstring module — cas PRE_REAL_CUTOVER sur schéma pré-migration).
    meta = {"sauvegarde_id": sid, "operation": operation, "chemin": str(dest),
            "git_commit": git_commit, "database_hash": database_hash, "source_hash": source_hash,
            "schema_version": schema_version, "taille_octets": taille,
            "validation_status": statut, "date_creation": fin,
            "categorie": categorie, "protegee": bool(protegee),
            "raison": raison or _RAISONS.get(categorie, categorie),
            "politique": POLITIQUE, "migration_avant": schema_version,
            "migration_cible": migration_cible,
            "nb_tables": insp.get("nb_tables"), "nb_lignes": insp.get("nb_lignes"),
            "integrity_check": insp.get("integrity_check"),
            "foreign_key_check": insp.get("foreign_key_check"),
            "foreign_key_check_source": fk_source,
            "date_locale": _local(_parse_utc(fin)).isoformat(timespec="seconds"),
            "date_debut": debut, "date_fin": fin, "duree_s": round(duree, 3)}
    meta["secondaire"] = _copier_secondaire(dest, database_hash) if ok else {
        "configure": cfg.BACKUP_SECONDARY_DIR is not None, "ok": False,
        "erreur": "Copie locale non valide : rien n'est copié."}
    _ecrire_sidecar(dest, meta)
    if meta["secondaire"].get("ok"):
        _copier_sidecar_secondaire(dest, meta)

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
    _journaliser_run(source, db_path, sauvegarde_id=sid, categorie=categorie,
                     statut="SUCCESS" if ok else "FAILED", debut=debut, fin=fin, duree_s=duree,
                     erreur="" if ok else "Copie non valide (integrity_check / foreign_key_check).")

    return {"ok": ok, "sauvegarde_id": sid, "chemin": str(dest), "validation_status": statut,
            "source_hash": source_hash, "database_hash": database_hash,
            "schema_version": schema_version, "categorie": categorie, "taille_octets": taille,
            "nb_tables": meta["nb_tables"], "nb_lignes": meta["nb_lignes"],
            "integrity_check": meta["integrity_check"],
            "foreign_key_check": meta["foreign_key_check"], "duree_s": meta["duree_s"],
            "secondaire": meta["secondaire"]}


# ── Destination secondaire ─────────────────────────────────────────────────────────────────────

def secondaire_configure() -> bool:
    return cfg.BACKUP_SECONDARY_DIR is not None


def _copier_secondaire(dest: Path, database_hash: str) -> dict[str, Any]:
    """Copie la sauvegarde (fichier fermé, autonome) vers `BACKUP_SECONDARY_DIR`, vérifiée par
    empreinte. Toute erreur est rendue, jamais levée : la copie locale reste la référence."""
    sec = cfg.BACKUP_SECONDARY_DIR
    if sec is None:
        return {"configure": False}
    provisoire = None
    try:
        dossier = Path(sec)
        dossier.mkdir(parents=True, exist_ok=True)
        cible = dossier / dest.name
        provisoire = cible.with_name(cible.name + ".partiel")
        shutil.copyfile(dest, provisoire)
        if _sha256(provisoire) != database_hash:
            raise OSError("empreinte différente après copie")
        os.replace(provisoire, cible)
        return {"configure": True, "ok": True, "fichier": cible.name}
    except Exception as exc:   # noqa: BLE001
        if provisoire is not None:
            try:
                provisoire.unlink()
            except OSError:
                pass
        from app.services.path_sanitizer import sanitize_exception
        return {"configure": True, "ok": False,
                "erreur": f"{type(exc).__name__}: {sanitize_exception(exc)}"}


def _copier_sidecar_secondaire(dest: Path, meta: dict[str, Any]) -> None:
    try:
        cible = Path(cfg.BACKUP_SECONDARY_DIR) / dest.name
        _ecrire_sidecar(cible, meta)
    except Exception:   # noqa: BLE001 — le fichier secondaire est là ; ses métadonnées sont en local
        pass


# ── Vérification et restauration ───────────────────────────────────────────────────────────────

def verifier(sauvegarde_id: str, *, db_path: Path | None = None) -> dict[str, Any]:
    """Revérifie une sauvegarde existante : présence, empreinte, ouverture, `integrity_check`,
    `foreign_key_check` ; met à jour son statut en base.

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
    meta = None
    if row is None:
        meta = _trouver_sidecar_par_id(sauvegarde_id)
        if meta is None:
            return {"ok": False, "code": "E_INTROUVABLE", "message": "Sauvegarde introuvable."}
        row = {"chemin_fichier": meta["chemin"], "database_hash": meta["database_hash"]}

    chemin = Path(row["chemin_fichier"])
    insp: dict[str, Any] = {}
    if not chemin.exists():
        statut = ST_CORROMPU
        details = "Fichier de sauvegarde manquant."
    elif _sha256(chemin) != row["database_hash"]:
        statut = ST_CORROMPU
        details = "Empreinte différente de celle enregistrée à la création."
    else:
        insp = inspecter(chemin)
        attendu_fk = (meta or _lire_sidecar(chemin) or {}).get("foreign_key_check_source")
        if not insp.get("lisible"):
            statut, details = ST_CORROMPU, "Fichier illisible par SQLite."
        elif insp.get("integrity_check") != "ok":
            statut, details = ST_CORROMPU, "PRAGMA integrity_check a échoué."
        elif insp["foreign_key_check"] != (attendu_fk if attendu_fk is not None else 0):
            statut, details = ST_CORROMPU, "PRAGMA foreign_key_check différent de la source."
        else:
            statut, details = ST_VALIDE, ""

    if not via_sidecar:
        conn = get_db(db_path)
        try:
            conn.execute(
                "UPDATE sauvegardes_base SET validation_status=? WHERE sauvegarde_id_opaque=?",
                (statut, sauvegarde_id))
            conn.commit()
        finally:
            conn.close()

    return {"ok": statut == ST_VALIDE, "validation_status": statut, "details": details,
            "inspection": insp, "chemin": str(chemin)}


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


def _chemin_par_id(sauvegarde_id: str, db_path: Path | None) -> str | None:
    cible_verif = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if _table_existe_sans_modifier(cible_verif, "sauvegardes_base"):
        conn = get_db(db_path)
        try:
            row = conn.execute(
                "SELECT chemin_fichier FROM sauvegardes_base WHERE sauvegarde_id_opaque=?",
                (sauvegarde_id,)).fetchone()
            if row and Path(row["chemin_fichier"]).exists():
                return row["chemin_fichier"]
        finally:
            conn.close()
    meta = _trouver_sidecar_par_id(sauvegarde_id)
    return meta["chemin"] if meta else None


def _ecrire_par_api(copie: Path, cible: Path) -> None:
    """Remet `copie` dans `cible` PAR L'API SQLITE quand la cible est une base lisible : les
    connexions encore ouvertes et le fichier `-wal` de la cible restent cohérents (une copie de
    fichier brute laisserait un `-wal` périmé être rejoué par-dessus la base restaurée). Cible
    absente ou illisible (base détruite) : remplacement du fichier, résidus `-wal`/`-shm` retirés."""
    try:
        src = sqlite3.connect(str(copie))
        try:
            dst = sqlite3.connect(str(cible))
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
    except sqlite3.DatabaseError:
        for suffixe in ("-wal", "-shm"):
            residu = cible.with_name(cible.name + suffixe)
            if residu.exists():
                residu.unlink()
        shutil.copy2(copie, cible)


def restaurer(sauvegarde_id: str, *, cible: Path | None = None, confirmer: bool = False,
             db_path: Path | None = None) -> dict[str, Any]:
    """Restaure une sauvegarde EN PLACE sur `cible` (par défaut `cfg.DB_PATH`).

    Rollback explicite : refuse tant que `confirmer` n'est pas True — une restauration remplace
    entièrement le fichier cible, ce n'est jamais une opération anodine. Revérifie l'intégrité de
    la sauvegarde avant de l'appliquer : ne jamais restaurer un fichier connu corrompu. Aucun appel
    automatique sur une erreur MÉTIER : seuls la migration en échec et une base corrompue après une
    actualisation globale y recourent.
    """
    if not confirmer:
        return {"ok": False, "code": "E_CONFIRMATION_REQUISE",
                "message": "Restauration refusée sans confirmation explicite (confirmer=True)."}

    verif = verifier(sauvegarde_id, db_path=db_path)
    if not verif["ok"]:
        return {"ok": False, "code": "E_SAUVEGARDE_INVALIDE",
                "message": f"Sauvegarde {sauvegarde_id} non restaurable : {verif['details']}"}

    chemin_fichier = _chemin_par_id(sauvegarde_id, db_path) or verif.get("chemin")
    if chemin_fichier is None:
        return {"ok": False, "code": "E_INTROUVABLE", "message": "Sauvegarde introuvable."}

    cible_path = Path(cible) if cible is not None else Path(cfg.DB_PATH)
    _ecrire_par_api(Path(chemin_fichier), cible_path)

    if not _integrity_ok(cible_path):
        return {"ok": False, "code": "E_RESTAURATION_CORROMPUE",
                "message": "La restauration a produit un fichier illisible."}

    return {"ok": True, "sauvegarde_id": sauvegarde_id, "cible": str(cible_path)}


def tester_restauration(sauvegarde_id: str, *, dossier: Path | None = None,
                        db_path: Path | None = None) -> dict[str, Any]:
    """Restauration d'ESSAI dans un dossier isolé, jamais sur la base réelle : restaure, inspecte,
    compare aux métadonnées de la sauvegarde, puis supprime la copie d'essai."""
    import tempfile
    verif = verifier(sauvegarde_id, db_path=db_path)
    if not verif["ok"]:
        return {"ok": False, "code": "E_SAUVEGARDE_INVALIDE", "details": verif["details"]}
    chemin = Path(_chemin_par_id(sauvegarde_id, db_path) or verif["chemin"])
    meta = _lire_sidecar(chemin) or {}
    base_tmp = Path(tempfile.mkdtemp(prefix="essai_restauration_", dir=dossier))
    try:
        cible = base_tmp / "app.db"
        _ecrire_par_api(chemin, cible)
        insp = inspecter(cible)
    finally:
        shutil.rmtree(base_tmp, ignore_errors=True)
    identique = (meta.get("nb_tables") in (None, insp.get("nb_tables"))
                 and meta.get("nb_lignes") in (None, insp.get("nb_lignes")))
    ok = insp.get("lisible") is True and insp.get("integrity_check") == "ok" and identique
    return {"ok": ok, "sauvegarde_id": sauvegarde_id, "inspection": insp,
            "identique_aux_metadonnees": identique, "copie_supprimee": not base_tmp.exists()}


# ── Archives protégées ─────────────────────────────────────────────────────────────────────────

def enregistrer_archive(chemin: Path, *, libelle: str, sha256_attendu: str | None = None,
                        db_path: Path | None = None) -> dict[str, Any]:
    """Inscrit une copie EXISTANTE comme archive protégée, SANS toucher au fichier : seul un
    sidecar est ajouté à côté (s'il n'existe pas déjà). Idempotent. L'empreinte est vérifiée avant
    toute inscription — on n'inscrit jamais comme référence un fichier qui a changé."""
    chemin = Path(chemin)
    if not chemin.exists():
        return {"ok": False, "code": "E_INTROUVABLE", "message": "Archive absente."}
    empreinte = _sha256(chemin)
    if sha256_attendu and empreinte != sha256_attendu:
        return {"ok": False, "code": "E_EMPREINTE",
                "message": "L'empreinte de l'archive ne correspond pas à la référence."}
    existant = _lire_sidecar(chemin)
    if existant is not None:
        return {"ok": True, "deja_inscrite": True, "sauvegarde_id": existant.get("sauvegarde_id"),
                "sha256": empreinte}
    insp = inspecter(chemin)
    if empreinte != _sha256(chemin):    # l'inspection ne doit jamais avoir modifié le fichier
        return {"ok": False, "code": "E_EMPREINTE", "message": "Archive modifiée pendant l'examen."}
    ok = insp.get("lisible") is True and insp.get("integrity_check") == "ok"
    date_fichier = datetime.fromtimestamp(chemin.stat().st_mtime, tz=timezone.utc)
    sid = "ARC-" + uuid.uuid4().hex[:12].upper()
    meta = {"sauvegarde_id": sid, "operation": "ARCHIVE", "chemin": str(chemin),
            "libelle": libelle, "git_commit": None, "database_hash": empreinte,
            "source_hash": None, "schema_version": insp.get("schema_version"),
            "taille_octets": chemin.stat().st_size,
            "validation_status": ST_VALIDE if ok else ST_CORROMPU,
            "date_creation": date_fichier.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "categorie": CAT_ARCHIVE, "protegee": True, "raison": "ARCHIVE",
            "politique": POLITIQUE, "nb_tables": insp.get("nb_tables"),
            "nb_lignes": insp.get("nb_lignes"), "integrity_check": insp.get("integrity_check"),
            "foreign_key_check": insp.get("foreign_key_check"),
            "foreign_key_check_source": insp.get("foreign_key_check"),
            "inscrite_le": _maintenant()}
    _ecrire_sidecar(chemin, meta)
    return {"ok": ok, "deja_inscrite": False, "sauvegarde_id": sid, "sha256": empreinte}


# ── Rotation ───────────────────────────────────────────────────────────────────────────────────

def _semaine(d: datetime) -> tuple[int, int]:
    iso = d.isocalendar()
    return (iso[0], iso[1])


def plan_rotation(*, inclure_heritees: bool = False,
                  dossier: Path | None = None) -> dict[str, Any]:
    """Ce que la rotation conserverait et supprimerait — LECTURE SEULE.

    Quotidiennes : la plus récente de chacun des `BACKUP_RETENTION_DAILY` derniers jours, de
    chacune des `BACKUP_RETENTION_WEEKLY` dernières semaines ISO et de chacun des
    `BACKUP_RETENTION_MONTHLY` derniers mois (rôles DAILY/WEEKLY/MONTHLY, cumulables sur un même
    fichier). Avant migration : les `BACKUP_RETENTION_MIGRATION` plus récentes et toutes celles de
    moins de `BACKUP_RETENTION_MIGRATION_DAYS` jours. Avant actualisation globale : les
    `BACKUP_RETENTION_GLOBAL_REFRESH` plus récentes. Manuelles : jamais supprimées automatiquement.
    """
    entrees = catalogue(dossier=dossier)
    conserver: dict[str, dict[str, Any]] = {}
    supprimer: list[dict[str, Any]] = []
    roles = {CAT_QUOTIDIENNE: 0, CAT_HEBDOMADAIRE: 0, CAT_MENSUELLE: 0}

    def _garder(e, motifs):
        conserver.setdefault(e["sauvegarde_id"], {"entree": e, "motifs": []})["motifs"].extend(
            motifs)

    candidats = []
    for e in entrees:
        if e["protegee"]:
            _garder(e, ["protégée"])
        elif not e["geree"] and not inclure_heritees:
            _garder(e, ["héritée : hors rotation (nettoyage sur validation séparée)"])
        elif e["statut"] != ST_VALIDE:
            _garder(e, ["non valide : conservée comme preuve"])
        else:
            candidats.append(e)

    jours: set[date] = set()
    semaines: set[tuple[int, int]] = set()
    mois: set[tuple[int, int]] = set()
    for e in [c for c in candidats if c["categorie"] == CAT_QUOTIDIENNE]:
        d = e["date_locale"]
        r = []
        if d.date() not in jours and len(jours) < cfg.BACKUP_RETENTION_DAILY:
            jours.add(d.date())
            r.append(CAT_QUOTIDIENNE)
        if _semaine(d) not in semaines and len(semaines) < cfg.BACKUP_RETENTION_WEEKLY:
            semaines.add(_semaine(d))
            r.append(CAT_HEBDOMADAIRE)
        if (d.year, d.month) not in mois and len(mois) < cfg.BACKUP_RETENTION_MONTHLY:
            mois.add((d.year, d.month))
            r.append(CAT_MENSUELLE)
        if r:
            for role in r:
                roles[role] += 1
            _garder(e, r)
        else:
            supprimer.append(e)

    limite_migration = datetime.now(timezone.utc) - timedelta(
        days=cfg.BACKUP_RETENTION_MIGRATION_DAYS)
    migrations = [c for c in candidats if c["categorie"] == CAT_AVANT_MIGRATION]
    for i, e in enumerate(migrations):
        if i < cfg.BACKUP_RETENTION_MIGRATION:
            _garder(e, ["avant migration : parmi les plus récentes"])
        elif e["date_utc"] >= limite_migration:
            _garder(e, ["avant migration récente"])
        else:
            supprimer.append(e)

    actualisations = [c for c in candidats if c["categorie"] == CAT_AVANT_ACTUALISATION]
    for i, e in enumerate(actualisations):
        if i < cfg.BACKUP_RETENTION_GLOBAL_REFRESH:
            _garder(e, ["avant actualisation globale : parmi les plus récentes"])
        else:
            supprimer.append(e)

    for e in candidats:
        if e["categorie"] not in (CAT_QUOTIDIENNE, CAT_AVANT_MIGRATION, CAT_AVANT_ACTUALISATION):
            _garder(e, ["manuelle : jamais supprimée automatiquement"])

    # Garde-fou final : la dernière sauvegarde saine, toutes catégories confondues, reste.
    saines = [e for e in entrees if e["statut"] == ST_VALIDE and e["existe"]]
    if saines:
        derniere = saines[0]
        if any(s["sauvegarde_id"] == derniere["sauvegarde_id"] for s in supprimer):
            supprimer = [s for s in supprimer if s["sauvegarde_id"] != derniere["sauvegarde_id"]]
            _garder(derniere, ["dernière sauvegarde saine"])

    return {
        "conserver": [{**_resume(v["entree"]), "motifs": v["motifs"]} for v in conserver.values()],
        "supprimer": [_resume(e) for e in supprimer],
        "octets_liberes": sum(e["taille"] or 0 for e in supprimer),
        "roles": roles,
        "protegees": sum(1 for e in entrees if e["protegee"]),
        "heritees": sum(1 for e in entrees if not e["geree"] and not e["protegee"]),
        "inclure_heritees": inclure_heritees,
        "_a_supprimer": supprimer,
    }


def _resume(e: dict[str, Any]) -> dict[str, Any]:
    return {"sauvegarde_id": e["sauvegarde_id"], "nom": e["nom"], "categorie": e["categorie"],
            "date_locale": e["date_locale"].isoformat(timespec="seconds"),
            "taille": e["taille"], "statut": e["statut"], "protegee": e["protegee"],
            "geree": e["geree"]}


def purger(*, dry_run: bool = True, inclure_heritees: bool = False, confirmer: bool = False,
           db_path: Path | None = None) -> dict[str, Any]:
    """Applique la rotation. `dry_run=True` (défaut) : n'efface RIEN, rend exactement ce qui
    serait supprimé. Les sauvegardes héritées n'entrent dans la rotation qu'avec
    `inclure_heritees=True` ET `confirmer=True` (nettoyage validé séparément par l'utilisateur)."""
    if inclure_heritees and not dry_run and not confirmer:
        return {"ok": False, "code": "E_CONFIRMATION_REQUISE",
                "message": "Suppression des sauvegardes héritées refusée sans confirmation."}
    plan = plan_rotation(inclure_heritees=inclure_heritees)
    a_supprimer = plan.pop("_a_supprimer")
    if dry_run:
        return {"ok": True, "dry_run": True, **plan}

    supprimees, erreurs = [], []
    secondaire = cfg.BACKUP_SECONDARY_DIR
    for e in a_supprimer:
        if e["protegee"]:      # défense en profondeur : jamais, quel que soit le plan
            continue
        try:
            for p in (e["fichier"], _sidecar_path(e["fichier"])):
                if p.exists():
                    p.unlink()
            if secondaire is not None:
                for p in (Path(secondaire) / e["nom"],
                          _sidecar_path(Path(secondaire) / e["nom"])):
                    if p.exists():
                        p.unlink()
            supprimees.append(e["sauvegarde_id"])
        except OSError as exc:
            erreurs.append({"sauvegarde_id": e["sauvegarde_id"], "erreur": type(exc).__name__})

    cible = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if supprimees and _table_existe_sans_modifier(cible, "sauvegardes_base"):
        conn = get_db(db_path)
        try:
            for sid in supprimees:
                if _table_existe(conn, "sauvegardes_base_tracabilite"):
                    conn.execute("DELETE FROM sauvegardes_base_tracabilite "
                                 "WHERE sauvegarde_id_opaque=?", (sid,))
                conn.execute("DELETE FROM sauvegardes_base WHERE sauvegarde_id_opaque=?", (sid,))
            conn.commit()
        finally:
            conn.close()
    return {"ok": not erreurs, "dry_run": False, **plan, "supprimees": supprimees,
            "erreurs": erreurs}


# ── Sauvegarde quotidienne ─────────────────────────────────────────────────────────────────────

def _quotidiennes_du_jour(jour: date) -> list[dict[str, Any]]:
    return [e for e in catalogue()
            if e["categorie"] == CAT_QUOTIDIENNE and e["date_locale"].date() == jour]


def decision_quotidienne(*, maintenant: datetime | None = None) -> dict[str, Any]:
    """La sauvegarde du jour est-elle due ? `maintenant` = heure LOCALE naïve (injectable)."""
    maintenant = maintenant or datetime.now()
    heure = cfg.BACKUP_DAILY_HOUR
    echeance = maintenant.replace(hour=heure, minute=0, second=0, microsecond=0)
    if not cfg.BACKUP_DAILY_ENABLED:
        return {"due": False, "motif": "Sauvegarde quotidienne désactivée.", "prochaine": None}
    du_jour = _quotidiennes_du_jour(maintenant.date())
    valides = [e for e in du_jour if e["statut"] == ST_VALIDE]
    if valides:
        return {"due": False, "motif": "Sauvegarde du jour déjà faite.",
                "sauvegarde_id": valides[0]["sauvegarde_id"],
                "prochaine": echeance + timedelta(days=1)}
    if len(du_jour) >= ESSAIS_QUOTIDIENS_MAX:
        return {"due": False, "motif": f"{len(du_jour)} échecs aujourd'hui : reprise demain.",
                "prochaine": echeance + timedelta(days=1)}
    if maintenant < echeance:
        return {"due": False, "motif": f"Prévue à {heure:02d}:00.", "prochaine": echeance}
    return {"due": True, "motif": "Sauvegarde du jour à faire.", "prochaine": maintenant}


def sauvegarde_quotidienne(*, maintenant: datetime | None = None,
                           db_path: Path | None = None) -> dict[str, Any]:
    """Fait la sauvegarde du jour si elle est due, puis applique la rotation des sauvegardes
    gérées. Idempotente sur une journée : la décision est REVÉRIFIÉE sous verrou, donc deux
    processus ou deux redémarrages n'en produisent jamais deux. Jamais d'attente : si une autre
    sauvegarde tourne, ce battement passe son tour."""
    decision = decision_quotidienne(maintenant=maintenant)
    if not decision["due"]:
        return {"ok": True, "effectuee": False, **decision}
    source = Path(db_path) if db_path is not None else Path(cfg.DB_PATH)
    if not source.exists():
        return {"ok": False, "effectuee": False, "code": "E_SOURCE_ABSENTE"}
    verrou = _prendre_verrou(0)
    if verrou is None:
        return {"ok": True, "effectuee": False, "code": E_SAUVEGARDE_EN_COURS,
                "motif": "Une sauvegarde est déjà en cours : battement suivant."}
    try:
        decision = decision_quotidienne(maintenant=maintenant)
        if not decision["due"]:
            return {"ok": True, "effectuee": False, **decision}
        res = _sauvegarder_sous_verrou(OPERATION_QUOTIDIENNE, source, db_path,
                                       categorie=CAT_QUOTIDIENNE, protegee=False, raison=None,
                                       migration_cible=None)
    finally:
        _liberer_verrou(verrou)
    rotation = purger(dry_run=False, db_path=db_path) if res["ok"] else None
    return {"ok": res["ok"], "effectuee": True, "sauvegarde": res,
            "rotation": {k: rotation[k] for k in ("supprimees", "octets_liberes", "erreurs")}
            if rotation else None}


# ── État pour l'observabilité ──────────────────────────────────────────────────────────────────

def _espace_occupe(racine: Path) -> int:
    if not racine.exists():
        return 0
    total = 0
    for p in racine.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except OSError:
            continue
    return total


def etat(*, maintenant: datetime | None = None) -> dict[str, Any]:
    """Résumé lisible par l'écran d'observabilité — noms de fichiers seulement, jamais de chemin."""
    entrees = catalogue()
    saines = [e for e in entrees if e["statut"] == ST_VALIDE and e["existe"]]
    derniere = saines[0] if saines else None
    plan = plan_rotation()
    decision = decision_quotidienne(maintenant=maintenant)
    from app.services import ordonnanceur_service as ordo
    par_categorie: dict[str, int] = {}
    for e in entrees:
        par_categorie[e["categorie"]] = par_categorie.get(e["categorie"], 0) + 1
    return {
        "derniere": {"date_locale": derniere["date_locale"].strftime("%d/%m/%Y %H:%M"),
                     "categorie": derniere["categorie"], "taille": derniere["taille"],
                     "nom": derniere["nom"]} if derniere else None,
        "nb_sauvegardes": len(entrees),
        "par_categorie": par_categorie,
        "espace_octets": _espace_occupe(Path(cfg.BACKUPS_DIR)),
        "quotidienne_active": bool(cfg.BACKUP_DAILY_ENABLED),
        "quotidienne_armee": ordo.en_marche(),
        "heure_quotidienne": cfg.BACKUP_DAILY_HOUR,
        "prochaine_quotidienne": decision["prochaine"].strftime("%d/%m/%Y %H:%M")
        if decision.get("prochaine") else None,
        "motif_quotidienne": decision["motif"],
        "secondaire_configure": secondaire_configure(),
        "roles": plan["roles"],
        "protegees": plan["protegees"],
        "heritees": plan["heritees"],
        "a_supprimer_par_rotation": len(plan["supprimer"]),
        "en_cours": sauvegarde_en_cours(),
        "retention": {"daily": cfg.BACKUP_RETENTION_DAILY, "weekly": cfg.BACKUP_RETENTION_WEEKLY,
                      "monthly": cfg.BACKUP_RETENTION_MONTHLY,
                      "migration": cfg.BACKUP_RETENTION_MIGRATION,
                      "actualisation_globale": cfg.BACKUP_RETENTION_GLOBAL_REFRESH},
    }

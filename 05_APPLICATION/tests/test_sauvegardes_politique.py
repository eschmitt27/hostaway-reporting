"""Tests — politique de sauvegarde de app.db (mission « Fiabiliser les sauvegardes », 2026-10-03).

Tout est isolé : `APP_DATA_DIR`/`DATA_DIR`, `DB_PATH`, `BACKUPS_DIR` et la destination secondaire
pointent sous `tmp_path`. La vraie base n'est jamais ouverte (test 32).
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.config as cfg
from app.db import connection as dbconn
from app.db.connection import apply_migrations, get_db
from app.services import backup_service as bk
from app.services import migration_service as mig
from app.services import ordonnanceur_service as ordo
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch


@pytest.fixture(autouse=True)
def isolation(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setenv("APP_DATA_DIR", str(data))
    monkeypatch.setattr(cfg, "DATA_DIR", data)
    monkeypatch.setattr(cfg, "DB_PATH", data / "app.db")
    monkeypatch.setattr(cfg, "BACKUPS_DIR", data / "backups")
    monkeypatch.setattr(cfg, "BACKUP_SECONDARY_DIR", None)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", False)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_HOUR", 3)
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_DAILY", 7)
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_WEEKLY", 4)
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_MONTHLY", 6)
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_MIGRATION", 5)
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_MIGRATION_DAYS", 30)
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_GLOBAL_REFRESH", 5)
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", False)
    yield data
    ordo.arreter()


@pytest.fixture
def db(isolation):
    apply_migrations(cfg.DB_PATH)
    return cfg.DB_PATH


def _sha(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _sidecar(chemin: str | Path) -> dict:
    return json.loads(Path(str(chemin) + ".meta.json").read_text(encoding="utf-8"))


def _fausse(categorie: str, quand_utc: datetime, *, geree: bool = True, protegee: bool = False,
            statut: str = "VALIDE", sous_dossier: str = "") -> str:
    """Sauvegarde factice (fichier + sidecar) datée — pour éprouver la rotation sur des mois."""
    dossier = Path(cfg.BACKUPS_DIR) / sous_dossier
    dossier.mkdir(parents=True, exist_ok=True)
    sid = "BCK-" + os.urandom(6).hex().upper()
    f = dossier / f"{quand_utc:%Y%m%d_%H%M%S}_{categorie}_{sid}.db"
    f.write_bytes(b"x" * 10)
    operation = {bk.CAT_AVANT_ACTUALISATION: bk.OPERATION_ACTUALISATION_GLOBALE,
                 bk.CAT_QUOTIDIENNE: bk.OPERATION_QUOTIDIENNE}.get(categorie, categorie)
    meta = {"sauvegarde_id": sid, "operation": operation, "database_hash": _sha(f),
            "taille_octets": 10, "validation_status": statut,
            "date_creation": quand_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}
    if geree:
        meta.update({"categorie": categorie, "politique": bk.POLITIQUE, "protegee": protegee})
    Path(str(f) + ".meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return sid


def _quotidiennes(n_jours: int, *, fin: datetime | None = None) -> list[str]:
    fin = fin or datetime.now(timezone.utc).replace(hour=2, minute=0, second=0, microsecond=0)
    return [_fausse(bk.CAT_QUOTIDIENNE, fin - timedelta(days=i)) for i in range(n_jours)]


# ── 1-6 : la copie elle-même ───────────────────────────────────────────────────────────────────

def test_01_02_copie_coherente_d_une_base_wal_avec_ecritures_non_checkpointees(db):
    conn = get_db(db)            # WAL, connexion laissée OUVERTE : la ligne vit dans le -wal
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                 "empreinte_source, statut, nb_feuilles, nb_lignes) "
                 "VALUES ('IMP-WAL','2026-10-03T00:00:00','x','x','IMPORTE',1,1)")
    conn.commit()
    try:
        assert Path(str(db) + "-wal").stat().st_size > 0
        res = bk.sauvegarder("TEST", db_path=db)
    finally:
        conn.close()
    assert res["ok"] is True
    c = sqlite3.connect(res["chemin"])
    try:
        assert c.execute("SELECT COUNT(*) FROM ref_setup_imports WHERE import_id='IMP-WAL'"
                         ).fetchone()[0] == 1
        assert c.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        c.close()
    assert not Path(res["chemin"] + "-wal").exists(), "copie autonome : un seul fichier"


def test_03_04_05_06_empreinte_integrite_fk_et_manifeste(db):
    res = bk.sauvegarder("TEST", db_path=db)
    meta = _sidecar(res["chemin"])
    assert meta["database_hash"] == _sha(Path(res["chemin"])) == res["database_hash"]
    assert meta["integrity_check"] == "ok"
    assert meta["foreign_key_check"] == 0 == meta["foreign_key_check_source"]
    for cle in ("sauvegarde_id", "categorie", "date_creation", "taille_octets", "schema_version",
                "validation_status", "protegee", "nb_tables", "nb_lignes", "raison",
                "migration_avant", "duree_s", "date_debut", "date_fin", "secondaire"):
        assert cle in meta, cle
    assert meta["nb_tables"] > 100 and meta["validation_status"] == "VALIDE"
    assert meta["categorie"] == bk.CAT_MANUELLE and meta["protegee"] is False
    assert not list(Path(cfg.BACKUPS_DIR).glob("*.partiel")), "aucun fichier provisoire restant"


def test_copie_non_valide_si_les_fk_divergent_de_la_source(db, monkeypatch):
    vrai = bk.inspecter
    monkeypatch.setattr(bk, "inspecter", lambda p: {**vrai(p), "foreign_key_check": 3})
    res = bk.sauvegarder("TEST", db_path=db)
    assert res["ok"] is False and res["validation_status"] == "CORROMPU"


# ── 7-8, 26 : quotidienne ──────────────────────────────────────────────────────────────────────

def test_07_08_26_une_seule_quotidienne_par_jour(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", True)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_HOUR", 0)
    r1 = bk.sauvegarde_quotidienne(db_path=db)
    assert r1["effectuee"] is True and r1["ok"] is True
    assert r1["sauvegarde"]["categorie"] == bk.CAT_QUOTIDIENNE
    for _ in range(5):            # battements, redémarrages : toujours la même journée
        r = bk.sauvegarde_quotidienne(db_path=db)
        assert r["effectuee"] is False and r["motif"] == "Sauvegarde du jour déjà faite."
    assert len([e for e in bk.catalogue() if e["categorie"] == bk.CAT_QUOTIDIENNE]) == 1


def test_quotidienne_pas_avant_l_heure_ni_si_desactivee(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", True)
    avant = datetime.now().replace(hour=2, minute=59)
    d = bk.decision_quotidienne(maintenant=avant)
    assert d["due"] is False and d["prochaine"].hour == 3
    assert bk.sauvegarde_quotidienne(maintenant=avant, db_path=db)["effectuee"] is False
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", False)
    apres = datetime.now().replace(hour=4)
    assert bk.sauvegarde_quotidienne(maintenant=apres, db_path=db)["effectuee"] is False
    assert bk.catalogue() == []


def test_quotidienne_en_echec_n_est_pas_retentee_indefiniment(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", True)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_HOUR", 0)
    vrai = bk.inspecter
    monkeypatch.setattr(bk, "inspecter", lambda p: {**vrai(p), "integrity_check": "KO"})
    for _ in range(bk.ESSAIS_QUOTIDIENS_MAX):
        assert bk.sauvegarde_quotidienne(db_path=db)["ok"] is False
    r = bk.sauvegarde_quotidienne(db_path=db)
    assert r["effectuee"] is False and "reprise demain" in r["motif"]


# ── 9-12 : migrations protégées ────────────────────────────────────────────────────────────────

@pytest.fixture
def migrations_factices(tmp_path, monkeypatch, isolation):
    """Jeu de migrations FIXTURE (jamais une vraie migration créée pour tester)."""
    dossier = tmp_path / "migrations"
    dossier.mkdir()
    (dossier / "0001_base.sql").write_text(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY);\n"
        "CREATE TABLE IF NOT EXISTS run_history (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "run_id_opaque TEXT NOT NULL UNIQUE, operation TEXT NOT NULL, statut TEXT NOT NULL "
        "DEFAULT 'STARTED', sauvegarde_id_opaque TEXT, duree_s REAL, erreur TEXT, acteur TEXT, "
        "date_debut TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')), date_fin TEXT);\n"
        "CREATE TABLE IF NOT EXISTS parent (id INTEGER PRIMARY KEY);\n"
        "CREATE TABLE IF NOT EXISTS donnees (id INTEGER PRIMARY KEY, valeur TEXT);\n"
        "INSERT OR IGNORE INTO schema_migrations VALUES ('0001');\n", encoding="utf-8")
    monkeypatch.setattr(dbconn, "MIGRATIONS_DIR", dossier)
    apply_migrations(cfg.DB_PATH)
    c = sqlite3.connect(str(cfg.DB_PATH))
    c.execute("INSERT INTO donnees (valeur) VALUES ('précieux')")
    c.commit()
    c.close()
    return dossier


def _versions(db: Path) -> set[str]:
    c = sqlite3.connect(str(db))
    try:
        return {r[0] for r in c.execute("SELECT version FROM schema_migrations")}
    finally:
        c.close()


def test_10_aucune_migration_en_attente_aucune_sauvegarde(migrations_factices):
    res = mig.migrer_au_demarrage()
    assert res == {"ok": True, "migrations": [], "sauvegarde_id": None}
    assert bk.catalogue() == []


def test_09_12_migration_en_attente_sauvegarde_puis_migration(migrations_factices):
    (migrations_factices / "0002_colonne.sql").write_text(
        "ALTER TABLE donnees ADD COLUMN extra TEXT;\n"
        "INSERT OR IGNORE INTO schema_migrations VALUES ('0002');\n", encoding="utf-8")
    res = mig.migrer_au_demarrage()
    assert res["ok"] is True and res["migrations"] == ["0002"]
    assert res["version_avant"] == "0001" and res["version_apres"] == "0002"
    meta = next(e["meta"] for e in bk.catalogue() if e["sauvegarde_id"] == res["sauvegarde_id"])
    assert meta["categorie"] == bk.CAT_AVANT_MIGRATION and meta["raison"] == "AVANT_MIGRATION"
    assert meta["migration_avant"] == "0001" and meta["migration_cible"] == "0002"
    assert meta["integrity_check"] == "ok" and meta["foreign_key_check"] == 0
    assert meta["nb_tables"] >= 4 and meta["nb_lignes"] >= 2
    # La copie est bien l'état D'AVANT : sans la colonne.
    c = sqlite3.connect(meta["chemin"])
    try:
        assert "extra" not in [r[1] for r in c.execute("PRAGMA table_info(donnees)")]
    finally:
        c.close()
    assert "0002" in _versions(cfg.DB_PATH)
    # Redémarrage suivant : plus rien à jouer, plus de sauvegarde.
    assert mig.migrer_au_demarrage()["sauvegarde_id"] is None
    assert len(bk.catalogue()) == 1


def test_11_sauvegarde_impossible_migration_non_lancee(migrations_factices, monkeypatch):
    (migrations_factices / "0002_colonne.sql").write_text(
        "ALTER TABLE donnees ADD COLUMN extra TEXT;\n", encoding="utf-8")
    monkeypatch.setattr(bk, "sauvegarder", lambda *a, **k: {"ok": False, "code": "E_TEST"})
    res = mig.migrer_au_demarrage()
    assert res["ok"] is False and res["code"] == mig.E_SAUVEGARDE_ECHOUEE
    assert "0002" not in _versions(cfg.DB_PATH)


def test_migration_en_echec_restaure_la_base_d_avant(migrations_factices):
    (migrations_factices / "0002_casse.sql").write_text(
        "INSERT INTO donnees (valeur) VALUES ('écrit à moitié');\n"
        "CECI N EST PAS DU SQL;\n", encoding="utf-8")
    res = mig.migrer_au_demarrage()
    assert res["ok"] is False and res["code"] == "E_MIGRATION_ECHOUEE"
    assert res["rollback"]["ok"] is True
    c = sqlite3.connect(str(cfg.DB_PATH))
    try:
        assert [r[0] for r in c.execute("SELECT valeur FROM donnees")] == ["précieux"]
    finally:
        c.close()


def test_migration_qui_casse_les_fk_est_annulee(migrations_factices):
    (migrations_factices / "0002_fk.sql").write_text(
        "PRAGMA foreign_keys=OFF;\n"
        "CREATE TABLE enfant (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));\n"
        "INSERT INTO enfant (parent_id) VALUES (999);\n"
        "INSERT OR IGNORE INTO schema_migrations VALUES ('0002');\n", encoding="utf-8")
    res = mig.migrer_au_demarrage()
    assert res["ok"] is False and res["code"] == mig.E_FK_ECHOUEE
    assert "0002" not in _versions(cfg.DB_PATH)


def test_base_neuve_migree_sans_sauvegarde(isolation):
    res = mig.migrer_au_demarrage()
    assert res["ok"] is True and res["base_neuve"] is True and res["sauvegarde_id"] is None
    assert bk.catalogue() == []


# ── 13 : restauration ──────────────────────────────────────────────────────────────────────────

def test_13_restauration_d_essai_isolee_puis_restauration_explicite(db, tmp_path):
    res = bk.sauvegarder("TEST", db_path=db)
    essai = bk.tester_restauration(res["sauvegarde_id"], dossier=tmp_path, db_path=db)
    assert essai["ok"] is True and essai["identique_aux_metadonnees"] is True
    assert essai["copie_supprimee"] is True
    # Base modifiée APRÈS, connexion ouverte avec un -wal : la restauration passe par l'API SQLite.
    conn = get_db(db)
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                 "empreinte_source, statut, nb_feuilles, nb_lignes) "
                 "VALUES ('IMP-APRES','2026-10-03T00:00:00','x','x','IMPORTE',1,1)")
    conn.commit()
    try:
        assert bk.restaurer(res["sauvegarde_id"], db_path=db)["code"] == "E_CONFIRMATION_REQUISE"
        r = bk.restaurer(res["sauvegarde_id"], confirmer=True, db_path=db)
        assert r["ok"] is True
        assert conn.execute("SELECT COUNT(*) FROM ref_setup_imports WHERE import_id='IMP-APRES'"
                            ).fetchone()[0] == 0
    finally:
        conn.close()


# ── 14-21 : protection et rotation ─────────────────────────────────────────────────────────────

def test_14_15_sauvegarde_protegee_jamais_purgee(db):
    vieille = datetime.now(timezone.utc) - timedelta(days=900)
    sid = _fausse(bk.CAT_QUOTIDIENNE, vieille, protegee=True)
    _quotidiennes(10)
    res = bk.purger(dry_run=False, db_path=db)
    assert sid not in res["supprimees"]
    assert any(e["sauvegarde_id"] == sid for e in bk.catalogue())


def test_16_17_18_19_21_rotation_7_4_6_sans_copie_supplementaire(db):
    ids = _quotidiennes(400)
    plan = bk.purger(dry_run=True, db_path=db)
    assert plan["roles"] == {"DAILY": 7, "WEEKLY": 4, "MONTHLY": 6}
    gardes = {c["sauvegarde_id"] for c in plan["conserver"]}
    # 7 jours, 4 semaines, 6 mois : les rôles se recouvrent, un seul fichier par sauvegarde.
    assert len(gardes) < 7 + 4 + 6
    assert set(ids[:7]) <= gardes
    res = bk.purger(dry_run=False, db_path=db)
    restants = {e["sauvegarde_id"] for e in bk.catalogue()}
    assert restants == gardes
    assert len(res["supprimees"]) == 400 - len(gardes)
    assert len(list(Path(cfg.BACKUPS_DIR).glob("*.db"))) == len(gardes)


def test_20_dry_run_ne_supprime_rien_et_dit_exactement_quoi(db):
    _quotidiennes(20)
    avant = sorted(p.name for p in Path(cfg.BACKUPS_DIR).iterdir())
    plan = bk.purger(dry_run=True, db_path=db)
    assert plan["dry_run"] is True and len(plan["supprimer"]) > 0
    assert all({"nom", "categorie", "date_locale", "taille"} <= set(s) for s in plan["supprimer"])
    assert plan["octets_liberes"] == 10 * len(plan["supprimer"])
    assert sorted(p.name for p in Path(cfg.BACKUPS_DIR).iterdir()) == avant


def test_heritees_hors_rotation_sauf_confirmation(db):
    vieilles = [_fausse(bk.CAT_AVANT_ACTUALISATION,
                        datetime.now(timezone.utc) - timedelta(days=100 + i), geree=False)
                for i in range(12)]
    assert bk.purger(dry_run=False, db_path=db)["supprimees"] == []
    plan = bk.purger(dry_run=True, inclure_heritees=True, db_path=db)
    assert len(plan["supprimer"]) == 12 - cfg.BACKUP_RETENTION_GLOBAL_REFRESH
    refus = bk.purger(dry_run=False, inclure_heritees=True, db_path=db)
    assert refus["ok"] is False and refus["code"] == "E_CONFIRMATION_REQUISE"
    assert len(bk.catalogue()) == len(vieilles)


def test_retention_avant_migration_et_actualisation_globale(db):
    maintenant = datetime.now(timezone.utc)
    recentes = [_fausse(bk.CAT_AVANT_MIGRATION, maintenant - timedelta(days=i)) for i in range(8)]
    anciennes = [_fausse(bk.CAT_AVANT_MIGRATION, maintenant - timedelta(days=60 + i))
                 for i in range(3)]
    globales = [_fausse(bk.CAT_AVANT_ACTUALISATION, maintenant - timedelta(hours=i))
                for i in range(9)]
    manuelle = _fausse(bk.CAT_MANUELLE, maintenant - timedelta(days=999))
    sup = {s["sauvegarde_id"] for s in bk.purger(dry_run=True, db_path=db)["supprimer"]}
    assert not sup & set(recentes), "toutes les sauvegardes avant migration récentes restent"
    assert sup >= set(anciennes)
    assert sup & set(globales) == set(globales[5:])
    assert manuelle not in sup


def test_la_derniere_sauvegarde_saine_n_est_jamais_supprimee(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_GLOBAL_REFRESH", 1)
    a = _fausse(bk.CAT_AVANT_ACTUALISATION, datetime.now(timezone.utc) - timedelta(hours=1))
    _fausse(bk.CAT_AVANT_ACTUALISATION, datetime.now(timezone.utc), statut="CORROMPU")
    plan = bk.purger(dry_run=True, db_path=db)
    assert a not in [s["sauvegarde_id"] for s in plan["supprimer"]]


def test_archive_externe_inscrite_sans_toucher_au_fichier_et_protegee(db, tmp_path):
    dossier = Path(cfg.BACKUPS_DIR) / "archive_v1_2026-10-03"
    dossier.mkdir(parents=True)
    archive = dossier / "app_data_v1_2026-10-03_020652.db"
    src = sqlite3.connect(str(db))
    dst = sqlite3.connect(str(archive))
    src.backup(dst)
    dst.execute("PRAGMA journal_mode=DELETE")
    dst.close()
    src.close()
    os.utime(archive, (time.time() - 400 * 86400,) * 2)
    empreinte = _sha(archive)
    r = bk.enregistrer_archive(archive, libelle="ARCHIVE DE RÉFÉRENCE V1", sha256_attendu=empreinte)
    assert r["ok"] is True and r["deja_inscrite"] is False
    assert bk.enregistrer_archive(archive, libelle="x", sha256_attendu=empreinte)["deja_inscrite"]
    assert bk.enregistrer_archive(archive, libelle="x", sha256_attendu="0" * 64)["ok"] is False
    assert _sha(archive) == empreinte
    entree = next(e for e in bk.catalogue() if e["sauvegarde_id"] == r["sauvegarde_id"])
    assert entree["protegee"] and entree["categorie"] == bk.CAT_ARCHIVE
    plan = bk.purger(dry_run=True, inclure_heritees=True, db_path=db)
    assert r["sauvegarde_id"] not in [s["sauvegarde_id"] for s in plan["supprimer"]]
    assert bk.verifier(r["sauvegarde_id"], db_path=db)["ok"] is True
    assert _sha(archive) == empreinte


def test_dossier_archive_protege_meme_sans_marque(db):
    sid = _fausse(bk.CAT_QUOTIDIENNE, datetime.now(timezone.utc) - timedelta(days=700),
                  sous_dossier="archive_x")
    _quotidiennes(30)
    assert sid not in bk.purger(dry_run=False, db_path=db)["supprimees"]


# ── 22 : concurrence ───────────────────────────────────────────────────────────────────────────

def test_22_une_seule_sauvegarde_a_la_fois_refus_propre_sans_attente_infinie(db):
    verrou = bk._prendre_verrou(0)
    assert verrou is not None
    try:
        t0 = time.monotonic()
        res = bk.sauvegarder("TEST", db_path=db, attente_s=0.5)
        assert res["ok"] is False and res["code"] == bk.E_SAUVEGARDE_EN_COURS
        assert time.monotonic() - t0 < 5
        assert bk.sauvegarde_en_cours() is True
    finally:
        bk._liberer_verrou(verrou)
    assert bk.sauvegarder("TEST", db_path=db)["ok"] is True
    assert bk.sauvegarde_en_cours() is False


def test_22_verrou_d_un_autre_processus_respecte_puis_perime(db, monkeypatch):
    fichier = bk._chemin_verrou()
    fichier.parent.mkdir(parents=True, exist_ok=True)
    fichier.write_text('{"pid": 1}', encoding="utf-8")
    assert bk.sauvegarder("TEST", db_path=db, attente_s=0)["code"] == bk.E_SAUVEGARDE_EN_COURS
    monkeypatch.setattr(cfg, "BACKUP_LOCK_STALE_SECONDS", 60)
    os.utime(fichier, (time.time() - 3600,) * 2)
    assert bk.sauvegarder("TEST", db_path=db, attente_s=0)["ok"] is True


def test_22_sauvegardes_simultanees_jamais_en_parallele(db, monkeypatch):
    en_cours, max_simultanes, verrou_compteur = [0], [0], threading.Lock()
    vrai = bk._sauvegarder_sous_verrou

    def _lent(*a, **k):
        with verrou_compteur:
            en_cours[0] += 1
            max_simultanes[0] = max(max_simultanes[0], en_cours[0])
        time.sleep(0.3)
        try:
            return vrai(*a, **k)
        finally:
            with verrou_compteur:
                en_cours[0] -= 1

    monkeypatch.setattr(bk, "_sauvegarder_sous_verrou", _lent)
    resultats = []
    fils = [threading.Thread(target=lambda: resultats.append(
        bk.sauvegarder("TEST", db_path=db, attente_s=30))) for _ in range(3)]
    for f in fils:
        f.start()
    for f in fils:
        f.join(60)
    assert max_simultanes[0] == 1
    assert [r["ok"] for r in resultats] == [True, True, True]


def test_quotidienne_passe_son_tour_si_une_sauvegarde_tourne(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", True)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_HOUR", 0)
    verrou = bk._prendre_verrou(0)
    try:
        r = bk.sauvegarde_quotidienne(db_path=db)
    finally:
        bk._liberer_verrou(verrou)
    assert r["effectuee"] is False and r["code"] == bk.E_SAUVEGARDE_EN_COURS


# ── 23 : observabilité ─────────────────────────────────────────────────────────────────────────

def test_23_run_history_et_etat_observabilite(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", True)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_HOUR", 0)
    res = bk.sauvegarde_quotidienne(db_path=db)["sauvegarde"]
    c = sqlite3.connect(str(db))
    try:
        row = c.execute("SELECT operation, statut, acteur, duree_s, date_debut, date_fin "
                        "FROM run_history WHERE sauvegarde_id_opaque=?",
                        (res["sauvegarde_id"],)).fetchone()
    finally:
        c.close()
    assert row[0] == bk.OPERATION_HISTORIQUE and row[1] == "SUCCESS" and row[2] == "DAILY"
    assert row[3] is not None and row[4] and row[5]
    e = bk.etat()
    assert e["derniere"]["categorie"] == "DAILY" and e["nb_sauvegardes"] == 1
    assert e["espace_octets"] > 0 and e["secondaire_configure"] is False
    assert e["quotidienne_active"] is True and e["prochaine_quotidienne"]
    assert str(cfg.BACKUPS_DIR) not in json.dumps(e, default=str)


def test_23_ecran_observabilite_affiche_le_resume_sans_chemin(db, monkeypatch):
    from fastapi.testclient import TestClient
    from app.main import app
    bk.sauvegarder("TEST", db_path=db)
    with TestClient(app) as client:
        r = client.get("/observabilite/runs")
    assert r.status_code == 200
    assert 'data-testid="etat-sauvegardes"' in r.text
    assert "NON CONFIGURÉE" in r.text and "Dernière sauvegarde réussie" in r.text
    assert str(cfg.BACKUPS_DIR) not in r.text


# ── 24-25 : scheduler et redémarrage ───────────────────────────────────────────────────────────

def test_24_scheduler_existant_fait_la_quotidienne_sans_lancer_hostaway(db, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", True)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_HOUR", 0)
    hostaway = []
    monkeypatch.setattr(ordo, "tick", lambda **k: hostaway.append(1))
    r = ordo.demarrer(intervalle_s=0.05)
    assert r["ok"] is True and r["hostaway_automatique"] is False
    try:
        fin = time.monotonic() + 30
        while time.monotonic() < fin and not bk.catalogue():
            time.sleep(0.1)
        time.sleep(0.5)       # plusieurs battements de plus : toujours une seule quotidienne
    finally:
        ordo.arreter()
    assert len([e for e in bk.catalogue() if e["categorie"] == "DAILY"]) == 1
    assert hostaway == [], "la sauvegarde ne déclenche jamais Hostaway"


def test_24_scheduler_reste_inerte_si_rien_n_est_active():
    assert ordo.demarrer()["code"] == ordo.E_INACTIF


def test_25_redemarrages_sans_migration_ne_creent_aucune_sauvegarde(db, monkeypatch):
    main_module = importlib.import_module("app.main")

    async def _cycle():
        async with main_module.lifespan(main_module.app):
            pass

    for _ in range(3):
        asyncio.run(_cycle())
    assert bk.catalogue() == []


def test_25_demarrage_refuse_si_la_migration_echoue(migrations_factices):
    (migrations_factices / "0002_casse.sql").write_text("CECI N EST PAS DU SQL;", encoding="utf-8")
    main_module = importlib.import_module("app.main")

    async def _cycle():
        async with main_module.lifespan(main_module.app):
            pass

    with pytest.raises(RuntimeError, match="Démarrage refusé"):
        asyncio.run(_cycle())


# ── 27-28 : actualisation globale et Hostaway ──────────────────────────────────────────────────

def test_27_actualisation_globale_sauvegarde_avant_categorie_dediee(db, monkeypatch):
    monkeypatch.setattr(orch, "_appeler_service", lambda chemin, db_path: {"ok": True})
    res = orch.actualiser(db_path=db)
    entree = next(e for e in bk.catalogue() if e["sauvegarde_id"] == res["sauvegarde_id"])
    assert entree["categorie"] == bk.CAT_AVANT_ACTUALISATION and entree["geree"] is True


def test_28_actualisation_hostaway_sans_sauvegarde_complete(db, monkeypatch):
    appels = []
    monkeypatch.setattr(orch, "_appeler_service", lambda chemin, db_path: {"ok": True})
    monkeypatch.setattr(bk, "sauvegarder", lambda *a, **k: appels.append(a) or {"ok": True})
    for _ in range(3):
        orch.actualiser(cibles=[dag.HOSTAWAY_RAW], db_path=db)
    assert appels == []
    assert not Path(cfg.BACKUPS_DIR).exists() or not list(Path(cfg.BACKUPS_DIR).glob("*.db"))


# ── 29-31 : destination secondaire ─────────────────────────────────────────────────────────────

def test_29_secondaire_absente_le_local_fonctionne(db):
    res = bk.sauvegarder("TEST", db_path=db)
    assert res["ok"] is True and res["secondaire"] == {"configure": False}


def test_30_secondaire_configuree_copie_verifiee_et_rotation_miroir(db, tmp_path, monkeypatch):
    sec = tmp_path / "secondaire"
    monkeypatch.setattr(cfg, "BACKUP_SECONDARY_DIR", sec)
    res = bk.sauvegarder("TEST", db_path=db)
    assert res["secondaire"]["ok"] is True
    copie = sec / Path(res["chemin"]).name
    assert _sha(copie) == res["database_hash"]
    assert json.loads((sec / (copie.name + ".meta.json")).read_text(encoding="utf-8"))[
        "sauvegarde_id"] == res["sauvegarde_id"]
    # La rotation supprime aussi la copie secondaire d'une sauvegarde purgée.
    monkeypatch.setattr(cfg, "BACKUP_RETENTION_GLOBAL_REFRESH", 1)
    monkeypatch.setattr(orch, "_appeler_service", lambda chemin, db_path: {"ok": True})
    a = orch.actualiser(db_path=db)["sauvegarde_id"]
    orch.actualiser(db_path=db)
    supprimees = bk.purger(dry_run=False, db_path=db)["supprimees"]
    assert supprimees == [a]
    assert not list(sec.glob(f"*{a}*"))


def test_31_erreur_secondaire_ne_detruit_pas_le_local(db, tmp_path, monkeypatch):
    bloquant = tmp_path / "pas_un_dossier"
    bloquant.write_text("fichier", encoding="utf-8")
    monkeypatch.setattr(cfg, "BACKUP_SECONDARY_DIR", bloquant)
    res = bk.sauvegarder("TEST", db_path=db)
    assert res["ok"] is True
    assert res["secondaire"]["configure"] is True and res["secondaire"]["ok"] is False
    assert Path(res["chemin"]).exists() and _sha(Path(res["chemin"])) == res["database_hash"]
    assert _sidecar(res["chemin"])["secondaire"]["ok"] is False


# ── 32 : jamais la vraie base ──────────────────────────────────────────────────────────────────

def test_32_la_vraie_base_n_est_jamais_utilisee(isolation, tmp_path):
    for chemin in (cfg.DB_PATH, cfg.BACKUPS_DIR, cfg.DATA_DIR):
        assert Path(chemin).resolve().is_relative_to(tmp_path.resolve()), chemin
    assert "PilotageConciergerie" not in str(cfg.DB_PATH)
    assert os.environ["APP_DATA_DIR"] == str(isolation)

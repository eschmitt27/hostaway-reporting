"""Tests — sauvegarde/restauration de app.db (mission industrialisation socle technique).

Aucune écriture réelle : `db_path`/`cfg.BACKUPS_DIR` toujours isolés sous `tmp_path`.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import MIGRATIONS_DIR, apply_migrations, get_db
from app.services import backup_service


def _derniere_version_migration() -> str:
    """Dernière version de migration réellement présente sur disque — même règle que
    `apply_migrations` (dernier fichier `*.sql` trié). Dérivée, jamais une constante recopiée : une
    constante recopiée devient fausse dès la migration suivante (vécu — `0065` était périmée alors
    que le schéma courant est en `0071`)."""
    fichiers = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return fichiers[-1].stem.split("_", 1)[0]


@pytest.fixture(autouse=True)
def _isoler_backups(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups")


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "app.db"
    apply_migrations(p)
    return p


def test_sauvegarder_cree_un_fichier_valide(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    assert res["ok"] is True
    assert res["validation_status"] == "VALIDE"
    assert Path(res["chemin"]).exists()


def test_sauvegarder_journalise_metadonnees(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    liste = backup_service.lister(db_path=db)
    assert len(liste) == 1
    entree = liste[0]
    assert entree["sauvegarde_id_opaque"] == res["sauvegarde_id"]
    assert entree["operation"] == "TEST"
    assert entree["database_hash"]
    assert entree["taille_octets"] > 0
    assert entree["validation_status"] == "VALIDE"


def test_sauvegarder_ne_touche_pas_la_base_source(db):
    conn = get_db(db)
    try:
        avant = conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0]
    finally:
        conn.close()
    backup_service.sauvegarder("TEST", db_path=db)
    conn = get_db(db)
    try:
        apres = conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0]
    finally:
        conn.close()
    assert avant == apres


def test_verifier_detecte_fichier_corrompu(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    Path(res["chemin"]).write_bytes(b"CECI N'EST PAS UNE BASE SQLITE")
    verif = backup_service.verifier(res["sauvegarde_id"], db_path=db)
    assert verif["ok"] is False
    assert verif["validation_status"] == "CORROMPU"


def test_verifier_detecte_fichier_manquant(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    Path(res["chemin"]).unlink()
    verif = backup_service.verifier(res["sauvegarde_id"], db_path=db)
    assert verif["ok"] is False


def test_restaurer_refuse_sans_confirmation(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    r = backup_service.restaurer(res["sauvegarde_id"], db_path=db)
    assert r["ok"] is False
    assert r["code"] == "E_CONFIRMATION_REQUISE"


def test_restaurer_remet_en_place_le_contenu_sauvegarde(db, tmp_path):
    res = backup_service.sauvegarder("TEST", db_path=db)

    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-APRES','2026-01-01T00:00:00','x','x','IMPORTE',1,1)")
        conn.commit()
    finally:
        conn.close()

    cible = tmp_path / "restauree.db"
    r = backup_service.restaurer(res["sauvegarde_id"], cible=cible, confirmer=True, db_path=db)
    assert r["ok"] is True

    conn = get_db(cible)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM ref_setup_imports WHERE import_id='IMP-APRES'").fetchone()[0]
    finally:
        conn.close()
    assert n == 0, "la restauration doit revenir à l'état d'AVANT l'insertion"


def test_restaurer_refuse_une_sauvegarde_corrompue(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    Path(res["chemin"]).write_bytes(b"CORROMPU")
    r = backup_service.restaurer(res["sauvegarde_id"], confirmer=True, db_path=db)
    assert r["ok"] is False
    assert r["code"] == "E_SAUVEGARDE_INVALIDE"


def test_purger_ne_supprime_jamais_une_sauvegarde_manuelle(db):
    """Politique 2026-10-03 : la rotation porte sur les catégories automatiques ; une sauvegarde
    MANUELLE n'est jamais supprimée automatiquement (cf. test_sauvegardes_politique.py)."""
    for i in range(5):
        backup_service.sauvegarder(f"TEST_{i}", db_path=db)
    res = backup_service.purger(dry_run=False, db_path=db)
    assert res["supprimees"] == []
    assert len(backup_service.lister(db_path=db)) == 5


@pytest.fixture
def db_pre_migration_tracabilite(tmp_path):
    """Base SANS `sauvegardes_base`/`sauvegardes_base_tracabilite` — simule la vraie app.db AVANT
    sa migration (schéma 0016, ces tables n'existent qu'à partir de 0057/0061). Un backup pris à
    ce stade doit fonctionner sans dépendre d'aucune de ces deux tables (mission 14 §1)."""
    p = tmp_path / "app_pre_migration.db"
    conn = sqlite3.connect(str(p))
    try:
        conn.execute(
            "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, "
            "applied_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')))")
        conn.execute("INSERT INTO schema_migrations (version) VALUES ('0016')")
        conn.execute("CREATE TABLE audit_events (id INTEGER PRIMARY KEY, evenement TEXT)")
        conn.execute("INSERT INTO audit_events (evenement) VALUES ('SEED')")
        conn.commit()
    finally:
        conn.close()
    return p


def test_sauvegarder_fonctionne_sans_les_tables_de_tracabilite(db_pre_migration_tracabilite):
    """Mission 14 §1 — le backup PRE_REAL_CUTOVER doit fonctionner sur le schéma SOURCE actuel,
    jamais nécessiter de migrer la base d'abord pour pouvoir la sauvegarder."""
    source = db_pre_migration_tracabilite
    avant = backup_service._sha256(source)

    res = backup_service.sauvegarder("PRE_REAL_CUTOVER", db_path=source)

    assert res["ok"] is True
    assert res["schema_version"] == "0016"
    assert backup_service._sha256(source) == avant, "la source ne doit jamais être modifiée"

    backup = Path(res["chemin"])
    conn = sqlite3.connect(str(backup))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        conn.execute("PRAGMA foreign_keys=ON")
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute(
            "SELECT version FROM schema_migrations").fetchall() == [("0016",)]
        assert conn.execute("SELECT evenement FROM audit_events").fetchall() == [("SEED",)]
        # La copie n'a pas non plus les tables de traçabilité : elle ne les invente pas.
        has_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sauvegardes_base'"
        ).fetchone()
        assert has_table is None
    finally:
        conn.close()

    # Métadonnées disponibles quand même, via le sidecar — jamais perdues faute de tables.
    sidecar = backup.with_suffix(backup.suffix + ".meta.json")
    assert sidecar.exists()


def test_verifier_et_restaurer_fonctionnent_sans_les_tables_de_tracabilite(
        db_pre_migration_tracabilite, tmp_path):
    source = db_pre_migration_tracabilite
    res = backup_service.sauvegarder("PRE_REAL_CUTOVER", db_path=source)

    # Incident post-bascule : la source est détruite APRÈS la sauvegarde.
    source.write_bytes(b"SOURCE DETRUITE")

    verif = backup_service.verifier(res["sauvegarde_id"], db_path=source)
    assert verif["ok"] is True, verif

    cible = tmp_path / "restauree.db"
    r = backup_service.restaurer(res["sauvegarde_id"], cible=cible, confirmer=True, db_path=source)
    assert r["ok"] is True, r

    conn = sqlite3.connect(str(cible))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT evenement FROM audit_events").fetchall() == [("SEED",)]
    finally:
        conn.close()


def test_sauvegarder_utilise_l_api_sqlite_backup_pas_shutil_copy(db, monkeypatch):
    """Non-régression mission 14 : la sauvegarde de bascule ne doit pas dépendre de
    `wal_checkpoint` + `shutil.copy2` (fenêtre de course entre checkpoint et copie)."""
    import shutil as shutil_mod
    appele = {"copy2": False}
    original = shutil_mod.copy2

    def _espion(*a, **k):
        appele["copy2"] = True
        return original(*a, **k)

    monkeypatch.setattr(shutil_mod, "copy2", _espion)
    res = backup_service.sauvegarder("TEST", db_path=db)
    assert res["ok"] is True
    assert appele["copy2"] is False, "sauvegarder() ne doit plus utiliser shutil.copy2"


def test_sauvegarder_journalise_metadonnees_completes(db):
    res = backup_service.sauvegarder("TEST", db_path=db)
    assert res["source_hash"]
    assert res["database_hash"]
    assert res["schema_version"] == _derniere_version_migration()

    entree = backup_service.lister(db_path=db)[0]
    assert entree["source_hash"] == res["source_hash"]
    assert entree["schema_version"] == _derniere_version_migration()
    assert entree["database_hash"]
    assert entree["date_creation"]


def test_source_hash_distinct_de_database_hash_apres_ecriture_dans_source(db):
    """Une écriture dans la source APRÈS la sauvegarde ne doit jamais faire dériver la copie."""
    res = backup_service.sauvegarder("TEST", db_path=db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut, nb_feuilles, nb_lignes) "
            "VALUES ('IMP-DERIVE','2026-01-01T00:00:00','x','x','IMPORTE',1,1)")
        conn.commit()
    finally:
        conn.close()
    from app.services.backup_service import _sha256
    assert _sha256(db) != res["source_hash"], "la source a bien changé après la sauvegarde"
    assert _sha256(Path(res["chemin"])) == res["database_hash"], "la copie ne doit jamais changer"


def test_backup_survit_a_destruction_de_la_source(db, tmp_path):
    """CAS 2/3 du plan de rollback (mission 14 §10) : le FICHIER de sauvegarde doit rester
    intact, autonome et restaurable même si la base source est ensuite corrompue ou détruite.

    Limite structurelle documentée, pas une régression à corriger ici : le catalogue
    `sauvegardes_base` vit dans la base qu'il protège. Si la source est détruite, ce catalogue
    l'est aussi — `verifier()`/`restaurer()` (qui interrogent ce catalogue) ne sont donc plus
    utilisables SUR LA SOURCE détruite. Le fichier physique de sauvegarde, lui, est un `.db`
    SQLite autonome : restaurable par simple copie de fichier à partir du chemin déjà renvoyé
    par `sauvegarder()` (`res['chemin']`), sans dépendre d'aucun catalogue.
    """
    donnees_avant = [tuple(r) for r in get_db(db).execute(
        "SELECT * FROM schema_migrations ORDER BY version")]
    res = backup_service.sauvegarder("TEST", db_path=db)

    # Source détruite après la sauvegarde — simule un incident post-bascule (écrasement du fichier).
    db.write_bytes(b"SOURCE DETRUITE")

    fichier_backup = Path(res["chemin"])
    assert fichier_backup.exists(), "le fichier de sauvegarde doit survivre à la destruction de la source"

    conn = sqlite3.connect(str(fichier_backup))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()
    assert backup_service._sha256(fichier_backup) == res["database_hash"], \
        "le hash de la copie ne doit pas avoir bougé"

    # Restauration contrôlée : copie directe du fichier de sauvegarde vers une cible propre.
    cible = tmp_path / "restauree_apres_destruction.db"
    import shutil as shutil_mod
    shutil_mod.copy2(fichier_backup, cible)

    conn = sqlite3.connect(str(cible))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        conn.execute("PRAGMA foreign_keys=ON")
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        version = conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").fetchone()[0]
        assert version == res["schema_version"]
        donnees_apres = [tuple(r) for r in conn.execute(
            "SELECT * FROM schema_migrations ORDER BY version")]
        assert donnees_apres == donnees_avant, "les données restaurées doivent être identiques"
    finally:
        conn.close()


def test_purger_automatique_uniquement_apres_la_quotidienne_et_sans_les_heritees():
    """Principe mission §2 reformulé par la politique 2026-10-03 (« aucune suppression automatique
    sans règle claire ») : la SEULE suppression automatique est la rotation qui suit la sauvegarde
    quotidienne, et elle n'inclut jamais les sauvegardes héritées."""
    import inspect
    import re
    # L'ancien `git grep ... app/` lancé depuis la racine du dépôt ne trouvait jamais rien (le code
    # vit sous 05_APPLICATION/app) : parcours direct des sources, sans dépendre de git.
    appelants = [f"{f.name}:{n}" for f in (cfg.APP_ROOT / "app").rglob("*.py")
                 for n, l in enumerate(f.read_text(encoding="utf-8").splitlines(), 1)
                 if re.search(r"(?<![_\w])purger\(", l) and "def purger" not in l]
    assert len(appelants) == 1 and appelants[0].startswith("backup_service.py:"), appelants
    source = inspect.getsource(backup_service.sauvegarde_quotidienne)
    assert "purger(dry_run=False, db_path=db_path)" in source
    assert "inclure_heritees" not in source

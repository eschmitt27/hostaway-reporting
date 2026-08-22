"""Tests — sauvegarde/restauration de app.db (mission industrialisation socle technique).

Aucune écriture réelle : `db_path`/`cfg.BACKUPS_DIR` toujours isolés sous `tmp_path`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import backup_service


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


def test_purger_conserve_les_n_plus_recentes(db):
    for i in range(5):
        backup_service.sauvegarder(f"TEST_{i}", db_path=db)
    res = backup_service.purger(garder_n=2, db_path=db)
    assert res["conservees"] == 2
    assert len(backup_service.lister(db_path=db)) == 2


def test_purger_ne_supprime_jamais_automatiquement():
    """Non-régression du principe mission §2 : purger() n'est appelé nulle part automatiquement."""
    import subprocess
    result = subprocess.run(
        ["git", "grep", "-n", "backup_service.purger", "--", "app/"],
        cwd=str(cfg.PROJECT_ROOT), capture_output=True, text=True)
    appelants = [l for l in result.stdout.splitlines() if "def purger" not in l]
    assert appelants == [], f"purger() ne doit être appelé que sur demande explicite : {appelants}"

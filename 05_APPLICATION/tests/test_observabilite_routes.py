"""Tests — écran observabilité (mission industrialisation socle technique)."""
from __future__ import annotations

from app.services import backup_service, run_history_service as history


def test_ecran_vide_repond_200(client, tmp_db):
    r = client.get("/observabilite/runs")
    assert r.status_code == 200
    assert "Aucun run enregistré" in r.text
    assert "Aucune sauvegarde enregistrée" in r.text


def test_ecran_affiche_les_runs_et_sauvegardes(client, tmp_db, monkeypatch, tmp_path):
    import app.config as cfg
    monkeypatch.setattr(cfg, "BACKUPS_DIR", tmp_path / "backups")

    res = backup_service.sauvegarder("TEST", db_path=tmp_db)
    run_id = history.demarrer("IMPORT_TEST", acteur="recette", db_path=tmp_db)
    history.marquer_succes(run_id, db_path=tmp_db)

    r = client.get("/observabilite/runs")
    assert r.status_code == 200
    assert run_id in r.text
    assert res["sauvegarde_id"] in r.text
    assert "SUCCESS" in r.text
    assert "VALIDE" in r.text

"""APP-0 — Snapshot : copie + manifeste sha256 + SQLite + restauration en copie."""
import json
from pathlib import Path
import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services.snapshot_service import (
    create_snapshot, verify_snapshot, restore_to_workspace, list_snapshots
)


def setup_env(tmp_path):
    db = tmp_path / "app.db"
    snaps = tmp_path / "snapshots"
    restore = tmp_path / "restore_workspace"
    snaps.mkdir()
    restore.mkdir()
    cfg.DB_PATH = db
    cfg.SNAPSHOTS_DIR = snaps
    cfg.RESTORE_DIR = restore
    apply_migrations(db)
    return db, snaps, restore


def test_snapshot_creates_files_and_manifest(tmp_path):
    db, snaps, restore = setup_env(tmp_path)

    # Créer des fichiers source factices
    src_file = tmp_path / "SAISIE_Test.xlsx"
    src_file.write_bytes(b"fake excel content 123")

    result = create_snapshot("MANUEL", [src_file], db_path=db)
    assert result["files"] == 1
    snap_dir = Path(result["path"])
    assert snap_dir.exists()
    assert (snap_dir / "SAISIE_Test.xlsx").exists()

    # Vérifier manifeste en base
    conn = get_db(db)
    row = conn.execute("SELECT manifest FROM snapshots WHERE id=?", (result["id"],)).fetchone()
    conn.close()
    manifest = json.loads(row[0])
    assert len(manifest) == 1
    assert "sha256" in manifest[0]
    assert len(manifest[0]["sha256"]) == 64


def test_snapshot_verify(tmp_path):
    db, snaps, restore = setup_env(tmp_path)
    src = tmp_path / "SAISIE_A.xlsx"
    src.write_bytes(b"content_a")
    result = create_snapshot("MANUEL", [src], db_path=db)
    verify = verify_snapshot(result["id"], db_path=db)
    assert verify["ok"] is True
    assert verify["errors"] == []


def test_snapshot_verify_detects_corruption(tmp_path):
    db, snaps, restore = setup_env(tmp_path)
    src = tmp_path / "SAISIE_B.xlsx"
    src.write_bytes(b"original content")
    result = create_snapshot("MANUEL", [src], db_path=db)

    # Corrompre la copie snapshot
    snap_dir = Path(result["path"])
    (snap_dir / "SAISIE_B.xlsx").write_bytes(b"corrupted!")

    verify = verify_snapshot(result["id"], db_path=db)
    assert verify["ok"] is False
    assert len(verify["errors"]) > 0


def test_restore_is_copy_only(tmp_path):
    """La restauration crée une copie isolée — jamais écrasement de l'original."""
    db, snaps, restore = setup_env(tmp_path)
    src = tmp_path / "SAISIE_C.xlsx"
    original_content = b"original"
    src.write_bytes(original_content)
    result = create_snapshot("MANUEL", [src], db_path=db)

    r = restore_to_workspace(result["id"], db_path=db)
    assert r["ok"] is True
    workspace = Path(r["workspace"])
    assert workspace.exists()

    # L'original n'a pas été modifié
    assert src.read_bytes() == original_content

    # La copie existe dans le workspace
    restored = workspace / "SAISIE_C.xlsx"
    assert restored.exists()


def test_list_snapshots(tmp_path):
    db, snaps, restore = setup_env(tmp_path)
    src = tmp_path / "SAISIE_D.xlsx"
    src.write_bytes(b"d")
    create_snapshot("AVANT_CLOTURE", [src], db_path=db)
    create_snapshot("MANUEL", [src], db_path=db)
    snaps_list = list_snapshots(db_path=db)
    assert len(snaps_list) == 2
    types = {s["type"] for s in snaps_list}
    assert "AVANT_CLOTURE" in types
    assert "MANUEL" in types

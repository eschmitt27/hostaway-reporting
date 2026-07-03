import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any
from app.db.connection import get_db
from app.config import DB_PATH, SNAPSHOTS_DIR, RESTORE_DIR


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def create_snapshot(
    snapshot_type: str,
    scope_paths: list[Path],
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Copie horodatée + manifeste sha256 + enregistrement SQLite."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = SNAPSHOTS_DIR / f"{ts}_{snapshot_type}"
    dest.mkdir(parents=True, exist_ok=True)

    manifest = []
    for src in scope_paths:
        src = Path(src)
        if not src.exists():
            continue
        if src.is_file():
            rel = src.name
            dst_file = dest / rel
            shutil.copy2(src, dst_file)
            manifest.append({"file": str(src), "dest": str(dst_file), "sha256": _sha256(src), "size": src.stat().st_size})
        elif src.is_dir():
            for f in src.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(src)
                    dst_file = dest / src.name / rel
                    dst_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, dst_file)
                    manifest.append({"file": str(f), "dest": str(dst_file), "sha256": _sha256(f), "size": f.stat().st_size})

    manifest_json = json.dumps(manifest, ensure_ascii=False)

    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO snapshots (type, path, manifest, verified) VALUES (?, ?, ?, 0)",
            (snapshot_type, str(dest), manifest_json),
        )
        conn.commit()
        snapshot_id = cur.lastrowid
    finally:
        conn.close()

    return {"id": snapshot_id, "path": str(dest), "files": len(manifest), "ts": ts}


def verify_snapshot(snapshot_id: int, db_path: Path = DB_PATH) -> dict[str, Any]:
    """Revérifie les sha256 d'un snapshot existant."""
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
        if not row:
            return {"ok": False, "error": "snapshot introuvable"}
        manifest = json.loads(row["manifest"])
    finally:
        conn.close()

    errors = []
    for entry in manifest:
        dst = Path(entry["dest"])
        if not dst.exists():
            errors.append(f"manquant: {dst}")
        elif _sha256(dst) != entry["sha256"]:
            errors.append(f"hash diverge: {dst}")

    ok = len(errors) == 0
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE snapshots SET verified=? WHERE id=?", (1 if ok else 0, snapshot_id))
        conn.commit()
    finally:
        conn.close()

    return {"ok": ok, "errors": errors, "files_checked": len(manifest)}


def restore_to_workspace(snapshot_id: int, db_path: Path = DB_PATH) -> dict[str, Any]:
    """Restauration en copie isolée uniquement — jamais écrasement des sources actives."""
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
        if not row:
            return {"ok": False, "error": "snapshot introuvable"}
        manifest = json.loads(row["manifest"])
    finally:
        conn.close()

    workspace = RESTORE_DIR / f"restore_{snapshot_id}"
    workspace.mkdir(parents=True, exist_ok=True)

    for entry in manifest:
        src = Path(entry["dest"])
        if src.exists():
            dst = workspace / Path(entry["file"]).name
            shutil.copy2(src, dst)

    return {"ok": True, "workspace": str(workspace), "files": len(manifest)}


def list_snapshots(db_path: Path = DB_PATH) -> list[dict]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT id, ts, type, path, verified FROM snapshots ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

import json
from pathlib import Path
from app.db.connection import get_db


def log_event(
    action: str,
    details: dict | str | None = None,
    user_label: str = "local",
    db_path: Path | None = None,
) -> None:
    details_str = json.dumps(details, ensure_ascii=False) if isinstance(details, dict) else (details or "")
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO audit_events (action, details, user_label) VALUES (?, ?, ?)",
            (action, details_str, user_label),
        )
        conn.commit()
    finally:
        conn.close()


def get_recent_events(limit: int = 50, db_path: Path | None = None) -> list[dict]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT id, ts, action, details, user_label FROM audit_events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

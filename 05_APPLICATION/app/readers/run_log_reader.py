from pathlib import Path
from typing import Any
from app.readers.excel_reader import read_sheet
from app.config import MASTER_RUN_LOG


def get_last_runs(max_rows: int = 50) -> list[dict[str, Any]]:
    """Retourne les derniers runs depuis MASTER_RUN_Log.xlsx (lecture seule)."""
    rows = read_sheet(MASTER_RUN_LOG, sheet_name=None, max_rows=max_rows + 1)
    return rows[:max_rows]


def get_run_log_status() -> dict[str, Any]:
    """Statut rapide du log de runs : nombre de lignes, disponibilité."""
    if not Path(MASTER_RUN_LOG).exists():
        return {"available": False, "count": 0, "path": str(MASTER_RUN_LOG)}
    rows = get_last_runs(max_rows=500)
    return {"available": True, "count": len(rows), "path": str(MASTER_RUN_LOG)}

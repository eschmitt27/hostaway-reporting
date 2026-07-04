"""Runner Lot4A APP-2c execute hors processus FastAPI.

Entree : JSON contenant uniquement des chemins deja copies sous le dossier
dry-run. Sortie : JSON standardise avec resultats Lot4A et infos moteur.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

import openpyxl


def _norm_path(value: str) -> Path:
    return Path(value).resolve()


def _assert_under(path: Path, allowed_root: Path) -> Path:
    resolved = Path(path).resolve()
    root = Path(allowed_root).resolve()
    if resolved != root and root not in resolved.parents:
        raise RuntimeError(f"Chemin refuse hors dry-run: {resolved}")
    return resolved


def _write_master(path: Path, master_columns: list[str], result: dict[str, Any]) -> None:
    wb = openpyxl.Workbook()
    ws_master = wb.active
    ws_master.title = "MASTER"
    ws_vue = wb.create_sheet("VUE_ACTIVE")
    for ws, rows in (
        (ws_master, result.get("master_rows", [])),
        (ws_vue, result.get("vue_active_rows", [])),
    ):
        ws.append(master_columns)
        for rec in rows:
            ws.append([rec.get(col, "") for col in master_columns])
    wb.save(str(path))
    wb.close()


def _main(input_path: str, output_path: str) -> int:
    request_path = _norm_path(input_path)
    response_path = _norm_path(output_path)
    request = json.loads(request_path.read_text(encoding="utf-8"))

    allowed_root = _norm_path(request["allowed_root"])
    _assert_under(request_path, allowed_root)
    _assert_under(response_path, allowed_root)

    saisie_path = _assert_under(_norm_path(request["saisie_path"]), allowed_root)
    ref_path = _assert_under(_norm_path(request["ref_path"]), allowed_root)
    master_path = None
    if request.get("master_path"):
        master_path = _assert_under(_norm_path(request["master_path"]), allowed_root)

    project_root = _norm_path(request["project_root"])
    lot4a_root = project_root / "02_TRAVAIL"
    if str(lot4a_root) not in sys.path:
        sys.path.insert(0, str(lot4a_root))

    import numpy as np  # noqa: WPS433 - required engine dependency audit
    import pandas as pd  # noqa: WPS433 - required engine dependency audit
    import lib_lot4a_reservations_hh as lot4a  # type: ignore  # noqa: WPS433

    as_of_iso = request.get("as_of_iso") or datetime.now(timezone.utc).isoformat()
    saisie_rows = lot4a.read_saisie_values(saisie_path)
    taux_rows = lot4a.read_taux_rows(ref_path)
    result = lot4a.build_master(saisie_rows, taux_rows, as_of_iso)
    if master_path is not None:
        _write_master(master_path, list(lot4a.MASTER_COLUMNS), result)

    response = {
        "ok": True,
        "engine": {
            "python": sys.executable,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "lot4a_module": str(Path(lot4a.__file__).resolve()),
        },
        "saisie_rows": saisie_rows,
        "result": result,
    }
    response_path.write_text(json.dumps(response, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: lot4a_dryrun_runner.py input.json output.json")
    raise SystemExit(_main(sys.argv[1], sys.argv[2]))

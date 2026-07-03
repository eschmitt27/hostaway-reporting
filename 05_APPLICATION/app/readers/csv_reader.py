import csv
from pathlib import Path
from typing import Any


def read_csv(path: Path, max_rows: int | None = 1000) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            with open(p, newline="", encoding=encoding) as f:
                reader = csv.DictReader(f, delimiter=";")
                rows = []
                for i, row in enumerate(reader):
                    if max_rows and i >= max_rows:
                        break
                    rows.append(dict(row))
            return rows
        except (UnicodeDecodeError, Exception):
            continue
    return []

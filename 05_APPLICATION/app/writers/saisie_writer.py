# STUB — Lot APP-0
# L'écriture dans les fichiers SAISIE_*.xlsx est définie ici contractuellement
# mais reste inactive jusqu'aux lots de saisie (APP-2, APP-3).
# Toute tentative d'appel lève NotImplementedError explicite.

from pathlib import Path


def write_row(path: Path, sheet_name: str, row: dict) -> None:
    raise NotImplementedError(
        "Écriture SAISIE non activée au Lot APP-0. "
        "Activée à partir du lot de saisie correspondant (APP-2 / APP-3)."
    )


def append_rows(path: Path, sheet_name: str, rows: list[dict]) -> None:
    raise NotImplementedError(
        "Écriture SAISIE non activée au Lot APP-0."
    )

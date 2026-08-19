"""Reprise `MASTER_CTRL_Coherence.xlsx` (Lot11) → SQLite (`controles_lot11_constats`, 0041 +
`controles_lot11_constats_champs`, 0042 — mois/logement_id, nécessaires pour filtrer un constat
par période, ex. `controles_runner_service`).

Lot11 (`lot11_controles_coherence.py`, banque + réservations + ménages combinés) n'est pas migré
cette mission. Ce module lit le classeur produit par le moteur UNE FOIS et l'écrit en base — une
REPRISE, pas une source (même statut que `hostaway_adaptateurs.reprendre`) : le sens de lecture est
Excel → SQLite, jamais l'inverse, et l'application ne rouvre plus ce classeur ailleurs
(`menages_reader.controles_lot11()` lit désormais uniquement cette table).

Remplacement intégral à chaque reprise (DELETE + INSERT) — les constats sont un instantané du
dernier run Lot11, pas un historique à cumuler.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers.excel_reader import list_sheets, read_sheet

ONGLET = "MASTER"

E_MASTER_ABSENT = "LOT11_MASTER_ABSENT"
E_MIGRATION_ABSENTE = "MIGRATION_ABSENTE"


def _chemin(racine: Path | None = None) -> Path:
    if racine is not None:
        return Path(racine) / "02_TRAVAIL" / "Lot11_Controles" / "MASTER_CTRL_Coherence.xlsx"
    return cfg.MASTER_CTRL_COHERENCE


def master_disponible(racine: Path | None = None) -> bool:
    return _chemin(racine).exists()


def reprendre(*, racine: Path | None = None, db_path=None) -> dict[str, Any]:
    chemin = _chemin(racine)
    if not chemin.exists():
        return {"ok": False, "code": E_MASTER_ABSENT,
                "message": "Master Lot11 introuvable : la reprise n'a rien à lire."}

    feuilles = list_sheets(chemin)
    lignes = read_sheet(chemin, ONGLET, max_rows=None) if ONGLET in feuilles else []

    conn = get_db(db_path)
    try:
        if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='controles_lot11_constats'"
        ).fetchone() is None:
            return {"ok": False, "code": E_MIGRATION_ABSENTE,
                    "message": "Table controles_lot11_constats absente (migration 0041 non appliquée)."}
        conn.execute("DELETE FROM controles_lot11_constats")
        cols = ("ctrl_pk", "source_module", "source_table", "source_pk", "code_controle",
                "severity", "message", "impact_facture", "statut_resolution", "commentaire")
        conn.executemany(
            f"INSERT INTO controles_lot11_constats ({', '.join(cols)}) "
            f"VALUES ({', '.join(['?'] * len(cols))})",
            [tuple(l.get(c) for c in cols) for l in lignes])

        if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='controles_lot11_constats_champs'"
        ).fetchone() is not None:
            conn.execute("DELETE FROM controles_lot11_constats_champs")
            champs_cols = ("ctrl_pk", "mois", "logement_id", "proprietaire_id", "reservation_id",
                          "document_id", "date_detection")
            conn.executemany(
                f"INSERT OR IGNORE INTO controles_lot11_constats_champs "
                f"({', '.join(champs_cols)}) VALUES ({', '.join(['?'] * len(champs_cols))})",
                [tuple(l.get(c) for c in champs_cols) for l in lignes])
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "nb_constats": len(lignes)}

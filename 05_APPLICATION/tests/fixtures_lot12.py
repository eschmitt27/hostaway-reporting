"""Fabrique de datasets Lot12 en SQLite, pour les tests.

Remplace le classeur `MASTER_FACT_Proprietaires.xlsx` que les tests devaient fabriquer : depuis la
migration 0047, les lecteurs applicatifs lisent les tables `lot12_*` du run ACTIF, sans repli Excel.
Même principe que `fixtures_lot10.py`.

Insertion en SQL direct (pas via `lot12_prefactures_service`) : ces tests vérifient les LECTEURS et
les services applicatifs, pas le calcul Lot12 lui-même — celui-ci est couvert par la parité réelle
OLD/NEW (`02_TRAVAIL/lot12_generer_factures.py` vs `lot12_prefactures_service.py`).
"""
from __future__ import annotations

from typing import Any, Iterable

from app.db.connection import get_db

RUN_TEST = "L12-TEST"


def _inserer(conn, table: str, run_id: str, lignes: Iterable[dict[str, Any]]) -> None:
    for l in lignes:
        cols = ["run_id"] + list(l.keys())
        valeurs = [run_id] + [l[c] for c in l]
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(cols))})",
            valeurs)


def seeder(db_path, *, entetes=(), lignes=(), controle=(), dashboard=(), a_controler=(),
          run_id: str = RUN_TEST, statut: str = "SUCCES", actif: bool = True) -> str:
    """Crée un run Lot12 et son dataset."""
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE lot12_runs SET actif = 0 WHERE actif = 1")
        conn.execute(
            "INSERT INTO lot12_runs (run_id, statut, actif, nb_entetes, nb_lignes, nb_controle, "
            "nb_dashboard, nb_a_controler) VALUES (?,?,?,?,?,?,?,?)",
            (run_id, statut, 1 if actif else 0, len(list(entetes)), len(list(lignes)),
             len(list(controle)), len(list(dashboard)), len(list(a_controler))))
        _inserer(conn, "lot12_prefactures_entete", run_id, entetes)
        _inserer(conn, "lot12_prefactures_lignes", run_id, lignes)
        _inserer(conn, "lot12_controle_mensuel", run_id, controle)
        _inserer(conn, "lot12_dashboard_facturation", run_id, dashboard)
        _inserer(conn, "lot12_a_controler", run_id, a_controler)
        conn.commit()
    finally:
        conn.close()
    return run_id

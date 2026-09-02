"""Trace des recalculs MENAGES ciblés par mois (migration 0067).

Un seul writer : `orchestrateur_moteur.executer_menages_cible`. Permet de prouver, pour chaque
déclenchement du bouton "Actualiser <mois>", que le mois demandé par l'utilisateur est bien le mois
réellement traité par lot6d/6e/6f.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db


def enregistrer(*, mois_demande: str, mois_traite: str | None, declencheur: str, statut: str,
                db_path=None) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_runs_cibles (mois_demande, mois_traite, declencheur, statut) "
            "VALUES (?, ?, ?, ?)",
            (mois_demande, mois_traite, declencheur, statut))
        conn.commit()
    finally:
        conn.close()


def dernier(mois: str, *, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT id, mois_demande, mois_traite, declencheur, statut, horodatage "
            "FROM menages_runs_cibles WHERE mois_demande = ? ORDER BY id DESC LIMIT 1",
            (mois,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()

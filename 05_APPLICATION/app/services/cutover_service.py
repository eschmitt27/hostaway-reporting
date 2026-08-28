"""Cutover comptable — remise à zéro explicite du seul domaine Banque/Comptabilité avant
activation réelle (mission 14d).

RÈGLE PROJET (jamais inventée ici, confirmée par l'utilisateur au fil des missions 14b-14d)
    CONSERVER : réservations, Hostaway, historique opérationnel, analytique, référentiels,
    règles, audit système.
    REMETTRE À ZÉRO : Banque, comptabilité, créances, dettes, comptes/soldes propriétaires,
    mémoire des rapprochements/paiements historiques.

Ce module ne fait PAS de DELETE dispersés au fil de l'eau : une seule fonction, un seul
domaine explicite, une seule transaction, jamais un effet de bord d'une autre opération.

AUDIT PRÉALABLE (mission 14d §9, sur la vraie app.db, en lecture seule)
Seules 4 tables du domaine portaient des données non nulles, et ce sont des fixtures de
recette/dev (acteur `recette`/`local`, `CHG_SEED_*`, fichier `releve_recette.csv`) — jamais
des données opérationnelles réelles : `banque_imports` (2), `banque_rapprochements` (3),
`banque_rapprochement_evenements` (4), `banque_suggestion_decisions` (2). Toutes les autres
tables du domaine (comptabilité, créances/dettes, comptes/soldes propriétaires, ménages —
`intervenant_menage_dettes`/`paiements`) étaient déjà à 0. `TABLES_DOMAINE` reste la liste
explicite du domaine entier, pas seulement ce qui est non-vide aujourd'hui — une future
donnée réelle dans une de ces tables doit être couverte, pas oubliée.

FAIL-CLOSED
Refuse sans confirmation explicite. Une seule transaction : intégrale ou aucune. Revérifie
`integrity_check`/`foreign_key_check` juste après, avant de rendre la main.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from app.db.connection import get_db

# Ordre : les tables d'événements/décisions (dépendent logiquement des tables de rapprochement)
# d'abord, puis les tables « racine » du domaine — même en l'absence de contrainte FK déclarée
# (vérifié : aucune sur ces tables), respecter l'ordre logique reste la discipline correcte.
TABLES_DOMAINE = (
    # Banque — décisions/événements avant les objets qu'ils référencent
    "banque_classement_decisions",
    "banque_suggestion_decisions",
    "banque_rapprochement_evenements",
    "banque_rapprochements",
    "banque_classifications",
    "banque_classification_signaux",
    "banque_controles",
    "banque_controle_runs",
    "banque_overrides",
    "banque_import_source",
    "banque_mouvements",
    "banque_imports",
    # Rapprochements génériques (réservations/charges ↔ règlements) et règlements fournisseurs
    "rapprochements_reglements",
    "reglement_repartitions",
    "reglements_fournisseurs",
    # Comptes/soldes/relevés propriétaires
    "proprietaires_paiement",
    "proprietaires_releve_evenements",
    "proprietaires_releve_cycle",
    "proprietaires_releves",
    # Comptabilité — écritures avant journaux/périodes
    "ecriture_evenements",
    "ecriture_ligne_ventilation",
    "ecriture_lignes",
    "ecritures",
    "od_lignes",
    "operations_caisse",
    "periodes_comptables",
    # Ménages — dettes/paiements intervenants (mémoire de paiement historique, pas le comptage)
    "intervenant_menage_paiements",
    "intervenant_menage_dettes",
)

E_CONFIRMATION_REQUISE = "E_CONFIRMATION_REQUISE"
E_INTEGRITE_POST_RESET = "E_INTEGRITE_POST_RESET"


def _table_existe(conn, nom: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone() is not None


def previsualiser(*, db_path: Path | None = None) -> dict[str, Any]:
    """Compte les lignes de chaque table du domaine, sans rien modifier."""
    conn = get_db(db_path)
    try:
        comptes = {}
        for t in TABLES_DOMAINE:
            comptes[t] = (conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                         if _table_existe(conn, t) else None)
    finally:
        conn.close()
    total = sum(n for n in comptes.values() if n)
    return {"tables": comptes, "total_lignes_a_supprimer": total}


def reset_domaine_bancaire(*, confirmer: bool = False, db_path: Path | None = None
                          ) -> dict[str, Any]:
    """Remet à zéro le domaine Banque/Comptabilité — une transaction, tout ou rien.

    Refuse sans `confirmer=True` : ce n'est jamais une opération anodine. Ne touche à AUCUNE
    table hors `TABLES_DOMAINE` (réservations, Hostaway, référentiels, règles, audit système
    intacts par construction — ce module ne les mentionne même pas).
    """
    if not confirmer:
        return {"ok": False, "code": E_CONFIRMATION_REQUISE,
                "message": "Reset refusé sans confirmation explicite (confirmer=True)."}

    avant = previsualiser(db_path=db_path)

    conn = get_db(db_path)
    try:
        supprimes = {}
        try:
            for t in TABLES_DOMAINE:
                if not _table_existe(conn, t):
                    continue
                n_avant = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                conn.execute(f'DELETE FROM "{t}"')
                supprimes[t] = n_avant
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    conn = get_db(db_path)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        conn.close()

    if integrity != "ok" or fk_violations:
        return {"ok": False, "code": E_INTEGRITE_POST_RESET,
                "message": f"integrity_check={integrity}, foreign_key_check="
                           f"{len(fk_violations)} violation(s) — voir procédure de restauration.",
                "avant": avant, "supprimes": supprimes}

    apres = previsualiser(db_path=db_path)
    return {"ok": True, "avant": avant, "supprimes": supprimes, "apres": apres,
            "integrity_check": integrity, "foreign_key_check": []}

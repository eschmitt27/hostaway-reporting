"""Lecture des datasets de réservations — SQLite uniquement.

    reservations_calculees (Lot 4bis) · reservations_resolues (Lot 4quater)
    reservations_historique_cloture (Lot 4ter)

DEUX ÉTAPES, DEUX TABLES, ET CE N'EST PAS LA MÊME CHOSE
`calculees` est la table LIVE : toutes les réservations telles que le calcul les voit aujourd'hui.
`resolues` applique la bascule mois ouvert / mois clos — pour un mois clos, elle rend les valeurs
FIGÉES, pas les valeurs recalculées. Un écran qui montrerait le live pour un mois clos afficherait
des montants qui ne correspondent plus à ce qui a été facturé.

Le choix par défaut est donc `resolues` : c'est la source que les lots aval consomment, et celle qui
dit ce qui fait foi.

LE DATASET COURANT
Chaque étape a un dataset ACTIF. Les précédents sont conservés — un recalcul doit rester comparable
au précédent — mais un seul est courant, et c'est celui-ci qu'on lit sauf demande explicite.

FAIL-CLOSED
Sans dataset, les fonctions rendent une liste vide et l'appelant l'apprend par `etat()`. Se rabattre
sur un classeur ferait afficher, après un recalcul, l'état d'avant sans que rien ne le signale.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

ETAPE_CALCULEES = "CALCULEES"
ETAPE_RESOLUES = "RESOLUES"

ETAT_OK = "OK"
ETAT_NON_INITIALISE = "RESERVATIONS_NON_INITIALISEES"
ETAT_VIDE = "RESERVATIONS_DATASET_VIDE"

MESSAGES = {
    ETAT_NON_INITIALISE: ("Aucun jeu de réservations en base : lancez une actualisation Hostaway "
                          "puis le calcul des réservations."),
    ETAT_VIDE: "Le jeu de réservations courant ne contient aucune ligne.",
}

# Colonnes communes aux deux étapes, dans l'ordre du moteur.
COLONNES_BASE = (
    "reservation_calc_id", "row_hash", "source", "reservation_id_hostaway", "reservation_hh_id",
    "mois", "logement_id", "proprietaire_id", "date_arrivee", "date_depart", "nuits",
    "guest_count", "source_guest_count", "montant_retenu", "source_montant", "code_impact",
    "impact_resultat_reel", "impact_resultat_comptable", "statut_controle", "niveau_anomalie",
    "code_anomalie", "commentaire", "source_module", "source_table", "source_pk",
    "date_integration")

# Colonnes que seule la résolution ajoute.
COLONNES_RESOLUTION = ("canal", "etat_mois", "origine_initiale", "source_ligne", "methode",
                       "payout_calcule", "menage_retenu", "assiette_commission")

COLONNES_HISTORIQUE = (
    "cle_historisation", "reservation_calc_id", "reservation_id_hostaway", "reservation_hh_id",
    "canal", "logement_id", "proprietaire_id", "mois", "date_arrivee", "date_depart", "nuits",
    "guest_count", "montant_retenu", "payout_calcule", "menage_retenu", "assiette_commission",
    "code_impact", "impact_resultat_reel", "impact_resultat_comptable", "statut_controle",
    "niveau_anomalie", "code_anomalie", "origine_initiale", "source_ligne", "source_montant",
    "methode", "mois_cloture", "fige_le", "row_hash")

# Le moteur et les écrans nomment `guestCount` ; la base nomme `guest_count`. La traduction est
# rendue à la lecture pour que les gabarits existants continuent de fonctionner sans réécriture.
_VERS_MOTEUR = {"row_hash": "ROW_HASH", "guest_count": "guestCount",
                "source_guest_count": "source_guestCount"}

_TABLES = {ETAPE_CALCULEES: ("reservations_calculees", COLONNES_BASE),
           ETAPE_RESOLUES: ("reservations_resolues", COLONNES_BASE + COLONNES_RESOLUTION)}


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def table_presente(nom: str, *, db_path=None) -> bool:
    """Les tables de réservations n'existent qu'à partir de la migration 0034.

    Une base antérieure est un état LÉGITIME — la base réelle en production n'est pas encore migrée.
    Laisser remonter « no such table » transformerait cet état normal en erreur 500.
    """
    conn = get_db(db_path)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nom,)).fetchone())
    finally:
        conn.close()


def _traduire(ligne: dict[str, Any]) -> dict[str, Any]:
    return {_VERS_MOTEUR.get(k, k): v for k, v in ligne.items()}


# ── Datasets ────────────────────────────────────────────────────────────────────────────────────

_COLS_DATASET = ("dataset_id", "etape", "extraction_id", "run_id", "date_calcul", "nb_lignes",
                 "statut", "actif", "message")


def dataset_courant(etape: str = ETAPE_RESOLUES, *, db_path=None) -> dict[str, Any] | None:
    """Jeu actif d'une étape, ou None."""
    if not table_presente("reservations_datasets", db_path=db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            f"SELECT {', '.join(_COLS_DATASET)} FROM reservations_datasets "
            "WHERE etape = ? AND actif = 1", (etape,)).fetchone()
    finally:
        conn.close()
    return dict(zip(_COLS_DATASET, r)) if r else None


def datasets(*, limite: int = 20, db_path=None) -> list[dict[str, Any]]:
    if not table_presente("reservations_datasets", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(zip(_COLS_DATASET, r)) for r in conn.execute(
            f"SELECT {', '.join(_COLS_DATASET)} FROM reservations_datasets "
            "ORDER BY date_calcul DESC, id DESC LIMIT ?", (limite,))]
    finally:
        conn.close()


def etat(etape: str = ETAPE_RESOLUES, *, db_path=None) -> str:
    courant = dataset_courant(etape, db_path=db_path)
    if courant is None:
        return ETAT_NON_INITIALISE
    return ETAT_OK if courant["nb_lignes"] else ETAT_VIDE


def disponible(etape: str = ETAPE_RESOLUES, *, db_path=None) -> bool:
    return etat(etape, db_path=db_path) == ETAT_OK


# ── Lignes ──────────────────────────────────────────────────────────────────────────────────────

def lignes(etape: str = ETAPE_RESOLUES, *, dataset_id: str = "", db_path=None
           ) -> list[dict[str, Any]]:
    """Réservations d'un dataset. Par défaut, le jeu courant de l'étape demandée."""
    table, colonnes = _TABLES[etape]
    if not table_presente(table, db_path=db_path):
        return []
    if not dataset_id:
        courant = dataset_courant(etape, db_path=db_path)
        if courant is None:
            return []
        dataset_id = courant["dataset_id"]

    conn = get_db(db_path)
    try:
        # L'ordre d'insertion est celui de l'extraction : le conserver rend deux lectures
        # identiques, et les écrans paginés stables d'un rafraîchissement à l'autre.
        rows = conn.execute(
            f"SELECT {', '.join(colonnes)} FROM {table} WHERE dataset_id = ? ORDER BY id",
            (dataset_id,)).fetchall()
    finally:
        conn.close()
    return [_traduire(dict(zip(colonnes, r))) for r in rows]


def par_source(source: str, *, etape: str = ETAPE_RESOLUES, db_path=None
               ) -> list[dict[str, Any]]:
    """Réservations d'une source donnée (`HOSTAWAY_VRBO_A_CONTROLER`, etc.)."""
    return [r for r in lignes(etape, db_path=db_path) if _txt(r.get("source")) == source]


def index_par_reservation(*, etape: str = ETAPE_RESOLUES, db_path=None
                          ) -> dict[str, dict[str, Any]]:
    """{identifiant de réservation: ligne}.

    L'identifiant Hostaway prime, l'identifiant hors Hostaway sert à défaut : c'est l'ordre que le
    moteur applique, et une réservation hors Hostaway n'a pas d'identifiant de plateforme.
    """
    index: dict[str, dict[str, Any]] = {}
    for r in lignes(etape, db_path=db_path):
        for cle in (_txt(r.get("reservation_id_hostaway")), _txt(r.get("reservation_hh_id"))):
            if cle:
                index[cle] = r
    return index


def existe(reservation_id: str, *, etape: str = ETAPE_RESOLUES, db_path=None) -> bool:
    """La réservation est-elle connue du jeu courant, côté Hostaway ou hors Hostaway.

    Requête ciblée plutôt que chargement complet : cette question est posée à chaque saisie de
    charge, et rapatrier 1 500 lignes pour en tester une serait payé à chaque frappe.
    """
    table, _ = _TABLES[etape]
    if not table_presente(table, db_path=db_path):
        return False
    courant = dataset_courant(etape, db_path=db_path)
    if courant is None:
        return False
    rid = _txt(reservation_id)
    if not rid:
        return False
    conn = get_db(db_path)
    try:
        return bool(conn.execute(
            f"SELECT 1 FROM {table} WHERE dataset_id = ? "
            "AND (TRIM(COALESCE(reservation_id_hostaway,'')) = ? "
            "  OR TRIM(COALESCE(reservation_hh_id,'')) = ?) LIMIT 1",
            (courant["dataset_id"], rid, rid)).fetchone())
    finally:
        conn.close()


# ── Historique des mois clos ────────────────────────────────────────────────────────────────────

def historique(*, mois: str = "", db_path=None) -> list[dict[str, Any]]:
    """Réservations figées. Aucun `dataset_id` : elles appartiennent au mois, pas à un recalcul."""
    if not table_presente("reservations_historique_cloture", db_path=db_path):
        return []
    sql = (f"SELECT {', '.join(COLONNES_HISTORIQUE)} FROM reservations_historique_cloture")
    args: list = []
    if mois:
        sql += " WHERE mois = ?"
        args.append(mois)
    sql += " ORDER BY id"
    conn = get_db(db_path)
    try:
        return [_traduire(dict(zip(COLONNES_HISTORIQUE, r)))
                for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def mois_historises(*, db_path=None) -> list[str]:
    if not table_presente("reservations_historique_cloture", db_path=db_path):
        return []
    conn = get_db(db_path)
    try:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT mois FROM reservations_historique_cloture ORDER BY mois")]
    finally:
        conn.close()


# ── Fraîcheur ───────────────────────────────────────────────────────────────────────────────────

def fraicheur(*, db_path=None) -> dict[str, Any]:
    """État des réservations, pour l'affichage.

    Croise le dataset courant et l'extraction Hostaway dont il découle. Ni l'un ni l'autre ne se
    déduit d'un fichier : une donnée peut être fraîche avec un classeur ancien, et l'inverse.
    """
    from app.services import hostaway_raw_service as raw

    courant = dataset_courant(ETAPE_RESOLUES, db_path=db_path)
    extraction = raw.fraicheur(db_path=db_path)
    if courant is None:
        return {"disponible": False, "code": ETAT_NON_INITIALISE,
                "message": MESSAGES[ETAT_NON_INITIALISE], "extraction": extraction}
    return {
        "disponible": bool(courant["nb_lignes"]),
        "code": "" if courant["nb_lignes"] else ETAT_VIDE,
        "message": "" if courant["nb_lignes"] else MESSAGES[ETAT_VIDE],
        "dataset_id": courant["dataset_id"],
        "date_calcul": courant["date_calcul"],
        "nb_lignes": courant["nb_lignes"],
        "statut": courant["statut"],
        "run_id": courant["run_id"],
        "extraction": extraction,
    }

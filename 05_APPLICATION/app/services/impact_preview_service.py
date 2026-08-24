"""Aperçu structurel des impacts d'une modification de règle temporelle — Mission 6 quater.

Ne recalcule AUCUN montant économique. Identifie seulement, à partir des données déjà présentes,
« combien d'objets sont potentiellement concernés » par une période modifiée — jamais un montant
financier inventé (§10/§12 de la mission : « 14 réservations potentiellement concernées », jamais
« impact financier : 287,43 € » si ce chiffre n'a pas réellement été recalculé).

Réutilise le DAG existant (`orchestrateur_dag`) pour les « datasets aval à invalider » — aucune
deuxième carte de dépendances.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db
from app.services import orchestrateur_dag as dag


def _datasets_aval() -> list[str]:
    """Descendants non-export de REF_SETUP — mêmes datasets que `invalider_dag_referentiel()`."""
    return [n for n in dag.descendants(dag.REF_SETUP) if dag.NOEUDS[n].type_noeud != dag.TYPE_EXPORT]


def _compter(conn, sql: str, params: tuple) -> int:
    try:
        return conn.execute(sql, params).fetchone()[0]
    except Exception:   # noqa: BLE001 — table absente (base non initialisée) : 0, jamais une erreur
        return 0


def previsualiser_taux_commission(logement_id: str, date_debut: str, *, db_path=None) -> dict[str, Any]:
    """Réservations et factures potentiellement concernées par un changement de taux, à partir de
    `date_debut` (inclus) — comptage structurel uniquement, aucun montant recalculé."""
    conn = get_db(db_path)
    try:
        reservations = _compter(
            conn, "SELECT COUNT(*) FROM reservations_resolues WHERE logement_id = ? "
            "AND date_arrivee >= ?", (logement_id, date_debut))
        factures = _compter(
            conn, "SELECT COUNT(*) FROM factures_proprietaires "
            "WHERE logement_id = ? AND date_facture >= ?", (logement_id, date_debut))
    finally:
        conn.close()
    return {
        "type_regle": "ASSIETTE_COMMISSION/TAUX_COMMISSION", "logement_id": logement_id,
        "periode_debut": date_debut, "reservations_potentielles": reservations,
        "factures_proprietaires_potentielles": factures, "datasets_aval": _datasets_aval(),
    }


def previsualiser_canape(logement_id: str, date_debut: str, *, db_path=None) -> dict[str, Any]:
    """Réservations potentiellement concernées par un changement de paramètre/formule canapé."""
    conn = get_db(db_path)
    try:
        reservations = _compter(
            conn, "SELECT COUNT(*) FROM reservations_resolues WHERE logement_id = ? "
            "AND date_arrivee >= ? AND guest_count IS NOT NULL", (logement_id, date_debut))
    finally:
        conn.close()
    return {
        "type_regle": "CANAPE_FORMULE/ref_canape_parametres", "logement_id": logement_id,
        "periode_debut": date_debut, "reservations_potentielles": reservations,
        "datasets_aval": _datasets_aval(),
    }


def previsualiser_cout_menage(type_logement_id: str, date_debut: str, *, db_path=None) -> dict[str, Any]:
    """Logements de ce type, et réservations qu'ils portent depuis `date_debut` — un ménage n'a pas
    de date propre indépendante de la réservation qu'il clôt, donc c'est la meilleure approximation
    structurelle disponible sans recalculer le coût ménage lui-même."""
    conn = get_db(db_path)
    try:
        logements = [r[0] for r in conn.execute(
            "SELECT logement_id FROM ref_logements WHERE type_logement_id = ?",
            (type_logement_id,)).fetchall()]
        reservations = 0
        if logements:
            placeholders = ", ".join("?" * len(logements))
            reservations = _compter(
                conn, f"SELECT COUNT(*) FROM reservations_resolues WHERE logement_id IN "
                f"({placeholders}) AND date_arrivee >= ?", (*logements, date_debut))
    finally:
        conn.close()
    return {
        "type_regle": "COUT_MENAGE/ref_couts_standards_menage", "type_logement_id": type_logement_id,
        "periode_debut": date_debut, "nb_logements_concernes": len(logements),
        "reservations_potentielles": reservations, "datasets_aval": _datasets_aval(),
    }


def previsualiser_regle_repartition(date_debut: str, *, db_path=None) -> dict[str, Any]:
    """Factures fournisseurs potentiellement concernées par un changement de la règle de
    répartition des charges communes, à partir de `date_debut`."""
    conn = get_db(db_path)
    try:
        factures = _compter(
            conn, "SELECT COUNT(*) FROM factures WHERE date_facture >= ?", (date_debut,))
    finally:
        conn.close()
    return {
        "type_regle": "REGLE_REPARTITION_CHARGE_COMMUNE", "periode_debut": date_debut,
        "factures_potentielles": factures, "datasets_aval": _datasets_aval(),
    }


def previsualiser_impacts_regle(rule_code: str, *, logement_id: str = "",
                                type_logement_id: str = "", date_debut: str,
                                db_path=None) -> dict[str, Any]:
    """Point d'entrée générique par `rule_code` (mission §10) — délègue à la fonction dédiée."""
    code = str(rule_code or "").strip().upper()
    if code in ("ASSIETTE_COMMISSION", "TAUX_COMMISSION"):
        return previsualiser_taux_commission(logement_id, date_debut, db_path=db_path)
    if code in ("CANAPE_FORMULE", "REF_CANAPE_PARAMETRES"):
        return previsualiser_canape(logement_id, date_debut, db_path=db_path)
    if code in ("COUT_MENAGE", "REF_COUTS_STANDARDS_MENAGE"):
        return previsualiser_cout_menage(type_logement_id, date_debut, db_path=db_path)
    if code == "REGLE_REPARTITION_CHARGE_COMMUNE":
        return previsualiser_regle_repartition(date_debut, db_path=db_path)
    return {"type_regle": code, "periode_debut": date_debut, "datasets_aval": _datasets_aval(),
            "message": "Type de règle non reconnu pour l'aperçu structurel."}

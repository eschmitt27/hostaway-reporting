"""Cycle de préparation du relevé — machine à états, snapshot, détection de dérive (APP-3E).

Distinct du statut de facturation (`proprietaires_suivi_service`, ST_A_FACTURER...) : ce module gère
la préparation humaine du relevé lui-même (NON_DEMARRE..VALIDE..ROUVERT/ANNULE), fige un snapshot au
passage en VALIDE, et détecte toute évolution ultérieure des données sources — jamais un recalcul.
Version optimiste, historique append-only (réutilise `proprietaires_releve_evenements`).
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.db.connection import get_db

ETAT_NON_DEMARRE = "NON_DEMARRE"
ETAT_EN_PREPARATION = "EN_PREPARATION"
ETAT_BLOQUE = "BLOQUE"
ETAT_A_VALIDER = "A_VALIDER"
ETAT_VALIDE = "VALIDE"
ETAT_ROUVERT = "ROUVERT"
ETAT_ANNULE = "ANNULE"

ETATS = {ETAT_NON_DEMARRE, ETAT_EN_PREPARATION, ETAT_BLOQUE, ETAT_A_VALIDER, ETAT_VALIDE,
         ETAT_ROUVERT, ETAT_ANNULE}

_TRANSITIONS: dict[str, set[str]] = {
    ETAT_NON_DEMARRE: {ETAT_EN_PREPARATION},
    ETAT_EN_PREPARATION: {ETAT_BLOQUE, ETAT_A_VALIDER, ETAT_ANNULE},
    ETAT_BLOQUE: {ETAT_EN_PREPARATION, ETAT_ANNULE},
    ETAT_A_VALIDER: {ETAT_VALIDE, ETAT_EN_PREPARATION, ETAT_ANNULE},
    ETAT_VALIDE: {ETAT_ROUVERT},
    ETAT_ROUVERT: {ETAT_EN_PREPARATION, ETAT_ANNULE},
    ETAT_ANNULE: set(),
}

_SNAPSHOT_CHAMPS = (
    "proprietaire_id", "mois", "logements", "version_moteur", "statut_moteur", "statut_app5c",
    "revenus", "cancellation_payout", "menages", "taux_commission", "base_commission",
    "commission", "charges", "fournisseurs", "aircover", "net_exploitation", "acomptes",
    "reversements", "ajustements", "solde", "controles", "reserves", "empreintes_sources",
)


class CycleRefuse(Exception):
    """Transition refusée (état invalide, version obsolète, motif manquant)."""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def charger(releve_id_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM proprietaires_releve_cycle WHERE releve_id_opaque=?",
            (releve_id_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def creer_ou_charger(releve_id_opaque: str, db_path=None) -> dict[str, Any]:
    existant = charger(releve_id_opaque, db_path)
    if existant is not None:
        return existant
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO proprietaires_releve_cycle (releve_id_opaque) VALUES (?)",
            (releve_id_opaque,))
        conn.commit()
    finally:
        conn.close()
    return charger(releve_id_opaque, db_path)


def _journaliser(conn, opaque, type_evt, ancien, nouveau, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO proprietaires_releve_evenements "
        "(releve_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)", (opaque, type_evt, ancien, nouveau, commentaire, acteur))


def _empreinte(donnees: dict[str, Any]) -> str:
    payload = json.dumps(donnees, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _transition(cycle: dict, nouvel_etat: str, *, version_attendue: int | None, commentaire: str,
                acteur: str, db_path, extra_sql: str = "", extra_vals: tuple = ()) -> dict[str, Any]:
    opaque = cycle["releve_id_opaque"]
    ancien = cycle["etat_cycle"]
    version_lue = cycle["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise CycleRefuse(f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if nouvel_etat not in _TRANSITIONS.get(ancien, set()):
        raise CycleRefuse(f"Transition {ancien} → {nouvel_etat} interdite.")

    conn = get_db(db_path)
    try:
        sql = ("UPDATE proprietaires_releve_cycle SET etat_cycle=?, version=version+1, "
              f"date_modification=? {extra_sql} WHERE releve_id_opaque=? AND version=?")
        vals = [nouvel_etat, _now(), *extra_vals, opaque, version_lue]
        cur = conn.execute(sql, vals)
        if cur.rowcount == 0:
            conn.rollback()
            raise CycleRefuse("Conflit de version — le cycle a été modifié entretemps.")
        _journaliser(conn, opaque, "CYCLE_TRANSITION", ancien, nouvel_etat, commentaire, acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger(opaque, db_path)


def demarrer(cycle: dict, *, acteur: str = "", version_attendue: int | None = None,
            db_path=None) -> dict[str, Any]:
    return _transition(cycle, ETAT_EN_PREPARATION, version_attendue=version_attendue,
                       commentaire="", acteur=acteur, db_path=db_path)


def marquer_bloque(cycle: dict, *, commentaire: str = "", acteur: str = "",
                   version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    return _transition(cycle, ETAT_BLOQUE, version_attendue=version_attendue,
                       commentaire=commentaire, acteur=acteur, db_path=db_path)


def marquer_a_valider(cycle: dict, *, acteur: str = "", version_attendue: int | None = None,
                      db_path=None) -> dict[str, Any]:
    return _transition(cycle, ETAT_A_VALIDER, version_attendue=version_attendue,
                       commentaire="", acteur=acteur, db_path=db_path)


def valider(cycle: dict, snapshot_donnees: dict[str, Any], *, bloquants: list[str] | None = None,
           acteur: str = "", version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    if bloquants:
        raise CycleRefuse(f"Validation impossible : contrôles bloquants ouverts ({', '.join(bloquants)}).")
    if cycle["etat_cycle"] != ETAT_A_VALIDER:
        raise CycleRefuse(f"Validation impossible depuis l'état {cycle['etat_cycle']}.")
    filtre = {k: snapshot_donnees.get(k) for k in _SNAPSHOT_CHAMPS}
    snap_json = json.dumps(filtre, sort_keys=True, default=str)
    empreinte = _empreinte(filtre)
    return _transition(
        cycle, ETAT_VALIDE, version_attendue=version_attendue, commentaire="Validation du relevé",
        acteur=acteur, db_path=db_path,
        extra_sql=", snapshot_json=?, snapshot_empreinte=?, date_snapshot=?",
        extra_vals=(snap_json, empreinte, _now()))


def rouvrir(cycle: dict, motif: str, *, acteur: str = "", version_attendue: int | None = None,
           db_path=None) -> dict[str, Any]:
    motif = (motif or "").strip()
    if not motif:
        raise CycleRefuse("Motif de réouverture requis.")
    return _transition(cycle, ETAT_ROUVERT, version_attendue=version_attendue,
                       commentaire=motif, acteur=acteur, db_path=db_path)


def annuler(cycle: dict, motif: str, *, acteur: str = "", version_attendue: int | None = None,
           db_path=None) -> dict[str, Any]:
    motif = (motif or "").strip()
    if not motif:
        raise CycleRefuse("Motif d'annulation requis.")
    return _transition(cycle, ETAT_ANNULE, version_attendue=version_attendue,
                       commentaire=motif, acteur=acteur, db_path=db_path,
                       extra_sql=", motif_annulation=?", extra_vals=(motif,))


def detecter_derive(cycle: dict, donnees_courantes: dict[str, Any]) -> dict[str, Any]:
    """Compare l'état courant au snapshot figé — jamais un recalcul, une comparaison texte/valeur.
    Retourne {'derive': bool, 'champs_modifies': [...]}."""
    if cycle["etat_cycle"] not in (ETAT_VALIDE, ETAT_ROUVERT) or not cycle.get("snapshot_json"):
        return {"derive": False, "champs_modifies": []}
    snapshot = json.loads(cycle["snapshot_json"])
    courant = {k: donnees_courantes.get(k) for k in _SNAPSHOT_CHAMPS}
    modifies = [k for k in _SNAPSHOT_CHAMPS
                if json.dumps(snapshot.get(k), sort_keys=True, default=str) !=
                   json.dumps(courant.get(k), sort_keys=True, default=str)]
    return {"derive": bool(modifies), "champs_modifies": modifies}

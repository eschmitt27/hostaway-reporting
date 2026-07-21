"""Rapprochement déclaratif règlement propriétaire ↔ mouvement bancaire (APP-3F).

Machine à états dédiée. LECTURE SEULE côté bancaire : aucun paiement, aucun virement, aucune
écriture dans le fichier bancaire, aucune API bancaire, aucun IBAN stocké. « Rapprochement
déclaratif interne — ne constitue ni un ordre de paiement ni une preuve bancaire certifiée. »
Version optimiste, historique append-only (`rapprochement_evenements`), aucune suppression physique.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db

MENTION = ("Rapprochement déclaratif interne — ne constitue ni un ordre de paiement ni une preuve "
           "bancaire certifiée.")

ST_NON_RAPPROCHE = "NON_RAPPROCHE"
ST_PROPOSITION_DISPONIBLE = "PROPOSITION_DISPONIBLE"
ST_A_CONTROLER = "A_CONTROLER"
ST_RAPPROCHE = "RAPPROCHE"
ST_ECARTE = "ECARTE"
ST_ANOMALIE = "ANOMALIE"
ST_ROUVERT = "ROUVERT"
ST_ANNULE = "ANNULE"

STATUTS = {ST_NON_RAPPROCHE, ST_PROPOSITION_DISPONIBLE, ST_A_CONTROLER, ST_RAPPROCHE, ST_ECARTE,
           ST_ANOMALIE, ST_ROUVERT, ST_ANNULE}

_TRANSITIONS: dict[str, set[str]] = {
    ST_NON_RAPPROCHE: {ST_PROPOSITION_DISPONIBLE, ST_ANOMALIE, ST_ANNULE},
    ST_PROPOSITION_DISPONIBLE: {ST_A_CONTROLER, ST_ECARTE, ST_ANOMALIE, ST_ANNULE, ST_NON_RAPPROCHE},
    ST_A_CONTROLER: {ST_RAPPROCHE, ST_ECARTE, ST_ANOMALIE, ST_ANNULE},
    ST_RAPPROCHE: {ST_ROUVERT},
    ST_ECARTE: {ST_ROUVERT, ST_ANNULE},
    ST_ANOMALIE: {ST_ROUVERT, ST_ANNULE},
    ST_ROUVERT: {ST_PROPOSITION_DISPONIBLE, ST_A_CONTROLER, ST_ANNULE, ST_NON_RAPPROCHE},
    ST_ANNULE: set(),
}


class RapprochementRefuse(Exception):
    """Transition/action refusée (état invalide, version obsolète, motif manquant, garde métier)."""


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def id_opaque(valeur: str) -> str:
    base = (cfg.BANQUE_OPAQUE_SALT + "|RAP|" + _txt(valeur)).encode("utf-8")
    return "RAP-" + hashlib.sha256(base).hexdigest()[:10]


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def charger(rapprochement_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM rapprochements_reglements WHERE rapprochement_id_opaque=?",
                         (rapprochement_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def charger_par_releve(releve_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM rapprochements_reglements WHERE releve_id_opaque=? AND statut<>'ANNULE'",
            (releve_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def creer_ou_charger(releve_opaque: str, db_path=None) -> dict[str, Any]:
    existant = charger_par_releve(releve_opaque, db_path)
    if existant is not None:
        return existant
    opaque = id_opaque(f"{releve_opaque}|{datetime.now().timestamp()}")
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO rapprochements_reglements (rapprochement_id_opaque, releve_id_opaque) "
            "VALUES (?,?)", (opaque, releve_opaque))
        conn.execute(
            "INSERT INTO rapprochement_evenements (rapprochement_id_opaque, type_evenement, "
            "nouveau_statut) VALUES (?,?,?)", (opaque, "CREATION", ST_NON_RAPPROCHE))
        conn.commit()
    finally:
        conn.close()
    return charger(opaque, db_path)


def _journaliser(conn, opaque, type_evt, ancien, nouveau, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO rapprochement_evenements (rapprochement_id_opaque, type_evenement, "
        "ancien_statut, nouveau_statut, commentaire, acteur) VALUES (?,?,?,?,?,?)",
        (opaque, type_evt, ancien, nouveau, commentaire, acteur))


def _transition(rap: dict, nouveau: str, *, version_attendue, commentaire, acteur, db_path,
                extra_sql="", extra_vals=()) -> dict[str, Any]:
    opaque = rap["rapprochement_id_opaque"]
    ancien = rap["statut"]
    version_lue = rap["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise RapprochementRefuse(
            f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if nouveau not in _TRANSITIONS.get(ancien, set()):
        raise RapprochementRefuse(f"Transition {ancien} → {nouveau} interdite.")
    conn = get_db(db_path)
    try:
        sql = ("UPDATE rapprochements_reglements SET statut=?, version=version+1, "
              f"date_modification=? {extra_sql} WHERE rapprochement_id_opaque=? AND version=?")
        vals = [nouveau, _now(), *extra_vals, opaque, version_lue]
        try:
            cur = conn.execute(sql, vals)
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            raise RapprochementRefuse(f"Conflit d'unicité (mouvement/règlement déjà rapproché) : {exc}")
        if cur.rowcount == 0:
            conn.rollback()
            raise RapprochementRefuse("Conflit de version — le rapprochement a été modifié entretemps.")
        _journaliser(conn, opaque, "TRANSITION", ancien, nouveau, commentaire, acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger(opaque, db_path)


def enregistrer_proposition(rap: dict, mouvement_opaque: str, *, criteres: list[str], score: int,
                            mouvement_empreinte: str, ecart_montant: float | None,
                            ecart_jours: int | None, acteur: str = "", version_attendue=None,
                            db_path=None) -> dict[str, Any]:
    """Passe en PROPOSITION_DISPONIBLE avec le candidat sélectionné — jamais une confirmation."""
    return _transition(
        rap, ST_PROPOSITION_DISPONIBLE, version_attendue=version_attendue, commentaire="",
        acteur=acteur, db_path=db_path,
        extra_sql=", mouvement_id_opaque=?, criteres_json=?, score_explicable=?, "
                  "mouvement_empreinte=?, ecart_montant=?, ecart_jours=?",
        extra_vals=(mouvement_opaque, json.dumps(criteres), score, mouvement_empreinte,
                    ecart_montant, ecart_jours))


def passer_a_controler(rap: dict, *, acteur: str = "", version_attendue=None, db_path=None) -> dict[str, Any]:
    if not rap.get("mouvement_id_opaque"):
        raise RapprochementRefuse("Aucun candidat sélectionné — impossible de passer à contrôler.")
    return _transition(rap, ST_A_CONTROLER, version_attendue=version_attendue, commentaire="",
                       acteur=acteur, db_path=db_path)


def confirmer(rap: dict, *, reglement_paye: bool, mouvement_present: bool, sens_sortant: bool,
              acteur: str = "", commentaire: str = "", version_attendue=None, db_path=None) -> dict[str, Any]:
    """Confirmation HUMAINE uniquement. Refuse si le règlement n'est pas marqué payé, si le mouvement
    a disparu, ou si le mouvement est entrant."""
    if not reglement_paye:
        raise RapprochementRefuse("Le règlement n'est pas marqué comme payé (APP-3E).")
    if not mouvement_present:
        raise RapprochementRefuse("Le mouvement a disparu de la source — réouverture nécessaire.")
    if not sens_sortant:
        raise RapprochementRefuse("Un mouvement entrant ne peut pas être confirmé comme règlement sortant.")
    return _transition(rap, ST_RAPPROCHE, version_attendue=version_attendue, commentaire=commentaire,
                       acteur=acteur, db_path=db_path, extra_sql=", decision=?", extra_vals=("CONFIRME",))


def ecarter(rap: dict, motif: str, *, acteur: str = "", version_attendue=None, db_path=None) -> dict[str, Any]:
    motif = _txt(motif)
    if not motif:
        raise RapprochementRefuse("Motif d'écartement requis.")
    return _transition(rap, ST_ECARTE, version_attendue=version_attendue, commentaire=motif,
                       acteur=acteur, db_path=db_path, extra_sql=", decision=?, motif=?",
                       extra_vals=("ECARTE", motif))


def signaler_anomalie(rap: dict, motif: str, *, acteur: str = "", version_attendue=None,
                      db_path=None) -> dict[str, Any]:
    motif = _txt(motif)
    if not motif:
        raise RapprochementRefuse("Motif d'anomalie requis.")
    return _transition(rap, ST_ANOMALIE, version_attendue=version_attendue, commentaire=motif,
                       acteur=acteur, db_path=db_path, extra_sql=", decision=?, motif=?",
                       extra_vals=("ANOMALIE", motif))


def rouvrir(rap: dict, motif: str, *, acteur: str = "", version_attendue=None, db_path=None) -> dict[str, Any]:
    motif = _txt(motif)
    if not motif:
        raise RapprochementRefuse("Motif de réouverture requis.")
    return _transition(rap, ST_ROUVERT, version_attendue=version_attendue, commentaire=motif,
                       acteur=acteur, db_path=db_path, extra_sql=", motif=?", extra_vals=(motif,))


def annuler(rap: dict, motif: str, *, acteur: str = "", version_attendue=None, db_path=None) -> dict[str, Any]:
    motif = _txt(motif)
    if not motif:
        raise RapprochementRefuse("Motif d'annulation requis.")
    return _transition(rap, ST_ANNULE, version_attendue=version_attendue, commentaire=motif,
                       acteur=acteur, db_path=db_path, extra_sql=", motif=?", extra_vals=(motif,))


def historique(rapprochement_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM rapprochement_evenements WHERE rapprochement_id_opaque=? ORDER BY id DESC",
            (rapprochement_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def lister(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM rapprochements_reglements ORDER BY date_modification DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

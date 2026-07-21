"""Affectation logique des charges (APP-3E) — SQLite isolée uniquement.

Le montant, le coût et le caractère refacturé d'une charge restent exclusivement produits par le
moteur / la source réelle des charges — jamais recalculés ici. Cette couche journalise UNIQUEMENT
l'affectation humaine (fournisseur, logement, propriétaire, mois, nature, refacturable, statut de
contrôle, justificatif logique). Version optimiste, historique append-only, aucune suppression
physique (même pattern que `fournisseurs_referentiel_service.py`).
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db

STATUTS_CONTROLE = {"A_CONTROLER", "CONFORME", "ANOMALIE"}


class AffectationRefusee(Exception):
    """Action refusée (données invalides, transition impossible, version obsolète)."""


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def id_opaque(valeur: str) -> str:
    base = (cfg.PROPRIETAIRE_OPAQUE_SALT + "|CHA|" + _txt(valeur)).encode("utf-8")
    return "CHA-" + hashlib.sha256(base).hexdigest()[:10]


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def charger_par_opaque(affectation_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM charges_affectations WHERE affectation_id_opaque=? AND actif=1",
            (affectation_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def charger_par_charge(charge_id: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM charges_affectations WHERE charge_id=? AND actif=1", (charge_id,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def lister_par_proprietaire_mois(proprietaire_id: str, mois: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM charges_affectations WHERE proprietaire_id=? AND mois=? AND actif=1 "
            "ORDER BY date_creation", (proprietaire_id, mois)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _journaliser(conn, opaque, type_evt, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO charges_affectation_evenements "
        "(affectation_id_opaque, type_evenement, commentaire, acteur) VALUES (?,?,?,?)",
        (opaque, type_evt, commentaire, acteur))


def affecter(charge_id: str, *, source_empreinte: str = "", fournisseur_id_opaque: str | None = None,
             logement_id: str = "", proprietaire_id: str = "", mois: str = "", nature: str = "",
             refacturable: bool | None = None, justificatif_logique: str = "", commentaire: str = "",
             acteur: str = "", db_path=None) -> dict[str, Any]:
    charge_id = _txt(charge_id)
    if not charge_id:
        raise AffectationRefusee("Identifiant de charge requis.")
    if charger_par_charge(charge_id, db_path) is not None:
        raise AffectationRefusee(f"Une affectation existe déjà pour la charge {charge_id}.")

    opaque = id_opaque(f"{charge_id}|{datetime.now().timestamp()}")
    refact_val = None if refacturable is None else (1 if refacturable else 0)
    conn = get_db(db_path)
    try:
        try:
            conn.execute(
                "INSERT INTO charges_affectations "
                "(affectation_id_opaque, charge_id, source_empreinte, fournisseur_id_opaque, "
                " logement_id, proprietaire_id, mois, nature, refacturable, justificatif_logique, "
                " statut_controle, commentaire) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (opaque, charge_id, source_empreinte, fournisseur_id_opaque, logement_id,
                 proprietaire_id, mois, nature, refact_val, justificatif_logique, "A_CONTROLER",
                 commentaire))
            _journaliser(conn, opaque, "CREATION", commentaire, acteur)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            raise AffectationRefusee("Conflit d'identifiant — réessayez.")
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def modifier(affectation: dict, *, fournisseur_id_opaque: str | None = None, logement_id: str | None = None,
             proprietaire_id: str | None = None, mois: str | None = None, nature: str | None = None,
             refacturable: bool | None = None, justificatif_logique: str | None = None,
             statut_controle: str | None = None, commentaire: str | None = None, acteur: str = "",
             version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    opaque = affectation["affectation_id_opaque"]
    version_lue = affectation["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise AffectationRefusee(
            f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if statut_controle is not None and statut_controle not in STATUTS_CONTROLE:
        raise AffectationRefusee(f"Statut de contrôle invalide : « {statut_controle} ».")

    cols = ["version = version + 1", "date_modification = ?"]
    vals: list[Any] = [_now()]
    champs = {
        "fournisseur_id_opaque": fournisseur_id_opaque, "logement_id": logement_id,
        "proprietaire_id": proprietaire_id, "mois": mois, "nature": nature,
        "justificatif_logique": justificatif_logique, "statut_controle": statut_controle,
        "commentaire": commentaire,
    }
    for col, val in champs.items():
        if val is not None:
            cols.append(f"{col} = ?"); vals.append(val)
    if refacturable is not None:
        cols.append("refacturable = ?"); vals.append(1 if refacturable else 0)
    vals.append(opaque); vals.append(version_lue)

    conn = get_db(db_path)
    try:
        cur = conn.execute(
            f"UPDATE charges_affectations SET {', '.join(cols)} "
            "WHERE affectation_id_opaque=? AND version=?", vals)
        if cur.rowcount == 0:
            conn.rollback()
            raise AffectationRefusee("Conflit de version — l'affectation a été modifiée entretemps.")
        _journaliser(conn, opaque, "MODIFICATION", commentaire or "", acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def historique(affectation_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM charges_affectation_evenements WHERE affectation_id_opaque=? ORDER BY id DESC",
            (affectation_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

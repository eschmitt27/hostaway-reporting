"""Référentiel fournisseur minimal (APP-3D) — SQLite isolée uniquement.

Aucun module fournisseur n'existait (audit : `/fournisseurs` gère des charges, pas des fournisseurs).
Champs volontairement minimaux : jamais d'IBAN, de coordonnées bancaires, de numéro de carte, de
secret ni de pièce d'identité. Désactivation logique uniquement — aucune suppression physique.
Version optimiste (même pattern que `clotures_service.py`/`proprietaires_suivi_service.py`).
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db

TYPES = {"MENAGE", "MAINTENANCE", "FOURNITURE", "AUTRE"}
STATUTS = {"ACTIF", "INACTIF"}


class FournisseurRefuse(Exception):
    """Action refusée (données invalides, doublon probable, transition impossible)."""


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def id_opaque(valeur: str) -> str:
    base = (cfg.PROPRIETAIRE_OPAQUE_SALT + "|FRS|" + _txt(valeur)).encode("utf-8")
    return "FRS-" + hashlib.sha256(base).hexdigest()[:10]


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def lister(actif_seul: bool = False, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if actif_seul:
            rows = conn.execute(
                "SELECT * FROM fournisseurs WHERE actif=1 AND statut='ACTIF' ORDER BY nom").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM fournisseurs WHERE actif=1 ORDER BY nom").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def charger_par_opaque(fournisseur_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM fournisseurs WHERE fournisseur_id_opaque=? AND actif=1",
            (fournisseur_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def rechercher_doublons(nom: str, db_path=None) -> list[dict[str, Any]]:
    """Recherche insensible à la casse/aux espaces — signale, ne bloque jamais silencieusement."""
    nom_norm = _txt(nom).lower()
    if not nom_norm:
        return []
    return [f for f in lister(db_path=db_path) if f["nom"].strip().lower() == nom_norm]


def _journaliser(conn, opaque, type_evt, ancien, nouveau, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO fournisseur_evenements "
        "(fournisseur_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)", (opaque, type_evt, ancien, nouveau, commentaire, acteur))


def creer(nom: str, type_fournisseur: str, *, commentaire: str = "", acteur: str = "",
         db_path=None) -> dict[str, Any]:
    nom = _txt(nom)
    type_fournisseur = _txt(type_fournisseur).upper()
    if not nom:
        raise FournisseurRefuse("Nom du fournisseur requis.")
    if type_fournisseur not in TYPES:
        raise FournisseurRefuse(f"Type invalide : « {type_fournisseur} ». Attendu : {', '.join(sorted(TYPES))}.")

    opaque = id_opaque(f"{nom}|{datetime.now().timestamp()}")   # unicité même en cas de doublon de nom volontaire
    conn = get_db(db_path)
    try:
        try:
            conn.execute(
                "INSERT INTO fournisseurs (fournisseur_id_opaque, nom, type, statut, commentaire) "
                "VALUES (?,?,?,?,?)", (opaque, nom, type_fournisseur, "ACTIF", commentaire))
            _journaliser(conn, opaque, "CREATION", None, "ACTIF", commentaire, acteur)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            raise FournisseurRefuse("Conflit d'identifiant — réessayez.")
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def modifier(fournisseur: dict, *, nom: str | None = None, type_fournisseur: str | None = None,
            commentaire: str | None = None, acteur: str = "", version_attendue: int | None = None,
            db_path=None) -> dict[str, Any]:
    opaque = fournisseur["fournisseur_id_opaque"]
    version_lue = fournisseur["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise FournisseurRefuse(
            f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")

    cols = ["version = version + 1", "date_modification = ?"]
    vals: list[Any] = [_now()]
    if nom is not None:
        nom = _txt(nom)
        if not nom:
            raise FournisseurRefuse("Nom du fournisseur requis.")
        cols.append("nom = ?"); vals.append(nom)
    if type_fournisseur is not None:
        type_fournisseur = _txt(type_fournisseur).upper()
        if type_fournisseur not in TYPES:
            raise FournisseurRefuse(f"Type invalide : « {type_fournisseur} ».")
        cols.append("type = ?"); vals.append(type_fournisseur)
    if commentaire is not None:
        cols.append("commentaire = ?"); vals.append(commentaire)
    vals.append(opaque); vals.append(version_lue)

    conn = get_db(db_path)
    try:
        cur = conn.execute(
            f"UPDATE fournisseurs SET {', '.join(cols)} WHERE fournisseur_id_opaque=? AND version=?", vals)
        if cur.rowcount == 0:
            conn.rollback()
            raise FournisseurRefuse("Conflit de version — le fournisseur a été modifié entretemps.")
        _journaliser(conn, opaque, "MODIFICATION", fournisseur["statut"], fournisseur["statut"],
                    commentaire or "", acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def desactiver(fournisseur: dict, *, acteur: str = "", commentaire: str = "",
              version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    return _transition_statut(fournisseur, "INACTIF", acteur=acteur, commentaire=commentaire,
                              version_attendue=version_attendue, db_path=db_path)


def reactiver(fournisseur: dict, *, acteur: str = "", commentaire: str = "",
             version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    return _transition_statut(fournisseur, "ACTIF", acteur=acteur, commentaire=commentaire,
                              version_attendue=version_attendue, db_path=db_path)


def _transition_statut(fournisseur: dict, nouveau_statut: str, *, acteur: str, commentaire: str,
                       version_attendue: int | None, db_path) -> dict[str, Any]:
    opaque = fournisseur["fournisseur_id_opaque"]
    ancien = fournisseur["statut"]
    version_lue = fournisseur["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise FournisseurRefuse(
            f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if ancien == nouveau_statut:
        raise FournisseurRefuse(f"Le fournisseur est déjà au statut {nouveau_statut}.")

    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "UPDATE fournisseurs SET statut=?, version=version+1, date_modification=? "
            "WHERE fournisseur_id_opaque=? AND version=?",
            (nouveau_statut, _now(), opaque, version_lue))
        if cur.rowcount == 0:
            conn.rollback()
            raise FournisseurRefuse("Conflit de version — le fournisseur a été modifié entretemps.")
        type_evt = "DESACTIVATION" if nouveau_statut == "INACTIF" else "REACTIVATION"
        _journaliser(conn, opaque, type_evt, ancien, nouveau_statut, commentaire, acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def historique(fournisseur_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM fournisseur_evenements WHERE fournisseur_id_opaque=? ORDER BY id DESC",
            (fournisseur_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

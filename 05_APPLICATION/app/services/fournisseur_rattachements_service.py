"""Association historisée fournisseur ↔ logement (APP-3E) — SQLite isolée uniquement.

Relation DURABLE (période avec date de début/fin) entre un fournisseur du référentiel applicatif et
un logement, pour un type de prestation. Distincte de l'intervenant ménage du moteur (lecture seule)
et de l'affectation flat par charge. Ne porte aucun montant. Version optimiste, historique
append-only, aucune suppression physique. Refuse un fournisseur inactif, un chevauchement de période
ouverte, une version obsolète.
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import fournisseurs_referentiel_service as frs

TYPES_PRESTATION = {"MENAGE", "MAINTENANCE", "FOURNITURE", "AUTRE"}


class AssociationRefusee(Exception):
    """Action refusée (fournisseur inactif/inconnu, chevauchement, version obsolète, données invalides)."""


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def id_opaque(valeur: str) -> str:
    base = (cfg.PROPRIETAIRE_OPAQUE_SALT + "|FLG|" + _txt(valeur)).encode("utf-8")
    return "FLG-" + hashlib.sha256(base).hexdigest()[:10]


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def charger_par_opaque(association_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute("SELECT * FROM fournisseur_rattachements WHERE association_id_opaque=?",
                         (association_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def lister_par_logement(logement_id: str, actif_seul: bool = False, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        sql = "SELECT * FROM fournisseur_rattachements WHERE logement_id=?"
        if actif_seul:
            sql += " AND statut='ACTIF' AND date_fin IS NULL"
        sql += " ORDER BY date_debut DESC"
        return [dict(r) for r in conn.execute(sql, (logement_id,)).fetchall()]
    finally:
        conn.close()


def periode_ouverte(fournisseur_opaque: str, logement_id: str, type_prestation: str,
                    db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM fournisseur_rattachements WHERE fournisseur_id_opaque=? AND logement_id=? "
            "AND type_prestation=? AND date_fin IS NULL AND statut='ACTIF'",
            (fournisseur_opaque, logement_id, type_prestation)).fetchone()
        return _row(r)
    finally:
        conn.close()


def _journaliser(conn, opaque, type_evt, ancien, nouveau, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO fournisseur_rattachement_evenements "
        "(association_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)", (opaque, type_evt, ancien, nouveau, commentaire, acteur))


def associer(fournisseur_opaque: str, logement_id: str, *, type_prestation: str = "AUTRE",
             date_debut: str, motif: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    fournisseur_opaque = _txt(fournisseur_opaque)
    logement_id = _txt(logement_id)
    type_prestation = _txt(type_prestation).upper() or "AUTRE"
    date_debut = _txt(date_debut)
    if not logement_id:
        raise AssociationRefusee("Logement requis.")
    if type_prestation not in TYPES_PRESTATION:
        raise AssociationRefusee(f"Type de prestation invalide : « {type_prestation} ».")
    if not date_debut:
        raise AssociationRefusee("Date de début requise.")

    fournisseur = frs.charger_par_opaque(fournisseur_opaque, db_path)
    if fournisseur is None:
        raise AssociationRefusee("Fournisseur inconnu du référentiel.")
    if fournisseur.get("statut") != "ACTIF":
        raise AssociationRefusee("Fournisseur inactif — réactivez-le avant de l'associer.")
    if periode_ouverte(fournisseur_opaque, logement_id, type_prestation, db_path) is not None:
        raise AssociationRefusee(
            "Une période ouverte existe déjà pour ce fournisseur/logement/type — fermez-la d'abord.")

    opaque = id_opaque(f"{fournisseur_opaque}|{logement_id}|{type_prestation}|{datetime.now().timestamp()}")
    conn = get_db(db_path)
    try:
        try:
            conn.execute(
                "INSERT INTO fournisseur_rattachements (association_id_opaque, fournisseur_id_opaque, "
                "logement_id, type_prestation, date_debut, motif, acteur) VALUES (?,?,?,?,?,?,?)",
                (opaque, fournisseur_opaque, logement_id, type_prestation, date_debut, motif, acteur))
            _journaliser(conn, opaque, "CREATION", None, "ACTIF", motif, acteur)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            raise AssociationRefusee("Chevauchement de période ouverte refusé.")
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def fermer(association: dict, date_fin: str, *, motif: str = "", acteur: str = "",
          version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    opaque = association["association_id_opaque"]
    version_lue = association["version"]
    date_fin = _txt(date_fin)
    if version_attendue is not None and version_attendue != version_lue:
        raise AssociationRefusee(f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if not date_fin:
        raise AssociationRefusee("Date de fin requise.")
    if association["date_fin"] is not None:
        raise AssociationRefusee("Période déjà fermée.")

    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "UPDATE fournisseur_rattachements SET date_fin=?, motif=COALESCE(?, motif), "
            "version=version+1, date_modification=? WHERE association_id_opaque=? AND version=?",
            (date_fin, motif or None, _now(), opaque, version_lue))
        if cur.rowcount == 0:
            conn.rollback()
            raise AssociationRefusee("Conflit de version — l'association a été modifiée entretemps.")
        _journaliser(conn, opaque, "FERMETURE", "ACTIF", "ACTIF", motif, acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def changer_fournisseur(logement_id: str, ancien_association: dict, nouveau_fournisseur_opaque: str, *,
                        date_bascule: str, type_prestation: str = "AUTRE", motif: str = "",
                        acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ferme l'ancienne période à `date_bascule` puis ouvre la nouvelle le même jour — jamais deux
    périodes ouvertes en parallèle."""
    fermer(ancien_association, date_bascule, motif=motif, acteur=acteur,
           version_attendue=ancien_association["version"], db_path=db_path)
    return associer(nouveau_fournisseur_opaque, logement_id, type_prestation=type_prestation,
                    date_debut=date_bascule, motif=motif, acteur=acteur, db_path=db_path)


def desactiver(association: dict, *, motif: str = "", acteur: str = "",
              version_attendue: int | None = None, db_path=None) -> dict[str, Any]:
    opaque = association["association_id_opaque"]
    version_lue = association["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise AssociationRefusee(f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if association["statut"] == "INACTIF":
        raise AssociationRefusee("Association déjà inactive.")
    conn = get_db(db_path)
    try:
        cur = conn.execute(
            "UPDATE fournisseur_rattachements SET statut='INACTIF', version=version+1, date_modification=? "
            "WHERE association_id_opaque=? AND version=?", (_now(), opaque, version_lue))
        if cur.rowcount == 0:
            conn.rollback()
            raise AssociationRefusee("Conflit de version.")
        _journaliser(conn, opaque, "DESACTIVATION", "ACTIF", "INACTIF", motif, acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(opaque, db_path)


def historique(association_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM fournisseur_rattachement_evenements WHERE association_id_opaque=? ORDER BY id DESC",
            (association_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

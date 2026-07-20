"""Service suivi humain des relevés propriétaires (APP-3D) — jamais la facture réelle.

La vérité financière reste exclusivement dans le moteur (MASTER_CALC_NetProprietaire,
MASTER_CALC_Commissions, MASTER_FACT_Proprietaires) — jamais recalculée ni écrite ici. Ce module
journalise uniquement le statut de facturation humain, l'historique et les décisions, dans une base
SQLite isolée. Même garde de version optimiste atomique et même schéma de transaction que
`clotures_service.py` (APP-5C), pattern déjà éprouvé, réutilisé tel quel.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db

_RE_MOIS = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

ST_NON_CONCERNE = "NON_CONCERNE"
ST_A_FACTURER = "A_FACTURER"
ST_FACTURE = "FACTURE"
ST_AVOIR_A_EMETTRE = "AVOIR_A_EMETTRE"
ST_AVOIR_EMIS = "AVOIR_EMIS"

STATUTS = {ST_NON_CONCERNE, ST_A_FACTURER, ST_FACTURE, ST_AVOIR_A_EMETTRE, ST_AVOIR_EMIS}
STATUTS_LIBELLES = {
    ST_NON_CONCERNE: "Non concerné", ST_A_FACTURER: "À facturer", ST_FACTURE: "Facturé",
    ST_AVOIR_A_EMETTRE: "Avoir à émettre", ST_AVOIR_EMIS: "Avoir émis",
}
TRANSITIONS: dict[str, set[str]] = {
    ST_NON_CONCERNE: {ST_A_FACTURER},
    ST_A_FACTURER: {ST_FACTURE},
    ST_FACTURE: {ST_AVOIR_A_EMETTRE, ST_A_FACTURER},   # ST_A_FACTURER = réouverture
    ST_AVOIR_A_EMETTRE: {ST_AVOIR_EMIS},
    ST_AVOIR_EMIS: set(),
}


class ReleveRefuse(Exception):
    """Action refusée (transition invalide, bloqueur présent, justification manquante)."""


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def id_opaque(prefixe: str, valeur: str) -> str:
    base = (cfg.PROPRIETAIRE_OPAQUE_SALT + "|" + prefixe + "|" + _txt(valeur)).encode("utf-8")
    return prefixe + "-" + hashlib.sha256(base).hexdigest()[:10]


def releve_id_opaque(proprietaire_id: str, mois: str) -> str:
    return id_opaque("REG", f"{_txt(proprietaire_id)}|{_txt(mois)}")


def mois_valide(mois: str) -> bool:
    return bool(_RE_MOIS.match(_txt(mois)))


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def charger_par_prop_mois(proprietaire_id: str, mois: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM proprietaires_releves WHERE proprietaire_id=? AND mois=? AND actif=1",
            (proprietaire_id, mois)).fetchone()
        return _row(r)
    finally:
        conn.close()


def charger_par_opaque(releve_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM proprietaires_releves WHERE releve_id_opaque=? AND actif=1",
            (releve_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def lister(mois: str = "", db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        if mois:
            rows = conn.execute(
                "SELECT * FROM proprietaires_releves WHERE actif=1 AND mois=? "
                "ORDER BY mois DESC, proprietaire_id", (mois,)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM proprietaires_releves WHERE actif=1 "
                "ORDER BY mois DESC, proprietaire_id").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def historique(releve_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM proprietaires_releve_evenements WHERE releve_id_opaque=? ORDER BY id DESC",
            (releve_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _journaliser_evenement(conn, releve_opaque, type_evt, ancien, nouveau, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO proprietaires_releve_evenements "
        "(releve_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)",
        (releve_opaque, type_evt, ancien, nouveau, commentaire, acteur))


def creer_ou_charger(proprietaire_id: str, mois: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée le relevé (proprietaire_id, mois) si absent (idempotent), sinon retourne l'existant.

    Refuse tout mois hors format strict AAAA-MM et tout proprietaire_id vide (jamais de ligne créée
    pour une valeur malformée — évite aussi toute injection dans le nom de fichier d'export)."""
    proprietaire_id = _txt(proprietaire_id)
    if not proprietaire_id:
        raise ReleveRefuse("Identifiant propriétaire manquant.")
    if not mois_valide(mois):
        raise ReleveRefuse(f"Mois invalide : « {mois} ». Format attendu AAAA-MM.")
    mois = _txt(mois)
    existante = charger_par_prop_mois(proprietaire_id, mois, db_path)
    if existante:
        return existante
    opaque = releve_id_opaque(proprietaire_id, mois)
    conn = get_db(db_path)
    try:
        try:
            conn.execute(
                "INSERT INTO proprietaires_releves (releve_id_opaque, proprietaire_id, mois, "
                "statut_facturation, cree_par) VALUES (?,?,?,?,?)",
                (opaque, proprietaire_id, mois, ST_NON_CONCERNE, acteur))
            _journaliser_evenement(conn, opaque, "CREATION", None, ST_NON_CONCERNE, acteur=acteur)
            conn.commit()
        except sqlite3.IntegrityError:
            # Course gagnée par une requête concurrente qui a créé le même (propriétaire, mois)
            # entretemps — la contrainte UNIQUE de la base tranche, pas Python.
            conn.rollback()
    finally:
        conn.close()
    return charger_par_prop_mois(proprietaire_id, mois, db_path)


def _transition(releve: dict, nouveau_statut: str, *, acteur: str = "", commentaire: str = "",
                justification: str = "", version_attendue: int | None = None, db_path=None,
                extra_cols: dict | None = None, conn=None) -> dict[str, Any]:
    """Transition atomique : garde de version appliquée par SQL (`WHERE ... AND version=?`), pas
    seulement vérifiée côté Python — élimine la fenêtre de course entre deux requêtes concurrentes."""
    opaque = releve["releve_id_opaque"]
    ancien = releve["statut_facturation"]
    if nouveau_statut not in TRANSITIONS.get(ancien, set()):
        raise ReleveRefuse(f"Transition {ancien} → {nouveau_statut} interdite.")

    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        version_lue = releve["version"]
        if version_attendue is not None and version_attendue != version_lue:
            raise ReleveRefuse(
                f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")

        cols = ["statut_facturation = ?", "version = version + 1"]
        vals: list[Any] = [nouveau_statut]
        if extra_cols:
            for k, v in extra_cols.items():
                cols.append(f"{k} = ?")
                vals.append(v)
        vals.append(opaque)
        vals.append(version_lue)
        cur = conn.execute(
            f"UPDATE proprietaires_releves SET {', '.join(cols)} "
            f"WHERE releve_id_opaque = ? AND version = ?", vals)
        if cur.rowcount == 0:
            if connexion_locale:
                conn.rollback()
            raise ReleveRefuse(
                f"Conflit de version — le relevé a été modifié entretemps (version {version_lue} "
                "attendue). Rechargez la page et réessayez.")
        _journaliser_evenement(conn, opaque, "TRANSITION", ancien, nouveau_statut,
                               commentaire=commentaire or justification, acteur=acteur)
        if connexion_locale:
            conn.commit()
            resultat = charger_par_opaque(opaque, db_path)
        else:
            r = conn.execute(
                "SELECT * FROM proprietaires_releves WHERE releve_id_opaque=?", (opaque,)).fetchone()
            resultat = _row(r)
    except Exception:
        if connexion_locale:
            conn.rollback()
        raise
    finally:
        if connexion_locale:
            conn.close()
    return resultat


def marquer_a_facturer(releve: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    return _transition(releve, ST_A_FACTURER, acteur=acteur, version_attendue=version_attendue,
                       db_path=db_path, extra_cols={"date_preparation": _now()})


def marquer_facture(releve: dict, *, acteur: str = "", commentaire: str = "", version_attendue=None,
                    db_path=None):
    if not commentaire.strip():
        raise ReleveRefuse("Commentaire de validation requis avant de marquer « Facturé ».")
    return _transition(releve, ST_FACTURE, acteur=acteur, commentaire=commentaire,
                       version_attendue=version_attendue, db_path=db_path,
                       extra_cols={"date_validation": _now(), "commentaire_validation": commentaire,
                                  "valide_par": acteur})


def marquer_avoir_a_emettre(releve: dict, *, acteur: str = "", motif: str = "",
                            version_attendue=None, db_path=None):
    if not motif.strip():
        raise ReleveRefuse("Motif requis pour demander un avoir.")
    return _transition(releve, ST_AVOIR_A_EMETTRE, acteur=acteur, commentaire=motif,
                       version_attendue=version_attendue, db_path=db_path)


def marquer_avoir_emis(releve: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    return _transition(releve, ST_AVOIR_EMIS, acteur=acteur, version_attendue=version_attendue,
                       db_path=db_path)


def rouvrir(releve: dict, *, acteur: str = "", justification: str = "", version_attendue=None,
           db_path=None):
    """Réouverture d'un relevé FACTURÉ (retour à A_FACTURER, historique conservé)."""
    if not justification.strip():
        raise ReleveRefuse("Justification requise pour rouvrir un relevé facturé.")
    return _transition(releve, ST_A_FACTURER, acteur=acteur, justification=justification,
                       version_attendue=version_attendue, db_path=db_path,
                       extra_cols={"date_reouverture": _now(),
                                  "justification_reouverture": justification})

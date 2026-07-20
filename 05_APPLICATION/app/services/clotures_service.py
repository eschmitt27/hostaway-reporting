"""Service clôture mensuelle (APP-5C) — suivi humain uniquement, jamais la clôture réelle.

La clôture RÉELLE reste exclusivement pilotée par REF_Cloture_Mensuelle (REF_Setup.xlsm, moteur,
D024) : statuts OUVERT/EN_CONTROLE/CLOTURE, jamais écrits par ce module. Ici, un automate SQLite
distinct journalise la PRÉPARATION et la VALIDATION HUMAINE (revue des contrôles APP-5B, checklist,
décision de passage) — jamais une nouvelle vérité, jamais une écriture réelle. Composé exclusivement
à partir des services APP-5B existants (aucune duplication de la logique de contrôle).
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import controles_actionnable_service as act

_RE_MOIS = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# ── Automate ─────────────────────────────────────────────────────────────────
ST_NON_DEMARREE = "NON_DEMARREE"
ST_EN_PREPARATION = "EN_PREPARATION"
ST_A_VALIDER = "A_VALIDER"
ST_VALIDEE = "VALIDEE"
ST_ROUVERTE = "ROUVERTE"
ST_ARCHIVEE = "ARCHIVEE"

STATUTS = {ST_NON_DEMARREE, ST_EN_PREPARATION, ST_A_VALIDER, ST_VALIDEE, ST_ROUVERTE, ST_ARCHIVEE}
STATUTS_LIBELLES = {
    ST_NON_DEMARREE: "Non démarrée", ST_EN_PREPARATION: "En préparation", ST_A_VALIDER: "À valider",
    ST_VALIDEE: "Validée", ST_ROUVERTE: "Rouverte", ST_ARCHIVEE: "Archivée",
}
TRANSITIONS: dict[str, set[str]] = {
    ST_NON_DEMARREE: {ST_EN_PREPARATION},
    ST_EN_PREPARATION: {ST_A_VALIDER},
    ST_A_VALIDER: {ST_EN_PREPARATION, ST_VALIDEE},
    ST_VALIDEE: {ST_ROUVERTE, ST_ARCHIVEE},
    ST_ROUVERTE: {ST_EN_PREPARATION},
    ST_ARCHIVEE: set(),
}


class ClotureRefusee(Exception):
    """Action de clôture refusée (transition invalide, bloqueur présent, justification manquante)."""


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Identifiants opaques ──────────────────────────────────────────────────────

def id_opaque(prefixe: str, valeur: str) -> str:
    base = (cfg.CLOTURE_OPAQUE_SALT + "|" + prefixe + "|" + _txt(valeur)).encode("utf-8")
    return prefixe + "-" + hashlib.sha256(base).hexdigest()[:10]


def cloture_id_opaque(mois: str) -> str:
    return id_opaque("CLO", mois)


def mois_valide(mois: str) -> bool:
    """Format strict AAAA-MM (janvier=01 à décembre=12). Aucune autre forme acceptée."""
    return bool(_RE_MOIS.match(_txt(mois)))


# ── Lecture ──────────────────────────────────────────────────────────────────

def _row(r) -> dict[str, Any]:
    return dict(r) if r is not None else None


def charger_par_mois(mois: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM clotures_mensuelles WHERE mois=? AND actif=1", (mois,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def charger_par_opaque(cloture_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=? AND actif=1",
            (cloture_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def lister(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM clotures_mensuelles WHERE actif=1 ORDER BY mois DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def historique(cloture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_evenements WHERE cloture_id_opaque=? ORDER BY id DESC",
            (cloture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Progression / bloqueurs (composé depuis APP-5B, aucune duplication) ──────

def elements_du_mois(mois: str, db_path=None) -> list[dict[str, Any]]:
    tous = act._tous_les_elements(db_path)
    return [e for e in tous if e["mois"] == mois]


def calcul_progression(mois: str, db_path=None) -> dict[str, Any]:
    els = elements_du_mois(mois, db_path)
    anomalies = [e for e in els if not e["est_info"]]
    bloqueurs = [e for e in anomalies
                if e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]]
    return {
        "mois": mois,
        "nb_total": len(els),
        "nb_anomalies": len(anomalies),
        "nb_bloqueurs": len(bloqueurs),
        "nb_a_traiter": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "OUVERT"),
        "nb_en_cours": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "EN_COURS"),
        "nb_resolus": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "RESOLU"),
        "nb_exceptions": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "ACCEPTE_AVEC_JUSTIFICATION"),
        "nb_reapparus": sum(1 for e in anomalies if e["etat"]["statut_suivi"] == "ROUVERT"),
        "nb_informatifs": sum(1 for e in els if e["est_info"]),
        "bloqueurs": bloqueurs,
        "cloturable": len(bloqueurs) == 0,
    }


# ── Création / transition ────────────────────────────────────────────────────

def _journaliser_evenement(conn, cloture_opaque, type_evt, ancien, nouveau, commentaire="",
                           preuve="", acteur=""):
    conn.execute(
        "INSERT INTO cloture_evenements (cloture_id_opaque, type_evenement, ancien_statut, "
        "nouveau_statut, commentaire, preuve, acteur) VALUES (?,?,?,?,?,?,?)",
        (cloture_opaque, type_evt, ancien, nouveau, commentaire, preuve, acteur))


def creer_ou_charger(mois: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée la clôture du mois si absente (idempotent), sinon retourne l'existante. Unicité mois.

    Refuse tout mois hors format strict AAAA-MM (jamais de ligne créée pour une valeur malformée —
    évite aussi toute injection dans le nom de fichier d'export ou les en-têtes HTTP)."""
    if not mois_valide(mois):
        raise ClotureRefusee(f"Mois invalide : « {mois} ». Format attendu AAAA-MM.")
    mois = _txt(mois)
    existante = charger_par_mois(mois, db_path)
    if existante:
        return existante
    opaque = cloture_id_opaque(mois)
    conn = get_db(db_path)
    try:
        try:
            conn.execute(
                "INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
                "VALUES (?,?,?,?)", (opaque, mois, ST_NON_DEMARREE, acteur))
            _journaliser_evenement(conn, opaque, "CREATION", None, ST_NON_DEMARREE, acteur=acteur)
            conn.commit()
        except sqlite3.IntegrityError:
            # Course gagnée par une requête concurrente qui a créé le même mois entretemps
            # (deux clics simultanés) — la contrainte UNIQUE de la base tranche, pas Python.
            conn.rollback()
    finally:
        conn.close()
    return charger_par_mois(mois, db_path)


def _transition(cloture: dict, nouveau_statut: str, *, acteur: str = "", commentaire: str = "",
                justification: str = "", version_attendue: int | None = None, db_path=None,
                extra_cols: dict | None = None, conn=None) -> dict[str, Any]:
    """Transition atomique : la garde de version est appliquée par SQL (`WHERE ... AND version=?`),
    pas seulement vérifiée côté Python — élimine la fenêtre de course (TOCTOU) entre deux requêtes
    concurrentes qui liraient le même état avant d'écrire (double clic, requêtes simultanées).

    `version_attendue` explicite (si fourni) doit correspondre à la version chargée par l'appelant ;
    la garde SQL utilise toujours `cloture["version"]` (la version réellement lue), jamais une valeur
    non vérifiée.
    """
    ancien = cloture["statut"]
    if nouveau_statut not in TRANSITIONS.get(ancien, set()):
        raise ClotureRefusee(f"Transition {ancien} → {nouveau_statut} interdite.")
    version_lue = cloture["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise ClotureRefusee(
            f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    opaque = cloture["cloture_id_opaque"]
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        cols = ["statut = ?", "version = version + 1"]
        vals: list[Any] = [nouveau_statut]
        extra_cols = extra_cols or {}
        for k, v in extra_cols.items():
            cols.append(f"{k} = ?")
            vals.append(v)
        vals.append(opaque)
        vals.append(version_lue)
        cur = conn.execute(
            f"UPDATE clotures_mensuelles SET {', '.join(cols)} "
            f"WHERE cloture_id_opaque = ? AND version = ?", vals)
        if cur.rowcount == 0:
            # Personne d'autre n'a pu écrire entre notre lecture et notre écriture SANS que ce
            # contrôle l'attrape : conflit réel (rejeu, double clic concurrent, écriture parallèle).
            if connexion_locale:
                conn.rollback()
            raise ClotureRefusee(
                f"Conflit de version — la clôture a été modifiée entretemps (version {version_lue} "
                "attendue). Rechargez la page et réessayez.")
        _journaliser_evenement(conn, opaque, "TRANSITION", ancien, nouveau_statut,
                               commentaire=commentaire or justification, acteur=acteur)
        if connexion_locale:
            conn.commit()
            resultat = charger_par_opaque(opaque, db_path)
        else:
            # Connexion partagée (transaction non encore commitée par l'appelant) : relire via la
            # MÊME connexion — une connexion séparée ne verrait pas l'écriture non commitée (WAL).
            r = conn.execute(
                "SELECT * FROM clotures_mensuelles WHERE cloture_id_opaque=?", (opaque,)).fetchone()
            resultat = _row(r)
    finally:
        if connexion_locale:
            conn.close()
    return resultat


def demarrer_preparation(cloture: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    return _transition(cloture, ST_EN_PREPARATION, acteur=acteur, version_attendue=version_attendue,
                       db_path=db_path, extra_cols={"date_preparation": _now()})


def passer_a_valider(cloture: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    """Passage à A_VALIDER + snapshot — ATOMIQUE (une seule transaction SQL, un seul commit).

    Sans cela, un arrêt du processus entre la transition et le snapshot laisserait une clôture en
    A_VALIDER sans instantané figé. La garde de version protège aussi ce passage contre un double
    déclenchement concurrent."""
    conn = get_db(db_path)
    try:
        res = _transition(cloture, ST_A_VALIDER, acteur=acteur, version_attendue=version_attendue,
                          db_path=db_path, conn=conn)
        snapshot(res, db_path=db_path, conn=conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger_par_opaque(cloture["cloture_id_opaque"], db_path)


def valider(cloture: dict, *, acteur: str = "", commentaire: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    """Valide la clôture : refuse si un bloqueur subsiste (jamais d'écriture réelle)."""
    if not commentaire.strip():
        raise ClotureRefusee("Commentaire de validation requis.")
    progression = calcul_progression(cloture["mois"], db_path)
    if not progression["cloturable"]:
        raise ClotureRefusee(
            f"{progression['nb_bloqueurs']} bloqueur(s) subsiste(nt) — validation refusée.")
    return _transition(cloture, ST_VALIDEE, acteur=acteur, commentaire=commentaire,
                       version_attendue=version_attendue, db_path=db_path,
                       extra_cols={"date_validation": _now(), "commentaire_validation": commentaire,
                                   "valide_par": acteur})


def rouvrir(cloture: dict, *, acteur: str = "", justification: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    if not justification.strip():
        raise ClotureRefusee("Justification requise pour rouvrir une clôture validée.")
    return _transition(cloture, ST_ROUVERTE, acteur=acteur, justification=justification,
                       version_attendue=version_attendue, db_path=db_path,
                       extra_cols={"date_reouverture": _now(), "justification_reouverture": justification})


def archiver(cloture: dict, *, acteur: str = "", version_attendue=None, db_path=None):
    return _transition(cloture, ST_ARCHIVEE, acteur=acteur, version_attendue=version_attendue,
                       db_path=db_path)


# ── Snapshot ──────────────────────────────────────────────────────────────────

def snapshot(cloture: dict, db_path=None, conn=None) -> int:
    """Persiste un instantané logique FIGÉ des contrôles du mois (jamais de fichier métier réel).

    Écrit toujours (jamais recalculé silencieusement après coup) : `snapshot_actif` relit ensuite
    ces lignes, pas l'état courant du moteur. `conn` optionnel permet de partager la transaction avec
    la transition qui déclenche ce snapshot (atomicité passer_a_valider)."""
    opaque = cloture["cloture_id_opaque"]
    els = elements_du_mois(cloture["mois"], db_path)
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        conn.execute("UPDATE cloture_elements SET actif=0 WHERE cloture_id_opaque=? AND actif=1",
                    (opaque,))
        for e in els:
            conn.execute(
                "INSERT INTO cloture_elements (cloture_id_opaque, ctrl_opaque, code_controle, "
                "entite_type, entite_opaque, severite, statut_moteur, statut_humain, bloque_cloture) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (opaque, e["ctrl_opaque"], e["code"], e["module"], e["entite_id"], e["niveau"],
                 "PRESENTE" if e["etat"]["anomalie_moteur_presente"] else "ABSENTE",
                 e["etat"]["statut_suivi"],
                 1 if (e["etat"]["anomalie_moteur_presente"] and not e["etat"]["exception_active"]
                       and not e["est_info"]) else 0))
        if connexion_locale:
            conn.commit()
        return len(els)
    finally:
        if connexion_locale:
            conn.close()


def snapshot_actif(cloture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_elements WHERE cloture_id_opaque=? AND actif=1",
            (cloture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Documents (preuves) ───────────────────────────────────────────────────────

def ajouter_document(cloture_opaque: str, nom_logique: str, type_document: str = "PREUVE",
                     chemin_logique: str = "", db_path=None) -> str:
    from app.services.path_sanitizer import sanitize_text
    doc_opaque = id_opaque("DOC", cloture_opaque + nom_logique + _now())
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO cloture_documents (document_id_opaque, cloture_id_opaque, nom_logique, "
            "type_document, chemin_logique) VALUES (?,?,?,?,?)",
            (doc_opaque, cloture_opaque, nom_logique, type_document, sanitize_text(chemin_logique)))
        conn.execute(
            "INSERT INTO cloture_evenements (cloture_id_opaque, type_evenement, commentaire, preuve) "
            "VALUES (?,?,?,?)", (cloture_opaque, "PREUVE_AJOUTEE", nom_logique, doc_opaque))
        conn.commit()
    finally:
        conn.close()
    return doc_opaque


def documents(cloture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM cloture_documents WHERE cloture_id_opaque=? AND actif=1",
            (cloture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()

"""Périodes comptables et clôture (migration `0023`).

Distinctes de la clôture APPLICATIVE du pilotage des calculs (`clotures_mensuelles`, migration
`0008`) : celle-ci porte sur l'exécution des lots de calcul, celle-ci porte sur les ÉCRITURES
comptables. Une période comptable clôturée refuse toute écriture directe sur cette période — seule
une contrepassation ou une réouverture formelle permet une correction.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

ST_OUVERTE = "OUVERTE"
ST_EN_CONTROLE = "EN_CONTROLE"
ST_VALIDEE = "VALIDEE"
ST_CLOTUREE = "CLOTUREE"
ST_ROUVERTE = "ROUVERTE"
STATUTS = (ST_OUVERTE, ST_EN_CONTROLE, ST_VALIDEE, ST_CLOTUREE, ST_ROUVERTE)

TRANSITIONS: dict[str, set[str]] = {
    ST_OUVERTE: {ST_EN_CONTROLE},
    ST_EN_CONTROLE: {ST_OUVERTE, ST_VALIDEE},
    ST_VALIDEE: {ST_CLOTUREE, ST_EN_CONTROLE},
    ST_CLOTUREE: {ST_ROUVERTE},
    ST_ROUVERTE: {ST_EN_CONTROLE},
}

# Statuts sous lesquels une écriture directe sur la période est refusée.
STATUTS_FERMES = (ST_CLOTUREE,)

E_FLAGS = "E_FLAGS_DESACTIVES"
E_PERIODE_FORMAT = "V01_PERIODE_FORMAT_INVALIDE"
E_INTROUVABLE = "E01_PERIODE_INTROUVABLE"
E_STATUT = "E02_TRANSITION_INTERDITE"
E_CLOTURE_BLOQUEE = "E03_CLOTURE_BLOQUEE_PAR_CONTROLES"
E_REOUVERTURE_SANS_JUSTIFICATION = "E04_REOUVERTURE_SANS_JUSTIFICATION"
E_PERIODE_CLOTUREE = "E05_PERIODE_CLOTUREE_ECRITURE_REFUSEE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation.",
    E_PERIODE_FORMAT: "La période doit être au format AAAA-MM.",
    E_INTROUVABLE: "Période comptable introuvable.",
    E_STATUT: "Transition de statut interdite.",
    E_CLOTURE_BLOQUEE: "Clôture refusée : des contrôles bloquants ou des anomalies subsistent.",
    E_REOUVERTURE_SANS_JUSTIFICATION: "La réouverture d'une période clôturée exige une justification.",
    E_PERIODE_CLOTUREE: "Cette période est clôturée : aucune écriture directe n'est autorisée.",
}


def _flags_actifs() -> bool:
    return bool(cfg.COMPTABILITE_REAL_WRITE_ENABLED and cfg.COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED)


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _valide_periode(periode: str) -> bool:
    if not periode or len(periode) != 7 or periode[4] != "-":
        return False
    try:
        annee, mois = int(periode[:4]), int(periode[5:7])
        return 2000 <= annee <= 2100 and 1 <= mois <= 12
    except ValueError:
        return False


def _evenement(conn, periode: str, ancien: str | None, nouveau: str | None, *,
              commentaire: str = "", resume: dict | None = None, acteur: str = "") -> None:
    conn.execute(
        "INSERT INTO periode_evenements (periode, type_evenement, ancien_statut, nouveau_statut, "
        "commentaire, resume_json, acteur) VALUES (?,?,?,?,?,?,?)",
        (periode, "TRANSITION", ancien, nouveau, commentaire or None,
         json.dumps(resume) if resume else None, acteur or "local"))


def obtenir_ou_creer(periode: str, db_path=None) -> dict[str, Any]:
    """Une période comptable existe implicitement dès qu'on la consulte (OUVERTE par défaut) —
    pas de création manuelle requise pour commencer à travailler dessus."""
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM periodes_comptables WHERE periode=?", (periode,)).fetchone()
        if row is not None:
            return dict(row)
        conn.execute("INSERT OR IGNORE INTO periodes_comptables (periode, statut) VALUES (?, ?)",
                    (periode, ST_OUVERTE))
        conn.commit()
        row = conn.execute("SELECT * FROM periodes_comptables WHERE periode=?", (periode,)).fetchone()
    finally:
        conn.close()
    return dict(row)


def charger(periode: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM periodes_comptables WHERE periode=?", (periode,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def lister(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM periodes_comptables ORDER BY periode DESC").fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def historique(periode: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM periode_evenements WHERE periode=? ORDER BY id DESC", (periode,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def est_fermee(periode: str, db_path=None) -> bool:
    """Une période sans ligne (jamais consultée) est réputée OUVERTE — le statut fermé n'existe
    qu'après une clôture explicite."""
    p = charger(periode, db_path)
    return bool(p and p["statut"] in STATUTS_FERMES)


def _transition(periode: str, cible: str, *, commentaire: str = "", resume: dict | None = None,
                acteur: str = "", db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if not _valide_periode(periode):
        return _refus(E_PERIODE_FORMAT, periode)
    p = obtenir_ou_creer(periode, db_path)
    ancien = p["statut"]
    if cible not in TRANSITIONS.get(ancien, set()):
        return _refus(E_STATUT, f"{ancien} -> {cible}")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE periodes_comptables SET statut=?, date_modification=?, version=version+1 "
                     "WHERE periode=?", (cible, _now(), periode))
        _evenement(conn, periode, ancien, cible, commentaire=commentaire, resume=resume, acteur=acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "periode": periode, "statut": cible}


def passer_en_controle(periode: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    return _transition(periode, ST_EN_CONTROLE, acteur=acteur, db_path=db_path)


def rouvrir_pour_controle(periode: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """EN_CONTROLE -> OUVERTE (avant validation) ou ROUVERTE -> EN_CONTROLE (après réouverture
    formelle) : les deux sont gérées, la table TRANSITIONS distingue les deux départs possibles."""
    p = charger(periode, db_path)
    if p and p["statut"] == ST_ROUVERTE:
        return _transition(periode, ST_EN_CONTROLE, acteur=acteur, db_path=db_path)
    return _transition(periode, ST_OUVERTE, acteur=acteur, db_path=db_path)


def valider(periode: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    return _transition(periode, ST_VALIDEE, acteur=acteur, db_path=db_path)


def cloturer(periode: str, *, acteur: str = "", commentaire: str = "", db_path=None) -> dict[str, Any]:
    """Clôture une période VALIDEE. Refuse si des contrôles comptables BLOQUANTS subsistent sur
    cette période (cf. `comptabilite_controles_service.controler`)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    from app.services import comptabilite_controles_service as ctrl
    rapport = ctrl.controler(periode=periode, db_path=db_path)
    bloquants = [a for a in rapport["anomalies"] if a["severite"] == ctrl.BLOQUANT]
    if bloquants:
        return _refus(E_CLOTURE_BLOQUEE, f"{len(bloquants)} anomalie(s) bloquante(s)")
    resume = {
        "nb_ecritures": rapport["nb_ecritures"],
        "total_debit": rapport["total_debit"],
        "total_credit": rapport["total_credit"],
        "nb_anomalies": len(rapport["anomalies"]),
    }
    return _transition(periode, ST_CLOTUREE, commentaire=commentaire, resume=resume,
                       acteur=acteur, db_path=db_path)


def rouvrir(periode: str, *, justification: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    if not justification or not justification.strip():
        return _refus(E_REOUVERTURE_SANS_JUSTIFICATION)
    return _transition(periode, ST_ROUVERTE, commentaire=justification, acteur=acteur, db_path=db_path)

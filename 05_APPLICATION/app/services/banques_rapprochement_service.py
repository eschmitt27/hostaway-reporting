"""Rapprochement mouvement bancaire ↔ objet métier (module Banque, suite APP-4A/4B).

Généralise le patron déjà éprouvé de `rapprochements_reglements` (migration 0014 : identifiants
opaques, critères explicables, statuts, historique append-only) aux objets réellement rapprochables
d'un mouvement bancaire : réservation, payout plateforme, charge fournisseur, règlement de charge,
reversement propriétaire, remboursement associé, remboursement voyageur, mouvement interne, ou
non identifié.

Principes :
- persistant (SQLite, migration 0015) — jamais un lien uniquement visuel ;
- pas de relation 1-1 forcée : plusieurs lignes actives possibles par mouvement (mouvement couvrant
  plusieurs objets) et par objet (objet réglé par plusieurs mouvements) — le contrôle porte sur la
  SOMME des montants rapprochés actifs, jamais sur un couple unique ;
- un mouvement déjà rapproché à 100 % de son montant ne peut plus recevoir de nouveau lien tant que
  le lien existant n'a pas été annulé (contrôle « double rapprochement ») ;
- une proposition automatique n'est jamais silencieusement validée : elle reste au statut PROPOSE
  jusqu'à confirmation humaine explicite.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

TYPES_OBJET = (
    "RESERVATION", "PAYOUT_PLATEFORME", "CHARGE_FOURNISSEUR", "REGLEMENT_CHARGE",
    "REVERSEMENT_PROPRIETAIRE", "REMBOURSEMENT_ASSOCIE", "REMBOURSEMENT_VOYAGEUR",
    "MOUVEMENT_INTERNE", "NON_IDENTIFIE",
)

ST_PROPOSE = "PROPOSE"
ST_CONFIRME = "CONFIRME"
ST_REFUSE = "REFUSE"
ST_ANNULE = "ANNULE"
STATUTS_ACTIFS = (ST_PROPOSE, ST_CONFIRME)

E_TYPE_OBJET_INCONNU = "V01_TYPE_OBJET_INCONNU"
E_MONTANT_INVALIDE = "V02_MONTANT_INVALIDE"
E_DEPASSEMENT = "V03_DEPASSEMENT_MONTANT"
E_DEJA_RAPPROCHE = "V04_MOUVEMENT_DEJA_RAPPROCHE_INTEGRALEMENT"
E_INTROUVABLE = "E01_RAPPROCHEMENT_INTROUVABLE"

MESSAGES = {
    E_TYPE_OBJET_INCONNU: "Type d'objet inconnu.",
    E_MONTANT_INVALIDE: "Le montant rapproché doit être un nombre strictement positif.",
    E_DEPASSEMENT: "Le montant rapproché dépasserait le montant du mouvement.",
    E_DEJA_RAPPROCHE: "Ce mouvement est déjà rapproché pour l'intégralité de son montant.",
    E_INTROUVABLE: "Rapprochement introuvable.",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code)}


def montant_deja_rapproche(mouvement_id_opaque: str, db_path=None,
                          exclure_id_opaque: str = "") -> float:
    """Somme des montants rapprochés ACTIFS (PROPOSE + CONFIRME) pour ce mouvement."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT montant_rapproche, rapprochement_id_opaque FROM banque_rapprochements "
            "WHERE mouvement_id_opaque=? AND statut IN (?,?)",
            (mouvement_id_opaque, ST_PROPOSE, ST_CONFIRME)).fetchall()
    finally:
        conn.close()
    return sum(r["montant_rapproche"] for r in rows
              if r["rapprochement_id_opaque"] != exclure_id_opaque)


def lister(mouvement_id_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_rapprochements WHERE mouvement_id_opaque=? "
            "ORDER BY id DESC", (mouvement_id_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _evenement(conn, opaque: str, type_evt: str, ancien: str | None, nouveau: str | None,
              commentaire: str, acteur: str) -> None:
    conn.execute(
        "INSERT INTO banque_rapprochement_evenements "
        "(rapprochement_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)",
        (opaque, type_evt, ancien, nouveau, commentaire, acteur or "local"))


def enregistrer(mouvement_id_opaque: str, type_objet: str, objet_id: str, montant_rapproche: float,
                *, montant_mouvement: float, statut: str = ST_PROPOSE, source: str = "MANUEL",
                score_explicable: int | None = None, criteres: dict | None = None,
                commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Crée un lien de rapprochement. `montant_mouvement` = |montant| du mouvement (fourni par
    l'appelant, jamais recalculé ici — la vérité du montant reste NORM_Banque)."""
    if type_objet not in TYPES_OBJET:
        return _refus(E_TYPE_OBJET_INCONNU)
    try:
        montant_f = float(montant_rapproche)
    except (TypeError, ValueError):
        return _refus(E_MONTANT_INVALIDE)
    if montant_f <= 0:
        return _refus(E_MONTANT_INVALIDE)

    deja = montant_deja_rapproche(mouvement_id_opaque, db_path)
    montant_mvt_abs = abs(float(montant_mouvement))
    if deja >= montant_mvt_abs - 1e-9:
        return _refus(E_DEJA_RAPPROCHE)
    if deja + montant_f > montant_mvt_abs + 1e-9:
        return _refus(E_DEPASSEMENT)

    opaque = "BRP-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
            "type_objet, objet_id, montant_rapproche, statut, source, score_explicable, "
            "criteres_json, commentaire, acteur) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (opaque, mouvement_id_opaque, type_objet, objet_id or None, montant_f, statut, source,
             score_explicable, json.dumps(criteres or {}, ensure_ascii=False), commentaire, acteur))
        _evenement(conn, opaque, "CREATION", None, statut, commentaire, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "rapprochement_id_opaque": opaque, "statut": statut,
            "montant_rapproche": montant_f, "montant_restant": round(montant_mvt_abs - deja - montant_f, 2)}


def _transition(opaque: str, nouveau_statut: str, *, commentaire: str = "", acteur: str = "",
                db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT statut FROM banque_rapprochements WHERE rapprochement_id_opaque=?",
            (opaque,)).fetchone()
        if row is None:
            return _refus(E_INTROUVABLE)
        ancien = row["statut"]
        conn.execute(
            "UPDATE banque_rapprochements SET statut=?, date_modification=?, version=version+1 "
            "WHERE rapprochement_id_opaque=?", (nouveau_statut, _now(), opaque))
        type_evt = {"CONFIRME": "CONFIRMATION", "REFUSE": "REFUS", "ANNULE": "ANNULATION"}[nouveau_statut]
        _evenement(conn, opaque, type_evt, ancien, nouveau_statut, commentaire, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "rapprochement_id_opaque": opaque, "statut": nouveau_statut}


def confirmer(opaque: str, *, commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    return _transition(opaque, ST_CONFIRME, commentaire=commentaire, acteur=acteur, db_path=db_path)


def refuser(opaque: str, *, commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    return _transition(opaque, ST_REFUSE, commentaire=commentaire, acteur=acteur, db_path=db_path)


def annuler(opaque: str, *, commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    return _transition(opaque, ST_ANNULE, commentaire=commentaire, acteur=acteur, db_path=db_path)


def historique_evenements(opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_rapprochement_evenements WHERE rapprochement_id_opaque=? "
            "ORDER BY id DESC", (opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Propositions automatiques (jamais une validation silencieuse) ────────────

def proposer(mouvement: dict[str, Any], objets_candidats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Score explicable, sans écriture. `objets_candidats` : liste de dicts
    {type_objet, objet_id, montant, date, reference?}. Retourne les candidats triés par score
    décroissant avec une raison lisible — jamais une correspondance silencieuse."""
    montant_mvt = abs(float(mouvement.get("montant") or 0))
    date_mvt = str(mouvement.get("date_operation") or "")
    libelle_mvt = str(mouvement.get("libelle") or "").upper()
    propositions = []
    for c in objets_candidats:
        criteres: list[str] = []
        score = 0
        ref = str(c.get("reference") or "")
        if ref and ref.upper() in libelle_mvt:
            criteres.append("référence exacte trouvée dans le libellé")
            score += 3
        montant_c = abs(float(c.get("montant") or 0))
        if montant_mvt and montant_c and abs(montant_mvt - montant_c) < 0.01:
            criteres.append("montant exact")
            score += 2
        date_c = str(c.get("date") or "")
        if date_mvt and date_c:
            try:
                from datetime import date as _d
                d1 = _d.fromisoformat(date_mvt[:10])
                d2 = _d.fromisoformat(date_c[:10])
                ecart = abs((d1 - d2).days)
                if ecart <= 3:
                    criteres.append(f"date proche ({ecart} j)")
                    score += 1
            except ValueError:
                pass
        if not criteres:
            continue
        raison = "Correspondance exacte" if score >= 5 else (
            "Correspondance probable" if score >= 2 else "Correspondance faible")
        propositions.append({**c, "score": score, "criteres": criteres, "raison": raison})
    propositions.sort(key=lambda p: -p["score"])
    return propositions


# ── Vue « mouvements non rapprochés » ────────────────────────────────────────

def etat_rapprochement(mouvement_id_opaque: str, montant_mouvement: float,
                       db_path=None) -> dict[str, Any]:
    """NON_RAPPROCHE | PARTIEL | RAPPROCHE, selon la somme des liens actifs."""
    deja = montant_deja_rapproche(mouvement_id_opaque, db_path)
    total = abs(float(montant_mouvement))
    if deja <= 1e-9:
        return {"statut": "NON_RAPPROCHE", "montant_rapproche": 0.0, "montant_restant": total}
    if deja >= total - 1e-9:
        return {"statut": "RAPPROCHE", "montant_rapproche": round(deja, 2), "montant_restant": 0.0}
    return {"statut": "PARTIEL", "montant_rapproche": round(deja, 2),
            "montant_restant": round(total - deja, 2)}

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
ST_A_CONTROLER = "A_CONTROLER"
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


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    out = {"ok": False, "code": code, "message": MESSAGES.get(code, code)}
    if detail:
        out["detail"] = detail
    return out


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


# ── Rapprochement groupé (recherche bornée, jamais une confirmation silencieuse) ─────────────

FENETRE_JOURS_GROUPE_DEFAUT = 30
MAX_CANDIDATS_GROUPE_DEFAUT = 20
MAX_OBJETS_GROUPE_DEFAUT = 5
TOLERANCE_GROUPE_DEFAUT = 0.01
MAX_ITERATIONS_GROUPE_DEFAUT = 20000

E_GROUPE_VIDE = "V05_GROUPE_SANS_OBJET"
E_GROUPE_OBJET_DEJA_UTILISE = "V07_GROUPE_OBJET_DEJA_UTILISE_DANS_LE_GROUPE"
MESSAGES[E_GROUPE_VIDE] = "Un groupe doit contenir au moins un objet."
MESSAGES[E_GROUPE_OBJET_DEJA_UTILISE] = "Un même objet ne peut pas apparaître deux fois dans un groupe."


def _reste_objet(c: dict[str, Any]) -> float:
    """`reste` explicite fourni par l'appelant (ex. reste_a_rapprocher), sinon `montant` brut —
    jamais recalculé ici, la vérité du reste appartient au service métier de l'objet."""
    v = c.get("reste") if c.get("reste") is not None else c.get("montant")
    return round(abs(float(v or 0)), 2)


def proposer_groupes(mouvement: dict[str, Any], objets_candidats: list[dict[str, Any]], *,
                     fenetre_jours: int = FENETRE_JOURS_GROUPE_DEFAUT,
                     max_candidats: int = MAX_CANDIDATS_GROUPE_DEFAUT,
                     max_objets_groupe: int = MAX_OBJETS_GROUPE_DEFAUT,
                     tolerance: float = TOLERANCE_GROUPE_DEFAUT,
                     max_iterations: int = MAX_ITERATIONS_GROUPE_DEFAUT) -> dict[str, Any]:
    """Recherche BORNÉE de combinaisons d'objets dont la somme des restes égale (à `tolerance`
    près) le montant du mouvement bancaire. Jamais une recherche exhaustive sur toute la base :
    fenêtre de date, nombre de candidats et taille de groupe sont tous bornés en amont ; le nombre
    de combinaisons explorées est compté et la recherche s'arrête proprement à `max_iterations`
    (résultat partiel signalé, jamais un calcul silencieusement incomplet présenté comme complet).

    Ne persiste rien — un groupe proposé n'est confirmé qu'via `confirmer_groupe`, jamais ici."""
    import itertools
    from datetime import date as _d

    montant_mvt = round(abs(float(mouvement.get("montant") or 0)), 2)
    date_mvt_str = str(mouvement.get("date_operation") or "")
    try:
        date_mvt = _d.fromisoformat(date_mvt_str[:10]) if date_mvt_str else None
    except ValueError:
        date_mvt = None

    # 1) Filtrage fenêtre de date + tri déterministe (date puis objet_id) AVANT toute recherche.
    candidats: list[dict[str, Any]] = []
    for c in objets_candidats:
        reste = _reste_objet(c)
        if reste <= 1e-9:
            continue
        if date_mvt is not None and c.get("date"):
            try:
                d_c = _d.fromisoformat(str(c["date"])[:10])
                if abs((date_mvt - d_c).days) > fenetre_jours:
                    continue
            except ValueError:
                pass
        candidats.append({**c, "_reste": reste})
    candidats.sort(key=lambda c: (str(c.get("date") or ""), str(c.get("objet_id") or "")))
    candidats = candidats[:max_candidats]

    # 2) Recherche bornée : tailles croissantes 1..max_objets_groupe, arrêt sur limite d'itérations.
    combinaisons_valides: list[tuple[dict, ...]] = []
    iterations = 0
    limite_atteinte = False
    for taille in range(1, min(max_objets_groupe, len(candidats)) + 1):
        for combo in itertools.combinations(candidats, taille):
            iterations += 1
            if iterations > max_iterations:
                limite_atteinte = True
                break
            somme = round(sum(c["_reste"] for c in combo), 2)
            if abs(somme - montant_mvt) <= tolerance:
                combinaisons_valides.append(combo)
        if limite_atteinte:
            break

    ambigu = len(combinaisons_valides) > 1
    groupes = []
    for combo in combinaisons_valides:
        groupes.append({
            "objets": [{"type_objet": c.get("type_objet"), "objet_id": c.get("objet_id"),
                       "montant_affecte": c["_reste"], "date": c.get("date")} for c in combo],
            "nb_objets": len(combo),
            "somme": round(sum(c["_reste"] for c in combo), 2),
            "ecart": round(sum(c["_reste"] for c in combo) - montant_mvt, 2),
            "partiel": False,
            "statut_propose": ST_A_CONTROLER if ambigu else ST_PROPOSE,
        })

    return {
        "montant_mouvement": montant_mvt,
        "nb_candidats_examines": len(candidats),
        "iterations": iterations,
        "limite_atteinte": limite_atteinte,
        "ambigu": ambigu,
        "groupes": groupes,
    }


def confirmer_groupe(mouvement_id_opaque: str, montant_mouvement: float,
                     affectations: list[dict[str, Any]], *, statut: str = ST_PROPOSE,
                     source: str = "MANUEL", commentaire: str = "", acteur: str = "",
                     db_path=None) -> dict[str, Any]:
    """Enregistre ATOMIQUEMENT tous les liens d'un groupe (tout ou rien) — jamais une confirmation
    automatique : appelée uniquement après décision humaine explicite sur un groupe proposé par
    `proposer_groupes`. `affectations` : [{type_objet, objet_id, montant_affecte}, ...]."""
    if not affectations:
        return _refus(E_GROUPE_VIDE)
    objets_vus = set()
    for a in affectations:
        cle = (a.get("type_objet"), a.get("objet_id"))
        if cle in objets_vus:
            return _refus(E_GROUPE_OBJET_DEJA_UTILISE, str(cle))
        objets_vus.add(cle)

    somme = round(sum(abs(float(a.get("montant_affecte") or 0)) for a in affectations), 2)
    if somme <= 0:
        return _refus(E_MONTANT_INVALIDE)
    montant_mvt_abs = round(abs(float(montant_mouvement)), 2)

    deja = montant_deja_rapproche(mouvement_id_opaque, db_path)
    if deja + somme > montant_mvt_abs + 1e-9:
        return _refus(E_DEPASSEMENT)

    conn = get_db(db_path)
    opaques = []
    try:
        for a in affectations:
            if a.get("type_objet") not in TYPES_OBJET:
                conn.rollback()
                return _refus(E_TYPE_OBJET_INCONNU, str(a.get("type_objet")))
            opaque = "BRP-" + uuid.uuid4().hex[:12].upper()
            conn.execute(
                "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
                "type_objet, objet_id, montant_rapproche, statut, source, commentaire, acteur) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (opaque, mouvement_id_opaque, a["type_objet"], a.get("objet_id"),
                 round(abs(float(a["montant_affecte"])), 2), statut, source, commentaire, acteur))
            _evenement(conn, opaque, "CREATION", None, statut,
                      commentaire or "rapprochement groupé", acteur)
            opaques.append(opaque)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"ok": True, "rapprochements_ids_opaques": opaques, "statut": statut,
            "nb_objets": len(affectations), "montant_total": somme}


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

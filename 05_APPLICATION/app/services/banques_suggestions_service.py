"""Suggestions de rapprochement bancaire — moteur de propositions explicables, JAMAIS de validation
silencieuse (module Banque, suite de `banques_rapprochement_service.py`).

Principes :
- les suggestions sont **recalculées à la demande** à partir des données réelles (mouvements
  NORM_Banque + objets métier réellement disponibles) : jamais un cache qui dériverait de la vérité ;
- aucun objet fictif n'est créé pour les besoins de la suggestion : les candidats viennent des
  sources existantes (charges, réservations, propriétaires...) ;
- chaque suggestion porte des **raisons lisibles** (critères concordants ET divergents) et un
  niveau : EXACT / PROBABLE / FAIBLE ;
- **aucune** suggestion n'est appliquée automatiquement, même EXACT : elle produit au mieux un
  rapprochement au statut `PROPOSE`, qui reste à confirmer par un humain
  (cf. `banques_rapprochement_service.confirmer`) ;
- une suggestion refusée ne réapparaît pas tant que ni le mouvement ni l'objet candidat n'ont
  changé — mémorisé par une empreinte (migration 0016).
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import banques_rapprochement_service as rappro

NIVEAU_EXACT = "EXACT"
NIVEAU_PROBABLE = "PROBABLE"
NIVEAU_FAIBLE = "FAIBLE"

DEC_REFUSEE = "REFUSEE"
DEC_IGNOREE = "IGNOREE_TEMPORAIREMENT"
DEC_ACCEPTEE = "ACCEPTEE"

# Seuils de score (documentés ici, jamais dispersés dans le code appelant).
SEUIL_EXACT = 5
SEUIL_PROBABLE = 3


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def empreinte_candidat(mouvement: dict[str, Any], candidat: dict[str, Any]) -> str:
    """Empreinte de l'état (mouvement, objet) au moment d'une décision. Si l'un des deux change
    (montant, date, libellé, solde de l'objet), l'empreinte change et une suggestion refusée
    redevient proposable — c'est exactement la règle demandée."""
    payload = json.dumps({
        "mvt_montant": mouvement.get("montant"),
        "mvt_date": _txt(mouvement.get("date_operation")),
        "mvt_libelle": _txt(mouvement.get("libelle")),
        "obj_type": _txt(candidat.get("type_objet")),
        "obj_id": _txt(candidat.get("objet_id")),
        "obj_montant": candidat.get("montant"),
        "obj_date": _txt(candidat.get("date")),
    }, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _ecart_jours(d1: str, d2: str) -> int | None:
    try:
        return abs((date.fromisoformat(d1[:10]) - date.fromisoformat(d2[:10])).days)
    except (ValueError, TypeError):
        return None


def evaluer(mouvement: dict[str, Any], candidat: dict[str, Any],
            montant_disponible: float | None = None) -> dict[str, Any]:
    """Score explicable d'un couple (mouvement, candidat). Retourne toujours un dict — c'est
    l'appelant qui décide d'écarter les scores nuls."""
    montant_mvt = abs(float(mouvement.get("montant") or 0))
    dispo = montant_mvt if montant_disponible is None else float(montant_disponible)
    libelle = _txt(mouvement.get("libelle")).upper()
    date_mvt = _txt(mouvement.get("date_operation"))

    concordants: list[str] = []
    divergents: list[str] = []
    score = 0

    # Identifiant / référence exacte dans le libellé — le critère le plus fort.
    for cle, label in (("objet_id", "identifiant"), ("reference", "référence")):
        val = _txt(candidat.get(cle))
        if val and val.upper() in libelle:
            concordants.append(f"{label} « {val} » présent dans le libellé")
            score += 3
            break

    # Montant.
    montant_obj = abs(float(candidat.get("montant") or 0))
    if montant_obj:
        if abs(montant_mvt - montant_obj) < 0.01:
            concordants.append("montant exact")
            score += 2
        elif montant_obj <= dispo + 1e-9:
            concordants.append(
                f"montant compatible avec un rapprochement partiel ({montant_obj:.2f} € ≤ {dispo:.2f} € disponibles)")
            score += 1
        else:
            divergents.append(
                f"montant de l'objet ({montant_obj:.2f} €) supérieur au disponible ({dispo:.2f} €)")

    # Date.
    date_obj = _txt(candidat.get("date"))
    if date_mvt and date_obj:
        ecart = _ecart_jours(date_mvt, date_obj)
        if ecart is None:
            divergents.append("date de l'objet illisible")
        elif ecart == 0:
            concordants.append("date exacte")
            score += 2
        elif ecart <= 5:
            concordants.append(f"date proche ({ecart} j d'écart)")
            score += 1
        else:
            divergents.append(f"date éloignée ({ecart} j d'écart)")

    # Tiers / propriétaire / fournisseur / plateforme présents dans le libellé.
    for cle, label in (("proprietaire_id", "propriétaire"), ("fournisseur", "fournisseur"),
                       ("plateforme", "plateforme")):
        val = _txt(candidat.get(cle))
        if val and val.upper() in libelle:
            concordants.append(f"{label} « {val} » reconnu dans le libellé")
            score += 1

    niveau = (NIVEAU_EXACT if score >= SEUIL_EXACT
              else NIVEAU_PROBABLE if score >= SEUIL_PROBABLE else NIVEAU_FAIBLE)
    montant_propose = min(montant_obj, dispo) if montant_obj else dispo
    return {
        "mouvement_id_opaque": mouvement.get("id_opaque"),
        "type_objet": candidat.get("type_objet"),
        "objet_id": candidat.get("objet_id"),
        "montant_propose": round(montant_propose, 2),
        "score": score,
        "niveau": niveau,
        "criteres_concordants": concordants,
        "criteres_divergents": divergents,
        "raison": _raison(niveau, concordants),
        "date_generation": _now(),
        "libelle_objet": _txt(candidat.get("libelle")) or _txt(candidat.get("objet_id")),
    }


def _raison(niveau: str, concordants: list[str]) -> str:
    if not concordants:
        return "Aucun critère concordant"
    tete = {NIVEAU_EXACT: "Correspondance exacte",
            NIVEAU_PROBABLE: "Correspondance probable",
            NIVEAU_FAIBLE: "Correspondance faible"}[niveau]
    return f"{tete} — " + " ; ".join(concordants)


# ── Décisions persistées (refus / ignore / acceptation) ─────────────────────

def enregistrer_decision(mouvement_id_opaque: str, suggestion: dict[str, Any], decision: str,
                         empreinte: str, *, commentaire: str = "", acteur: str = "",
                         db_path=None) -> None:
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO banque_suggestion_decisions (mouvement_id_opaque, type_objet, objet_id, "
            "decision, empreinte_candidat, montant_propose, score, niveau, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (mouvement_id_opaque, suggestion.get("type_objet"), _txt(suggestion.get("objet_id")),
             decision, empreinte, suggestion.get("montant_propose"), suggestion.get("score"),
             suggestion.get("niveau"), commentaire, acteur or "local"))
        conn.commit()
    finally:
        conn.close()


def _decisions_courantes(mouvement_id_opaque: str, db_path=None) -> dict[tuple, dict[str, Any]]:
    """{(type_objet, objet_id): décision la plus récente} pour ce mouvement."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_suggestion_decisions WHERE mouvement_id_opaque=? ORDER BY id ASC",
            (mouvement_id_opaque,)).fetchall()
    finally:
        conn.close()
    out: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        out[(r["type_objet"], r["objet_id"])] = dict(r)   # les plus récentes écrasent
    return out


def historique_decisions(mouvement_id_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_suggestion_decisions WHERE mouvement_id_opaque=? ORDER BY id DESC",
            (mouvement_id_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def suggerer(mouvement: dict[str, Any], candidats: list[dict[str, Any]], *,
             db_path=None) -> list[dict[str, Any]]:
    """Suggestions pour un mouvement, filtrées des décisions passées encore valables.

    `mouvement` doit contenir : id_opaque, montant, date_operation, libelle.
    `candidats` : objets réels {type_objet, objet_id, montant, date, reference?, ...}.
    """
    opaque = _txt(mouvement.get("id_opaque"))
    dispo = rappro.etat_rapprochement(opaque, mouvement.get("montant") or 0,
                                      db_path=db_path)["montant_restant"]
    if dispo <= 1e-9:
        return []                       # mouvement déjà entièrement rapproché : plus rien à proposer

    decisions = _decisions_courantes(opaque, db_path)
    out: list[dict[str, Any]] = []
    for c in candidats:
        s = evaluer(mouvement, c, montant_disponible=dispo)
        if s["score"] <= 0:
            continue                    # aucun critère concordant : jamais une suggestion vide
        emp = empreinte_candidat(mouvement, c)
        d = decisions.get((s["type_objet"], _txt(s["objet_id"])))
        if d and d["decision"] in (DEC_REFUSEE, DEC_IGNOREE) and d["empreinte_candidat"] == emp:
            continue                    # refusée/ignorée et rien n'a changé depuis : on ne la ressort pas
        s["empreinte"] = emp
        s["deja_refusee_puis_modifiee"] = bool(
            d and d["decision"] in (DEC_REFUSEE, DEC_IGNOREE) and d["empreinte_candidat"] != emp)
        out.append(s)
    out.sort(key=lambda s: (-s["score"], s["objet_id"] or ""))
    return out


def accepter(mouvement: dict[str, Any], suggestion: dict[str, Any], *, montant: float | None = None,
             commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Transforme une suggestion en rapprochement **au statut PROPOSE** (jamais CONFIRME) : même une
    correspondance EXACT reste soumise à confirmation humaine explicite."""
    montant_final = float(montant if montant is not None else suggestion["montant_propose"])
    res = rappro.enregistrer(
        _txt(mouvement.get("id_opaque")), suggestion["type_objet"], _txt(suggestion.get("objet_id")),
        montant_final, montant_mouvement=mouvement.get("montant") or 0,
        statut=rappro.ST_PROPOSE, source="AUTO", score_explicable=suggestion.get("score"),
        criteres={"concordants": suggestion.get("criteres_concordants", []),
                  "divergents": suggestion.get("criteres_divergents", []),
                  "niveau": suggestion.get("niveau")},
        commentaire=commentaire or suggestion.get("raison", ""), acteur=acteur, db_path=db_path)
    if res.get("ok"):
        enregistrer_decision(_txt(mouvement.get("id_opaque")), suggestion, DEC_ACCEPTEE,
                             suggestion.get("empreinte") or empreinte_candidat(mouvement, suggestion),
                             commentaire=commentaire, acteur=acteur, db_path=db_path)
    return res


def refuser(mouvement: dict[str, Any], suggestion: dict[str, Any], *, definitif: bool = True,
            commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Refus (définitif tant que rien ne change) ou mise de côté temporaire."""
    enregistrer_decision(
        _txt(mouvement.get("id_opaque")), suggestion,
        DEC_REFUSEE if definitif else DEC_IGNOREE,
        suggestion.get("empreinte") or empreinte_candidat(mouvement, suggestion),
        commentaire=commentaire, acteur=acteur, db_path=db_path)
    return {"ok": True, "decision": DEC_REFUSEE if definitif else DEC_IGNOREE}

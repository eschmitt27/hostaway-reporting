"""File humaine de classement Banque — mouvements `statut_classification=A_ENVOYER_IA`
(migration 0026, décisions append-only).

Aucune IA externe, aucun appel réseau, aucune modification du moteur de classification (Excel
NORM_Banque reste la vérité, jamais réécrit ici). Une décision humaine ne fait que journaliser un
choix — elle ne devient jamais une nouvelle règle déterministe automatiquement.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers import banques_reader as reader
from app.readers.banques_reader import to_texte, to_nombre, to_date, to_mois, masquer_compte
from app.services.banques_controle_service import (
    id_opaque, resoudre_opaque, libelle_categorie, LIBELLES_CATEGORIE)

TYPES_DECISION = ("CATEGORISER", "MAINTENIR_A_CONTROLER", "NON_CLASSE", "REPORTER")

E_MOUVEMENT_INTROUVABLE = "E01_MOUVEMENT_INTROUVABLE"
E_TYPE_DECISION_INCONNU = "V01_TYPE_DECISION_INCONNU"
E_CATEGORIE_INCONNUE = "V02_CATEGORIE_INCONNUE"
E_CATEGORIE_MANQUANTE = "V03_CATEGORIE_MANQUANTE_POUR_CATEGORISER"

MESSAGES = {
    E_MOUVEMENT_INTROUVABLE: "Mouvement introuvable.",
    E_TYPE_DECISION_INCONNU: "Type de décision inconnu.",
    E_CATEGORIE_INCONNUE: "Catégorie inconnue — seules les catégories déjà utilisées par le moteur sont autorisées.",
    E_CATEGORIE_MANQUANTE: "Une catégorie est requise pour une décision CATEGORISER.",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _mouvement_brut(mid_interne: str) -> dict[str, Any] | None:
    for r in reader.mouvements().lignes:
        if to_texte(r.get("mouvement_id")) == mid_interne:
            return r
    return None


def categories_disponibles() -> list[str]:
    """Catégories fermées connues du moteur (catalogue curé + catégories réellement présentes sur
    la source courante) — jamais une catégorie libre inventée par l'humain."""
    presentes = {to_texte(r.get("categorie")) for r in reader.mouvements().lignes
                if to_texte(r.get("categorie"))}
    return sorted(presentes | set(LIBELLES_CATEGORIE.keys()))


def _ligne_vue(r: dict[str, Any], opq: str, decision_active: dict | None) -> dict[str, Any]:
    return {
        "id_opaque": opq,
        "date_operation": to_date(r.get("date_operation")),
        "mois": to_mois(r.get("date_operation")),
        "montant": to_nombre(r.get("montant")),
        "sens": to_texte(r.get("sens")),
        "compte_masque": masquer_compte(r.get("compte_id")),
        "libelle": to_texte(r.get("libelle")),
        "categorie_moteur": to_texte(r.get("categorie")),
        "categorie_libelle": libelle_categorie(r.get("categorie")),
        "niveau_risque": to_texte(r.get("niveau_risque")),
        "regle_id_appliquee": to_texte(r.get("regle_id_appliquee")),
        "statut_humain": (decision_active or {}).get("type_decision") or "SANS_DECISION",
        "derniere_decision": decision_active,
    }


def _decisions_actives(db_path=None) -> dict[str, dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_classement_decisions WHERE actif=1 ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = dict(r)
        out.setdefault(d["mouvement_id_opaque"], d)   # la plus récente (ORDER BY id DESC) gagne
    return out


def lister(*, db_path=None, periode: str = "", montant_min: float | None = None,
          montant_max: float | None = None, sens: str = "", categorie: str = "",
          statut_humain: str = "", uniquement_sans_decision: bool = False) -> list[dict[str, Any]]:
    src = reader.mouvements()
    if not src.etat.disponible:
        return []
    decisions = _decisions_actives(db_path)
    out = []
    for r in src.lignes:
        if to_texte(r.get("statut_classification")) != "A_ENVOYER_IA":
            continue
        mid = to_texte(r.get("mouvement_id"))
        if not mid:
            continue
        opq = id_opaque(mid)
        ligne = _ligne_vue(r, opq, decisions.get(opq))
        if periode and ligne["mois"] != periode:
            continue
        if montant_min is not None and (ligne["montant"] or 0) < montant_min:
            continue
        if montant_max is not None and (ligne["montant"] or 0) > montant_max:
            continue
        if sens and ligne["sens"] != sens:
            continue
        if categorie and ligne["categorie_moteur"] != categorie:
            continue
        if statut_humain and ligne["statut_humain"] != statut_humain:
            continue
        if uniquement_sans_decision and ligne["statut_humain"] != "SANS_DECISION":
            continue
        out.append(ligne)
    return out


def compter() -> int:
    src = reader.mouvements()
    if not src.etat.disponible:
        return 0
    return sum(1 for r in src.lignes if to_texte(r.get("statut_classification")) == "A_ENVOYER_IA")


def charger(id_opaque_mvt: str, *, db_path=None) -> dict[str, Any] | None:
    mid = resoudre_opaque(id_opaque_mvt)
    if mid is None:
        return None
    r = _mouvement_brut(mid)
    if r is None:
        return None
    decisions = _decisions_actives(db_path)
    ligne = _ligne_vue(r, id_opaque_mvt, decisions.get(id_opaque_mvt))
    ligne["historique"] = historique(id_opaque_mvt, db_path=db_path)
    return ligne


def previsualiser(id_opaque_mvt: str, type_decision: str, *, nouvelle_categorie: str = "",
                  justification: str = "", anomalie_moteur: str = "",
                  future_regle: str = "") -> dict[str, Any]:
    mid = resoudre_opaque(id_opaque_mvt)
    if mid is None:
        return _refus(E_MOUVEMENT_INTROUVABLE, id_opaque_mvt)
    type_decision = to_texte(type_decision)
    if type_decision not in TYPES_DECISION:
        return _refus(E_TYPE_DECISION_INCONNU, type_decision)
    if type_decision == "CATEGORISER":
        if not nouvelle_categorie:
            return _refus(E_CATEGORIE_MANQUANTE)
        if nouvelle_categorie not in categories_disponibles():
            return _refus(E_CATEGORIE_INCONNUE, nouvelle_categorie)
    r = _mouvement_brut(mid)
    ancienne = to_texte(r.get("categorie")) if r else ""
    return {"ok": True, "apercu": {
        "mouvement_opaque": id_opaque_mvt, "type_decision": type_decision,
        "ancienne_categorie": ancienne or "NON_CLASSE",
        "nouvelle_categorie": nouvelle_categorie if type_decision == "CATEGORISER" else ancienne,
        "justification": justification, "anomalie_moteur": anomalie_moteur,
        "future_regle": future_regle,
        "effet_statut": "Le statut humain de ce mouvement sera mis à jour dans le journal de recette.",
        "effet_moteur": "Aucun — BANQUE_LOT8_IMPORT.xlsx n'est jamais modifié par cette décision.",
        "effet_comptable": "Aucun.", "effet_reel": "Aucun — décision de recette uniquement.",
    }}


def decider(id_opaque_mvt: str, type_decision: str, *, nouvelle_categorie: str = "",
           justification: str = "", anomalie_moteur: str = "", future_regle: str = "",
           acteur: str = "", db_path=None) -> dict[str, Any]:
    verif = previsualiser(id_opaque_mvt, type_decision, nouvelle_categorie=nouvelle_categorie,
                          justification=justification, anomalie_moteur=anomalie_moteur,
                          future_regle=future_regle)
    if not verif["ok"]:
        return verif
    mid = resoudre_opaque(id_opaque_mvt)
    r = _mouvement_brut(mid)
    ancienne = to_texte(r.get("categorie")) if r else ""
    nouvelle = nouvelle_categorie if type_decision == "CATEGORISER" else ancienne

    opaque = "BCD-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE banque_classement_decisions SET actif=0 WHERE mouvement_id_opaque=?",
                    (id_opaque_mvt,))
        conn.execute(
            "INSERT INTO banque_classement_decisions (decision_opaque, mouvement_id_opaque, "
            "ancienne_categorie, nouvelle_categorie, type_decision, justification, "
            "anomalie_moteur, future_regle, acteur) VALUES (?,?,?,?,?,?,?,?,?)",
            (opaque, id_opaque_mvt, ancienne or None, nouvelle or None, type_decision,
             justification or None, anomalie_moteur or None, future_regle or None, acteur or "local"))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "decision_opaque": opaque, "type_decision": type_decision}


def historique(id_opaque_mvt: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM banque_classement_decisions WHERE mouvement_id_opaque=? "
            "ORDER BY id DESC", (id_opaque_mvt,)).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]

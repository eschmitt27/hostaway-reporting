"""Préparation des règlements propriétaires SANS virement (APP-3E).

MARQUE_COMME_PAYE est une DÉCLARATION HUMAINE — jamais une preuve bancaire, jamais un virement,
jamais un appel API bancaire. Aucun IBAN, aucun RIB, aucune coordonnée bancaire n'est stocké ici.
Machine à états distincte du cycle de préparation du relevé (`proprietaires_releve_cycle_service`)
et du statut de facturation (`proprietaires_suivi_service`). Version optimiste, historique
append-only (réutilise `proprietaires_releve_evenements`).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db.connection import get_db

ST_NON_PREPARE = "NON_PREPARE"
ST_A_CONTROLER = "A_CONTROLER"
ST_PRET_A_PAYER = "PRET_A_PAYER"
ST_MARQUE_COMME_PAYE = "MARQUE_COMME_PAYE"
ST_BLOQUE = "BLOQUE"
ST_ANNULE = "ANNULE"
ST_ROUVERT = "ROUVERT"

STATUTS = {ST_NON_PREPARE, ST_A_CONTROLER, ST_PRET_A_PAYER, ST_MARQUE_COMME_PAYE, ST_BLOQUE,
           ST_ANNULE, ST_ROUVERT}

_TRANSITIONS: dict[str, set[str]] = {
    ST_NON_PREPARE: {ST_A_CONTROLER},
    ST_A_CONTROLER: {ST_PRET_A_PAYER, ST_BLOQUE, ST_ANNULE},
    ST_BLOQUE: {ST_A_CONTROLER, ST_ANNULE},
    ST_PRET_A_PAYER: {ST_MARQUE_COMME_PAYE, ST_A_CONTROLER, ST_ANNULE},
    ST_MARQUE_COMME_PAYE: {ST_ROUVERT},
    ST_ROUVERT: {ST_A_CONTROLER, ST_ANNULE},
    ST_ANNULE: set(),
}

# Champs interdits — jamais stockés, jamais exportés depuis ce module.
CHAMPS_BANCAIRES_INTERDITS = ("iban", "rib", "compte_bancaire", "numero_compte", "bic", "swift")


class PaiementRefuse(Exception):
    """Action refusée (transition invalide, version obsolète, contrôle bloquant, champ interdit)."""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _row(r) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def charger(releve_id_opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT * FROM proprietaires_paiement WHERE releve_id_opaque=?",
            (releve_id_opaque,)).fetchone()
        return _row(r)
    finally:
        conn.close()


def creer_ou_charger(releve_id_opaque: str, db_path=None) -> dict[str, Any]:
    existant = charger(releve_id_opaque, db_path)
    if existant is not None:
        return existant
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO proprietaires_paiement (releve_id_opaque) VALUES (?)",
            (releve_id_opaque,))
        conn.commit()
    finally:
        conn.close()
    return charger(releve_id_opaque, db_path)


def lister_a_payer(db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM proprietaires_paiement ORDER BY date_modification DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _journaliser(conn, opaque, ancien, nouveau, commentaire="", acteur=""):
    conn.execute(
        "INSERT INTO proprietaires_releve_evenements "
        "(releve_id_opaque, type_evenement, ancien_statut, nouveau_statut, commentaire, acteur) "
        "VALUES (?,?,?,?,?,?)", (opaque, "PAIEMENT_TRANSITION", ancien, nouveau, commentaire, acteur))


def _transition(paiement: dict, nouveau: str, *, version_attendue, commentaire, acteur, db_path,
                extra_sql="", extra_vals=()) -> dict[str, Any]:
    opaque = paiement["releve_id_opaque"]
    ancien = paiement["statut_paiement"]
    version_lue = paiement["version"]
    if version_attendue is not None and version_attendue != version_lue:
        raise PaiementRefuse(f"Conflit de version (attendu {version_attendue}, courant {version_lue}).")
    if nouveau not in _TRANSITIONS.get(ancien, set()):
        raise PaiementRefuse(f"Transition {ancien} → {nouveau} interdite.")

    conn = get_db(db_path)
    try:
        sql = ("UPDATE proprietaires_paiement SET statut_paiement=?, version=version+1, "
              f"date_modification=? {extra_sql} WHERE releve_id_opaque=? AND version=?")
        vals = [nouveau, _now(), *extra_vals, opaque, version_lue]
        cur = conn.execute(sql, vals)
        if cur.rowcount == 0:
            conn.rollback()
            raise PaiementRefuse("Conflit de version — le paiement a été modifié entretemps.")
        _journaliser(conn, opaque, ancien, nouveau, commentaire, acteur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return charger(opaque, db_path)


def demarrer_controle(paiement: dict, *, acteur: str = "", version_attendue=None,
                      db_path=None) -> dict[str, Any]:
    return _transition(paiement, ST_A_CONTROLER, version_attendue=version_attendue,
                       commentaire="", acteur=acteur, db_path=db_path)


def bloquer(paiement: dict, commentaire: str, *, acteur: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    return _transition(paiement, ST_BLOQUE, version_attendue=version_attendue,
                       commentaire=commentaire, acteur=acteur, db_path=db_path)


def marquer_pret_a_payer(paiement: dict, *, bloquants: list[str] | None = None, acteur: str = "",
                         version_attendue=None, db_path=None) -> dict[str, Any]:
    if bloquants:
        raise PaiementRefuse(
            f"Passage à PRÊT À PAYER impossible : contrôles bloquants ouverts ({', '.join(bloquants)}).")
    return _transition(paiement, ST_PRET_A_PAYER, version_attendue=version_attendue,
                       commentaire="", acteur=acteur, db_path=db_path)


def marquer_paye(paiement: dict, *, reference_interne: str = "", commentaire: str = "", acteur: str = "",
                 version_attendue=None, db_path=None) -> dict[str, Any]:
    """Déclaration humaine uniquement — jamais une écriture bancaire. `reference_interne` est un
    texte libre non bancaire (jamais un IBAN/RIB, contrôlé en amont côté route/formulaire)."""
    ref_basse = (reference_interne or "").lower()
    if any(c in ref_basse for c in CHAMPS_BANCAIRES_INTERDITS):
        raise PaiementRefuse("Référence interne suspecte — aucune donnée bancaire n'est autorisée ici.")
    return _transition(paiement, ST_MARQUE_COMME_PAYE, version_attendue=version_attendue,
                       commentaire=commentaire, acteur=acteur, db_path=db_path,
                       extra_sql=", reference_interne_paiement=?, date_paiement=?",
                       extra_vals=(reference_interne, _now()))


def rouvrir(paiement: dict, motif: str, *, acteur: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    motif = (motif or "").strip()
    if not motif:
        raise PaiementRefuse("Motif de réouverture requis.")
    return _transition(paiement, ST_ROUVERT, version_attendue=version_attendue,
                       commentaire=motif, acteur=acteur, db_path=db_path,
                       extra_sql=", motif_paiement=?", extra_vals=(motif,))


def annuler(paiement: dict, motif: str, *, acteur: str = "", version_attendue=None,
           db_path=None) -> dict[str, Any]:
    motif = (motif or "").strip()
    if not motif:
        raise PaiementRefuse("Motif d'annulation requis.")
    return _transition(paiement, ST_ANNULE, version_attendue=version_attendue,
                       commentaire=motif, acteur=acteur, db_path=db_path,
                       extra_sql=", motif_paiement=?", extra_vals=(motif,))


def controler_passage_pret_a_payer(*, cycle_etat: str, derive: bool, statut_app5c_compatible: bool,
                                   statut_moteur_compatible: bool, montant_moteur_disponible: bool,
                                   proprietaire_connu: bool, source_obligatoire_disponible: bool,
                                   charges_bloquantes: list[str] | None = None,
                                   doublon: bool = False, writer_reel_actif: bool = False) -> list[str]:
    """Détection pure des blocages avant passage à PRET_A_PAYER — jamais un recalcul de montant."""
    codes: list[str] = []
    if cycle_etat != "VALIDE":
        codes.append("RELEVE_NON_VALIDE")
    if derive:
        codes.append("SNAPSHOT_OBSOLETE")
    if not statut_app5c_compatible:
        codes.append("APP5C_INCOMPATIBLE")
    if not statut_moteur_compatible:
        codes.append("MOIS_MOTEUR_INCOMPATIBLE")
    if not montant_moteur_disponible:
        codes.append("DONNEE_MOTEUR_INDISPONIBLE")
    if not proprietaire_connu:
        codes.append("PROPRIETAIRE_INCONNU")
    if not source_obligatoire_disponible:
        codes.append("SOURCE_OBLIGATOIRE_ABSENTE")
    for c in (charges_bloquantes or []):
        codes.append(c)
    if doublon:
        codes.append("DOUBLON")
    if writer_reel_actif:
        codes.append("WRITER_REEL_ACTIVE")
    return list(dict.fromkeys(codes))

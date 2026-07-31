"""Réconciliations (Phase 2, mission Analytique/Résultats).

Chaque fonction compare deux sources déjà réelles — jamais un troisième calcul. Statut toujours
parmi `OK`, `ECART_TOLERE`, `A_CONTROLER`, `BLOQUANT`, `NON_DISPONIBLE` — jamais une exception.
Tolérance 0,01 € partout, sauf mention contraire explicite.
"""
from __future__ import annotations

from typing import Any

from app.db.connection import get_db

TOLERANCE = 0.01

ST_OK = "OK"
ST_ECART_TOLERE = "ECART_TOLERE"
ST_A_CONTROLER = "A_CONTROLER"
ST_BLOQUANT = "BLOQUANT"
ST_NON_DISPONIBLE = "NON_DISPONIBLE"


def _resultat(montant_gauche: float | None, montant_droit: float | None, *, libelle_gauche: str,
             libelle_droit: str, detail: list[dict[str, Any]] | None = None,
             tolerance: float = TOLERANCE) -> dict[str, Any]:
    if montant_gauche is None or montant_droit is None:
        return {"statut": ST_NON_DISPONIBLE, "libelle_gauche": libelle_gauche,
                "libelle_droit": libelle_droit, "montant_gauche": montant_gauche,
                "montant_droit": montant_droit, "ecart": None, "tolerance": tolerance,
                "detail": detail or []}
    ecart = round(montant_gauche - montant_droit, 2)
    statut = ST_OK if ecart == 0 else (ST_ECART_TOLERE if abs(ecart) <= tolerance else ST_A_CONTROLER)
    return {"statut": statut, "libelle_gauche": libelle_gauche, "libelle_droit": libelle_droit,
            "montant_gauche": round(montant_gauche, 2), "montant_droit": round(montant_droit, 2),
            "ecart": ecart, "tolerance": tolerance, "detail": detail or []}


# ── A. Lot9 ↔ Lot10 ──────────────────────────────────────────────────────────

def lot9_vs_lot10(*, mois: str = "") -> dict[str, Any]:
    """Non disponible : aucun lecteur Lot9 n'existe à ce niveau applicatif (SQLite/app). Les
    contrôles Lot9↔Lot10 existent déjà dans le moteur (CTR-LOT10-*, cf. `lot10_calculer_resultats.py`)
    — les répliquer ici recalculerait ce que le moteur contrôle déjà. Signalé honnêtement plutôt
    qu'improvisé."""
    return {"statut": ST_NON_DISPONIBLE, "libelle_gauche": "Lot9 (VUE_FLUX)",
            "libelle_droit": "Lot10 (résultats)", "montant_gauche": None, "montant_droit": None,
            "ecart": None, "tolerance": TOLERANCE, "detail": [],
            "raison": "Aucun lecteur Lot9 au niveau applicatif — contrôle déjà porté par le moteur "
                      "(CTR-LOT10-*), pas dupliqué ici."}


# ── B. Lot10 ↔ Analytique ────────────────────────────────────────────────────

def lot10_vs_analytique(*, mois: str = "", vision: str = "REEL") -> dict[str, Any]:
    """L'Analytique LIT Lot10 directement (`comptabilite_analytique_service`) : par construction,
    aucun écart n'est possible sauf source manquante ou filtre non appliqué. Vérifie que la somme
    par logement égale bien le total global de la vision — défense en profondeur, pas une
    hypothèse."""
    from app.services import comptabilite_analytique_service as ana
    m_global = ana.mesures_globales()
    m_log = ana.mesures_par_logement(mois=mois, vision=vision)
    if m_global["statut"] != "OK" or m_log["statut"] != "OK":
        return {"statut": ST_NON_DISPONIBLE, "libelle_gauche": "Lot10 GLOBAL",
                "libelle_droit": "Analytique (somme par logement)", "montant_gauche": None,
                "montant_droit": None, "ecart": None, "tolerance": TOLERANCE, "detail": []}
    droit = round(sum(l["resultat"] for l in m_log["lignes"]), 2)
    gauche = m_global["visions"].get(vision, {}).get("resultat")
    return _resultat(gauche, droit, libelle_gauche=f"Lot10 GLOBAL ({vision})",
                     libelle_droit="Analytique — somme PAR_MOIS_LOGEMENT")


# ── C. Analytique ↔ Comptabilité ──────────────────────────────────────────────

def analytique_vs_comptabilite(*, mois: str = "", vision: str = "COMPTABLE",
                               db_path=None) -> dict[str, Any]:
    """Compare le résultat Lot10 (vision COMPTABLE par défaut — c'est la vision que la
    Comptabilité applicative traduit) à la somme des écritures ACHATS/VENTES déjà générées et
    validées sur la période. Un écart est ATTENDU tant que toutes les factures ne sont pas encore
    passées en écriture — ce n'est pas une anomalie, c'est ce que cette réconciliation sert à
    montrer (A_CONTROLER, pas BLOQUANT)."""
    from app.services import comptabilite_analytique_service as ana
    m_global = ana.mesures_globales()
    if m_global["statut"] != "OK":
        return {"statut": ST_NON_DISPONIBLE, "libelle_gauche": f"Lot10 GLOBAL ({vision})",
                "libelle_droit": "Comptabilité (écritures)", "montant_gauche": None,
                "montant_droit": None, "ecart": None, "tolerance": TOLERANCE, "detail": []}
    gauche = m_global["visions"].get(vision, {}).get("resultat")

    conn = get_db(db_path)
    try:
        clause = "AND periode=?" if mois else ""
        params: tuple = (mois,) if mois else ()
        produits = conn.execute(
            "SELECT COALESCE(SUM(total_credit),0) FROM ecritures WHERE journal='VENTES' "
            f"AND statut IN ('VALIDEE','CONTREPASSEE') {clause}", params).fetchone()[0]
        charges = conn.execute(
            "SELECT COALESCE(SUM(total_debit),0) FROM ecritures WHERE journal='ACHATS' "
            f"AND statut IN ('VALIDEE','CONTREPASSEE') {clause}", params).fetchone()[0]
    finally:
        conn.close()
    droit = round((produits or 0) - (charges or 0), 2)
    return _resultat(gauche, droit, libelle_gauche=f"Lot10 GLOBAL ({vision})",
                     libelle_droit="Comptabilité — VENTES(crédit) − ACHATS(débit) validés")


# ── D. Banque ↔ journal BANQUE ────────────────────────────────────────────────

def banque_vs_journal_banque(*, mois: str = "", db_path=None) -> dict[str, Any]:
    conn = get_db(db_path)
    try:
        clause_rap = "AND date_creation LIKE ?" if mois else ""
        params_rap: tuple = (f"{mois}%",) if mois else ()
        montant_rap = conn.execute(
            "SELECT COALESCE(SUM(montant_rapproche),0) FROM banque_rapprochements "
            f"WHERE type_objet='REGLEMENT_CHARGE' AND statut='CONFIRME' {clause_rap}",
            params_rap).fetchone()[0]

        clause_ecr = "AND periode=?" if mois else ""
        params_ecr: tuple = (mois,) if mois else ()
        montant_ecr = conn.execute(
            "SELECT COALESCE(SUM(total_debit),0) FROM ecritures WHERE journal='BANQUE' "
            f"AND statut IN ('VALIDEE','CONTREPASSEE') {clause_ecr}", params_ecr).fetchone()[0]

        manquants = conn.execute(
            "SELECT rapprochement_id_opaque, montant_rapproche FROM banque_rapprochements "
            "WHERE type_objet='REGLEMENT_CHARGE' AND statut='CONFIRME' "
            "AND rapprochement_id_opaque NOT IN ("
            "  SELECT origine_id_opaque FROM ecritures WHERE journal='BANQUE' "
            "  AND origine_id_opaque IS NOT NULL)").fetchall()
    finally:
        conn.close()
    detail = [{"objet_manquant": r["rapprochement_id_opaque"], "montant": r["montant_rapproche"]}
              for r in manquants]
    res = _resultat(montant_rap, montant_ecr, libelle_gauche="Banque (rapprochements confirmés)",
                    libelle_droit="Journal BANQUE (écritures validées)", detail=detail)
    if detail and res["statut"] == ST_OK:
        res["statut"] = ST_A_CONTROLER   # des objets manquent malgré une somme qui coïncide par ailleurs
    return res


# ── E. Factures ↔ auxiliaires ─────────────────────────────────────────────────

def factures_vs_auxiliaires(*, fournisseur_id_opaque: str = "", db_path=None) -> dict[str, Any]:
    from app.services import factures_service as fact
    from app.services import comptabilite_ecritures_service as compta

    factures = fact.lister(fournisseur=fournisseur_id_opaque, db_path=db_path)
    ouvertes = [f for f in factures if f["statut"] in fact.STATUTS_OUVERTS]
    montant_factures = round(sum(f["solde_restant"] for f in ouvertes), 2)

    if fournisseur_id_opaque:
        solde = compta.solde_auxiliaire(fournisseur_id_opaque, db_path=db_path)
        montant_aux = round(-solde["solde"], 2)   # compte PASSIF : solde négatif = dette
    else:
        montant_aux = None

    detail = [{"facture": f["facture_ref"], "solde_restant": f["solde_restant"]} for f in ouvertes]
    return _resultat(montant_factures, montant_aux, libelle_gauche="Factures (solde restant ouvert)",
                     libelle_droit="Auxiliaire fournisseur (dette)", detail=detail)


# ── F. Ménages ↔ charges ──────────────────────────────────────────────────────

def menages_vs_charges(*, mois: str = "", db_path=None) -> dict[str, Any]:
    """Compare le nombre de ménages réglés liés à une charge (`menages.charge_id`) au nombre de
    ces charges effectivement retrouvées dans le MASTER Lot3 — signale un ménage lié à une charge
    qui n'existe plus/pas dans la source réelle."""
    from app.readers import charges_reader

    conn = get_db(db_path)
    try:
        clause = "AND mois=?" if mois else ""
        params: tuple = (mois,) if mois else ()
        rows = conn.execute(
            f"SELECT menage_id_opaque, charge_id FROM menages WHERE charge_id IS NOT NULL "
            f"AND statut <> 'ANNULE' {clause}", params).fetchall()
    finally:
        conn.close()
    if not rows:
        return {"statut": ST_NON_DISPONIBLE, "libelle_gauche": "Ménages liés à une charge",
                "libelle_droit": "Charges retrouvées (MASTER Lot3)", "montant_gauche": None,
                "montant_droit": None, "ecart": None, "tolerance": TOLERANCE, "detail": []}
    manquants = []
    trouves = 0
    for r in rows:
        if charges_reader.find_charge(r["charge_id"]) is not None:
            trouves += 1
        else:
            manquants.append({"menage_id_opaque": r["menage_id_opaque"], "charge_id": r["charge_id"]})
    total = len(rows)
    statut = ST_OK if not manquants else ST_A_CONTROLER
    return {"statut": statut, "libelle_gauche": "Ménages liés à une charge",
            "libelle_droit": "Charges retrouvées (MASTER Lot3)", "montant_gauche": total,
            "montant_droit": trouves, "ecart": total - trouves, "tolerance": 0, "detail": manquants}


# ── G. Commissions ↔ VENTES Lot12 ─────────────────────────────────────────────

def commissions_vs_ventes(mois: str, *, db_path=None) -> dict[str, Any]:
    from app.services import ventes_lot12_adapter_service as adapter
    from app.services import comptabilite_ecritures_service as compta

    lignes = adapter.lignes_du_mois(mois)
    if not lignes:
        return {"statut": ST_NON_DISPONIBLE, "libelle_gauche": "Lot12 (montant_du_conciergerie)",
                "libelle_droit": "Journal VENTES", "montant_gauche": None, "montant_droit": None,
                "ecart": None, "tolerance": TOLERANCE, "detail": []}
    gauche = round(sum(l.get("montant_du_conciergerie") or 0 for l in lignes), 2)

    conn = get_db(db_path)
    try:
        droit = conn.execute(
            "SELECT COALESCE(SUM(total_debit),0) FROM ecritures WHERE journal='VENTES' "
            "AND periode=? AND statut IN ('VALIDEE','PROPOSEE','CONTREPASSEE')", (mois,)).fetchone()[0]
    finally:
        conn.close()
    manquants = [l["proprietaire_id"] for l in lignes if (l.get("montant_du_conciergerie") or 0) != 0]
    return _resultat(gauche, round(droit, 2), libelle_gauche="Lot12 (montant_du_conciergerie)",
                     libelle_droit="Journal VENTES (généré)",
                     detail=[{"proprietaires_attendus": manquants}])


# ── H. Total analytique ↔ résultat global ─────────────────────────────────────

def total_analytique_vs_resultat_global(*, vision: str = "REEL") -> dict[str, Any]:
    """Alias explicite de la réconciliation B pour ce vision — nommé séparément car le brief le
    demande comme point de contrôle final distinct (agrégat unique, pas par logement)."""
    return lot10_vs_analytique(vision=vision)


TOUTES = {
    "LOT9_LOT10": lot9_vs_lot10,
    "LOT10_ANALYTIQUE": lot10_vs_analytique,
    "ANALYTIQUE_COMPTABILITE": analytique_vs_comptabilite,
    "BANQUE_JOURNAL_BANQUE": banque_vs_journal_banque,
    "FACTURES_AUXILIAIRES": factures_vs_auxiliaires,
    "MENAGES_CHARGES": menages_vs_charges,
    "COMMISSIONS_VENTES": commissions_vs_ventes,
    "TOTAL_ANALYTIQUE_RESULTAT_GLOBAL": total_analytique_vs_resultat_global,
}

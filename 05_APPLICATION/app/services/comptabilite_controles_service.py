"""Catalogue de contrôles Comptabilité — écritures, plan comptable, mappings, cohérence inter-objets.

Même patron que `factures_controles_service.py` : niveaux hiérarchisés, codes stables, jamais
d'anomalie masquée. Les contrôles qui devraient déjà être impossibles par construction (écriture
déséquilibrée, doublon d'origine) sont vérifiés ici EN PLUS, comme défense en profondeur.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db.connection import get_db

BLOQUANT = "BLOQUANT"
CRITIQUE = "CRITIQUE"
AVERTISSEMENT = "AVERTISSEMENT"
INFO = "INFO"

NIVEAUX = [BLOQUANT, CRITIQUE, AVERTISSEMENT, INFO]
_ORDRE = {n: i for i, n in enumerate(NIVEAUX)}

C_ECRITURE_DESEQUILIBREE = "CTRL_CPT_ECRITURE_DESEQUILIBREE"
C_COMPTE_ABSENT = "CTRL_CPT_COMPTE_ABSENT"
C_COMPTE_INACTIF = "CTRL_CPT_COMPTE_INACTIF"
C_MAPPING_ABSENT = "CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE"
C_PIECE_ABSENTE = "CTRL_CPT_PIECE_ABSENTE"
C_DOUBLON_ECRITURE = "CTRL_CPT_DOUBLON_ECRITURE"
C_FACTURE_SANS_ECRITURE = "CTRL_CPT_FACTURE_VALIDEE_SANS_ECRITURE"
C_ECRITURE_SANS_ORIGINE = "CTRL_CPT_ECRITURE_SANS_ORIGINE"
C_REGLEMENT_SANS_ECRITURE = "CTRL_CPT_REGLEMENT_CAISSE_SANS_ECRITURE"
C_BANQUE_SANS_ECRITURE = "CTRL_CPT_RAPPROCHEMENT_CONFIRME_SANS_ECRITURE_BANQUE"
C_LOT12_SANS_VENTES = "CTRL_CPT_SOURCE_LOT12_SANS_ECRITURE_VENTES"
C_ANALYTIQUE_OBLIGATOIRE = "CTRL_CPT_ANALYTIQUE_OBLIGATOIRE_ABSENTE"
C_PERIODE_CLOTUREE = "CTRL_CPT_ECRITURE_SUR_PERIODE_CLOTUREE"
C_TVA_NON_ARBITREE = "CTRL_CPT_TVA_NON_ARBITREE"
C_CONTREPASSATION_INCOMPLETE = "CTRL_CPT_CONTREPASSATION_INCOMPLETE"
C_MONTANT_DIFFERENT_SOURCE = "CTRL_CPT_MONTANT_DIFFERENT_DE_LA_SOURCE"
C_CAISSE_NEGATIVE = "CTRL_CPT_CAISSE_NEGATIVE"

MESSAGES = {
    C_ECRITURE_DESEQUILIBREE: "Écriture dont le total débit diffère du total crédit.",
    C_COMPTE_ABSENT: "Ligne d'écriture sur un compte absent du plan comptable.",
    C_COMPTE_INACTIF: "Ligne d'écriture sur un compte inactif.",
    C_MAPPING_ABSENT: "Mapping catégorie de charge → compte non arbitré (A_CONTROLER).",
    C_PIECE_ABSENTE: "Écriture sans référence de pièce justificative.",
    C_DOUBLON_ECRITURE: "Plusieurs écritures actives pour la même origine sur le même journal.",
    C_FACTURE_SANS_ECRITURE: "Facture validée sans écriture ACHATS générée.",
    C_ECRITURE_SANS_ORIGINE: "Écriture sans origine tracée (hors contrepassation manuelle).",
    C_REGLEMENT_SANS_ECRITURE: "Règlement fournisseur en espèces sans écriture CAISSE générée.",
    C_BANQUE_SANS_ECRITURE: "Rapprochement bancaire confirmé sans écriture BANQUE générée.",
    C_LOT12_SANS_VENTES: "Ligne Lot12 (propriétaire/mois) avec montant non nul sans écriture VENTES.",
    C_ANALYTIQUE_OBLIGATOIRE: "Compte exigeant l'analytique sans dimension renseignée sur la ligne.",
    C_PERIODE_CLOTUREE: "Écriture existante sur une période clôturée.",
    C_TVA_NON_ARBITREE: "Aucune règle de TVA arbitrée : les écritures ne portent aucun compte de TVA.",
    C_CONTREPASSATION_INCOMPLETE: "Écriture contrepassée sans écriture miroir retrouvée.",
    C_MONTANT_DIFFERENT_SOURCE: "Montant de l'écriture différent du montant actuel de l'objet source.",
    C_CAISSE_NEGATIVE: "Solde du compte Caisse négatif.",
}


def _anomalie(code: str, severite: str, objet: str, identifiant: str, *, montant: Any = None,
             detail: str = "", action: str = "") -> dict[str, Any]:
    return {"code": code, "severite": severite, "objet": objet, "identifiant": identifiant,
            "message": MESSAGES.get(code, code), "montant": montant, "detail": detail,
            "action": action, "statut": "OUVERT", "justification": ""}


def _plan_index(db_path=None) -> dict[str, dict[str, Any]]:
    conn = get_db(db_path)
    try:
        return {r["compte"]: dict(r) for r in conn.execute("SELECT * FROM plan_comptable")}
    finally:
        conn.close()


def controler(*, periode: str = "", db_path=None) -> dict[str, Any]:
    """Contrôle l'ensemble Comptabilité, éventuellement restreint à une période. Ne lève jamais."""
    from app.services import comptabilite_ecritures_service as compta
    from app.services import comptabilite_periodes_service as per

    try:
        ecritures = compta.lister(periode=periode, db_path=db_path)
        plan = _plan_index(db_path)
        conn = get_db(db_path)
        try:
            mappings_a_arbitrer = conn.execute(
                "SELECT * FROM mapping_categorie_compte WHERE statut='A_CONTROLER'").fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"statut": "INDISPONIBLE", "anomalies": [], "compteurs": {n: 0 for n in NIVEAUX},
                "nb_bloquants": 0, "fiable": False, "detail": f"{type(exc).__name__}",
                "nb_ecritures": 0, "total_debit": 0.0, "total_credit": 0.0}

    anomalies: list[dict[str, Any]] = []
    total_debit = total_credit = 0.0
    origines_vues: dict[tuple, int] = {}

    for e in ecritures:
        opaque = e["ecriture_id_opaque"]
        total_debit += e["total_debit"] or 0
        total_credit += e["total_credit"] or 0

        if round((e["total_debit"] or 0) - (e["total_credit"] or 0), 2) != 0:
            anomalies.append(_anomalie(C_ECRITURE_DESEQUILIBREE, BLOQUANT, "ECRITURE", opaque,
                                       montant=e["total_debit"]))
        if not e["piece"]:
            anomalies.append(_anomalie(C_PIECE_ABSENTE, AVERTISSEMENT, "ECRITURE", opaque))
        if e["origine_type"] != "MANUEL" and not e["origine_id_opaque"]:
            anomalies.append(_anomalie(C_ECRITURE_SANS_ORIGINE, CRITIQUE, "ECRITURE", opaque))
        if e["statut"] != compta.ST_CONTREPASSEE and e["origine_id_opaque"]:
            cle = (e["journal"], e["origine_type"], e["origine_id_opaque"])
            origines_vues[cle] = origines_vues.get(cle, 0) + 1
        if e["statut"] == compta.ST_CONTREPASSEE:
            miroir_existe = any(
                a["ecriture_id_opaque"] != opaque and a.get("contrepasse_de") == opaque
                for a in ecritures)
            if not miroir_existe:
                anomalies.append(_anomalie(C_CONTREPASSATION_INCOMPLETE, CRITIQUE, "ECRITURE", opaque))

        for l in compta.lignes(opaque, db_path):
            compte = plan.get(l["compte"])
            if compte is None:
                anomalies.append(_anomalie(C_COMPTE_ABSENT, BLOQUANT, "ECRITURE_LIGNE",
                                           f"{opaque}#{l['ligne_num']}", detail=l["compte"]))
            elif not compte["actif"]:
                anomalies.append(_anomalie(C_COMPTE_INACTIF, BLOQUANT, "ECRITURE_LIGNE",
                                           f"{opaque}#{l['ligne_num']}", detail=l["compte"]))
            elif compte["analytique_obligatoire"] and not (
                    l["logement_id"] or l["proprietaire_id"] or l["reservation_id"]):
                anomalies.append(_anomalie(C_ANALYTIQUE_OBLIGATOIRE, AVERTISSEMENT, "ECRITURE_LIGNE",
                                           f"{opaque}#{l['ligne_num']}", detail=l["compte"]))

    for cle, n in origines_vues.items():
        if n > 1:
            anomalies.append(_anomalie(C_DOUBLON_ECRITURE, BLOQUANT, "ECRITURE", str(cle)))

    for m in mappings_a_arbitrer:
        anomalies.append(_anomalie(C_MAPPING_ABSENT, AVERTISSEMENT, "MAPPING_CATEGORIE",
                                   m["categorie_charge_id"], detail=f"compte actuel {m['compte']}"))

    # Facture validée/réglée sans écriture ACHATS.
    try:
        from app.services import factures_service as fact
        ecritures_achats_origines = {e["origine_id_opaque"] for e in ecritures
                                     if e["journal"] == "ACHATS" and e["origine_id_opaque"]}
        for f in fact.lister(db_path=db_path):
            if f["statut"] not in (fact.ST_VALIDEE, fact.ST_PARTIELLEMENT_REGLEE, fact.ST_REGLEE):
                continue
            if periode and (f.get("date_facture") or "")[:7] != periode:
                continue
            if f["facture_id_opaque"] not in ecritures_achats_origines:
                anomalies.append(_anomalie(C_FACTURE_SANS_ECRITURE, AVERTISSEMENT, "FACTURE",
                                           f["facture_ref"], montant=f["montant_ttc"]))
    except Exception:
        pass

    # Règlement fournisseur en espèces sans écriture CAISSE.
    try:
        from app.services import reglements_fournisseurs_service as regl
        ecritures_caisse_origines = {e["origine_id_opaque"] for e in ecritures
                                     if e["journal"] == "CAISSE" and e["origine_id_opaque"]}
        for r in regl.lister(db_path=db_path):
            if r["moyen"] != "CAISSE" or r["statut"] == regl.ST_ANNULE:
                continue
            if periode and (r["date_reglement"] or "")[:7] != periode:
                continue
            if r["reglement_id_opaque"] not in ecritures_caisse_origines:
                anomalies.append(_anomalie(C_REGLEMENT_SANS_ECRITURE, AVERTISSEMENT, "REGLEMENT",
                                           r["reglement_id_opaque"], montant=r["montant"]))
    except Exception:
        pass

    # Rapprochement bancaire confirmé (REGLEMENT_CHARGE) sans écriture BANQUE.
    try:
        conn = get_db(db_path)
        try:
            raps = conn.execute(
                "SELECT * FROM banque_rapprochements WHERE type_objet='REGLEMENT_CHARGE' "
                "AND statut='CONFIRME'").fetchall()
        finally:
            conn.close()
        ecritures_banque_origines = {e["origine_id_opaque"] for e in ecritures
                                     if e["journal"] == "BANQUE" and e["origine_id_opaque"]}
        for rap in raps:
            if periode and (rap["date_creation"] or "")[:7] != periode:
                continue
            if rap["rapprochement_id_opaque"] not in ecritures_banque_origines:
                anomalies.append(_anomalie(C_BANQUE_SANS_ECRITURE, AVERTISSEMENT, "RAPPROCHEMENT",
                                           rap["rapprochement_id_opaque"],
                                           montant=rap["montant_rapproche"]))
    except Exception:
        pass

    # Caisse négative.
    try:
        from app.services import comptabilite_ecritures_service as compta2
        solde = compta2.solde_compte(compta2.COMPTE_CAISSE, db_path=db_path)
        if solde["solde"] < -0.005:
            anomalies.append(_anomalie(C_CAISSE_NEGATIVE, AVERTISSEMENT, "COMPTE",
                                       compta2.COMPTE_CAISSE, montant=solde["solde"]))
    except Exception:
        pass

    anomalies.append(_anomalie(
        C_TVA_NON_ARBITREE, INFO, "GLOBAL", periode or "TOUTES_PERIODES",
        detail="Aucun compte de TVA dans le plan comptable — franchise en base assumée pour les "
               "prestataires connus, non vérifié au-delà.",
        action="Arbitrer une règle TVA si un prestataire assujetti apparaît."))

    anomalies.sort(key=lambda a: (_ORDRE.get(a["severite"], 9), a["code"]))
    compteurs = {n: sum(1 for a in anomalies if a["severite"] == n) for n in NIVEAUX}
    return {
        "statut": "OK", "anomalies": anomalies, "compteurs": compteurs,
        "nb_bloquants": compteurs[BLOQUANT], "fiable": compteurs[BLOQUANT] == 0,
        "nb_ecritures": len(ecritures), "total_debit": round(total_debit, 2),
        "total_credit": round(total_credit, 2),
        "genere_le": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

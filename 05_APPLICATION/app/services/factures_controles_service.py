"""Catalogue de contrôles Factures / Règlements / lien Banque / lien Charges.

Même patron que `banques_controles_catalogue_service.py` : niveaux hiérarchisés, codes stables,
aucune anomalie masquée. Les contrôles qui REFUSENT déjà une écriture (doublon certain, dépassement
de solde…) sont vérifiés ici *a posteriori* comme défense en profondeur : si une donnée incohérente
existe malgré tout en base, elle doit être visible plutôt que silencieuse.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import factures_banque_service as pont
from app.services import factures_service as fact
from app.services import reglements_fournisseurs_service as regl

BLOQUANT = "BLOQUANT"
CRITIQUE = "CRITIQUE"
AVERTISSEMENT = "AVERTISSEMENT"
INFO = "INFO"
RESOLU = "RESOLU"
JUSTIFIE = "JUSTIFIE"

NIVEAUX = [BLOQUANT, CRITIQUE, AVERTISSEMENT, INFO, RESOLU, JUSTIFIE]
_ORDRE = {n: i for i, n in enumerate(NIVEAUX)}

# ── Codes — Factures ────────────────────────────────────────────────────────
F_DOUBLON_CERTAIN = "CTRL_FAC_DOUBLON_CERTAIN"
F_DOUBLON_PROBABLE = "CTRL_FAC_DOUBLON_PROBABLE"
F_FOURNISSEUR_ABSENT = "CTRL_FAC_FOURNISSEUR_ABSENT"
F_FOURNISSEUR_ARCHIVE = "CTRL_FAC_FOURNISSEUR_ARCHIVE"
F_REF_ABSENTE = "CTRL_FAC_REFERENCE_ABSENTE"
F_DATE_INVALIDE = "CTRL_FAC_DATE_INVALIDE"
F_ECHEANCE_ANTERIEURE = "CTRL_FAC_ECHEANCE_ANTERIEURE"
F_TVA_INCOHERENTE = "CTRL_FAC_HT_TVA_DIFFERENT_TTC"
F_DEVISE_INCONNUE = "CTRL_FAC_DEVISE_INCONNUE"
F_JUSTIFICATIF_ABSENT = "CTRL_FAC_JUSTIFICATIF_ABSENT"
F_VALIDEE_SANS_CHARGE = "CTRL_FAC_VALIDEE_SANS_CHARGE"
F_CHARGE_MULTI_FACTURES = "CTRL_FAC_CHARGE_LIEE_A_PLUSIEURS_FACTURES"
F_ANNULEE_AVEC_REGLEMENT = "CTRL_FAC_ANNULEE_AVEC_REGLEMENT_ACTIF"
F_REGLEE_SOLDE_NON_NUL = "CTRL_FAC_REGLEE_AVEC_SOLDE_NON_NUL"
F_OUVERTE_SOLDE_NUL = "CTRL_FAC_OUVERTE_AVEC_SOLDE_NUL"
F_EN_RETARD = "CTRL_FAC_EN_RETARD"

# ── Codes — Règlements ──────────────────────────────────────────────────────
R_MONTANT_INVALIDE = "CTRL_REG_MONTANT_NUL_OU_NEGATIF"
R_SUPERIEUR_SOLDE = "CTRL_REG_SUPERIEUR_AU_SOLDE"
R_FOURNISSEUR_INCOHERENT = "CTRL_REG_FOURNISSEUR_INCOHERENT"
R_FACTURE_INEXISTANTE = "CTRL_REG_FACTURE_INEXISTANTE"
R_REPARTITION_INCOHERENTE = "CTRL_REG_REPARTITION_INCOHERENTE"
R_ANNULE_COMPTABILISE = "CTRL_REG_ANNULE_ENCORE_COMPTABILISE"
R_BANQUE_SANS_MOUVEMENT = "CTRL_REG_BANQUE_SANS_RAPPROCHEMENT"
R_RAPPROCHE_AU_DELA = "CTRL_REG_RAPPROCHE_AU_DELA_DE_SON_MONTANT"

# ── Codes — Banque ──────────────────────────────────────────────────────────
B_ANNULE_ENCORE_ACTIF = "CTRL_BQ_RAPPROCHEMENT_ANNULE_ENCORE_ACTIF"

MESSAGES = {
    F_DOUBLON_CERTAIN: "Deux factures partagent la même référence pour le même fournisseur.",
    F_DOUBLON_PROBABLE: "Facture de même montant et de date proche chez le même fournisseur.",
    F_FOURNISSEUR_ABSENT: "Facture sans fournisseur.",
    F_FOURNISSEUR_ARCHIVE: "Facture rattachée à un fournisseur archivé.",
    F_REF_ABSENTE: "Facture sans référence.",
    F_DATE_INVALIDE: "Date de facture absente ou illisible.",
    F_ECHEANCE_ANTERIEURE: "Échéance antérieure à la date de facture.",
    F_TVA_INCOHERENTE: "Montant HT + TVA ne correspond pas au TTC.",
    F_DEVISE_INCONNUE: "Devise autre que l'euro.",
    F_JUSTIFICATIF_ABSENT: "Facture validée sans justificatif.",
    F_VALIDEE_SANS_CHARGE: "Facture validée sans charge économique rattachée.",
    F_CHARGE_MULTI_FACTURES: "Une même charge est rattachée à plusieurs factures.",
    F_ANNULEE_AVEC_REGLEMENT: "Facture annulée alors qu'un règlement actif l'impute encore.",
    F_REGLEE_SOLDE_NON_NUL: "Facture marquée réglée alors qu'un solde reste dû.",
    F_OUVERTE_SOLDE_NUL: "Facture encore ouverte alors que son solde est nul.",
    F_EN_RETARD: "Facture échue non réglée.",
    R_MONTANT_INVALIDE: "Règlement au montant nul ou négatif.",
    R_SUPERIEUR_SOLDE: "Les règlements actifs dépassent le montant de la facture.",
    R_FOURNISSEUR_INCOHERENT: "Règlement imputé sur une facture d'un autre fournisseur.",
    R_FACTURE_INEXISTANTE: "Répartition pointant une facture inexistante.",
    R_REPARTITION_INCOHERENTE: "La somme des répartitions diffère du montant du règlement.",
    R_ANNULE_COMPTABILISE: "Règlement annulé encore compté dans un solde.",
    R_BANQUE_SANS_MOUVEMENT: "Règlement déclaré « banque » sans rapprochement bancaire.",
    R_RAPPROCHE_AU_DELA: "Les rapprochements dépassent le montant du règlement.",
    B_ANNULE_ENCORE_ACTIF: "Rapprochement annulé encore compté comme actif.",
}

DEVISES_CONNUES = {"EUR"}


def _anomalie(code: str, severite: str, objet: str, identifiant: str, *, montant: Any = None,
              detail: str = "", action: str = "") -> dict[str, Any]:
    return {"code": code, "severite": severite, "objet": objet, "identifiant": identifiant,
            "message": MESSAGES.get(code, code), "montant": montant, "detail": detail,
            "action": action, "statut": "OUVERT", "justification": ""}


def _fournisseurs_index(db_path=None) -> dict[str, dict[str, Any]]:
    """Fournisseurs UTILISABLES. Deux colonnes distinctes dans le schéma 0010, à ne pas confondre :
    `actif` = 1 signifie « ligne courante » (0 = ligne historisée), tandis que `statut` porte
    ACTIF/INACTIF, c'est-à-dire l'archivage métier."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM fournisseurs WHERE actif=1 AND statut='ACTIF'").fetchall()
        return {r["fournisseur_id_opaque"]: dict(r) for r in rows}
    finally:
        conn.close()


def _controler_facture(f: dict[str, Any], frs_idx: dict, refs_vues: dict, charges_vues: dict,
                       db_path=None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    fid = f["facture_id_opaque"]
    ref = f["facture_ref"]
    aujourdhui = date.today().isoformat()

    def add(code, sev, **kw):
        kw.setdefault("montant", f["montant_ttc"])      # surchargeable par l'appelant
        out.append(_anomalie(code, sev, "FACTURE", ref or fid, **kw))

    if not f["fournisseur_id_opaque"]:
        add(F_FOURNISSEUR_ABSENT, BLOQUANT, action="Rattacher un fournisseur.")
    elif f["fournisseur_id_opaque"] not in frs_idx:
        add(F_FOURNISSEUR_ARCHIVE, AVERTISSEMENT,
            detail=f["fournisseur_id_opaque"],
            action="Vérifier : un fournisseur archivé reste visible sur les anciennes factures.")

    if not ref:
        add(F_REF_ABSENTE, CRITIQUE, action="Saisir la référence portée par la pièce.")
    else:
        cle = (f["fournisseur_id_opaque"], ref)
        if refs_vues.get(cle, 0) > 1 and f["statut"] != fact.ST_ANNULEE:
            add(F_DOUBLON_CERTAIN, BLOQUANT, detail=ref, action="Annuler la facture en double.")

    if not f["date_facture"]:
        add(F_DATE_INVALIDE, CRITIQUE, action="Saisir la date de facture.")
    elif f["date_echeance"] and f["date_echeance"] < f["date_facture"]:
        add(F_ECHEANCE_ANTERIEURE, CRITIQUE,
            detail=f"{f['date_echeance']} < {f['date_facture']}", action="Corriger les dates.")

    ht, tva, ttc = f["montant_ht"], f["montant_tva"], f["montant_ttc"]
    if ht is not None and tva is not None and ttc is not None and abs((ht + tva) - ttc) > 0.02:
        add(F_TVA_INCOHERENTE, CRITIQUE, detail=f"{ht} + {tva} != {ttc}",
            action="Corriger les montants.")

    if (f["devise"] or "EUR") not in DEVISES_CONNUES:
        add(F_DEVISE_INCONNUE, CRITIQUE, detail=f["devise"])

    if f["statut"] in (fact.ST_VALIDEE, fact.ST_PARTIELLEMENT_REGLEE, fact.ST_REGLEE):
        if not f["justificatif"]:
            add(F_JUSTIFICATIF_ABSENT, AVERTISSEMENT, action="Joindre la pièce fournisseur.")
        if not f["charge_id"]:
            add(F_VALIDEE_SANS_CHARGE, CRITIQUE,
                action="Rattacher la charge économique créée par le parcours Charges.")

    if f["charge_id"] and charges_vues.get(f["charge_id"], 0) > 1:
        add(F_CHARGE_MULTI_FACTURES, BLOQUANT, detail=f["charge_id"],
            action="Une charge ne doit correspondre qu'à une seule facture.")

    solde = f["solde_restant"]
    if f["statut"] == fact.ST_ANNULEE and f["montant_regle"] > 0.005:
        add(F_ANNULEE_AVEC_REGLEMENT, BLOQUANT, montant=f["montant_regle"],
            action="Annuler le règlement ou rouvrir la facture.")
    if f["statut"] == fact.ST_REGLEE and solde > 0.005:
        add(F_REGLEE_SOLDE_NON_NUL, BLOQUANT, montant=solde)
    if f["statut"] in (fact.ST_VALIDEE, fact.ST_PARTIELLEMENT_REGLEE) and abs(solde) < 0.005:
        add(F_OUVERTE_SOLDE_NUL, AVERTISSEMENT, action="Passer la facture en REGLEE.")
    if f["statut"] in fact.STATUTS_OUVERTS and solde > 0.005 and f["date_echeance"] \
            and f["date_echeance"] < aujourdhui:
        add(F_EN_RETARD, AVERTISSEMENT, montant=solde, detail=f["date_echeance"],
            action="Régler ou justifier le retard.")

    if f["montant_regle"] > (f["montant_ttc"] or 0) + 0.005:
        add(R_SUPERIEUR_SOLDE, BLOQUANT, montant=f["montant_regle"])

    return out


def _controler_reglement(r: dict[str, Any], db_path=None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rid = r["reglement_id_opaque"]

    def add(code, sev, **kw):
        kw.setdefault("montant", r["montant"])          # surchargeable par l'appelant
        out.append(_anomalie(code, sev, "REGLEMENT", rid, **kw))

    if (r["montant"] or 0) <= 0:
        add(R_MONTANT_INVALIDE, BLOQUANT)

    detail = regl.charger(rid, db_path)
    reps = detail["repartitions"] if detail else []
    somme = round(sum(x["montant"] for x in reps), 2)
    if reps and abs(somme - (r["montant"] or 0)) > 0.02:
        add(R_REPARTITION_INCOHERENTE, BLOQUANT,
            detail=f"répartitions {somme} != règlement {r['montant']}",
            action="Corriger la ventilation du paiement groupé.")

    for rep in reps:
        f = fact.charger(rep["facture_id_opaque"], db_path)
        if f is None:
            add(R_FACTURE_INEXISTANTE, BLOQUANT, detail=rep["facture_id_opaque"])
            continue
        if f["fournisseur_id_opaque"] != r["fournisseur_id_opaque"]:
            add(R_FOURNISSEUR_INCOHERENT, BLOQUANT, detail=f["facture_ref"])

    if r["statut"] != regl.ST_ANNULE:
        etat = pont.etat_reglement(rid, db_path)
        if etat["montant_rapproche"] > (r["montant"] or 0) + 0.005:
            add(R_RAPPROCHE_AU_DELA, BLOQUANT, montant=etat["montant_rapproche"])
        if r["moyen"] == "BANQUE" and etat["statut"] == "NON_RAPPROCHE":
            add(R_BANQUE_SANS_MOUVEMENT, AVERTISSEMENT,
                action="Rapprocher le mouvement bancaire correspondant.")

    return out


def controler(db_path=None) -> dict[str, Any]:
    """Contrôle l'ensemble factures + règlements. Ne lève jamais."""
    try:
        factures = fact.lister(db_path=db_path)
        reglements = regl.lister(db_path=db_path)
    except Exception as exc:
        return {"statut": "INDISPONIBLE", "anomalies": [], "detail": f"{type(exc).__name__}",
                "compteurs": {n: 0 for n in NIVEAUX}, "nb_bloquants": 0, "fiable": False}

    frs_idx = _fournisseurs_index(db_path)
    refs_vues: dict[tuple, int] = {}
    charges_vues: dict[str, int] = {}
    for f in factures:
        if f["statut"] == fact.ST_ANNULEE:
            continue
        refs_vues[(f["fournisseur_id_opaque"], f["facture_ref"])] = \
            refs_vues.get((f["fournisseur_id_opaque"], f["facture_ref"]), 0) + 1
        if f["charge_id"]:
            charges_vues[f["charge_id"]] = charges_vues.get(f["charge_id"], 0) + 1

    anomalies: list[dict[str, Any]] = []
    for f in factures:
        anomalies.extend(_controler_facture(f, frs_idx, refs_vues, charges_vues, db_path))
    for r in reglements:
        anomalies.extend(_controler_reglement(r, db_path))

    # Doublons probables (même fournisseur, même montant, dates proches, références différentes).
    for f in factures:
        if f["statut"] == fact.ST_ANNULEE:
            continue
        for autre in fact.doublons_probables(f["fournisseur_id_opaque"], f["montant_ttc"],
                                             f["date_facture"] or "", db_path=db_path):
            if autre["facture_ref"] != f["facture_ref"] and autre["facture_ref"] > f["facture_ref"]:
                anomalies.append(_anomalie(
                    F_DOUBLON_PROBABLE, AVERTISSEMENT, "FACTURE", f["facture_ref"],
                    montant=f["montant_ttc"], detail=f"proche de {autre['facture_ref']}",
                    action="Vérifier qu'il ne s'agit pas de la même pièce."))

    anomalies.sort(key=lambda a: (_ORDRE.get(a["severite"], 9), a["code"]))
    compteurs = {n: sum(1 for a in anomalies if a["severite"] == n) for n in NIVEAUX}
    return {
        "statut": "OK", "anomalies": anomalies, "compteurs": compteurs,
        "nb_bloquants": compteurs[BLOQUANT], "fiable": compteurs[BLOQUANT] == 0,
        "genere_le": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

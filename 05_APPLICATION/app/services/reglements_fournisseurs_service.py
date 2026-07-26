"""Règlements fournisseurs — le paiement effectué (distinct de la facture, de la charge et du
rapprochement bancaire).

Supporte : paiement total, partiel, plusieurs paiements sur une facture, paiement groupé de
plusieurs factures, banque / caisse / paiement personnel associé, acompte, avoir, remboursement.

Le montant d'un règlement est la SOMME de ses répartitions par facture (`reglement_repartitions`) :
un paiement groupé est donc nativement représentable, sans champ dénormalisé qui pourrait dériver.

Le lien vers le mouvement bancaire n'est PAS refait ici : il passe par
`banques_rapprochement_service` (service unique et partagé, décision §13 de la consigne).
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.services import factures_service as fact

MOYENS = ("BANQUE", "CAISSE", "PERSONNEL_ASSOCIE", "ACOMPTE", "AVOIR", "REMBOURSEMENT_FOURNISSEUR")

ST_ENREGISTRE = "ENREGISTRE"
ST_RAPPROCHE = "RAPPROCHE"
ST_ANNULE = "ANNULE"

E_FLAGS = "E_FLAGS_DESACTIVES"
E_MOYEN_INCONNU = "V01_MOYEN_INCONNU"
E_MONTANT_INVALIDE = "V02_MONTANT_INVALIDE"
E_SANS_FACTURE = "V03_REGLEMENT_SANS_FACTURE"
E_FACTURE_INTROUVABLE = "V04_FACTURE_INTROUVABLE"
E_FACTURE_ANNULEE = "V05_FACTURE_ANNULEE"
E_FOURNISSEUR_INCOHERENT = "V06_FOURNISSEUR_INCOHERENT"
E_DEPASSEMENT = "V07_MONTANT_SUPERIEUR_AU_SOLDE"
E_REPARTITION = "V08_REPARTITION_INCOHERENTE"
E_INTROUVABLE = "E01_REGLEMENT_INTROUVABLE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation : l'enregistrement est impossible.",
    E_MOYEN_INCONNU: "Moyen de paiement inconnu.",
    E_MONTANT_INVALIDE: "Le montant doit être un nombre strictement positif.",
    E_SANS_FACTURE: "Un règlement doit porter sur au moins une facture.",
    E_FACTURE_INTROUVABLE: "Facture introuvable.",
    E_FACTURE_ANNULEE: "Cette facture est annulée : elle ne peut plus être réglée.",
    E_FOURNISSEUR_INCOHERENT: "Une facture réglée appartient à un autre fournisseur.",
    E_DEPASSEMENT: "Le montant réglé dépasserait le solde restant de la facture.",
    E_REPARTITION: "La somme des répartitions ne correspond pas au montant du règlement.",
    E_INTROUVABLE: "Règlement introuvable.",
}


def _flags_actifs() -> bool:
    return bool(cfg.FACTURES_REAL_WRITE_ENABLED and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED)


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _nombre(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def enregistrer(fournisseur_opaque: str, repartitions: list[dict[str, Any]], *,
                date_reglement: str, moyen: str, compte: str = "", commentaire: str = "",
                acteur: str = "", db_path=None) -> dict[str, Any]:
    """Enregistre un règlement ventilé sur une ou plusieurs factures.

    `repartitions` : [{facture_id_opaque, montant}, ...] — une seule entrée = paiement simple,
    plusieurs = paiement groupé.
    """
    if not _flags_actifs():
        return _refus(E_FLAGS)
    if moyen not in MOYENS:
        return _refus(E_MOYEN_INCONNU, moyen)
    if not repartitions:
        return _refus(E_SANS_FACTURE)

    total = 0.0
    for r in repartitions:
        m = _nombre(r.get("montant"))
        if m is None or m <= 0:
            return _refus(E_MONTANT_INVALIDE, _txt(r.get("montant")))
        fid = _txt(r.get("facture_id_opaque"))
        f = fact.charger(fid, db_path)
        if f is None:
            return _refus(E_FACTURE_INTROUVABLE, fid)
        if f["statut"] == fact.ST_ANNULEE:
            return _refus(E_FACTURE_ANNULEE, fid)
        if f["fournisseur_id_opaque"] != fournisseur_opaque:
            return _refus(E_FOURNISSEUR_INCOHERENT, fid)
        if m > f["solde_restant"] + 1e-9:
            return _refus(E_DEPASSEMENT,
                          f"{fid} : {m:.2f} € > solde {f['solde_restant']:.2f} €")
        total += m

    opaque = "REG-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO reglements_fournisseurs (reglement_id_opaque, fournisseur_id_opaque, "
            "date_reglement, montant, moyen, compte, statut, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (opaque, fournisseur_opaque, date_reglement, round(total, 2), moyen,
             compte or None, ST_ENREGISTRE, commentaire or None, acteur or "local"))
        for r in repartitions:
            conn.execute(
                "INSERT INTO reglement_repartitions (reglement_id_opaque, facture_id_opaque, montant) "
                "VALUES (?,?,?)",
                (opaque, _txt(r.get("facture_id_opaque")), _nombre(r.get("montant"))))
        conn.commit()
    finally:
        conn.close()

    # Met à jour le statut des factures touchées (REGLEE / PARTIELLEMENT_REGLEE) — jamais un
    # montant stocké, uniquement le statut, qui reste dérivé du solde recalculé.
    for r in repartitions:
        _rafraichir_statut_facture(_txt(r.get("facture_id_opaque")), acteur=acteur, db_path=db_path)

    return {"ok": True, "reglement_id_opaque": opaque, "montant": round(total, 2),
            "nb_factures": len(repartitions)}


def _rafraichir_statut_facture(facture_opaque: str, *, acteur: str = "", db_path=None) -> None:
    """Recalcule le statut de règlement d'une facture depuis son solde, DANS LES DEUX SENS.

    Le statut de règlement est une valeur DÉRIVÉE : il doit redescendre quand le solde remonte
    (annulation d'un règlement), pas seulement monter quand on paie. Sans cela une facture restait
    « REGLEE » avec un solde non nul après annulation — incohérence réellement observée en recette
    et détectée par `CTRL_FAC_REGLEE_AVEC_SOLDE_NON_NUL`.

    Les statuts portés par une décision humaine (ANNULEE, LITIGE) ne sont jamais écrasés ici.
    Comme il s'agit d'une dérivation et non d'une transition humaine, la table `TRANSITIONS` n'a
    pas à s'appliquer (elle interdit REGLEE -> VALIDEE, qui est pourtant le retour légitime après
    annulation d'un paiement).
    """
    f = fact.charger(facture_opaque, db_path)
    if f is None or f["statut"] in (fact.ST_ANNULEE, fact.ST_LITIGE):
        return
    solde = f["solde_restant"]
    regle = f["montant_regle"]
    if solde <= 0.005:
        cible = fact.ST_REGLEE
    elif regle > 0.005:
        cible = fact.ST_PARTIELLEMENT_REGLEE
    elif f["statut"] in (fact.ST_REGLEE, fact.ST_PARTIELLEMENT_REGLEE):
        cible = fact.ST_VALIDEE      # plus aucun règlement actif : retour à l'état validé
    else:
        return
    if cible == f["statut"]:
        return
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE factures SET statut=?, version=version+1 WHERE facture_id_opaque=?",
                     (cible, facture_opaque))
        conn.execute(
            "INSERT INTO facture_evenements (facture_id_opaque, type_evenement, ancien_statut, "
            "nouveau_statut, commentaire, acteur) VALUES (?,?,?,?,?,?)",
            (facture_opaque, "REGLEMENT", f["statut"], cible, "statut dérivé du solde",
             acteur or "local"))
        conn.commit()
    finally:
        conn.close()


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM reglements_fournisseurs WHERE reglement_id_opaque=?",
                           (opaque,)).fetchone()
        if row is None:
            return None
        reps = conn.execute(
            "SELECT * FROM reglement_repartitions WHERE reglement_id_opaque=? ORDER BY id",
            (opaque,)).fetchall()
    finally:
        conn.close()
    r = dict(row)
    r["repartitions"] = [dict(x) for x in reps]
    return r


def lister(*, fournisseur: str = "", non_rapproches_seulement: bool = False,
           db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM reglements_fournisseurs ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        d = dict(r)
        if fournisseur and d["fournisseur_id_opaque"] != fournisseur:
            continue
        if non_rapproches_seulement and d["statut"] == ST_RAPPROCHE:
            continue
        out.append(d)
    return out


def reglements_de_facture(facture_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT g.*, r.montant AS montant_impute FROM reglement_repartitions r "
            "JOIN reglements_fournisseurs g ON g.reglement_id_opaque = r.reglement_id_opaque "
            "WHERE r.facture_id_opaque=? ORDER BY g.id DESC", (facture_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def annuler(opaque: str, *, commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Annule un règlement : ses répartitions cessent de compter dans le solde des factures
    (jamais de suppression physique)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    reg = charger(opaque, db_path)
    if reg is None:
        return _refus(E_INTROUVABLE, opaque)
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE reglements_fournisseurs SET statut=?, version=version+1 "
                     "WHERE reglement_id_opaque=?", (ST_ANNULE, opaque))
        conn.commit()
    finally:
        conn.close()
    for r in reg["repartitions"]:
        _rafraichir_statut_facture(r["facture_id_opaque"], acteur=acteur, db_path=db_path)
    return {"ok": True, "reglement_id_opaque": opaque, "statut": ST_ANNULE}


def marquer_rapproche(opaque: str, mouvement_id_opaque: str, *, db_path=None) -> dict[str, Any]:
    """Trace le mouvement bancaire lié. Le LIEN faisant foi reste celui de
    `banques_rapprochement_service` — cette colonne n'est qu'un raccourci d'affichage."""
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM reglements_fournisseurs WHERE reglement_id_opaque=?",
                        (opaque,)).fetchone() is None:
            return _refus(E_INTROUVABLE, opaque)
        conn.execute("UPDATE reglements_fournisseurs SET mouvement_id_opaque=?, statut=? "
                     "WHERE reglement_id_opaque=?", (mouvement_id_opaque, ST_RAPPROCHE, opaque))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "reglement_id_opaque": opaque, "mouvement_id_opaque": mouvement_id_opaque}

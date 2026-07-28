"""Factures fournisseurs — dette et pièce fournisseur (module Fournisseurs/Factures/Règlements).

Quatre objets distincts, jamais confondus (cf. `32_AUDIT_FOURNISSEURS_FACTURES.md`) :
facture (ici) / charge (Excel, parcours Charges existant) / règlement
(`reglements_service.py`) / rapprochement (`banques_rapprochement_service.py`).

Ce service **ne crée jamais de charge** : il enregistre au mieux le lien vers une charge produite
par le parcours Charges existant, sans contourner ses contrôles.

`montant_regle` et `solde_restant` ne sont jamais stockés : ils sont recalculés depuis les
répartitions de règlements, pour qu'ils ne puissent pas dériver de la vérité.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime, timezone
from typing import Any

import app.config as cfg
from app.db.connection import get_db

ST_BROUILLON = "BROUILLON"
ST_A_CONTROLER = "A_CONTROLER"
ST_VALIDEE = "VALIDEE"
ST_PARTIELLEMENT_REGLEE = "PARTIELLEMENT_REGLEE"
ST_REGLEE = "REGLEE"
ST_ANNULEE = "ANNULEE"
ST_LITIGE = "LITIGE"

STATUTS = [ST_BROUILLON, ST_A_CONTROLER, ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_REGLEE,
           ST_ANNULEE, ST_LITIGE]
# Statuts « vivants » : la facture représente encore une dette potentielle.
STATUTS_OUVERTS = (ST_A_CONTROLER, ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_LITIGE)

E_FLAGS = "E_FLAGS_DESACTIVES"
E_FOURNISSEUR_MANQUANT = "V01_FOURNISSEUR_MANQUANT"
E_FOURNISSEUR_ARCHIVE = "V02_FOURNISSEUR_ARCHIVE"
E_REF_MANQUANTE = "V03_REFERENCE_MANQUANTE"
E_MONTANT_INVALIDE = "V04_MONTANT_INVALIDE"
E_DOUBLON_CERTAIN = "V05_DOUBLON_CERTAIN"
E_TVA_INCOHERENTE = "V06_HT_PLUS_TVA_DIFFERENT_TTC"
E_DATE_INCOHERENTE = "V07_ECHEANCE_ANTERIEURE_FACTURE"
E_INTROUVABLE = "E01_FACTURE_INTROUVABLE"
E_STATUT = "E02_TRANSITION_INTERDITE"
E_CHARGE_DEJA_LIEE = "E03_CHARGE_DEJA_LIEE"
E_LIGNE_CHARGE_MANQUANTE = "V08_LIGNE_CHARGE_MANQUANTE"
E_LIGNE_MONTANT_INVALIDE = "V09_LIGNE_MONTANT_INVALIDE"
E_LIGNE_FACTURE_MONO_CHARGE = "E04_FACTURE_DEJA_MONO_CHARGE"

MESSAGES = {
    E_FLAGS: "Écriture désactivée sur cette installation : l'enregistrement est impossible.",
    E_FOURNISSEUR_MANQUANT: "Le fournisseur est obligatoire.",
    E_FOURNISSEUR_ARCHIVE: "Ce fournisseur est archivé : il ne peut plus recevoir de nouvelle facture.",
    E_REF_MANQUANTE: "La référence de facture est obligatoire.",
    E_MONTANT_INVALIDE: "Le montant TTC doit être un nombre différent de zéro.",
    E_DOUBLON_CERTAIN: "Une facture avec cette référence existe déjà pour ce fournisseur.",
    E_TVA_INCOHERENTE: "Montant HT + TVA ne correspond pas au montant TTC.",
    E_DATE_INCOHERENTE: "La date d'échéance est antérieure à la date de facture.",
    E_INTROUVABLE: "Facture introuvable.",
    E_STATUT: "Transition de statut interdite.",
    E_CHARGE_DEJA_LIEE: "Cette charge est déjà rattachée à une autre facture.",
    E_LIGNE_CHARGE_MANQUANTE: "La charge est obligatoire pour ajouter une ligne.",
    E_LIGNE_MONTANT_INVALIDE: "Le montant TTC de la ligne doit être un nombre différent de zéro.",
    E_LIGNE_FACTURE_MONO_CHARGE: "Cette facture porte déjà une charge unique (charge_id) : "
                                  "utiliser soit charge_id, soit des lignes, jamais les deux.",
}

# Transitions autorisées. Une facture VALIDEE n'est jamais supprimée : elle est ANNULEE (tracée).
TRANSITIONS: dict[str, set[str]] = {
    ST_BROUILLON: {ST_A_CONTROLER, ST_VALIDEE, ST_ANNULEE},
    ST_A_CONTROLER: {ST_VALIDEE, ST_LITIGE, ST_ANNULEE},
    ST_VALIDEE: {ST_PARTIELLEMENT_REGLEE, ST_REGLEE, ST_LITIGE, ST_ANNULEE},
    ST_PARTIELLEMENT_REGLEE: {ST_REGLEE, ST_LITIGE, ST_ANNULEE},
    ST_REGLEE: {ST_LITIGE},
    ST_LITIGE: {ST_VALIDEE, ST_PARTIELLEMENT_REGLEE, ST_REGLEE, ST_ANNULEE},
    ST_ANNULEE: set(),
}


def _flags_actifs() -> bool:
    return bool(cfg.FACTURES_REAL_WRITE_ENABLED and cfg.FACTURES_REAL_WRITE_CONFIRMATION_ENABLED)


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "message": MESSAGES.get(code, code), "detail": detail}


def _nombre(v: Any) -> float | None:
    if v is None or _txt(v) == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def empreinte(fournisseur: str, ref: str, montant_ttc: Any, date_facture: str) -> str:
    base = f"{fournisseur}|{ref.upper()}|{montant_ttc}|{date_facture}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def valider(form: dict[str, Any], db_path=None, fournisseur_actif: bool | None = None) -> list[dict[str, str]]:
    """Contrôles métier AVANT écriture. Liste vide = valide."""
    erreurs: list[dict[str, str]] = []

    def err(code: str, detail: str = ""):
        erreurs.append({"code": code, "message": MESSAGES.get(code, code), "detail": detail})

    frs = _txt(form.get("fournisseur_id_opaque"))
    if not frs:
        err(E_FOURNISSEUR_MANQUANT)
    elif fournisseur_actif is False:
        err(E_FOURNISSEUR_ARCHIVE, frs)

    ref = _txt(form.get("facture_ref"))
    if not ref:
        err(E_REF_MANQUANTE)

    ttc = _nombre(form.get("montant_ttc"))
    if ttc is None or abs(ttc) < 0.005:
        err(E_MONTANT_INVALIDE, _txt(form.get("montant_ttc")))

    ht, tva = _nombre(form.get("montant_ht")), _nombre(form.get("montant_tva"))
    if ht is not None and tva is not None and ttc is not None:
        if abs((ht + tva) - ttc) > 0.02:
            err(E_TVA_INCOHERENTE, f"{ht} + {tva} != {ttc}")

    d_fac, d_ech = _txt(form.get("date_facture")), _txt(form.get("date_echeance"))
    if d_fac and d_ech:
        try:
            if date.fromisoformat(d_ech) < date.fromisoformat(d_fac):
                err(E_DATE_INCOHERENTE, f"{d_ech} < {d_fac}")
        except ValueError:
            pass

    if frs and ref and _doublon_certain(frs, ref, db_path):
        err(E_DOUBLON_CERTAIN, ref)

    return erreurs


def _doublon_certain(fournisseur: str, ref: str, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM factures WHERE fournisseur_id_opaque=? AND facture_ref=? "
            "AND statut <> ?", (fournisseur, ref, ST_ANNULEE)).fetchone()
        return row is not None
    finally:
        conn.close()


def doublons_probables(fournisseur: str, montant_ttc: Any, date_facture: str,
                       db_path=None) -> list[dict[str, Any]]:
    """Même fournisseur + même montant + date proche (±5 j), référence différente."""
    ttc = _nombre(montant_ttc)
    if ttc is None:
        return []
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM factures WHERE fournisseur_id_opaque=? AND statut <> ?",
            (fournisseur, ST_ANNULEE)).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        if abs((r["montant_ttc"] or 0) - ttc) > 0.01:
            continue
        if date_facture and r["date_facture"]:
            try:
                ecart = abs((date.fromisoformat(date_facture) -
                             date.fromisoformat(r["date_facture"])).days)
                if ecart > 5:
                    continue
            except ValueError:
                pass
        out.append(dict(r))
    return out


def _evenement(conn, opaque: str, type_evt: str, ancien: str | None, nouveau: str | None,
               commentaire: str, acteur: str) -> None:
    conn.execute(
        "INSERT INTO facture_evenements (facture_id_opaque, type_evenement, ancien_statut, "
        "nouveau_statut, commentaire, acteur) VALUES (?,?,?,?,?,?)",
        (opaque, type_evt, ancien, nouveau, commentaire, acteur or "local"))


def creer(form: dict[str, Any], *, acteur: str = "", db_path=None,
          fournisseur_actif: bool | None = None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)

    erreurs = valider(form, db_path, fournisseur_actif)
    if erreurs:
        return {"ok": False, "code": "V_INVALIDE", "message": "Saisie invalide.", "erreurs": erreurs}

    opaque = "FAC-" + uuid.uuid4().hex[:12].upper()
    statut = _txt(form.get("statut")) or ST_A_CONTROLER
    if statut not in STATUTS:
        statut = ST_A_CONTROLER
    frs = _txt(form.get("fournisseur_id_opaque"))
    ref = _txt(form.get("facture_ref"))
    ttc = _nombre(form.get("montant_ttc"))

    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, date_echeance, montant_ht, montant_tva, montant_ttc, devise, statut, "
            "justificatif, source, empreinte, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (opaque, frs, ref, _txt(form.get("date_facture")) or None,
             _txt(form.get("date_echeance")) or None, _nombre(form.get("montant_ht")),
             _nombre(form.get("montant_tva")), ttc, _txt(form.get("devise")) or "EUR", statut,
             _txt(form.get("justificatif")) or None, _txt(form.get("source")) or "SAISIE",
             empreinte(frs, ref, ttc, _txt(form.get("date_facture"))),
             _txt(form.get("commentaire")) or None, acteur or "local"))
        _evenement(conn, opaque, "CREATION", None, statut, _txt(form.get("commentaire")), acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "statut": statut}


def charger(opaque: str, db_path=None) -> dict[str, Any] | None:
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT * FROM factures WHERE facture_id_opaque=?", (opaque,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    f = dict(row)
    f.update(solde(opaque, db_path))
    f["lignes"] = lignes(opaque, db_path)
    f["montant_lignes_ttc"] = round(sum(l["montant_ttc"] for l in f["lignes"]), 2)
    return f


def solde(opaque: str, db_path=None) -> dict[str, Any]:
    """Montant réglé et solde restant — TOUJOURS recalculés, jamais lus d'une colonne stockée."""
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT montant_ttc, statut FROM factures WHERE facture_id_opaque=?",
                         (opaque,)).fetchone()
        if f is None:
            return {"montant_regle": 0.0, "solde_restant": 0.0}
        rows = conn.execute(
            "SELECT r.montant FROM reglement_repartitions r "
            "JOIN reglements_fournisseurs g ON g.reglement_id_opaque = r.reglement_id_opaque "
            "WHERE r.facture_id_opaque=? AND g.statut <> 'ANNULE'", (opaque,)).fetchall()
    finally:
        conn.close()
    regle = round(sum(r["montant"] for r in rows), 2)
    total = f["montant_ttc"] or 0
    return {"montant_regle": regle, "solde_restant": round(total - regle, 2)}


def lister(*, statut: str = "", fournisseur: str = "", echues_seulement: bool = False,
           db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute("SELECT * FROM factures ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    out = []
    aujourdhui = date.today().isoformat()
    for r in rows:
        f = dict(r)
        if statut and f["statut"] != statut:
            continue
        if fournisseur and f["fournisseur_id_opaque"] != fournisseur:
            continue
        f.update(solde(f["facture_id_opaque"], db_path))
        f["echue"] = bool(f["date_echeance"] and f["date_echeance"] < aujourdhui
                          and f["statut"] in STATUTS_OUVERTS and f["solde_restant"] > 0.005)
        if echues_seulement and not f["echue"]:
            continue
        out.append(f)
    return out


def changer_statut(opaque: str, nouveau: str, *, commentaire: str = "", acteur: str = "",
                   db_path=None) -> dict[str, Any]:
    if not _flags_actifs():
        return _refus(E_FLAGS)
    conn = get_db(db_path)
    try:
        row = conn.execute("SELECT statut FROM factures WHERE facture_id_opaque=?",
                           (opaque,)).fetchone()
        if row is None:
            return _refus(E_INTROUVABLE, opaque)
        ancien = row["statut"]
        if nouveau != ancien and nouveau not in TRANSITIONS.get(ancien, set()):
            return _refus(E_STATUT, f"{ancien} -> {nouveau}")
        conn.execute("UPDATE factures SET statut=?, date_modification=?, version=version+1 "
                     "WHERE facture_id_opaque=?", (nouveau, _now(), opaque))
        _evenement(conn, opaque, "VALIDATION" if nouveau == ST_VALIDEE else "MODIFICATION",
                   ancien, nouveau, commentaire, acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "statut": nouveau}


def lier_charge(opaque: str, charge_id: str, *, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Rattache une charge DÉJÀ créée par le parcours Charges. Ne crée jamais la charge elle-même,
    et refuse qu'une charge soit rattachée à deux factures."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    charge_id = _txt(charge_id)
    conn = get_db(db_path)
    try:
        if conn.execute("SELECT 1 FROM factures WHERE facture_id_opaque=?", (opaque,)).fetchone() is None:
            return _refus(E_INTROUVABLE, opaque)
        if conn.execute("SELECT 1 FROM facture_lignes WHERE facture_id_opaque=?",
                        (opaque,)).fetchone():
            return _refus(E_LIGNE_FACTURE_MONO_CHARGE, opaque)
        if conn.execute("SELECT 1 FROM factures WHERE charge_id=? AND facture_id_opaque<>? "
                        "AND statut <> ?", (charge_id, opaque, ST_ANNULEE)).fetchone():
            return _refus(E_CHARGE_DEJA_LIEE, charge_id)
        conn.execute("UPDATE factures SET charge_id=?, date_modification=?, version=version+1 "
                     "WHERE facture_id_opaque=?", (charge_id, _now(), opaque))
        _evenement(conn, opaque, "CHARGE_LIEE", None, None, f"charge {charge_id}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "charge_id": charge_id}


def lignes(opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM facture_lignes WHERE facture_id_opaque=? ORDER BY id",
            (opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def ajouter_ligne(opaque: str, charge_id: str, *, logement_id: str = "",
                  montant_ttc: Any = None, montant_ht: Any = None, montant_tva: Any = None,
                  commentaire: str = "", acteur: str = "", db_path=None) -> dict[str, Any]:
    """Ajoute une ligne de facture rattachant UNE charge (et, si connu, un logement) à une facture
    existante. Permet le cas multi-charges/multi-logements sans toucher au cas mono-charge
    historique (`factures.charge_id`) : une facture utilise l'un OU l'autre mécanisme, jamais les
    deux. Ne crée jamais la charge elle-même (même garde-fou que `lier_charge`)."""
    if not _flags_actifs():
        return _refus(E_FLAGS)
    charge_id = _txt(charge_id)
    if not charge_id:
        return _refus(E_LIGNE_CHARGE_MANQUANTE)
    ttc = _nombre(montant_ttc)
    if ttc is None or ttc == 0:
        return _refus(E_LIGNE_MONTANT_INVALIDE)
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT charge_id FROM factures WHERE facture_id_opaque=?",
                         (opaque,)).fetchone()
        if f is None:
            return _refus(E_INTROUVABLE, opaque)
        if f["charge_id"]:
            return _refus(E_LIGNE_FACTURE_MONO_CHARGE, opaque)
        if conn.execute("SELECT 1 FROM factures WHERE charge_id=? AND statut <> ?",
                        (charge_id, ST_ANNULEE)).fetchone():
            return _refus(E_CHARGE_DEJA_LIEE, charge_id)
        if conn.execute(
                "SELECT 1 FROM facture_lignes fl "
                "JOIN factures fc ON fc.facture_id_opaque = fl.facture_id_opaque "
                "WHERE fl.charge_id=? AND fc.statut <> ?", (charge_id, ST_ANNULEE)).fetchone():
            return _refus(E_CHARGE_DEJA_LIEE, charge_id)
        ligne_id = "FACL-" + uuid.uuid4().hex[:12].upper()
        conn.execute(
            "INSERT INTO facture_lignes (ligne_id_opaque, facture_id_opaque, charge_id, "
            "logement_id, montant_ht, montant_tva, montant_ttc, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ligne_id, opaque, charge_id, _txt(logement_id) or None,
             _nombre(montant_ht), _nombre(montant_tva), ttc,
             _txt(commentaire) or None, acteur or "local"))
        _evenement(conn, opaque, "LIGNE_AJOUTEE", None, None,
                  f"ligne {ligne_id} charge {charge_id}", acteur)
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "facture_id_opaque": opaque, "ligne_id_opaque": ligne_id,
            "charge_id": charge_id}


def historique(opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM facture_evenements WHERE facture_id_opaque=? ORDER BY id DESC",
            (opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def solde_fournisseur(fournisseur_opaque: str, db_path=None) -> dict[str, Any]:
    """Vue de synthèse — recalculée, jamais dénormalisée."""
    factures = lister(fournisseur=fournisseur_opaque, db_path=db_path)
    vivantes = [f for f in factures if f["statut"] != ST_ANNULEE]
    total_facture = round(sum(f["montant_ttc"] or 0 for f in vivantes), 2)
    total_regle = round(sum(f["montant_regle"] for f in vivantes), 2)
    return {
        "total_facture": total_facture,
        "total_regle": total_regle,
        "solde_a_payer": round(total_facture - total_regle, 2),
        "nb_factures": len(vivantes),
        "nb_echues": sum(1 for f in vivantes if f["echue"]),
        "nb_litige": sum(1 for f in vivantes if f["statut"] == ST_LITIGE),
        "factures": factures,
    }

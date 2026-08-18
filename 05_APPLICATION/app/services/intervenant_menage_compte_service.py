"""Compte FIFO d'un intervenant ménage interne — dette / paiement / allocation.

RÈGLE (mission Ménages §16-20/§36, confirmée, pas inventée) : la rémunération normale des ménages
internes est `nb ménages VALIDÉS × tarif standard interne applicable` — jamais heures × taux (cette
ancienne dualité, encore utilisée par le calcul analytique Lot6f/`lib_menage_costs`, n'est PAS
remise dans ce nouveau fonctionnement). Un frais/heures supplémentaire non affectable à un ménage
précis N'EST PAS géré ici : il passe par le module Charges existant (§15/§37).

TARIF
Résolu via `lib_menage_costs.resolve_fixed_internal_cost` (02_TRAVAIL, déjà existant, réutilisé tel
quel — priorité intervenant+logement > logement > intervenant+type > type > global, date-aware) sur
le référentiel `REF_Couts_Menage_Interne` de REF_Setup.xlsm, lu en lecture seule comme le fait déjà
Lot6f. Absent ou ambigu : la dette n'est PAS créée, le ménage reste marqué A_CONTROLER_METIER — le
montant n'est jamais inventé.

FIFO
Réutilise `compte_proprietaire_service.calculer_fifo` (fonction PURE, générique sur les clés
`facture_id_opaque`/`montant_total` pour les créances et `source_type`/`source_ref`/`source_date`/
`montant` pour les sources) : même algorithme que le compte propriétaire (0030), mais les tables et
objets métier restent totalement séparés (§20) — un intervenant ménage n'est pas un propriétaire.
"""
from __future__ import annotations

import hashlib
import os
import sys
import uuid
from datetime import datetime
from typing import Any

import app.config as cfg
from app.db.connection import get_db
from app.readers.excel_reader import read_sheet
from app.services.compte_proprietaire_service import calculer_fifo

_TRAVAIL_DIR = str(cfg.PROJECT_ROOT / "02_TRAVAIL")
if _TRAVAIL_DIR not in sys.path:
    sys.path.insert(0, _TRAVAIL_DIR)

SHEET_TARIF = "REF_Couts_Menage_Interne"

TOLERANCE = 0.005
A_CONTROLER_METIER = "A_CONTROLER_METIER"


def _round(x: float) -> float:
    return round(float(x or 0), 2)


def _maintenant() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── Tarif ────────────────────────────────────────────────────────────────────────────────────────

def _tarif_rows() -> list[dict[str, Any]]:
    if not cfg.REF_SETUP.exists():
        return []
    try:
        return read_sheet(cfg.REF_SETUP, SHEET_TARIF, max_rows=None)
    except Exception:
        return []


def _type_logement(logement_id: str, db_path=None) -> str:
    """Type de logement depuis `ref_logements` (0029). Table absente ou logement inconnu : chaîne
    vide — état légitime, pas une erreur. `resolve_fixed_internal_cost` sait fonctionner sans
    type_logement_id (priorité intervenant+logement / logement / global toujours disponibles)."""
    conn = get_db(db_path)
    try:
        if not conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='ref_logements'"
        ).fetchone():
            return ""
        row = conn.execute("SELECT type_logement_id FROM ref_logements WHERE logement_id = ?",
                           (logement_id,)).fetchone()
        return (row["type_logement_id"] or "") if row else ""
    finally:
        conn.close()


def tarif_menage(intervenant_id: str, logement_id: str, ref_date: str, *,
                 db_path=None) -> dict[str, Any]:
    """Tarif standard interne applicable à UN ménage. Ne devine rien : MISSING/AMBIGUOUS remontés
    tels quels, jamais transformés en 0 ou en valeur par défaut."""
    from lib_menage_costs import resolve_fixed_internal_cost

    type_logement_id = _type_logement(logement_id, db_path=db_path)
    resolution = resolve_fixed_internal_cost(
        _tarif_rows(), intervenant_id=intervenant_id, logement_id=logement_id,
        type_logement_id=type_logement_id, ref_date=ref_date, nb_menages=1)
    return {"statut": resolution.status, "montant": resolution.total, "tarif_ref_id": resolution.ref_id,
            "message": resolution.message}


# ── Dettes ───────────────────────────────────────────────────────────────────────────────────────

def generer_dettes(intervenant_id: str, *, db_path=None) -> dict[str, Any]:
    """Crée une dette pour chaque ménage INTERNE VALIDÉ de l'intervenant qui n'en a pas encore une.

    Idempotent : `menage_id_opaque` est UNIQUE sur `intervenant_menage_dettes` — rejouer sur les
    mêmes ménages ne crée jamais de doublon.
    """
    conn = get_db(db_path)
    try:
        menages = conn.execute(
            "SELECT menage_id_opaque, date_realisation, date_prevue, logement_id "
            "FROM menages WHERE fournisseur_id_opaque = ? AND type_menage = 'INTERNE' "
            "AND statut = 'VALIDE'", (intervenant_id,)).fetchall()
        deja = {r[0] for r in conn.execute(
            "SELECT menage_id_opaque FROM intervenant_menage_dettes").fetchall()}
    finally:
        conn.close()

    crees, a_controler = [], []
    for m in menages:
        if m["menage_id_opaque"] in deja:
            continue
        ref_date = m["date_realisation"] or m["date_prevue"]
        tarif = tarif_menage(intervenant_id, m["logement_id"], ref_date, db_path=db_path)
        if tarif["statut"] != "OK":
            a_controler.append({"menage_id_opaque": m["menage_id_opaque"],
                                "code": A_CONTROLER_METIER, "message": tarif["message"]})
            continue
        dette_id = "DIM-" + uuid.uuid4().hex[:12].upper()
        conn = get_db(db_path)
        try:
            conn.execute(
                "INSERT INTO intervenant_menage_dettes (dette_id_opaque, intervenant_id, "
                "menage_id_opaque, date_validation, tarif_unitaire, tarif_ref_id, montant) "
                "VALUES (?,?,?,?,?,?,?)",
                (dette_id, intervenant_id, m["menage_id_opaque"], ref_date, tarif["montant"],
                 tarif["tarif_ref_id"], tarif["montant"]))
            conn.commit()
        finally:
            conn.close()
        crees.append({"dette_id_opaque": dette_id, "menage_id_opaque": m["menage_id_opaque"],
                      "montant": tarif["montant"]})

    return {"nb_creees": len(crees), "dettes": crees, "a_controler": a_controler}


def dettes(intervenant_id: str, *, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT dette_id_opaque, menage_id_opaque, date_validation, tarif_unitaire, montant, "
            "statut FROM intervenant_menage_dettes WHERE intervenant_id = ? AND statut = 'ACTIVE' "
            "ORDER BY date_validation, dette_id_opaque", (intervenant_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Paiements ────────────────────────────────────────────────────────────────────────────────────

def enregistrer_paiement(intervenant_id: str, montant: float, date_paiement: str, *,
                         moyen: str = "", mouvement_banque_ref: str = "", commentaire: str = "",
                         acteur: str = "", db_path=None) -> dict[str, Any]:
    montant = _round(montant)
    if montant <= 0:
        return {"ok": False, "code": "MONTANT_INVALIDE"}
    paiement_id = "PIM-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO intervenant_menage_paiements (paiement_id_opaque, intervenant_id, "
            "date_paiement, montant, moyen, mouvement_banque_ref, commentaire, acteur) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (paiement_id, intervenant_id, date_paiement, montant, moyen or None,
             mouvement_banque_ref or None, commentaire or None, acteur or None))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "paiement_id_opaque": paiement_id}


def _paiements(conn, intervenant_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT paiement_id_opaque, date_paiement, montant FROM intervenant_menage_paiements "
        "WHERE intervenant_id = ? AND statut = 'VALIDE' ORDER BY date_paiement, paiement_id_opaque",
        (intervenant_id,)).fetchall()
    return [dict(r) for r in rows]


def _empreinte(dettes_l: list[dict], paiements_l: list[dict]) -> str:
    h = hashlib.sha256()
    for d in dettes_l:
        h.update(f"D|{d['dette_id_opaque']}|{d['montant']:.2f}|{d['date_validation']}\n".encode())
    for p in paiements_l:
        h.update(f"P|{p['paiement_id_opaque']}|{p['montant']:.2f}|{p['date_paiement']}\n".encode())
    return h.hexdigest()


def recalculer(intervenant_id: str, *, declencheur: str = "MANUEL",
              db_path=None) -> dict[str, Any]:
    """Recalcule intégralement les allocations FIFO de l'intervenant. Idempotent — mêmes entrées,
    mêmes allocations (garanti par `calculer_fifo`, testé côté compte propriétaire)."""
    conn = get_db(db_path)
    try:
        dettes_l = [{"facture_id_opaque": d["dette_id_opaque"], "montant_total": d["montant"]}
                   for d in dettes(intervenant_id, db_path=db_path)]
        paiements_l = _paiements(conn, intervenant_id)
        sources = [{"source_type": "PAIEMENT", "source_ref": p["paiement_id_opaque"],
                   "source_date": p["date_paiement"], "montant": p["montant"]}
                  for p in paiements_l]
        allocations = calculer_fifo(dettes_l, sources)

        recalcul_id = "RCM-" + uuid.uuid4().hex[:12].upper()
        empreinte = _empreinte(
            [{"dette_id_opaque": d["facture_id_opaque"], "montant": d["montant_total"],
              "date_validation": ""} for d in dettes_l], paiements_l)

        total_dettes = _round(sum(d["montant_total"] for d in dettes_l))
        total_alloue = _round(sum(a["montant_alloue"] for a in allocations))

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "DELETE FROM intervenant_menage_allocations WHERE intervenant_id = ?",
                (intervenant_id,))
            for a in allocations:
                conn.execute(
                    "INSERT INTO intervenant_menage_allocations (allocation_id_opaque, "
                    "intervenant_id, recalcul_id, paiement_id_opaque, dette_id_opaque, "
                    "montant_alloue, rang_fifo) VALUES (?,?,?,?,?,?,?)",
                    ("AIM-" + uuid.uuid4().hex[:12].upper(), intervenant_id, recalcul_id,
                     a["source_ref"], a["facture_id_opaque"], a["montant_alloue"], a["rang_fifo"]))
            conn.execute(
                "INSERT INTO intervenant_menage_recalculs (recalcul_id, intervenant_id, "
                "horodatage, empreinte_entrees, nb_dettes, nb_paiements, nb_allocations, "
                "montant_alloue, dette_restante, declencheur) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (recalcul_id, intervenant_id, _maintenant(), empreinte, len(dettes_l),
                 len(paiements_l), len(allocations), total_alloue,
                 _round(total_dettes - total_alloue), declencheur))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()

    return {"ok": True, "recalcul_id": recalcul_id, "nb_dettes": len(dettes_l),
            "nb_paiements": len(paiements_l), "nb_allocations": len(allocations),
            "montant_alloue": total_alloue, "dette_restante": _round(total_dettes - total_alloue)}


def position(intervenant_id: str, *, db_path=None) -> dict[str, Any]:
    """Position complète, comme `compte_proprietaire_service.position` : recalcule d'abord (les
    allocations sont dérivées, jamais fiables sans recalcul), puis lit le résultat."""
    recalculer(intervenant_id, declencheur="AUTO", db_path=db_path)

    conn = get_db(db_path)
    try:
        dettes_l = dettes(intervenant_id, db_path=db_path)
        rows = conn.execute(
            "SELECT paiement_id_opaque, dette_id_opaque, montant_alloue, rang_fifo "
            "FROM intervenant_menage_allocations WHERE intervenant_id = ? ORDER BY rang_fifo",
            (intervenant_id,)).fetchall()
    finally:
        conn.close()

    allocations = [dict(r) for r in rows]
    par_dette: dict[str, float] = {}
    for a in allocations:
        par_dette[a["dette_id_opaque"]] = _round(
            par_dette.get(a["dette_id_opaque"], 0) + a["montant_alloue"])

    lignes = []
    for d in dettes_l:
        regle = par_dette.get(d["dette_id_opaque"], 0.0)
        lignes.append({**d, "regle": regle, "solde": _round(d["montant"] - regle)})

    dette_totale = _round(sum(l["montant"] for l in lignes))
    dette_restante = _round(sum(l["solde"] for l in lignes))
    return {"intervenant_id": intervenant_id, "dettes": lignes, "allocations": allocations,
            "dette_totale": dette_totale, "dette_restante": dette_restante}

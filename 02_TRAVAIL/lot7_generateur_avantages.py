"""Lot7 — Générateur réel des avantages associés (Option A : Python/openpyxl déterministe).

MOTEUR RÉEL de Lot7. Le classeur MASTER_FACT_MAN_IK_Avantages.xlsx ne contient AUCUN Power
Query vivant (pas de connections.xml / DataMashup / queryTables) : l'onglet POWER_QUERY_CODE est
DOCUMENTAIRE. C'est ce module Python qui produit réellement MASTER_CALC_AVANTAGES à partir des
sources durables. Excel = support de restitution ; Python = moteur.

Sources durables lues (jamais écrites ici) :
  - SAISIE_Charges_Flux.xlsx (onglet SAISIE) : avantage issu d'une charge, porté par la colonne
    `avantage_associe_id` (bénéficiaire), distinct du moyen de paiement `mode_paiement_id`.
  - SOURCE_SAISIE (onglet du classeur Lot7) : saisie STRICTEMENT RÉSIDUELLE — avantages autonomes
    sans charge d'origine (virements TYPE_FLUX_001, IK TYPE_FLUX_015, remboursements TYPE_FLUX_005).

Priorité déterministe d'attribution de l'avantage d'une charge (une charge = une seule voie) :
  1. `avantage_associe_id` renseigné → totalité du montant attribuée à cet associé, quel que soit
     le moyen de paiement (PAY_001 banque pro inclus). Aucune autre règle avantage n'est appliquée.
  2. sinon → règle historique TYPE_FLUX_002 (dépense perso) par `associe_id` de paiement.
  3. jamais les deux voies pour une même `charge_id`.
PAY_003 / PAY_004 sans `avantage_associe_id` ne créent JAMAIS d'avantage automatique.
Une même `charge_id` n'est jamais comptée deux fois : la génération est idempotente.

Écriture : uniquement sur COPIE contrôlée (paramètre `sortie_path`). Le fichier métier réel n'est
jamais modifié par ce module. Aucun flag d'écriture réelle n'est touché.
"""
from __future__ import annotations

import os
import sys
from typing import Any, Iterable

import openpyxl

# lib_avantages est dans le même dossier 02_TRAVAIL.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib_avantages as av  # noqa: E402

SAISIE_SHEET = "SAISIE"
SOURCE_SAISIE_SHEET = "SOURCE_SAISIE"
MASTER_CALC_SHEET = "MASTER_CALC_AVANTAGES"

# Types de flux résiduels (SOURCE_SAISIE Lot7), hors charges.
TF_VIREMENT = "TYPE_FLUX_001"
TF_IK = "TYPE_FLUX_015"
TF_REMBOURSEMENT = "TYPE_FLUX_005"
# Charge société payée perso/liquide (déduite des nets, jamais un avantage brut).
TF_CHARGE_SOCIETE = frozenset({"TYPE_FLUX_004", "TYPE_FLUX_008"})

REMB_ASSOC_VERS_SOC = "ASSOCIE_VERS_SOCIETE"
REMB_SOC_VERS_ASSOC = "SOCIETE_VERS_ASSOCIE"

# En-tête canonique MASTER_CALC_AVANTAGES (15 colonnes) — identique à lot7_ik_avantages.MC_HDR.
MC_HEADERS: list[str] = [
    "pk_id", "mois", "associe_id",
    "avantage_brut_virements", "avantage_brut_ik",
    "avantage_brut_depenses_perso", "avantage_brut_montant_recupere_hh",
    "avantages_bruts_total", "charges_payees_pour_societe",
    "remboursements_associe_vers_societe", "remboursements_societe_vers_associe",
    "avantages_nets", "detail_sources", "statut_controle", "code_anomalie",
]


# ── Utilitaires ──────────────────────────────────────────────────────────────

def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _montant(v: Any) -> float:
    try:
        return round(float(v or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _mois_de(row: dict[str, Any]) -> str:
    """mois = YYYY-MM. Dérivé de date_charge (déterministe, sans dépendre du cache Excel).

    Repli sur la colonne `mois` si date_charge absente.
    """
    dc = _txt(row.get("date_charge"))
    if len(dc) >= 7 and dc[4] == "-":
        return dc[:7]
    return _txt(row.get("mois"))[:7]


# ── Lecture des sources durables (lecture seule) ─────────────────────────────

def charger_charges_saisie(saisie_charges_path: str) -> list[dict[str, Any]]:
    """Lit SAISIE_Charges_Flux.xlsx (onglet SAISIE). Retourne des charges normalisées.

    Chaque charge : charge_id, mois, montant, type_flux_id, associe_id, avantage_associe_id,
    mode_paiement_id. La source n'est jamais modifiée.
    """
    wb = openpyxl.load_workbook(saisie_charges_path, read_only=True, data_only=True)
    try:
        ws = wb[SAISIE_SHEET]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return []
    headers = [_txt(h) for h in rows[0]]

    def col(name: str) -> int | None:
        return headers.index(name) if name in headers else None

    idx = {k: col(k) for k in (
        "charge_id", "date_charge", "mois", "montant", "type_flux_id",
        "associe_id", "avantage_associe_id", "mode_paiement_id",
    )}
    charges: list[dict[str, Any]] = []
    for r in rows[1:]:
        if not any(c is not None for c in r):
            continue
        get = lambda k: r[idx[k]] if idx[k] is not None else None
        cid = _txt(get("charge_id"))
        if not cid:
            continue
        row = {
            "charge_id": cid,
            "date_charge": get("date_charge"),
            "mois": get("mois"),
            "montant": _montant(get("montant")),
            "type_flux_id": _txt(get("type_flux_id")),
            "associe_id": _txt(get("associe_id")),
            "avantage_associe_id": _txt(get("avantage_associe_id")),
            "mode_paiement_id": _txt(get("mode_paiement_id")),
        }
        row["mois"] = _mois_de(row)
        charges.append(row)
    return charges


def charger_source_saisie_residuelle(lot7_path: str) -> list[dict[str, Any]]:
    """Lit l'onglet SOURCE_SAISIE (résiduel) du classeur Lot7. Retourne les lignes de saisie.

    Ignore la ligne d'instruction (ligne 2 sans mois valide au format YYYY-MM).
    """
    wb = openpyxl.load_workbook(lot7_path, read_only=True, data_only=True)
    try:
        if SOURCE_SAISIE_SHEET not in wb.sheetnames:
            return []
        ws = wb[SOURCE_SAISIE_SHEET]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    if not rows:
        return []
    headers = [_txt(h) for h in rows[0]]
    out: list[dict[str, Any]] = []
    for r in rows[1:]:
        if not any(c is not None for c in r):
            continue
        d = {headers[i]: r[i] for i in range(min(len(headers), len(r)))}
        mois = _txt(d.get("mois"))[:7]
        # Ligne d'instruction / non-donnée : mois non conforme YYYY-MM.
        if len(mois) != 7 or mois[4] != "-":
            continue
        out.append({
            "mois": mois,
            "associe_id": _txt(d.get("associe_id")),
            "type_flux_id": _txt(d.get("type_flux_id")),
            "type_remboursement": _txt(d.get("type_remboursement")),
            "montant": _montant(d.get("montant")),
            "lien_origine": _txt(d.get("lien_origine")),
        })
    return out


# ── Agrégation déterministe ──────────────────────────────────────────────────

def agreger(
    charges: Iterable[dict[str, Any]],
    residuels: Iterable[dict[str, Any]] | None = None,
    charge_ids_lot3: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Agrège les avantages par (associe_id, mois) et construit les lignes MASTER_CALC_AVANTAGES.

    Renvoie {"rows": [...15 colonnes...], "anomalies": [...], "doublons_charge_id": int}.
    - avantage_brut_depenses_perso : avantage issu des charges (lib_avantages : flag prioritaire,
      sinon TYPE_FLUX_002, dédup par charge_id).
    - virements / ik / remboursements : issus de SOURCE_SAISIE résiduelle.
    - charges_payees_pour_societe : charges TYPE_FLUX_004/008 SANS avantage_associe_id (une voie).
    - anti double comptage : une ligne résiduelle dont lien_origine = charge_id déjà en Lot3 est
      REJETÉE (anomalie SAISIE_LOT7_SOURCE_DEJA_EXISTANTE).
    """
    charges = list(charges)
    residuels = list(residuels or [])
    lot3 = {_txt(c) for c in (charge_ids_lot3 or []) if _txt(c)}
    anomalies: list[dict[str, Any]] = []

    # 1) Avantage issu des charges (voie unique par charge_id via lib_avantages).
    agg_charges = av.aggregate_avantages(charges)  # {(associe,mois): {avantage_brut, nb_charges,...}}
    doublons = _compter_doublons_charge_id(charges, anomalies)

    # 2) Charges payées pour la société (TF004/008, hors charges à flag avantage) — déduction.
    ch_payees: dict[tuple[str, str], float] = {}
    for c in charges:
        if _txt(c.get("avantage_associe_id")):
            continue  # charge à flag = avantage (voie 1), jamais aussi charge_payee.
        if _txt(c.get("type_flux_id")) in TF_CHARGE_SOCIETE:
            key = (_txt(c.get("associe_id")), _mois_de(c))
            if key[0]:
                ch_payees[key] = round(ch_payees.get(key, 0.0) + _montant(c.get("montant")), 2)

    # 3) SOURCE_SAISIE résiduelle (virements / IK / remboursements), avec anti-double-comptage.
    virements: dict[tuple[str, str], float] = {}
    ik: dict[tuple[str, str], float] = {}
    remb_avs: dict[tuple[str, str], float] = {}
    remb_sva: dict[tuple[str, str], float] = {}
    for s in residuels:
        origine = _txt(s.get("lien_origine"))
        if origine and origine in lot3:
            anomalies.append({
                "code": "SAISIE_LOT7_SOURCE_DEJA_EXISTANTE",
                "niveau": "BLOQUANT",
                "detail": f"SOURCE_SAISIE lien_origine={origine} déjà présent en Lot3 (charge)",
            })
            continue  # rejet : jamais deux voies pour la même charge.
        key = (_txt(s.get("associe_id")), _txt(s.get("mois")))
        if not key[0]:
            continue
        tf = _txt(s.get("type_flux_id"))
        m = _montant(s.get("montant"))
        if tf == TF_VIREMENT:
            virements[key] = round(virements.get(key, 0.0) + m, 2)
        elif tf == TF_IK:
            ik[key] = round(ik.get(key, 0.0) + m, 2)
        elif tf == TF_REMBOURSEMENT:
            if _txt(s.get("type_remboursement")) == REMB_ASSOC_VERS_SOC:
                remb_avs[key] = round(remb_avs.get(key, 0.0) + m, 2)
            elif _txt(s.get("type_remboursement")) == REMB_SOC_VERS_ASSOC:
                remb_sva[key] = round(remb_sva.get(key, 0.0) + m, 2)

    # 4) Union des clés (associe, mois) de toutes les sources.
    cles: set[tuple[str, str]] = set(agg_charges) | set(ch_payees) | set(virements) \
        | set(ik) | set(remb_avs) | set(remb_sva)

    rows: list[list[Any]] = []
    for (associe, mois) in sorted(cles):
        v = round(virements.get((associe, mois), 0.0), 2)
        k = round(ik.get((associe, mois), 0.0), 2)
        dp = round(agg_charges.get((associe, mois), {}).get("avantage_brut", 0.0), 2)
        hh = 0.0  # Lot4 (montant récupéré HH) — déféré, non traité dans cette mission.
        bruts = round(v + k + dp + hh, 2)
        ch = round(ch_payees.get((associe, mois), 0.0), 2)
        ravs = round(remb_avs.get((associe, mois), 0.0), 2)
        rsva = round(remb_sva.get((associe, mois), 0.0), 2)
        nets = round(bruts - ch - ravs + rsva, 2)
        # detail_sources : MONTANTS agrégés uniquement (jamais une liste d'identifiants concaténée).
        detail = f"CHARGES={dp} | SOURCE_SAISIE={round(v + k, 2)} | REMB={round(rsva - ravs, 2)}"
        statut = "A_CONTROLER" if nets < 0 else "VALIDE"
        code = "AVANTAGE_NET_NEGATIF" if nets < 0 else ""
        rows.append([
            f"{associe}-{mois}", mois, associe,
            v, k, dp, hh, bruts, ch, ravs, rsva, nets, detail, statut, code,
        ])

    return {"rows": rows, "anomalies": anomalies, "doublons_charge_id": doublons}


def _compter_doublons_charge_id(
    charges: list[dict[str, Any]], anomalies: list[dict[str, Any]]
) -> int:
    """Compte les charge_id en double (déjà dédupliqués par lib_avantages) et trace une anomalie.

    Déduplication déterministe : première occurrence gardée. Anomalie non bloquante mais tracée.
    """
    vus: set[str] = set()
    doublons = 0
    for c in charges:
        cid = _txt(c.get("charge_id"))
        if not cid:
            continue
        if cid in vus:
            doublons += 1
            anomalies.append({
                "code": "CHARGE_ID_DOUBLON",
                "niveau": "A_CONTROLER",
                "detail": f"charge_id={cid} présent en double — dédupliqué (première occurrence gardée)",
            })
        else:
            vus.add(cid)
    return doublons


# ── Écriture idempotente sur COPIE ───────────────────────────────────────────

def ecrire_master_calc(workbook_path: str, rows: list[list[Any]]) -> None:
    """Écrit (idempotent) MASTER_CALC_AVANTAGES dans le classeur cible (une COPIE).

    L'onglet est recréé avec l'en-tête canonique puis les lignes ; une seconde exécution
    produit exactement le même contenu (jamais d'accumulation). Le fichier réel n'est écrit
    que si l'appelant fournit son chemin — ce module ne cible jamais le réel par défaut.
    """
    wb = openpyxl.load_workbook(workbook_path)
    try:
        # Préserve la position d'origine de l'onglet (format non cassé).
        idx = wb.sheetnames.index(MASTER_CALC_SHEET) if MASTER_CALC_SHEET in wb.sheetnames else None
        if idx is not None:
            del wb[MASTER_CALC_SHEET]
            ws = wb.create_sheet(MASTER_CALC_SHEET, idx)
        else:
            ws = wb.create_sheet(MASTER_CALC_SHEET)
        ws.append(MC_HEADERS)
        for row in rows:
            ws.append(row)
        wb.save(workbook_path)
    finally:
        wb.close()


def generer(
    saisie_charges_path: str,
    lot7_path: str,
    sortie_path: str,
) -> dict[str, Any]:
    """Orchestrateur : lit les sources durables, agrège, écrit MASTER_CALC_AVANTAGES sur COPIE.

    `sortie_path` DOIT être une copie contrôlée (jamais le classeur métier réel). Retourne un
    résumé { nb_lignes, anomalies, doublons_charge_id, cible }.
    """
    charges = charger_charges_saisie(saisie_charges_path)
    residuels = charger_source_saisie_residuelle(lot7_path)
    charge_ids = [c["charge_id"] for c in charges]
    res = agreger(charges, residuels, charge_ids_lot3=charge_ids)
    ecrire_master_calc(sortie_path, res["rows"])
    return {
        "nb_lignes": len(res["rows"]),
        "anomalies": res["anomalies"],
        "doublons_charge_id": res["doublons_charge_id"],
        "cible": sortie_path,
    }

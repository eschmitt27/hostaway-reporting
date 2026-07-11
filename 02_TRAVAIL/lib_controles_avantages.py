"""Lot11 — contrôles du suivi des avantages associés (Lot7B). Fonctions PURES, aucune écriture.

Toutes les fonctions prennent des structures déjà chargées (listes de dicts) et renvoient une liste
d'anomalies `{code, niveau, detail}`. Elles ne lisent ni n'écrivent aucun fichier : le contrôle ne
modifie JAMAIS les sources. Le suivi associé est HR strict — ces contrôles vérifient précisément qu'il
n'impacte ni le résultat ni le propriétaire, et qu'aucune charge Lot3 n'est doublée en Lot7.
"""
from __future__ import annotations

import os
import sys
from typing import Any, Iterable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib_avantages as av  # noqa: E402

TF_IK = "TYPE_FLUX_015"
TF_CHARGE_SOCIETE = frozenset({"TYPE_FLUX_004", "TYPE_FLUX_008"})
COLONNES_INTERDITES = frozenset({
    "proprietaire_id", "net_proprietaire", "resultat_reel", "resultat_comptable",
    "revenu_net_exploitation_proprietaire", "payout", "prefacture",
})


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _mois(row: dict[str, Any]) -> str:
    dc = _txt(row.get("date_charge"))
    if len(dc) >= 7 and dc[4] == "-":
        return dc[:7]
    return _txt(row.get("mois"))[:7]


def _ano(code: str, niveau: str, detail: str) -> dict[str, Any]:
    return {"code": code, "niveau": niveau, "detail": detail}


# ── Contrôles unitaires ──────────────────────────────────────────────────────

def ctrl_avantage_absent_du_suivi(
    charges: Iterable[dict[str, Any]], calc: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Chaque charge avec avantage_associe_id doit apparaître pour son bénéficiaire/mois dans le suivi."""
    cles = {(_txt(r.get("associe_id")), _txt(r.get("mois"))) for r in calc}
    out: list[dict[str, Any]] = []
    for c in charges:
        if not _txt(c.get("avantage_associe_id")):
            continue
        benef = av.beneficiaire(c)
        key = (_txt(benef), _mois(c))
        if key not in cles:
            out.append(_ano("AVANTAGE_ABSENT_DU_SUIVI", "A_CONTROLER",
                            f"charge_id={_txt(c.get('charge_id'))} avantage_associe_id={benef} "
                            f"absent de MASTER_CALC_AVANTAGES pour {key[1]}"))
    return out


def ctrl_charge_id_double(charges: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Un même charge_id présent plus d'une fois → risque de double avantage (dédup déterministe)."""
    vus: set[str] = set()
    out: list[dict[str, Any]] = []
    for c in charges:
        cid = _txt(c.get("charge_id"))
        if not cid:
            continue
        if cid in vus:
            out.append(_ano("CHARGE_ID_AVANTAGE_DOUBLE", "A_CONTROLER",
                            f"charge_id={cid} en double (doit être compté une seule fois)"))
        else:
            vus.add(cid)
    return out


def ctrl_source_saisie_lien_deja_lot3(
    residuels: Iterable[dict[str, Any]], charge_ids_lot3: Iterable[str]
) -> list[dict[str, Any]]:
    """SOURCE_SAISIE (résiduelle) ne doit jamais référencer une charge déjà présente dans Lot3."""
    lot3 = {_txt(c) for c in charge_ids_lot3 if _txt(c)}
    out: list[dict[str, Any]] = []
    for s in residuels:
        origine = _txt(s.get("lien_origine"))
        if origine and origine in lot3:
            out.append(_ano("SOURCE_SAISIE_LIEN_DEJA_LOT3", "BLOQUANT",
                            f"SOURCE_SAISIE lien_origine={origine} déjà présent dans SAISIE_Charges_Flux"))
    return out


def ctrl_code_impact_hr(calc: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Toute ligne de suivi associé doit être HR (hors résultat réel ET comptable)."""
    out: list[dict[str, Any]] = []
    for r in calc:
        ci = _txt(r.get("code_impact"))
        if ci and ci != "HR":
            out.append(_ano("AVANTAGE_CODE_IMPACT_NON_HR", "BLOQUANT",
                            f"{_txt(r.get('associe_id'))}/{_txt(r.get('mois'))} code_impact={ci} (attendu HR)"))
    return out


def ctrl_pas_impact_proprietaire(entetes: Iterable[str]) -> list[dict[str, Any]]:
    """Aucune colonne d'impact propriétaire / résultat / préfacture dans la sortie suivi (structurel)."""
    presentes = COLONNES_INTERDITES & {_txt(h) for h in entetes}
    if presentes:
        return [_ano("AVANTAGE_IMPACT_PROPRIETAIRE_INTERDIT", "BLOQUANT",
                     f"colonnes interdites dans le suivi associé : {sorted(presentes)}")]
    return []


def ctrl_cle_suivi_presente(calc: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """mois et associe_id obligatoires sur chaque ligne de suivi."""
    out: list[dict[str, Any]] = []
    for r in calc:
        if not _txt(r.get("associe_id")) or not _txt(r.get("mois")):
            out.append(_ano("SUIVI_CLE_MANQUANTE", "BLOQUANT",
                            f"ligne suivi sans associe_id ou mois : {dict(r)}"))
    return out


def ctrl_ik_hors_charges_lot3(charges: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """L'IK (TYPE_FLUX_015) doit rester en Lot7, jamais dans SAISIE_Charges_Flux (Lot3)."""
    out: list[dict[str, Any]] = []
    for c in charges:
        if _txt(c.get("type_flux_id")) == TF_IK:
            out.append(_ano("IK_DANS_CHARGES_LOT3", "BLOQUANT",
                            f"charge_id={_txt(c.get('charge_id'))} IK (TYPE_FLUX_015) présente dans "
                            f"SAISIE_Charges_Flux — doit rester Lot7"))
    return out


def ctrl_charges_payees_reprises(
    charges: Iterable[dict[str, Any]], calc: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Toute charge TF004/008 (sans flag) doit être reprise en charges_payees_pour_societe du suivi."""
    index = {(_txt(r.get("associe_id")), _txt(r.get("mois"))): r for r in calc}
    out: list[dict[str, Any]] = []
    for c in charges:
        if _txt(c.get("avantage_associe_id")):
            continue
        if _txt(c.get("type_flux_id")) not in TF_CHARGE_SOCIETE:
            continue
        key = (_txt(c.get("associe_id")), _mois(c))
        r = index.get(key)
        if r is None or float(r.get("charges_payees_pour_societe") or 0.0) <= 0:
            out.append(_ano("CHARGE_PAYEE_NON_REPRISE", "A_CONTROLER",
                            f"charge_id={_txt(c.get('charge_id'))} ({key}) non reprise en "
                            f"charges_payees_pour_societe"))
    return out


def ctrl_avantage_net_coherent(calc: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """avantages_nets = avantages_bruts_total − charges_payees − remb_AVS + remb_SVA (à 0,01 près)."""
    out: list[dict[str, Any]] = []
    for r in calc:
        bruts = float(r.get("avantages_bruts_total") or 0.0)
        ch = float(r.get("charges_payees_pour_societe") or 0.0)
        ravs = float(r.get("remboursements_associe_vers_societe") or 0.0)
        rsva = float(r.get("remboursements_societe_vers_associe") or 0.0)
        attendu = round(bruts - ch - ravs + rsva, 2)
        reel = round(float(r.get("avantages_nets") or 0.0), 2)
        if abs(attendu - reel) > 0.001:
            out.append(_ano("AVANTAGE_NET_INCOHERENT", "BLOQUANT",
                            f"{_txt(r.get('associe_id'))}/{_txt(r.get('mois'))} "
                            f"net={reel} attendu={attendu}"))
    return out


# ── Agrégateur ───────────────────────────────────────────────────────────────

def controler_suivi_associes(
    charges: list[dict[str, Any]],
    residuels: list[dict[str, Any]],
    calc: list[dict[str, Any]],
    entetes: list[str],
    charge_ids_lot3: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Exécute tous les contrôles du suivi associé. Retourne la liste consolidée d'anomalies."""
    lot3 = charge_ids_lot3 if charge_ids_lot3 is not None else [c.get("charge_id") for c in charges]
    anomalies: list[dict[str, Any]] = []
    anomalies += ctrl_avantage_absent_du_suivi(charges, calc)
    anomalies += ctrl_charge_id_double(charges)
    anomalies += ctrl_source_saisie_lien_deja_lot3(residuels, lot3)
    anomalies += ctrl_code_impact_hr(calc)
    anomalies += ctrl_pas_impact_proprietaire(entetes)
    anomalies += ctrl_cle_suivi_presente(calc)
    anomalies += ctrl_ik_hors_charges_lot3(charges)
    anomalies += ctrl_charges_payees_reprises(charges, calc)
    anomalies += ctrl_avantage_net_coherent(calc)
    return anomalies

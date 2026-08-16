"""Lecteur read-only propriétaires & règlements — Lot APP-3c.

Sources (lecture seule) :
- Référentiel propriétaires : table SQLite `ref_proprietaires` (migration 0029).
  L'onglet REF_Proprietaires du classeur n'est plus lu — le référentiel est en base.
- MASTER_CALC_NetProprietaire.xlsx onglet REGLEMENT — relevé par logement×mois.
- MASTER_CALC_NetProprietaire.xlsx onglet VUE_MOIS — agrégation par propriétaire×mois.
- MASTER_FACT_Proprietaires.xlsx onglets FACT_FACTURE_ENTETE + FACT_FACTURE_LIGNES — préfacture 12 lignes.

Règles :
- revenu_net_exploitation jamais recalculé (D033).
- Bloc EXPLOITATION et bloc REGLEMENT non mélangés (EP1-EP7).
- AirCover affiché comme info séparée uniquement (ligne ACOMPTE_AIRBNB).
- Aucune écriture.
- Les lectures de MASTER_* restent des fichiers : elles relèvent de la migration du moteur, pas de
  celle du référentiel. La frontière est volontairement visible dans ce module.
"""
from typing import Any
from app.config import MASTER_NET_PROPRIETAIRE, MASTER_FACT_PROPRIETAIRES
from app.readers.excel_reader import read_sheet
from app.services import referentiel_service as referentiel

SHEET_REGLEMENT = "REGLEMENT"
SHEET_VUE_MOIS = "VUE_MOIS"
SHEET_ENTETE = "FACT_FACTURE_ENTETE"
SHEET_LIGNES = "FACT_FACTURE_LIGNES"

SOURCE_REF = "REF_Setup.xlsm"
SOURCE_CALC = "MASTER_CALC_NetProprietaire.xlsx"
SOURCE_FACT = "MASTER_FACT_Proprietaires.xlsx"


def ref_available() -> bool:
    """Le référentiel propriétaires est-il exploitable ? (importé en base, pas « fichier présent »)"""
    return referentiel.disponible()


def calc_available() -> bool:
    return MASTER_NET_PROPRIETAIRE.exists()


def fact_available() -> bool:
    return MASTER_FACT_PROPRIETAIRES.exists()


def _is_real_row(row: dict[str, Any], key: str) -> bool:
    v = row.get(key)
    if v is None:
        return False
    s = str(v).strip()
    return bool(s) and not s.startswith("[")


def read_proprietaires() -> list[dict[str, Any]]:
    """Liste des propriétaires depuis le référentiel SQLite.

    Le filtre `_is_real_row` est conservé : le classeur contenait des lignes de gabarit dont
    l'identifiant commence par « [ ». L'import les recopie fidèlement, donc le filtre reste utile.
    """
    if not referentiel.disponible():
        return []
    return [r for r in referentiel.proprietaires() if _is_real_row(r, "proprietaire_id")]


def find_proprietaire(prop_id: str) -> dict[str, Any] | None:
    pid = str(prop_id).strip()
    if not pid or not referentiel.disponible():
        return None
    row = referentiel.proprietaire(pid)
    return row if row is not None and _is_real_row(row, "proprietaire_id") else None


def read_reglement() -> list[dict[str, Any]]:
    """Toutes les lignes REGLEMENT (propriétaire×logement×mois)."""
    rows = read_sheet(MASTER_NET_PROPRIETAIRE, SHEET_REGLEMENT, max_rows=None)
    return [r for r in rows if _is_real_row(r, "proprietaire_id")]


def read_vue_mois() -> list[dict[str, Any]]:
    """Agrégation par propriétaire×mois depuis VUE_MOIS."""
    rows = read_sheet(MASTER_NET_PROPRIETAIRE, SHEET_VUE_MOIS, max_rows=None)
    return [r for r in rows if _is_real_row(r, "proprietaire_id")]


def read_reglement_prop(prop_id: str) -> list[dict[str, Any]]:
    """Lignes REGLEMENT pour un propriétaire donné, toutes années."""
    pid = str(prop_id).strip()
    return [r for r in read_reglement()
            if str(r.get("proprietaire_id") or "").strip() == pid]


def read_reglement_prop_mois(prop_id: str, mois: str) -> list[dict[str, Any]]:
    """Lignes REGLEMENT pour un propriétaire et un mois (une par logement)."""
    pid = str(prop_id).strip()
    m = str(mois).strip()
    return [r for r in read_reglement()
            if str(r.get("proprietaire_id") or "").strip() == pid
            and str(r.get("mois") or "").strip() == m]


def read_vue_mois_prop_mois(prop_id: str, mois: str) -> dict[str, Any] | None:
    """Agrégation VUE_MOIS pour un propriétaire×mois. None si absent."""
    pid = str(prop_id).strip()
    m = str(mois).strip()
    for r in read_vue_mois():
        if (str(r.get("proprietaire_id") or "").strip() == pid
                and str(r.get("mois") or "").strip() == m):
            return r
    return None


def read_prefacture_entetes_prop_mois(prop_id: str, mois: str) -> list[dict[str, Any]]:
    """En-têtes de préfacture pour un propriétaire×mois (une par logement)."""
    pid = str(prop_id).strip()
    m = str(mois).strip()
    rows = read_sheet(MASTER_FACT_PROPRIETAIRES, SHEET_ENTETE, max_rows=None)
    return [r for r in rows
            if str(r.get("proprietaire_id") or "").strip() == pid
            and str(r.get("mois") or "").strip() == m]


def read_prefacture_lignes(facture_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Lignes FACT_FACTURE_LIGNES groupées par facture_id."""
    fids = {str(fid).strip() for fid in facture_ids}
    rows = read_sheet(MASTER_FACT_PROPRIETAIRES, SHEET_LIGNES, max_rows=None)
    result: dict[str, list[dict[str, Any]]] = {fid: [] for fid in fids}
    for r in rows:
        fid = str(r.get("facture_id") or "").strip()
        if fid in result:
            result[fid].append(r)
    return result

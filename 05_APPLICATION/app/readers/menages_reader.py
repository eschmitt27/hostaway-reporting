"""Lecteur read-only ménages — Lot APP-2.

Sources (toutes en lecture seule) :
- MASTER_CTRL_Rapprochement_Menages.xlsx
  · TABLEAU_COMPARAISON  — ligne par (mois, logement, intervenant) ; 3 flux séparés
  · RESUME_APPARTEMENT   — synthèse par logement
  · RESUME_INTERVENANT   — synthèse par intervenant
  · CONTROLES            — anomalies rapprochement
- MASTER_CALC_GainPerte_Menages.xlsx
  · DETAIL_ECART_COUT    — écart coût standard / coût réel par ligne

Règles gravées :
- Les 3 flux (tâches Hostaway completed / déclarés M04 internes / déclarés externes)
  restent TOUJOURS dans des champs séparés ; jamais fusionnés, jamais recalculés.
- Aucune valorisation ne s'appuie sur les données Hostaway : le coût Hostaway
  (H6 cost=NULL, non autoritaire) n'est jamais lu ni exposé.
- Lecture seule stricte via excel_reader (openpyxl read_only=True).
"""
from typing import Any
from app.config import MASTER_RAPPROCHEMENT_MENAGES, MASTER_GAINPERTE_MENAGES
from app.readers.excel_reader import read_sheet

SHEET_COMPARAISON = "TABLEAU_COMPARAISON"
SHEET_RESUME_APT = "RESUME_APPARTEMENT"
SHEET_RESUME_INT = "RESUME_INTERVENANT"
SHEET_CONTROLES = "CONTROLES"
SHEET_GAINPERTE = "DETAIL_ECART_COUT"

SOURCE_RAPPROCHEMENT = "MASTER_CTRL_Rapprochement_Menages.xlsx"
SOURCE_GAINPERTE = "MASTER_CALC_GainPerte_Menages.xlsx"


def rapprochement_available() -> bool:
    return MASTER_RAPPROCHEMENT_MENAGES.exists()


def gainperte_available() -> bool:
    return MASTER_GAINPERTE_MENAGES.exists()


def read_tableau_comparaison() -> list[dict[str, Any]]:
    """Toutes les lignes de rapprochement (mois × logement × intervenant)."""
    return read_sheet(MASTER_RAPPROCHEMENT_MENAGES, SHEET_COMPARAISON, max_rows=None)


def find_ligne(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    """Ligne unique identifiée par (mois, logement_id, intervenant_id). None si absente."""
    m = str(mois).strip()
    l = str(logement_id).strip()
    i = str(intervenant_id).strip()
    for row in read_tableau_comparaison():
        if (
            str(row.get("mois") or "").strip() == m
            and str(row.get("logement_id") or "").strip() == l
            and str(row.get("intervenant_id") or "").strip() == i
        ):
            return row
    return None


def read_controles() -> list[dict[str, Any]]:
    """Anomalies de rapprochement (CONTROLES)."""
    return read_sheet(MASTER_RAPPROCHEMENT_MENAGES, SHEET_CONTROLES, max_rows=None)


def read_resume_appartement() -> list[dict[str, Any]]:
    return read_sheet(MASTER_RAPPROCHEMENT_MENAGES, SHEET_RESUME_APT, max_rows=None)


def read_resume_intervenant() -> list[dict[str, Any]]:
    return read_sheet(MASTER_RAPPROCHEMENT_MENAGES, SHEET_RESUME_INT, max_rows=None)


def read_gainperte_detail() -> list[dict[str, Any]]:
    """Détail écart coût standard / coût réel. Ne contient aucune valorisation Hostaway."""
    return read_sheet(MASTER_GAINPERTE_MENAGES, SHEET_GAINPERTE, max_rows=None)


def find_gainperte(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    """Ligne gain/perte identifiée par (mois, logement_id, intervenant_id). None si absente."""
    m = str(mois).strip()
    l = str(logement_id).strip()
    i = str(intervenant_id).strip()
    for row in read_gainperte_detail():
        if (
            str(row.get("mois") or "").strip() == m
            and str(row.get("logement_id") or "").strip() == l
            and str(row.get("intervenant_id") or "").strip() == i
        ):
            return row
    return None

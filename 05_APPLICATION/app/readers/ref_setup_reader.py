"""Lecteur read-only de REF_Setup.xlsm — enrichissement fiche logement UNIQUEMENT.

Règles APP-1 (arbitrages validés) :
- Lecture seule stricte (excel_reader → openpyxl read_only=True).
- Enrichissement autorisé uniquement par clé directe (logement_id, type_logement_id,
  proprietaire_id pour le seul historique de commission).
- INTERDIT : lire REF_Gestion_Logements_Hist ou REF_Proprietaires.
  La résolution propriétaire/gestion vient EXCLUSIVEMENT du CSV PBI, jamais reconstruite ici.
- Aucun calcul métier : les lignes sont retournées brutes.
"""
from typing import Any
from app.config import REF_SETUP
from app.readers.excel_reader import read_sheet

# Feuilles autorisées en lecture pour l'enrichissement logement.
# REF_Gestion_Logements_Hist et REF_Proprietaires en sont volontairement ABSENTES.
SHEET_LOGEMENTS = "REF_Logements"
SHEET_TYPES = "REF_Types_Logements"
SHEET_TAUX_COMMISSION = "REF_Taux_Commission"
SHEET_COUTS_STD_MENAGE = "REF_Couts_Standards_Menage"
SHEET_COUTS_MENAGE_INTERNE = "REF_Couts_Menage_Interne"


def ref_setup_available() -> bool:
    return REF_SETUP.exists()


def get_logement_ref(logement_id: str) -> dict[str, Any] | None:
    """Ligne brute de REF_Logements pour ce logement_id, ou None si absente."""
    for row in read_sheet(REF_SETUP, SHEET_LOGEMENTS, max_rows=None):
        if str(row.get("logement_id", "")).strip() == str(logement_id).strip():
            return row
    return None


def get_type_label(type_logement_id: str) -> str | None:
    """Libellé lisible d'un type (décodage code→label de l'attribut propre du logement)."""
    if not type_logement_id:
        return None
    for row in read_sheet(REF_SETUP, SHEET_TYPES, max_rows=None):
        if str(row.get("type_logement_id", "")).strip() == str(type_logement_id).strip():
            return row.get("type_logement")
    return None


def get_commission_rows(proprietaire_id: str) -> list[dict[str, Any]]:
    """Lignes BRUTES de REF_Taux_Commission rattachées au propriétaire déjà résolu par le CSV.

    Aucun tri par date, aucune sélection de ligne, aucun calcul, aucune notion d'« actuel ».
    """
    if not proprietaire_id:
        return []
    out = []
    for row in read_sheet(REF_SETUP, SHEET_TAUX_COMMISSION, max_rows=None):
        if str(row.get("proprietaire_id", "")).strip() == str(proprietaire_id).strip():
            out.append({
                "taux_commission": row.get("taux_commission"),
                "date_debut": row.get("date_debut"),
                "date_fin": row.get("date_fin"),
                "actif": row.get("actif"),
            })
    return out


def get_menage_interne_rows(logement_id: str) -> list[dict[str, Any]]:
    """Lignes BRUTES de REF_Couts_Menage_Interne rattachées par clé directe logement_id."""
    if not logement_id:
        return []
    out = []
    for row in read_sheet(REF_SETUP, SHEET_COUTS_MENAGE_INTERNE, max_rows=None):
        if str(row.get("logement_id", "")).strip() == str(logement_id).strip():
            out.append(row)
    return out


def get_menage_standard_rows(type_logement_id: str) -> list[dict[str, Any]]:
    """Lignes BRUTES de REF_Couts_Standards_Menage pour le type propre du logement.

    Décodage de référence sur l'attribut type_logement_id du logement. Aucun calcul.
    """
    if not type_logement_id:
        return []
    out = []
    for row in read_sheet(REF_SETUP, SHEET_COUTS_STD_MENAGE, max_rows=None):
        if str(row.get("type_logement_id", "")).strip() == str(type_logement_id).strip():
            out.append(row)
    return out

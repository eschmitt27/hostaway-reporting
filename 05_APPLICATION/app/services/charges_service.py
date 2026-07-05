"""Service charges fournisseurs — lecture MASTER Lot3 (APP-3a).

Règles :
- Lecture seule MASTER ; aucun recalcul d'aucune métrique.
- status=OK même si 0 lignes (liste vide normale avant toute saisie).
- status=ERROR uniquement si MASTER absent ou illisible.
- Aucun accès SQLite, aucune écriture Excel ou MASTER.
"""
from datetime import datetime
from typing import Any
from app.readers import charges_reader as reader


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_list(
    mois: str = "",
    logement_id: str = "",
    categorie_charge_id: str = "",
    code_impact: str = "",
    statut_controle: str = "",
    associe_id: str = "",
) -> dict[str, Any]:
    read_at = _now()

    if not reader.master_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_MASTER}.",
            "source": reader.SOURCE_MASTER,
            "read_at": read_at,
            "rows": [],
            "filters": _empty_filters(),
            "applied": {},
        }

    rows = reader.read_charges()

    filters = {
        "mois": sorted({str(r.get("mois") or "").strip() for r in rows if (r.get("mois") or "").strip()}),
        "logements": sorted({str(r.get("logement_id") or "").strip() for r in rows if (r.get("logement_id") or "").strip()}),
        "categories": sorted({str(r.get("categorie_charge_id") or "").strip() for r in rows if (r.get("categorie_charge_id") or "").strip()}),
        "codes_impact": sorted({str(r.get("code_impact") or "").strip() for r in rows if (r.get("code_impact") or "").strip()}),
        "statuts": sorted({str(r.get("statut_controle") or "").strip() for r in rows if (r.get("statut_controle") or "").strip()}),
        "associes": sorted({str(r.get("associe_id") or "").strip() for r in rows if (r.get("associe_id") or "").strip()}),
    }

    def _match(r: dict) -> bool:
        if mois and str(r.get("mois") or "").strip() != mois:
            return False
        if logement_id and str(r.get("logement_id") or "").strip() != logement_id:
            return False
        if categorie_charge_id and str(r.get("categorie_charge_id") or "").strip() != categorie_charge_id:
            return False
        if code_impact and str(r.get("code_impact") or "").strip() != code_impact:
            return False
        if statut_controle and str(r.get("statut_controle") or "").strip() != statut_controle:
            return False
        if associe_id and str(r.get("associe_id") or "").strip() != associe_id:
            return False
        return True

    filtered = [r for r in rows if _match(r)]

    return {
        "status": "OK",
        "error_message": None,
        "source": reader.SOURCE_MASTER,
        "read_at": read_at,
        "rows": filtered,
        "count_total": len(rows),
        "count_affiches": len(filtered),
        "filters": filters,
        "applied": {
            "mois": mois,
            "logement_id": logement_id,
            "categorie_charge_id": categorie_charge_id,
            "code_impact": code_impact,
            "statut_controle": statut_controle,
            "associe_id": associe_id,
        },
    }


def load_detail(charge_id: str) -> dict[str, Any] | None:
    """Fiche détail par charge_id. None si la ligne n'existe pas (→ 404 propre)."""
    read_at = _now()

    if not reader.master_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_MASTER}.",
            "charge_id": charge_id,
            "read_at": read_at,
        }

    charge = reader.find_charge(charge_id)
    if charge is None:
        return None

    return {
        "status": "OK",
        "error_message": None,
        "charge_id": charge_id,
        "charge": charge,
        "source": reader.SOURCE_MASTER,
        "read_at": read_at,
    }


def _empty_filters() -> dict[str, list]:
    return {
        "mois": [], "logements": [], "categories": [],
        "codes_impact": [], "statuts": [], "associes": [],
    }

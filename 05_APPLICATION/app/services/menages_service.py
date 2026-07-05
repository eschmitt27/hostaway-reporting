"""Service ménages — lecture rapprochement + outrepassage tracé (APP-2).

Règles :
- Lecture seule MASTER ; aucun recalcul d'aucune métrique métier.
- Outrepassage = écriture SQLite uniquement (motif + ts + audit_events).
  Jamais d'écriture dans un fichier Excel ou MASTER.
- Les 3 flux (Hostaway tasks / M04 internes / externes) restent champs séparés.
- Aucune valorisation ne s'appuie sur les données de coût Hostaway.
"""
from datetime import datetime
from typing import Any
from app.readers import menages_reader as reader
from app.db.connection import get_db


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_list(
    mois: str = "",
    logement_id: str = "",
    type_intervenant: str = "",
    statut_controle: str = "",
) -> dict[str, Any]:
    read_at = _now()

    if not reader.rapprochement_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_RAPPROCHEMENT}.",
            "source": reader.SOURCE_RAPPROCHEMENT,
            "read_at": read_at,
            "rows": [],
            "filters": _empty_filters(),
            "applied": {},
        }

    rows = reader.read_tableau_comparaison()
    if not rows:
        return {
            "status": "ERROR",
            "error_message": f"Source vide ou illisible : {reader.SOURCE_RAPPROCHEMENT}.",
            "source": reader.SOURCE_RAPPROCHEMENT,
            "read_at": read_at,
            "rows": [],
            "filters": _empty_filters(),
            "applied": {},
        }

    filters = {
        "mois": sorted({str(r.get("mois") or "").strip() for r in rows if (r.get("mois") or "").strip()}),
        "logements": sorted({str(r.get("logement_id") or "").strip() for r in rows if (r.get("logement_id") or "").strip()}),
        "types": sorted({str(r.get("type_intervenant") or "").strip() for r in rows if (r.get("type_intervenant") or "").strip()}),
        "statuts": sorted({str(r.get("statut_controle") or "").strip() for r in rows if (r.get("statut_controle") or "").strip()}),
    }

    overrides = _load_all_overrides()

    def _match(r: dict) -> bool:
        if mois and str(r.get("mois") or "").strip() != mois:
            return False
        if logement_id and str(r.get("logement_id") or "").strip() != logement_id:
            return False
        if type_intervenant and str(r.get("type_intervenant") or "").strip() != type_intervenant:
            return False
        if statut_controle:
            eff = _effective_statut(r, overrides)
            if eff != statut_controle:
                return False
        return True

    filtered = [_enrich_override(r, overrides) for r in rows if _match(r)]

    return {
        "status": "OK",
        "error_message": None,
        "source": reader.SOURCE_RAPPROCHEMENT,
        "read_at": read_at,
        "rows": filtered,
        "count_total": len(rows),
        "count_affiches": len(filtered),
        "filters": filters,
        "applied": {
            "mois": mois,
            "logement_id": logement_id,
            "type_intervenant": type_intervenant,
            "statut_controle": statut_controle,
        },
    }


def load_detail(mois: str, logement_id: str, intervenant_id: str) -> dict[str, Any] | None:
    """Charge la fiche ménage. None si la ligne n'existe pas (→ 404 propre)."""
    read_at = _now()

    if not reader.rapprochement_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_RAPPROCHEMENT}.",
            "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id,
            "read_at": read_at,
        }

    ligne = reader.find_ligne(mois, logement_id, intervenant_id)
    if ligne is None:
        return None

    gainperte = reader.find_gainperte(mois, logement_id, intervenant_id) if reader.gainperte_available() else None
    override = _load_override(mois, logement_id, intervenant_id)

    return {
        "status": "OK",
        "error_message": None,
        "mois": mois,
        "logement_id": logement_id,
        "intervenant_id": intervenant_id,
        "ligne": ligne,
        "gainperte": gainperte,
        "override": override,
        "statut_effectif": _effective_statut(ligne, {_override_key(mois, logement_id, intervenant_id): override} if override else {}),
        "source_rapprochement": reader.SOURCE_RAPPROCHEMENT,
        "source_gainperte": reader.SOURCE_GAINPERTE if gainperte is not None else None,
        "gainperte_disponible": reader.gainperte_available(),
        "read_at": read_at,
    }


def enregistrer_outrepassage(
    mois: str, logement_id: str, intervenant_id: str, motif: str
) -> dict[str, Any]:
    """Enregistre un outrepassage dans SQLite. Jamais d'écriture Excel ou MASTER."""
    motif = str(motif).strip()
    if not motif:
        return {"ok": False, "error": "Le motif est obligatoire."}

    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO menage_overrides (mois, logement_id, intervenant_id, motif, statut_override)
               VALUES (?, ?, ?, ?, 'JUSTIFIE')
               ON CONFLICT(mois, logement_id, intervenant_id)
               DO UPDATE SET motif=excluded.motif, ts=strftime('%Y-%m-%dT%H:%M:%SZ','now'), statut_override='JUSTIFIE'
            """,
            (mois, logement_id, intervenant_id, motif),
        )
        conn.execute(
            "INSERT INTO audit_events (action, details) VALUES (?, ?)",
            (
                "MENAGE_OVERRIDE",
                f"mois={mois} logement={logement_id} intervenant={intervenant_id} motif={motif[:200]}",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return {"ok": True, "mois": mois, "logement_id": logement_id, "intervenant_id": intervenant_id}


# --- Helpers privés ---

def _override_key(mois: str, logement_id: str, intervenant_id: str) -> str:
    return f"{mois}|{logement_id}|{intervenant_id}"


def _load_all_overrides() -> dict[str, dict]:
    """Retourne {key: override_row} depuis menage_overrides."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT mois, logement_id, intervenant_id, motif, statut_override, ts FROM menage_overrides"
        ).fetchall()
    finally:
        conn.close()
    result = {}
    for r in rows:
        key = _override_key(r["mois"], r["logement_id"], r["intervenant_id"])
        result[key] = dict(r)
    return result


def _load_override(mois: str, logement_id: str, intervenant_id: str) -> dict | None:
    """Override pour une ligne unique. None si absente."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT mois, logement_id, intervenant_id, motif, statut_override, ts FROM menage_overrides "
            "WHERE mois=? AND logement_id=? AND intervenant_id=?",
            (mois, logement_id, intervenant_id),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _effective_statut(row: dict, overrides: dict) -> str:
    """Statut effectif = statut_controle sauf si override JUSTIFIE."""
    key = _override_key(
        str(row.get("mois") or ""),
        str(row.get("logement_id") or ""),
        str(row.get("intervenant_id") or ""),
    )
    if key in overrides and (overrides[key] or {}).get("statut_override") == "JUSTIFIE":
        return "JUSTIFIE"
    return str(row.get("statut_controle") or "").strip()


def _enrich_override(row: dict, overrides: dict) -> dict:
    key = _override_key(
        str(row.get("mois") or ""),
        str(row.get("logement_id") or ""),
        str(row.get("intervenant_id") or ""),
    )
    enriched = dict(row)
    enriched["override"] = overrides.get(key)
    enriched["statut_effectif"] = _effective_statut(row, overrides)
    return enriched


def _empty_filters() -> dict[str, list]:
    return {"mois": [], "logements": [], "types": [], "statuts": []}

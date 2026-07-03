"""Service réservations hors Hostaway — LECTURE SEULE (Lot APP-2a), AUCUN CALCUL.

- Liste + détail depuis MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx (onglet MASTER).
- MASTER absent/vide/illisible → état d'erreur clair.
- Aucune écriture, aucun brouillon, aucune génération de PK, aucun appel writer.
- Les champs commission / acompte / taux / net / impacts sont AFFICHÉS TELS QUELS,
  signalés comme issus du fichier généré par le moteur (Power Query), jamais recalculés ici.
"""
from datetime import datetime
from typing import Any
from app.config import MASTER_RESERVATIONS_HH, SAISIE_RESERVATIONS_HH
from app.readers import reservations_hh_reader as reader

MASTER_SOURCE_NAME = "MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx"
MASTER_SHEET = "MASTER"
SAISIE_SOURCE_NAME = "SAISIE_ReservationsHorsHostaway.xlsx"

# Champs produits par le moteur (formules Excel / Power Query) — affichés, jamais calculés par l'app
CHAMPS_MOTEUR = {
    "nuits", "mois", "ROW_HASH",
    "taux_commission", "taux_commission_source",
    "commission", "acompte_facture",
    "impact_resultat_reel", "impact_resultat_comptable",
    "source_module", "source_table", "source_pk", "date_integration",
}

# Colonnes de filtre autorisées (présentes dans le MASTER)
FILTER_COLS = [
    "mois", "logement_id", "proprietaire_id", "canal_id",
    "source_financiere", "statut_controle", "code_impact", "comptabilisation",
]


def _read_at() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _file_mtime(path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except OSError:
        return "—"


def _error(msg: str, read_at: str) -> dict[str, Any]:
    return {
        "status": "ERROR",
        "error_message": msg,
        "source": MASTER_SOURCE_NAME,
        "sheet": MASTER_SHEET,
        "read_at": read_at,
        "master_mtime": _file_mtime(MASTER_RESERVATIONS_HH),
        "rows": [],
        "count_total": 0,
        "count_affiches": 0,
        "filters": {c: [] for c in FILTER_COLS},
        "applied": {},
    }


def load_list(q: str = "", **filters: str) -> dict[str, Any]:
    read_at = _read_at()

    if not MASTER_RESERVATIONS_HH.exists():
        return _error(
            f"Source introuvable : {MASTER_SOURCE_NAME}. "
            "La liste des réservations hors Hostaway ne peut pas être affichée.",
            read_at,
        )

    rows = reader.read_reservations()
    if not rows:
        # Fichier présent mais aucune réservation en cache (ex. Power Query non rafraîchie)
        return {
            "status": "EMPTY",
            "error_message": None,
            "source": MASTER_SOURCE_NAME,
            "sheet": MASTER_SHEET,
            "read_at": read_at,
            "master_mtime": _file_mtime(MASTER_RESERVATIONS_HH),
            "rows": [],
            "count_total": 0,
            "count_affiches": 0,
            "filters": {c: [] for c in FILTER_COLS},
            "applied": {c: filters.get(c, "") for c in FILTER_COLS} | {"q": q},
        }

    # Options de filtre bâties sur les valeurs réelles
    filter_options = {
        c: sorted({(str(r.get(c) or "").strip()) for r in rows if str(r.get(c) or "").strip()})
        for c in FILTER_COLS
    }

    ql = q.strip().lower()

    def _match(r: dict) -> bool:
        for c in FILTER_COLS:
            want = (filters.get(c) or "").strip()
            if want and str(r.get(c) or "").strip() != want:
                return False
        if ql:
            hay = " ".join(str(r.get(k) or "") for k in
                           ("reservation_hh_id", "logement_id", "proprietaire_id",
                            "canal_id", "source_financiere", "commentaire")).lower()
            if ql not in hay:
                return False
        return True

    filtered = [r for r in rows if _match(r)]

    return {
        "status": "OK",
        "error_message": None,
        "source": MASTER_SOURCE_NAME,
        "sheet": MASTER_SHEET,
        "saisie_amont": SAISIE_SOURCE_NAME,
        "read_at": read_at,
        "master_mtime": _file_mtime(MASTER_RESERVATIONS_HH),
        "rows": filtered,
        "count_total": len(rows),
        "count_affiches": len(filtered),
        "filters": filter_options,
        "applied": {c: (filters.get(c) or "") for c in FILTER_COLS} | {"q": q},
    }


def load_detail(reservation_hh_id: str) -> dict[str, Any] | None:
    read_at = _read_at()

    if not MASTER_RESERVATIONS_HH.exists():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {MASTER_SOURCE_NAME}.",
            "reservation_hh_id": reservation_hh_id,
            "read_at": read_at,
        }

    row = reader.find_reservation(reservation_hh_id)
    if row is None:
        return None  # 404 propre géré par la route

    origine = {
        "source": MASTER_SOURCE_NAME,
        "sheet": MASTER_SHEET,
        "saisie_amont": SAISIE_SOURCE_NAME,
        "read_at": read_at,
        "master_mtime": _file_mtime(MASTER_RESERVATIONS_HH),
        "note_moteur": (
            "Les montants dérivés (taux, commission, acompte, impacts) proviennent du fichier "
            "généré par le moteur (formules Excel / Power Query). L'application ne les recalcule pas."
        ),
        "note_refresh": "Actualisation Power Query manuelle requise après toute future saisie.",
    }

    return {
        "status": "OK",
        "error_message": None,
        "reservation_hh_id": reservation_hh_id,
        "row": row,
        "champs_moteur": CHAMPS_MOTEUR,
        "origine": origine,
        "read_at": read_at,
    }

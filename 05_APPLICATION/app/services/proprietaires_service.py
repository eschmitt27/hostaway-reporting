"""Service propriétaires & règlements — lecture MASTER Lot10/Lot12 (APP-3c).

- status=OK si source disponible (liste vide acceptable).
- status=ERROR uniquement si source absente ou illisible.
- Bloc EXPLOITATION et bloc REGLEMENT non mélangés (D033, EP1-EP7).
- revenu_net_exploitation affiché tel quel, jamais recalculé.
- Préfacture indisponible proprement si Lot12 absent (sans bloquer le relevé).
- Aucun accès SQLite, aucune écriture Excel.
"""
from datetime import datetime
from typing import Any
from app.readers import proprietaires_reader as reader
from app.services import referentiel_service as referentiel


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_list() -> dict[str, Any]:
    """Liste des propriétaires depuis REF_Setup."""
    read_at = _now()
    if not reader.ref_available():
        return {
            "status": "ERROR",
            "error_message": referentiel.MESSAGE_ABSENT,
            "code": referentiel.REFERENTIEL_ABSENT,
            "source": reader.SOURCE_REF,
            "read_at": read_at,
            "rows": [],
        }
    rows = reader.read_proprietaires()
    return {
        "status": "OK",
        "error_message": None,
        "source": reader.SOURCE_REF,
        "read_at": read_at,
        "rows": rows,
        "count": len(rows),
    }


def load_detail(prop_id: str) -> dict[str, Any] | None:
    """Fiche propriétaire + mois disponibles dans REGLEMENT."""
    read_at = _now()
    if not reader.ref_available():
        return {
            "status": "ERROR",
            "error_message": referentiel.MESSAGE_ABSENT,
            "code": referentiel.REFERENTIEL_ABSENT,
            "prop_id": prop_id,
            "read_at": read_at,
        }
    prop = reader.find_proprietaire(prop_id)
    if prop is None:
        return None

    mois_list: list[str] = []
    if reader.calc_available():
        lignes = reader.read_reglement_prop(prop_id)
        mois_list = sorted({str(r.get("mois") or "").strip()
                            for r in lignes if (r.get("mois") or "").strip()})

    return {
        "status": "OK",
        "error_message": None,
        "prop_id": prop_id,
        "prop": prop,
        "mois_disponibles": mois_list,
        "source_ref": reader.SOURCE_REF,
        "source_calc": reader.SOURCE_CALC if reader.calc_available() else None,
        "read_at": read_at,
    }


def load_releve(prop_id: str, mois: str) -> dict[str, Any]:
    """Relevé mensuel : blocs EXPLOITATION et REGLEMENT séparés.

    Renvoie status=NOT_FOUND si propriétaire inconnu ou aucune ligne pour ce mois.
    Ne mélange jamais les deux blocs (D033).
    """
    read_at = _now()
    if not reader.ref_available():
        return {
            "status": "ERROR",
            "error_message": referentiel.MESSAGE_ABSENT,
            "code": referentiel.REFERENTIEL_ABSENT,
            "prop_id": prop_id,
            "mois": mois,
            "read_at": read_at,
        }
    if not reader.calc_available():
        return {
            "status": "ERROR",
            "error_message": f"Source introuvable : {reader.SOURCE_CALC}.",
            "prop_id": prop_id,
            "mois": mois,
            "read_at": read_at,
        }
    prop = reader.find_proprietaire(prop_id)
    if prop is None:
        return {"status": "NOT_FOUND", "prop_id": prop_id, "mois": mois, "read_at": read_at}

    lignes_reglement = reader.read_reglement_prop_mois(prop_id, mois)
    if not lignes_reglement:
        return {"status": "NOT_FOUND", "prop_id": prop_id, "mois": mois, "read_at": read_at}

    vue = reader.read_vue_mois_prop_mois(prop_id, mois)

    # Bloc EXPLOITATION : colonnes issues de REGLEMENT, séparées des colonnes règlement
    _COLS_EXPLOITATION = [
        "logement_id", "charge_fixe_mensuelle", "charge_fixe_source",
        "total_payout_mois", "total_menage_mois", "total_commission_mois",
        "net_proprietaire_avant_charge_mois", "nb_reservations",
    ]
    # Bloc REGLEMENT : colonnes financières de règlement, séparées
    _COLS_REGLEMENT = [
        "logement_id", "montant_du_conciergerie", "acompte_conciergerie_recu_via_airbnb",
        "autres_acomptes_recus", "paiement_deja_recu", "reste_a_payer_conciergerie",
        "net_proprietaire_apres_charge_mois", "statut_reglement",
    ]

    def _extract(row: dict[str, Any], cols: list[str]) -> dict[str, Any]:
        return {c: row.get(c) for c in cols}

    bloc_exploitation = [_extract(r, _COLS_EXPLOITATION) for r in lignes_reglement]
    bloc_reglement = [_extract(r, _COLS_REGLEMENT) for r in lignes_reglement]

    return {
        "status": "OK",
        "error_message": None,
        "prop_id": prop_id,
        "mois": mois,
        "prop": prop,
        "bloc_exploitation": bloc_exploitation,
        "bloc_reglement": bloc_reglement,
        "vue_mois": vue,
        "source_calc": reader.SOURCE_CALC,
        "read_at": read_at,
        "prefacture_disponible": reader.fact_available(),
    }


def load_prefacture(prop_id: str, mois: str) -> dict[str, Any]:
    """12 lignes de préfacture par logement pour un propriétaire×mois.

    Retourne status=UNAVAILABLE si Lot12 absent (sans exception).
    Retourne status=NOT_FOUND si propriétaire ou mois inconnu.
    """
    read_at = _now()
    if not reader.fact_available():
        return {
            "status": "UNAVAILABLE",
            "error_message": f"Source indisponible : {reader.SOURCE_FACT}.",
            "prop_id": prop_id,
            "mois": mois,
            "read_at": read_at,
            "factures": [],
        }
    if reader.ref_available() and reader.find_proprietaire(prop_id) is None:
        return {"status": "NOT_FOUND", "prop_id": prop_id, "mois": mois, "read_at": read_at, "factures": []}

    entetes = reader.read_prefacture_entetes_prop_mois(prop_id, mois)
    if not entetes:
        return {"status": "NOT_FOUND", "prop_id": prop_id, "mois": mois, "read_at": read_at, "factures": []}

    facture_ids = [str(e.get("facture_id") or "").strip() for e in entetes]
    lignes_map = reader.read_prefacture_lignes(facture_ids)

    factures = []
    for entete in entetes:
        fid = str(entete.get("facture_id") or "").strip()
        lignes = lignes_map.get(fid, [])
        lignes_sorted = sorted(lignes, key=lambda r: int(r.get("ligne_num") or 0))
        factures.append({
            "entete": entete,
            "lignes_exploitation": [l for l in lignes_sorted if str(l.get("bloc") or "") == "EXPLOITATION"],
            "lignes_reglement": [l for l in lignes_sorted if str(l.get("bloc") or "") == "REGLEMENT"],
        })

    return {
        "status": "OK",
        "error_message": None,
        "prop_id": prop_id,
        "mois": mois,
        "factures": factures,
        "source_fact": reader.SOURCE_FACT,
        "read_at": read_at,
    }

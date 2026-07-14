"""Persistance durable des impacts d'une charge (normalisée, liée par charge_id).

Écrit dans la source de vérité SAISIE_Charges_Impacts.xlsx (onglets AFFECTATIONS / MENAGE /
RESERVE_REFACTURATION) et, pour les avantages, dans la source Lot7 existante (SOURCE_SAISIE),
avec `lien_origine = charge_id`.

Garde-fou : tant que CHARGES_REAL_WRITE_ENABLED est False, l'écriture réelle est INTERDITE.
Seule l'écriture sur COPIE contrôlée est autorisée (prévisualisation / recette).
Idempotent : réécrire un charge_id remplace ses lignes, ne les duplique jamais.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg
from app.services import charges_impact_service as impact

AFFECT_SHEET = "AFFECTATIONS"
MENAGE_SHEET = "MENAGE"
RESERVE_SHEET = "RESERVE_REFACTURATION"

STATUT_A_CONTROLER = "A_CONTROLER"
STATUT_RESERVE = "EN_ATTENTE"
ORIGINE = "NOUVELLE_CHARGE_GUIDEE"


def _row_hash(*parts: Any) -> str:
    payload = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_persistable(
    charge_id: str,
    mois: str,
    montant: float,
    guide: dict[str, Any],
    form_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Construit les lignes normalisées persistables à partir du charge_id + guide.

    - Affectations : une ligne par quote-part (charge non ménage à périmètre non vide).
    - Ménage : une ligne par intervenant (mode INTERVENANT) ou par logement (mode LOGEMENT).
    - Réserve : une ligne par quote-part refacturable (jamais pour une charge ménage).
    - Avantage : au plus UNE ligne Lot7 (lien_origine=charge_id).
    Somme des quotes-parts (affectations refacturables) = montant réparti.
    """
    form_data = form_data or {}
    impact_menage = bool(guide.get("impact_menage"))
    perimetre = guide.get("perimetre") or {}
    menage = guide.get("menage") or {}
    reserve = guide.get("reserve")

    # Propriétaire par logement (gestion active du mois) — pour tracer sans concaténer.
    prop_par_log = perimetre.get("proprietaire_par_logement") or {}

    affectations: list[dict[str, Any]] = []
    menage_rows: list[dict[str, Any]] = []
    reserve_rows: list[dict[str, Any]] = []
    avantage = None

    if not impact_menage:
        finaux = perimetre.get("logements_finaux", [])
        if finaux:
            quotes = impact.repartir_egal(montant, finaux)
            for i, q in enumerate(quotes, 1):
                lid = q["logement_id"]
                aid = f"{charge_id}-AFF-{i:03d}"
                affectations.append({
                    "affectation_id": aid,
                    "charge_id": charge_id,
                    "mois": mois,
                    "logement_id": lid,
                    "proprietaire_id": prop_par_log.get(lid),
                    "quote_part": q["quote_part"],
                    "statut": STATUT_A_CONTROLER,
                    "origine": ORIGINE,
                    "commentaire": None,
                    "ROW_HASH": _row_hash(aid, charge_id, lid, q["quote_part"]),
                })
    else:
        # Impact ménage : sélections analytiques (jamais de réserve refacturable).
        mode = menage.get("mode")
        if mode == impact.MENAGE_MODE_INTERVENANT:
            for i, iid in enumerate(menage.get("intervenants", []), 1):
                mid = f"{charge_id}-MEN-{i:03d}"
                menage_rows.append({
                    "menage_impact_id": mid, "charge_id": charge_id, "mois": mois,
                    "mode": mode, "intervenant_id": iid, "logement_id": None,
                    "statut": STATUT_A_CONTROLER, "origine": ORIGINE, "commentaire": None,
                    "ROW_HASH": _row_hash(mid, charge_id, mode, iid),
                })
        elif mode == impact.MENAGE_MODE_LOGEMENT:
            for i, lid in enumerate(menage.get("logements", []), 1):
                mid = f"{charge_id}-MEN-{i:03d}"
                menage_rows.append({
                    "menage_impact_id": mid, "charge_id": charge_id, "mois": mois,
                    "mode": mode, "intervenant_id": None, "logement_id": lid,
                    "statut": STATUT_A_CONTROLER, "origine": ORIGINE, "commentaire": None,
                    "ROW_HASH": _row_hash(mid, charge_id, mode, lid),
                })

    # Réserve refacturable — jamais pour une charge ménage.
    if not impact_menage and reserve:
        for i, e in enumerate(reserve.get("entrees", []), 1):
            rid = f"{charge_id}-RES-{i:03d}"
            reserve_rows.append({
                "reserve_id": rid,
                "charge_id": charge_id,
                "mois": e.get("mois", mois),
                "logement_id": e.get("logement_id"),
                "proprietaire_id": e.get("proprietaire_id"),
                "montant_refacturable": e.get("montant_refacturable"),
                "libelle": e.get("libelle"),
                "justificatif": e.get("justificatif"),
                "statut_traitement": STATUT_RESERVE,
                "trace_decision": None,
                "origine": ORIGINE,
                "commentaire": None,
                "ROW_HASH": _row_hash(rid, charge_id, e.get("logement_id"), e.get("montant_refacturable")),
            })

    # Avantage associé : PORTÉ PAR LA CHARGE (colonne avantage_associe_id de SAISIE_Charges_Flux),
    # jamais ressaisi dans Lot7 SOURCE_SAISIE (règle « NE PAS RESSAISIR », résiduelle) — évite le
    # double comptage. L'agrégation Lot7 (lib_avantages) attribue l'avantage par bénéficiaire.
    if guide.get("avantage_associe") and guide.get("associe_id"):
        avantage = {
            "charge_id": charge_id,
            "mois": mois,
            "avantage_associe_id": guide["associe_id"],
            "montant": montant,
            "porte_par": "SAISIE_Charges_Flux.avantage_associe_id",
        }

    return {
        "charge_id": charge_id,
        "affectations": affectations,
        "menage": menage_rows,
        "reserve": reserve_rows,
        "avantage": avantage,
        "controle_somme_quotes": impact.somme_quotes_parts(
            [{"quote_part": r["montant_refacturable"]} for r in reserve_rows]
        ) if reserve_rows else None,
    }


def _remplacer_lignes(ws, id_col_name: str, charge_id: str, rows: list[dict[str, Any]]) -> None:
    """Remplace (idempotent) les lignes d'un charge_id : supprime les existantes puis ré-écrit."""
    headers = [str(c.value).strip() if c.value is not None else "" for c in ws[1]]
    col = {h: i + 1 for i, h in enumerate(headers)}
    charge_col = col.get("charge_id")
    # 1. Supprimer lignes existantes de ce charge_id (de bas en haut)
    for r in range(ws.max_row, 1, -1):
        if charge_col and str(ws.cell(r, charge_col).value or "").strip() == charge_id:
            ws.delete_rows(r, 1)
    # 2. Ajouter les nouvelles lignes
    for row in rows:
        ws.append([row.get(h) for h in headers])


def persister_sur_copie(
    persistable: dict[str, Any],
    impacts_copy_path: Path,
    avantages_copy_path: Path | None = None,
) -> dict[str, Any]:
    """Écrit les impacts dans une COPIE contrôlée (jamais le fichier réel). Idempotent."""
    charge_id = persistable["charge_id"]
    p = Path(impacts_copy_path)
    wb = openpyxl.load_workbook(str(p))
    try:
        _remplacer_lignes(wb[AFFECT_SHEET], "affectation_id", charge_id, persistable["affectations"])
        _remplacer_lignes(wb[MENAGE_SHEET], "menage_impact_id", charge_id, persistable["menage"])
        _remplacer_lignes(wb[RESERVE_SHEET], "reserve_id", charge_id, persistable["reserve"])
        wb.save(str(p))
    finally:
        wb.close()

    # Avantage : porté par la charge (colonne avantage_associe_id, écrite sur la ligne SAISIE
    # par charges_preview_service._build_row_data). Aucune écriture Lot7 ici (anti double-comptage).
    avantage_porte = persistable.get("avantage") is not None

    return {
        "charge_id": charge_id,
        "affectations_ecrites": len(persistable["affectations"]),
        "menage_ecrits": len(persistable["menage"]),
        "reserve_ecrites": len(persistable["reserve"]),
        "avantage_porte_par_charge": avantage_porte,
        "cible_impacts": str(p),
    }


def persister_reel(token: str, **options: Any):
    """Écriture réelle d'une charge, à partir du SEUL token de prévisualisation.

    Aucune donnée métier n'est acceptée ici : tout est relu du manifest serveur. La chaîne complète
    (vérification du manifest et des empreintes → flags → verrou → résolution du charge_id →
    transaction deux fichiers → journal → lots aval) est portée par
    `charges_confirmation_service.confirmer`, seul point d'entrée de l'écriture réelle.

    Les flags restent le garde-fou : `confirmer_ecriture_charge` refuse avant toute écriture si
    `CHARGES_REAL_WRITE_ENABLED` ou `CHARGES_REAL_WRITE_CONFIRMATION_ENABLED` est False.

    Retourne un `ResultatConfirmation` (jamais d'exception : l'incident est dans le résultat).
    """
    # Import différé : charges_confirmation_service dépend de ce module (cycle sinon).
    from app.services import charges_confirmation_service as confirmation

    return confirmation.confirmer(token, **options)

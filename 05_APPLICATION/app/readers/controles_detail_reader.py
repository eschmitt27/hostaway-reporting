"""Reader DÉTAIL des contrôles agrégés (APP-5B) — LECTURE SEULE.

Le Lot11 agrège chaque anomalie (« 59 réservations sans commission », « 4 logements écart »…) en une
seule ligne MASTER. Ce reader relit les SOURCES détaillées réelles pour rouvrir chaque agrégat en
éléments actionnables, sans jamais recréer ni reclasser une anomalie : le grain vient de la source.

Sources détail :
  - VRBO sans montant            → SQLite, dataset RESOLUES (source=HOSTAWAY_VRBO_A_CONTROLER)
  - Réservations sans commission → MASTER_CALC_Commissions (onglet A_CONTROLER)
  - Écarts ménages externes      → MASTER_FACT_MEN_MenagesExternes (onglet VUE_ECART_HOSTAWAY)
  - Banque non classée           → SQLite (classification courante, statut RAPPROCHEMENT_REQUIS)

Aucune lecture de classeur. Aucun chemin absolu exposé. Aucune donnée voyageur superflue.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any


import app.config as cfg


_CACHE: dict[tuple, list[dict[str, Any]]] = {}


def vider_cache() -> None:
    _CACHE.clear()


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


# ── Sources détail ───────────────────────────────────────────────────────────

def reservations_vrbo() -> list[dict[str, Any]]:
    """Réservations VRBO du PÉRIMÈTRE MOTEUR (une ligne = une réservation).

    Lit exactement la source utilisée par Lot11 pour ce contrôle : le dataset RÉSOLU (Lot4quater), et
    non le dataset live (Lot4bis). La résolution a déjà sorti du périmètre « à contrôler » les
    réservations des mois clôturés : APP-5B reproduit donc exactement le compte moteur (mois ouverts
    uniquement). Prendre le live ferait réapparaître des anomalies déjà closes.
    """
    from app.services import reservations_dataset_service as ds

    return ds.par_source("HOSTAWAY_VRBO_A_CONTROLER", etape=ds.ETAPE_RESOLUES)


def reservations_vrbo_hors_perimetre() -> list[dict[str, Any]]:
    """Réservations VRBO présentes dans le dataset live mais HORS périmètre moteur.

    Ce sont les VRBO des mois clôturés, sortis du contrôle par la résolution. Vue TECHNIQUE séparée :
    jamais présentées comme anomalies de ce contrôle.
    """
    from app.services import reservations_dataset_service as ds

    ids_perimetre = {_txt(r.get("reservation_id_hostaway")) or _txt(r.get("reservation_calc_id"))
                     for r in reservations_vrbo()}
    out = []
    for r in ds.par_source("HOSTAWAY_VRBO_A_CONTROLER", etape=ds.ETAPE_CALCULEES):
        rid = _txt(r.get("reservation_id_hostaway")) or _txt(r.get("reservation_calc_id"))
        if rid not in ids_perimetre:
            out.append(r)
    return out


def reservations_index() -> dict[str, dict[str, Any]]:
    """{reservation_id_hostaway|hh|source_pk: ligne} pour enrichir les commissions.

    Le dataset LIVE, pas le résolu : cet index sert à retrouver le mois et le propriétaire d'une
    réservation citée par une commission, y compris pour un mois clôturé dont la ligne résolue vient
    de l'historique. Le live couvre tout le périmètre, ce qui est ce qu'on veut pour un simple
    rattachement — aucun montant n'est lu ici.
    """
    from app.services import reservations_dataset_service as ds

    idx: dict[str, dict[str, Any]] = {}
    for r in ds.lignes(ds.ETAPE_CALCULEES):
        for k in ("reservation_id_hostaway", "reservation_hh_id", "source_pk"):
            v = _txt(r.get(k))
            if v:
                idx.setdefault(v, r)
    return idx


def commissions_a_controler() -> list[dict[str, Any]]:
    """Réservations A_CONTROLER exclues du calcul de commission (une ligne = une réservation).

    SQLite (`lot10_commissions_a_controler`, migration 0044), dataset du run Lot10 actif — plus de
    lecture de `MASTER_CALC_Commissions.xlsx`. `listing_map_id`/`row_hash` sont réexposés sous les
    noms du classeur (`listingMapId`/`ROW_HASH`) : la base nomme en snake_case, les consommateurs
    connaissent le vocabulaire moteur.
    """
    from app.readers import proprietaires_reglements_reader as _lot10

    lignes = _lot10._src_sqlite("commissions_a_controler", "Commissions à contrôler",
                                "lot10_commissions_a_controler").lignes
    for ligne in lignes:
        if "listing_map_id" in ligne:
            ligne["listingMapId"] = ligne["listing_map_id"]
        if "row_hash" in ligne:
            ligne["ROW_HASH"] = ligne["row_hash"]
    return lignes


def assiette_negative_ramenee_zero() -> list[dict[str, Any]]:
    """Réservations à assiette brute négative (une ligne = une réservation), dataset Lot10 actif.

    Même source que le constat Lot11 `ASSIETTE_NEGATIVE_RAMENEE_ZERO`
    (`controles_lot11_service._groupe4_commissions`) : `assiette_commission < 0` dans
    `lot10_commissions`. Jamais recalculé ici — uniquement relu.
    """
    from app.readers import proprietaires_reglements_reader as _lot10

    lignes = _lot10.commissions().lignes
    return [l for l in lignes if (l.get("assiette_commission") or 0) < 0]


def ecarts_menages(code: str, db_path=None) -> list[dict[str, Any]]:
    """Écarts ménages d'un code donné (une ligne = un logement × mois), CALCULÉS en SQLite.

    Lisait auparavant l'onglet `VUE_ECART_HOSTAWAY` du classeur Lot6c. Or Lot11 calculait déjà
    exactement la même comparaison en SQLite : l'écran affichait donc le dernier calcul legacy
    pendant que le contrôle, lui, était à jour. Les deux passent maintenant par
    `menages_ecarts_service`, seul endroit où la règle existe.
    """
    from app.services import menages_ecarts_service

    return menages_ecarts_service.par_code(code, db_path=db_path)


def banque_non_classees(mois: str = "") -> list[dict[str, Any]]:
    """Mouvements à classer (statut_classification=RAPPROCHEMENT_REQUIS), option. par mois.

    Seul contrôle de ce module dont la source n'est plus un classeur : la Banque est en base. Les
    colonnes rendues sont inchangées, et le filtrage reste fait ici — pas en SQL — pour que ce reader
    continue de se lire comme les autres.
    """
    from app.services import banque_vues_service as vues

    lignes = vues.mouvements_normalises()
    out = []
    for r in lignes:
        if _txt(r.get("statut_classification")) != "RAPPROCHEMENT_REQUIS":
            continue
        if mois and _txt(r.get("date_operation"))[:7] != mois:
            continue
        out.append(r)
    return out

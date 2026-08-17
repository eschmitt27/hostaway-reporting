"""Reader DÉTAIL des contrôles agrégés (APP-5B) — LECTURE SEULE.

Le Lot11 agrège chaque anomalie (« 59 réservations sans commission », « 4 logements écart »…) en une
seule ligne MASTER. Ce reader relit les SOURCES détaillées réelles pour rouvrir chaque agrégat en
éléments actionnables, sans jamais recréer ni reclasser une anomalie : le grain vient de la source.

Sources détail :
  - VRBO sans montant            → SQLite, dataset RESOLUES (source=HOSTAWAY_VRBO_A_CONTROLER)
  - Réservations sans commission → MASTER_CALC_Commissions (onglet A_CONTROLER)
  - Écarts ménages externes      → MASTER_FACT_MEN_MenagesExternes (onglet VUE_ECART_HOSTAWAY)
  - Banque non classée           → SQLite (classification courante, statut RAPPROCHEMENT_REQUIS)

openpyxl read_only=True. Aucun chemin absolu exposé. Aucune donnée voyageur superflue.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg


_CACHE: dict[tuple, list[dict[str, Any]]] = {}


def vider_cache() -> None:
    _CACHE.clear()


def _lire(path: Path, sheet: str) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        st = p.stat()
        cle = (str(p), sheet, st.st_mtime_ns, st.st_size)
        if cle in _CACHE:
            return _CACHE[cle]
        wb = openpyxl.load_workbook(str(p), read_only=True, data_only=True)
        try:
            if sheet not in wb.sheetnames:
                _CACHE[cle] = []
                return []
            ws = wb[sheet]
            rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
        finally:
            wb.close()
        if len(rows) <= 1:
            _CACHE[cle] = []
            return []
        hdr = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
        data = [dict(zip(hdr, r)) for r in rows[1:]]
        _CACHE[cle] = data
        return data
    except Exception:
        return []


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
    """Réservations A_CONTROLER exclues du calcul de commission (une ligne = une réservation)."""
    return _lire(cfg.MASTER_COMMISSIONS, "A_CONTROLER")


def ecarts_menages(code: str) -> list[dict[str, Any]]:
    """Lignes VUE_ECART_HOSTAWAY d'un code donné (une ligne = un logement × mois)."""
    lignes = _lire(cfg.MASTER_MENAGES_EXTERNES, "VUE_ECART_HOSTAWAY")
    return [r for r in lignes if _txt(r.get("code_controle")) == code]


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

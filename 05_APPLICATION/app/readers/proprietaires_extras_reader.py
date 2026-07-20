"""Reader Acomptes / AirCover / Imputations Airbnb / Ajustements post-clôture (APP-3D) — LECTURE
SEULE.

Sources brutes non encore lues par aucun autre module applicatif (jusqu'ici uniquement utilisées
comme simples contrôles de présence de fichier ailleurs dans l'app). Ne recalcule rien, n'écrit
jamais. openpyxl read_only=True. Aucun chemin absolu exposé, aucun IBAN.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

ONGLET_ACOMPTES = "SAISIE"
ONGLET_AIRCOVER = "MASTER"
ONGLET_IMPUTATIONS = "MASTER"
ONGLET_AJUSTEMENTS = "MASTER"

SOURCE_ACOMPTES = "SAISIE_AcomptesProprietaires.xlsx"
SOURCE_AIRCOVER = "SAISIE_AirCover.xlsx"
SOURCE_IMPUTATIONS = "SAISIE_ImputationsAirbnb.xlsx"
SOURCE_AJUSTEMENTS = "SAISIE_Ajustements_PostCloture.xlsx"

ETAT_OK = "OK"
ETAT_FICHIER_ABSENT = "FICHIER_ABSENT"
ETAT_ONGLET_ABSENT = "ONGLET_ABSENT"
ETAT_VIDE = "VIDE"
ETAT_ILLISIBLE = "ILLISIBLE"

_ETAT_LIBELLE = {
    ETAT_OK: "Alimentée", ETAT_FICHIER_ABSENT: "Fichier absent", ETAT_ONGLET_ABSENT: "Onglet absent",
    ETAT_VIDE: "Source vide", ETAT_ILLISIBLE: "Source illisible",
}


@dataclass(frozen=True)
class EtatSource:
    cle: str
    libelle: str
    fichier: str
    onglet: str
    etat: str
    nb_lignes: int = 0
    derniere_maj: str | None = None

    @property
    def disponible(self) -> bool:
        return self.etat == ETAT_OK

    @property
    def etat_libelle(self) -> str:
        return _ETAT_LIBELLE.get(self.etat, self.etat)


@dataclass(frozen=True)
class Source:
    etat: EtatSource
    lignes: list[dict[str, Any]] = field(default_factory=list)


_CACHE: dict[tuple, tuple[str, list[dict[str, Any]], str | None]] = {}


def vider_cache() -> None:
    _CACHE.clear()


def _lire(path: Path, sheet: str) -> tuple[str, list[dict[str, Any]], str | None]:
    p = Path(path)
    if not p.exists():
        return ETAT_FICHIER_ABSENT, [], None
    try:
        st = p.stat()
        cle = (str(p), sheet, st.st_mtime_ns, st.st_size)
        if cle in _CACHE:
            return _CACHE[cle]
        maj = _dt.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M")
        wb = openpyxl.load_workbook(str(p), read_only=True, data_only=True)
        try:
            if sheet not in wb.sheetnames:
                res = (ETAT_ONGLET_ABSENT, [], maj); _CACHE[cle] = res; return res
            ws = wb[sheet]
            rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]
        finally:
            wb.close()
        if len(rows) <= 1:
            res = (ETAT_VIDE, [], maj)
        else:
            hdr = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
            res = (ETAT_OK, [dict(zip(hdr, r)) for r in rows[1:]], maj)
        _CACHE[cle] = res
        return res
    except Exception:
        return ETAT_ILLISIBLE, [], None


def _src(path: Path, cle: str, libelle: str, fichier: str, sheet: str) -> Source:
    etat, lignes, maj = _lire(path, sheet)
    return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=fichier, onglet=sheet,
                                  etat=etat, nb_lignes=len(lignes), derniere_maj=maj), lignes=lignes)


def to_texte(v: Any) -> str:
    return "" if v is None else str(v).strip()


def to_nombre(v: Any) -> float | int | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(str(v).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def to_mois(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.strftime("%Y-%m")
    return str(v)[:7]


def to_date(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.strftime("%Y-%m-%d")
    return str(v)[:10]


# ── Sources ──────────────────────────────────────────────────────────────────

def acomptes() -> Source:
    return _src(cfg.SAISIE_ACOMPTES_PROPRIETAIRES, "acomptes", "Acomptes propriétaires",
               SOURCE_ACOMPTES, ONGLET_ACOMPTES)


def aircover() -> Source:
    return _src(cfg.SAISIE_AIRCOVER, "aircover", "AirCover", SOURCE_AIRCOVER, ONGLET_AIRCOVER)


def imputations_airbnb() -> Source:
    return _src(cfg.SAISIE_IMPUTATIONS_AIRBNB, "imputations", "Imputations Airbnb",
               SOURCE_IMPUTATIONS, ONGLET_IMPUTATIONS)


def ajustements_post_cloture() -> Source:
    return _src(cfg.SAISIE_AJUSTEMENTS_POST_CLOTURE, "ajustements", "Ajustements post-clôture",
               SOURCE_AJUSTEMENTS, ONGLET_AJUSTEMENTS)


def acomptes_prop_mois(proprietaire_id: str, mois: str) -> list[dict[str, Any]]:
    src = acomptes()
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois")) == mois]


def aircover_prop_mois(proprietaire_id: str, mois: str) -> list[dict[str, Any]]:
    src = aircover()
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois")) == mois]


def imputations_prop_mois(proprietaire_id: str, mois: str) -> list[dict[str, Any]]:
    src = imputations_airbnb()
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois")) == mois]


def ajustements_prop_mois(proprietaire_id: str, mois: str) -> list[dict[str, Any]]:
    """Filtre sur `mois_effet` — un ajustement post-clôture s'applique au mois où il produit son
    effet, pas nécessairement au mois d'origine de l'anomalie qu'il corrige."""
    src = ajustements_post_cloture()
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois_effet")) == mois]


def etats_sources() -> list[EtatSource]:
    return [acomptes().etat, aircover().etat, imputations_airbnb().etat, ajustements_post_cloture().etat]

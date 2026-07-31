"""Reader Propriétaires & règlements (APP-3C) — LECTURE SEULE.

Lit les sorties moteur du pilotage propriétaire :
  - MASTER_CALC_NetProprietaire.xlsx : VUE_MOIS (mois × propriétaire), REGLEMENT (mois × logement × prop)
  - MASTER_CALC_Commissions.xlsx     : COMMISSIONS (par réservation), A_CONTROLER
  - MASTER_CALC_Resultats.xlsx       : PAR_MOIS_PROPRIETAIRE
  - MASTER_FACT_Proprietaires.xlsx   : FACT_FACTURE_ENTETE, DASHBOARD_FACTURATION, A_CONTROLER

Ne recalcule aucune commission, aucun net : ces valeurs viennent du moteur. Aucune écriture.
N'expose jamais l'adresse du propriétaire ni de chemin absolu. openpyxl read_only=True.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl

import app.config as cfg

# ── Onglets ──────────────────────────────────────────────────────────────────
ONGLET_VUE_MOIS = "VUE_MOIS"
ONGLET_REGLEMENT = "REGLEMENT"
ONGLET_COMMISSIONS = "COMMISSIONS"
ONGLET_RESULTATS = "PAR_MOIS_PROPRIETAIRE"
ONGLET_RESULTATS_LOGEMENT = "PAR_MOIS_LOGEMENT"
ONGLET_RESULTATS_GLOBAL = "GLOBAL"
ONGLET_FACT_ENTETE = "FACT_FACTURE_ENTETE"
ONGLET_DASHBOARD = "DASHBOARD_FACTURATION"
ONGLET_A_CONTROLER = "A_CONTROLER"

# ── Libellés de source (noms de fichier uniquement) ──────────────────────────
SOURCE_NET = "MASTER_CALC_NetProprietaire.xlsx"
SOURCE_COMMISSIONS = "MASTER_CALC_Commissions.xlsx"
SOURCE_RESULTATS = "MASTER_CALC_Resultats.xlsx"
SOURCE_FACT = "MASTER_FACT_Proprietaires.xlsx"

# ── États ────────────────────────────────────────────────────────────────────
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
    global _CACHE_NOMS_LOGEMENTS
    _CACHE.clear()
    _CACHE_NOMS_LOGEMENTS = None


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


# ── Convertisseurs ───────────────────────────────────────────────────────────

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


# ── Sources ──────────────────────────────────────────────────────────────────

def net_vue_mois() -> Source:
    return _src(cfg.MASTER_NET_PROPRIETAIRE, "net_vue_mois", "Net propriétaire (mois)", SOURCE_NET, ONGLET_VUE_MOIS)


def net_reglement() -> Source:
    return _src(cfg.MASTER_NET_PROPRIETAIRE, "net_reglement", "Règlements (mois × logement)", SOURCE_NET, ONGLET_REGLEMENT)


def commissions() -> Source:
    return _src(cfg.MASTER_COMMISSIONS, "commissions", "Commissions (par réservation)", SOURCE_COMMISSIONS, ONGLET_COMMISSIONS)


def resultats() -> Source:
    return _src(cfg.MASTER_RESULTATS, "resultats", "Résultats par mois/propriétaire", SOURCE_RESULTATS, ONGLET_RESULTATS)


def resultats_par_logement() -> Source:
    """PAR_MOIS_LOGEMENT — mois × logement × vision (REEL/COMPTABLE/HORS_COMPTA empilées),
    déjà calculé par Lot10 (`build_resultats`). Jamais recalculé ici."""
    return _src(cfg.MASTER_RESULTATS, "resultats_logement", "Résultats par mois/logement/vision",
               SOURCE_RESULTATS, ONGLET_RESULTATS_LOGEMENT)


def resultats_global() -> Source:
    """GLOBAL — un total par vision (REEL/COMPTABLE/HORS_COMPTA), avec la vérification de
    cohérence REEL=COMPTABLE+HC déjà faite par Lot10 (`commentaire_hc`)."""
    return _src(cfg.MASTER_RESULTATS, "resultats_global", "Résultats globaux par vision",
               SOURCE_RESULTATS, ONGLET_RESULTATS_GLOBAL)


def factures_entetes() -> Source:
    return _src(cfg.MASTER_FACT_PROPRIETAIRES, "factures", "Factures propriétaires (entêtes)", SOURCE_FACT, ONGLET_FACT_ENTETE)


def dashboard_facturation() -> Source:
    return _src(cfg.MASTER_FACT_PROPRIETAIRES, "dashboard", "Tableau de bord facturation", SOURCE_FACT, ONGLET_DASHBOARD)


def controles_factures() -> Source:
    return _src(cfg.MASTER_FACT_PROPRIETAIRES, "controles", "Contrôles facturation", SOURCE_FACT, ONGLET_A_CONTROLER)


def ref_logements() -> Source:
    """REF_Setup onglet REF_Logements — noms officiels des logements (affichage)."""
    return _src(cfg.REF_SETUP, "ref_logements", "Référentiel logements", "REF_Setup.xlsm", "REF_Logements")


_CACHE_NOMS_LOGEMENTS: dict[str, str] | None = None


def noms_logements() -> dict[str, str]:
    """{logement_id: nom_logement_officiel} depuis REF_Setup. Mémorisé ; vidé par vider_cache."""
    global _CACHE_NOMS_LOGEMENTS
    if _CACHE_NOMS_LOGEMENTS is not None:
        return _CACHE_NOMS_LOGEMENTS
    idx: dict[str, str] = {}
    try:
        for r in ref_logements().lignes:
            lid = to_texte(r.get("logement_id"))
            if not lid or lid == "logement_id":
                continue
            nom = to_texte(r.get("nom_logement_officiel")) or to_texte(r.get("nom_court"))
            idx[lid] = nom or lid
    except Exception:
        idx = {}
    _CACHE_NOMS_LOGEMENTS = idx
    return idx


def libelle_logement(logement_id: str) -> str:
    """« Nom officiel » si connu, sinon repli « Logement non identifié — <id> »."""
    lid = to_texte(logement_id)
    if not lid:
        return ""
    nom = noms_logements().get(lid)
    return nom if nom else f"Logement non identifié — {lid}"


def etats_sources() -> list[EtatSource]:
    return [net_vue_mois().etat, net_reglement().etat, commissions().etat,
            resultats().etat, factures_entetes().etat, dashboard_facturation().etat]

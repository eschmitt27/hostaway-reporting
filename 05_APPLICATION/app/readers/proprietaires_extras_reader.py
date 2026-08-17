"""Reader Acomptes / AirCover / Imputations Airbnb / Ajustements post-clôture (APP-3D) — LECTURE
SEULE.

Ne recalcule rien, n'écrit jamais. Aucun chemin absolu exposé, aucun IBAN.

Les ACOMPTES viennent de la base : ce sont les mouvements de trésorerie propriétaires validés, saisis
dans l'application. Les trois autres sources restent des classeurs, et seront migrées séparément —
mélanger leur migration à celle des acomptes rendrait une régression difficile à situer.
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

# Libellé d'origine de la source acomptes. Elle vient désormais de la base ; le nom est conservé le
# temps de la parité, puis à retirer.
SOURCE_ACOMPTES = "Mouvements de trésorerie propriétaires (base)"

# Nature d'un mouvement de trésorerie qui EST un acompte propriétaire.
NATURE_ACOMPTE = "ACOMPTE_PROPRIETAIRE"
SOURCE_AIRCOVER = "SAISIE_AirCover.xlsx"
SOURCE_IMPUTATIONS = "SAISIE_ImputationsAirbnb.xlsx"
SOURCE_AJUSTEMENTS = "SAISIE_Ajustements_PostCloture.xlsx"

ETAT_OK = "OK"
ETAT_FICHIER_ABSENT = "FICHIER_ABSENT"
ETAT_ONGLET_ABSENT = "ONGLET_ABSENT"
ETAT_VIDE = "VIDE"
ETAT_ILLISIBLE = "ILLISIBLE"
# Propre aux sources en base : la table n'existe pas encore. Distinct de VIDE, qui veut dire « la
# table existe et ne contient rien » — le premier appelle une migration, le second une saisie.
ETAT_NON_INITIALISE = "NON_INITIALISE"

_ETAT_LIBELLE = {
    ETAT_OK: "Alimentée", ETAT_FICHIER_ABSENT: "Fichier absent", ETAT_ONGLET_ABSENT: "Onglet absent",
    ETAT_VIDE: "Source vide", ETAT_ILLISIBLE: "Source illisible",
    ETAT_NON_INITIALISE: "Non initialisée en base",
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
    """Acomptes propriétaires — lus en BASE, plus dans `SAISIE_AcomptesProprietaires.xlsx`.

    Un acompte propriétaire est un mouvement de trésorerie de nature `ACOMPTE_PROPRIETAIRE` : il n'y
    a pas deux objets, il y en a un seul, saisi dans l'application. Tenir une seconde liste dans un
    classeur ferait exister deux vérités qui divergeraient dès la première saisie faite d'un côté
    seulement.

    Seuls les mouvements VALIDÉS sont rendus : un brouillon n'est pas encore un acompte reçu, et le
    compter fausserait le solde d'un propriétaire.

    Les noms de colonnes du Lot 5 sont conservés — plusieurs écrans et contrôles les lisent, et les
    renommer en même temps qu'on change de source rendrait indémêlable ce qui casse quoi.
    """
    from app.services import proprietaires_tresorerie_service as tresorerie

    def _etat(code: str, nb: int = 0, maj=None) -> Source:
        return Source(etat=EtatSource(cle="acomptes", libelle="Acomptes propriétaires",
                                      fichier=SOURCE_ACOMPTES, onglet="—", etat=code,
                                      nb_lignes=nb, derniere_maj=maj))

    try:
        if not tresorerie.table_presente():
            return _etat(ETAT_NON_INITIALISE)
        mouvements = [m for m in tresorerie.lister(statut=tresorerie.ST_VALIDE)
                      if to_texte(m.get("nature")) == NATURE_ACOMPTE]
    except Exception:
        return _etat(ETAT_ILLISIBLE)

    lignes = [{
        "acompte_id": m.get("mouvement_opaque"),
        "mois": to_mois(m.get("date_mouvement")),
        "proprietaire_id": m.get("proprietaire_id"),
        "logement_id": m.get("logement_id"),
        "facture_ref": m.get("reference_metier"),
        "source_acompte": m.get("source_type"),
        "source_hh_id": m.get("source_id"),
        "montant_acompte": m.get("montant"),
        "mode_paiement_id": m.get("mode_reglement"),
        "statut_controle": m.get("statut"),
        "commentaire": m.get("justification"),
        "date_mouvement": m.get("date_mouvement"),
        "sens": m.get("sens"),
    } for m in mouvements]

    return Source(
        etat=EtatSource(cle="acomptes", libelle="Acomptes propriétaires", fichier=SOURCE_ACOMPTES,
                        onglet="—", etat=ETAT_OK if lignes else ETAT_VIDE, nb_lignes=len(lignes),
                        derniere_maj=max((to_texte(m.get("cree_le")) for m in mouvements),
                                         default=None)),
        lignes=lignes,
    )


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

"""Reader Acomptes / AirCover / Imputations Airbnb / Ajustements post-clôture (APP-3D) — LECTURE
SEULE.

Ne recalcule rien, n'écrit jamais. Aucun chemin absolu exposé, aucun IBAN.

Les quatre sources viennent désormais de la base : les Acomptes via les mouvements de trésorerie
propriétaires (déjà migré), AirCover/Imputations Airbnb/Ajustements post-clôture via les tables
dédiées de la migration 0054. `SAISIE_AirCover.xlsx` / `SAISIE_ImputationsAirbnb.xlsx` /
`SAISIE_Ajustements_PostCloture.xlsx` ne sont plus lus au runtime : les trois ne contenaient de
toute façon aucune ligne de donnée réelle (header seul, colonnes reprises verbatim dans les tables
SQLite), donc aucune reprise historique n'était nécessaire.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.db.connection import get_db

ONGLET_ACOMPTES = "SAISIE"
ONGLET_AIRCOVER = "MASTER"
ONGLET_IMPUTATIONS = "MASTER"
ONGLET_AJUSTEMENTS = "MASTER"

# Libellés d'origine des sources acomptes/AirCover/imputations/ajustements. Elles viennent désormais
# toutes de la base ; les noms sont conservés à l'affichage (diagnostics, écrans) le temps de la
# parité visuelle avec l'historique.
SOURCE_ACOMPTES = "Mouvements de trésorerie propriétaires (base)"

# Nature d'un mouvement de trésorerie qui EST un acompte propriétaire.
NATURE_ACOMPTE = "ACOMPTE_PROPRIETAIRE"
SOURCE_AIRCOVER = "aircover (base)"
SOURCE_IMPUTATIONS = "imputations_airbnb (base)"
SOURCE_AJUSTEMENTS = "ajustements_post_cloture (base)"

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


def _table_presente(nom_table: str, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nom_table,)
        ).fetchone() is not None
    finally:
        conn.close()


def _src_sqlite(nom_table: str, colonnes: tuple[str, ...], cle: str, libelle: str, fichier: str,
                 db_path=None) -> Source:
    """Lit une table extras (aircover / imputations_airbnb / ajustements_post_cloture) en base.

    `ETAT_NON_INITIALISE` si la table n'existe pas encore (migration non jouée) — distinct de
    `ETAT_VIDE` qui signifie que la table existe mais ne contient aucune ligne saisie.
    """
    if not _table_presente(nom_table, db_path):
        return Source(etat=EtatSource(cle=cle, libelle=libelle, fichier=fichier, onglet="—",
                                      etat=ETAT_NON_INITIALISE))
    conn = get_db(db_path)
    try:
        rows = conn.execute(f"SELECT {', '.join(colonnes)} FROM {nom_table}").fetchall()
    finally:
        conn.close()
    lignes = [dict(zip(colonnes, r)) for r in rows]
    return Source(
        etat=EtatSource(cle=cle, libelle=libelle, fichier=fichier, onglet="—",
                        etat=ETAT_OK if lignes else ETAT_VIDE, nb_lignes=len(lignes)),
        lignes=lignes,
    )


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


_COLONNES_AIRCOVER = ("aircover_id", "date_aircover", "montant", "beneficiaire_reel",
                     "proprietaire_id", "logement_id", "reservation_id", "mois", "justificatif",
                     "traitement", "statut_controle", "commentaire")
_COLONNES_IMPUTATIONS = ("imputation_airbnb_id", "transaction_banque_id", "reference_airbnb",
                        "proprietaire_id", "logement_id", "mois", "document_id", "montant_impute",
                        "date_imputation", "justificatif", "statut", "commentaire")
_COLONNES_AJUSTEMENTS = ("ajustement_id", "mois_origine", "mois_effet", "source_module",
                        "source_pk", "logement_id", "proprietaire_id", "type_ajustement", "montant",
                        "sens", "impact_reel", "impact_comptable", "motif", "justificatif", "auteur",
                        "date_saisie", "statut_validation")


def _renomme_date(lignes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`date_aircover` (nom de colonne SQL) redevient `date` (nom de champ métier/contrat)."""
    for r in lignes:
        r["date"] = r.pop("date_aircover")
    return lignes


def aircover(db_path=None) -> Source:
    src = _src_sqlite("aircover", _COLONNES_AIRCOVER, "aircover", "AirCover", SOURCE_AIRCOVER, db_path)
    return Source(etat=src.etat, lignes=_renomme_date(src.lignes))


def imputations_airbnb(db_path=None) -> Source:
    return _src_sqlite("imputations_airbnb", _COLONNES_IMPUTATIONS, "imputations",
                       "Imputations Airbnb", SOURCE_IMPUTATIONS, db_path)


def ajustements_post_cloture(db_path=None) -> Source:
    return _src_sqlite("ajustements_post_cloture", _COLONNES_AJUSTEMENTS, "ajustements",
                       "Ajustements post-clôture", SOURCE_AJUSTEMENTS, db_path)


def acomptes_prop_mois(proprietaire_id: str, mois: str) -> list[dict[str, Any]]:
    src = acomptes()
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois")) == mois]


def aircover_prop_mois(proprietaire_id: str, mois: str, db_path=None) -> list[dict[str, Any]]:
    src = aircover(db_path)
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois")) == mois]


def imputations_prop_mois(proprietaire_id: str, mois: str, db_path=None) -> list[dict[str, Any]]:
    src = imputations_airbnb(db_path)
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois")) == mois]


def ajustements_prop_mois(proprietaire_id: str, mois: str, db_path=None) -> list[dict[str, Any]]:
    """Filtre sur `mois_effet` — un ajustement post-clôture s'applique au mois où il produit son
    effet, pas nécessairement au mois d'origine de l'anomalie qu'il corrige."""
    src = ajustements_post_cloture(db_path)
    return [r for r in src.lignes if to_texte(r.get("proprietaire_id")) == proprietaire_id
           and to_mois(r.get("mois_effet")) == mois]


def etats_sources(db_path=None) -> list[EtatSource]:
    return [acomptes().etat, aircover(db_path).etat, imputations_airbnb(db_path).etat,
            ajustements_post_cloture(db_path).etat]

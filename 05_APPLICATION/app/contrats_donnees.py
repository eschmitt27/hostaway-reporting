"""Contrats de données typés — frontières de saisie (mission durcissement Phase 1).

Dataclasses, pas Pydantic : le projet n'a aucune dépendance Pydantic (vérifié — 0 import dans tout
`app/`) et utilise déjà `@dataclass` dans 12 modules (readers, résultats de confirmation). Ajouter
Pydantic ici mélangerait deux conventions pour un même besoin.

CE QUE CES CONTRATS FONT
Vérifient la STRUCTURE : champs obligatoires présents, types corrects, montant numérique fini,
date au format AAAA-MM-JJ. Rejettent avec un message clair (`ContratInvalideError`).

CE QU'ILS NE FONT PAS
Ils ne dupliquent aucune règle métier. La validation économique complète (statuts autorisés selon
le contexte, cohérence référentielle, transitions) reste dans `valider()`/`previsualiser()` de
chaque service — ces fonctions restent la seule source de vérité métier. Un contrat structurel
peut accepter une donnée que le service refusera ensuite pour une raison métier ; l'inverse
(contrat qui rejette une donnée métier valide) serait un bug de ce module.

USAGE
Appelés en complément de `valider()`, jamais à sa place — after `valider()["ok"]` est vrai, pour
garantir qu'aucune coercition de type imprévue ne traverse la frontière avant écriture SQLite.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

_RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RE_MOIS = re.compile(r"^\d{4}-\d{2}$")


class ContratInvalideError(ValueError):
    """La structure d'une donnée entrante ne respecte pas son contrat de type."""


def _texte_obligatoire(d: dict[str, Any], champ: str) -> str:
    v = d.get(champ)
    if v is None or not str(v).strip():
        raise ContratInvalideError(f"{champ} obligatoire, absent ou vide.")
    return str(v).strip()


def _montant(d: dict[str, Any], champ: str) -> float:
    v = d.get(champ)
    if v is None or isinstance(v, bool):
        raise ContratInvalideError(f"{champ} obligatoire (montant numérique).")
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise ContratInvalideError(f"{champ} n'est pas un nombre : {v!r}.")
    if f != f or f in (float("inf"), float("-inf")):  # NaN / infini
        raise ContratInvalideError(f"{champ} n'est pas une valeur numérique finie : {v!r}.")
    return f


def _montant_optionnel(d: dict[str, Any], champ: str) -> float | None:
    """Comme `_montant`, mais absent/vide = None accepté — certains montants ne sont connus
    qu'après une saisie ultérieure (cf. `ReservationHH`, réservation placeholder sans montant
    retenu tant que la saisie HH n'est pas complétée)."""
    v = d.get(champ)
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    return _montant(d, champ)


def _date_iso(d: dict[str, Any], champ: str) -> str:
    v = d.get(champ)
    s = str(v).strip() if v is not None else ""
    if not _RE_DATE.match(s):
        raise ContratInvalideError(f"{champ} doit être au format AAAA-MM-JJ, reçu {v!r}.")
    try:
        date.fromisoformat(s)
    except ValueError:
        raise ContratInvalideError(f"{champ} n'est pas une date calendaire valide : {v!r}.")
    return s


def _mois_iso(d: dict[str, Any], champ: str) -> str:
    v = d.get(champ)
    s = str(v).strip() if v is not None else ""
    if not _RE_MOIS.match(s):
        raise ContratInvalideError(f"{champ} doit être au format AAAA-MM, reçu {v!r}.")
    return s


# ── Objets prioritaires (mission §12) ────────────────────────────────────────

@dataclass(frozen=True)
class Charge:
    """Structure d'une charge saisie — miroir de `charges_saisie_service.CHAMPS_SAISIE`."""

    date_charge: str
    montant: float
    categorie_charge_id: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Charge":
        return cls(
            date_charge=_date_iso(d, "date_charge"),
            montant=_montant(d, "montant"),
            categorie_charge_id=_texte_obligatoire(d, "categorie_charge_id"),
        )


@dataclass(frozen=True)
class ReservationHH:
    """Structure d'une réservation hors Hostaway saisie.

    `montant_retenu` optionnel (audité Mission 11) : `reservations_hh_saisie_service.valider()`
    ne l'exige jamais (`OBLIGATOIRES` ne le liste pas) — une réservation peut être saisie en
    placeholder (montant connu plus tard, ex. `DIRECT_SANS_SAISIE_HH`) avant d'être complétée.
    Un contrat qui l'exigerait rejetterait cet état réel valide (§26/§37 de la mission :
    assouplir le contrat, jamais forcer les données)."""

    mois: str
    logement_id: str
    date_arrivee: str
    date_depart: str
    montant_retenu: float | None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReservationHH":
        arrivee = _date_iso(d, "date_arrivee")
        depart = _date_iso(d, "date_depart")
        if depart < arrivee:
            raise ContratInvalideError(
                f"date_depart ({depart}) antérieure à date_arrivee ({arrivee}).")
        return cls(
            mois=_mois_iso(d, "mois"),
            logement_id=_texte_obligatoire(d, "logement_id"),
            date_arrivee=arrivee,
            date_depart=depart,
            montant_retenu=_montant_optionnel(d, "montant_retenu"),
        )


@dataclass(frozen=True)
class MouvementBanque:
    """Structure d'un mouvement bancaire brut — miroir de `banque_mouvements` (migration 0032)."""

    bank_account_id: str
    date_operation: str
    sens: str
    montant: float
    libelle_brut: str

    _SENS_VALIDES = ("DEBIT", "CREDIT")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MouvementBanque":
        sens = _texte_obligatoire(d, "sens")
        if sens not in cls._SENS_VALIDES:
            raise ContratInvalideError(
                f"sens doit être DEBIT ou CREDIT, reçu {sens!r}.")
        return cls(
            bank_account_id=_texte_obligatoire(d, "bank_account_id"),
            date_operation=_date_iso(d, "date_operation"),
            sens=sens,
            montant=_montant(d, "montant"),
            libelle_brut=_texte_obligatoire(d, "libelle_brut"),
        )


@dataclass(frozen=True)
class MouvementTresorerieProprietaire:
    """Structure d'un mouvement de trésorerie propriétaire (acompte, reversement…)."""

    proprietaire_id: str
    sens: str
    nature: str
    montant: float
    date_mouvement: str

    _SENS_VALIDES = ("PROPRIETAIRE_VERS_SOCIETE", "SOCIETE_VERS_PROPRIETAIRE")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MouvementTresorerieProprietaire":
        sens = _texte_obligatoire(d, "sens")
        if sens not in cls._SENS_VALIDES:
            raise ContratInvalideError(
                f"sens doit être {cls._SENS_VALIDES[0]} ou {cls._SENS_VALIDES[1]}, reçu {sens!r}.")
        return cls(
            proprietaire_id=_texte_obligatoire(d, "proprietaire_id"),
            sens=sens,
            nature=_texte_obligatoire(d, "nature"),
            montant=_montant(d, "montant"),
            date_mouvement=_date_iso(d, "date_mouvement"),
        )

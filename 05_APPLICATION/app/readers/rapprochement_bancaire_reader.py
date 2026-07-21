"""Contrat de lecture APP-3F sur les mouvements bancaires — LECTURE SEULE, masquée.

Délègue la lecture du fichier à `banques_reader` (jamais dupliqué, jamais écrit) et n'expose au
reste d'APP-3F qu'un contrat minimal masqué : identifiant opaque `MVT-<hash>`, date, montant, sens,
libellé masqué, référence normalisée, source logique, empreinte, état de disponibilité.
JAMAIS d'IBAN/RIB/BIC/numéro de compte, jamais le libellé brut, jamais le nom/chemin du fichier,
jamais un id SQLite.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

import app.config as cfg
from app.readers import banques_reader as banques

# États repris tels quels du reader bancaire (source de vérité de l'état).
ETAT_OK = banques.ETAT_OK
ETAT_FICHIER_ABSENT = banques.ETAT_FICHIER_ABSENT
ETAT_ONGLET_ABSENT = banques.ETAT_ONGLET_ABSENT
ETAT_VIDE = banques.ETAT_VIDE
ETAT_ILLISIBLE = banques.ETAT_ILLISIBLE
ETAT_NON_ALIMENTE = banques.ETAT_NON_ALIMENTE

_RE_REF = re.compile(r"[^A-Za-z0-9]+")


@dataclass(frozen=True)
class MouvementContrat:
    mouvement_opaque: str            # MVT-xxxx (jamais le mouvement_id brut)
    date: str                        # ISO AAAA-MM-JJ
    mois: str                        # AAAA-MM
    montant: float | None
    sens: str                        # DEBIT|CREDIT (normalisé majuscule)
    libelle_masque: str              # libellé nettoyé, tronqué ; jamais le brut complet
    reference_normalisee: str        # alphanumérique majuscule, jamais une donnée bancaire
    source_logique: str = "MOUVEMENTS_BANCAIRES"
    empreinte: str = ""              # hash stable du mouvement (détection disparition/modif)


@dataclass(frozen=True)
class SourceContrat:
    etat: str
    disponible: bool
    mouvements: list[MouvementContrat] = field(default_factory=list)


def id_opaque_mouvement(mouvement_id: Any) -> str:
    base = (cfg.BANQUE_OPAQUE_SALT + "|MVT|" + banques.to_texte(mouvement_id)).encode("utf-8")
    return "MVT-" + hashlib.sha256(base).hexdigest()[:10]


def _empreinte(mouvement_id: str, date: str, montant, sens: str, libelle: str) -> str:
    payload = f"{mouvement_id}|{date}|{montant}|{sens}|{libelle}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _libelle_masque(libelle: str) -> str:
    """Libellé nettoyé et tronqué — jamais le libellé brut complet (peut contenir des données tierces)."""
    s = banques.to_texte(libelle)
    s = re.sub(r"\s+", " ", s).strip()
    return (s[:40] + "…") if len(s) > 40 else s


def _reference_normalisee(row: dict[str, Any]) -> str:
    # Référence interne normalisée (alphanumérique) — jamais un IBAN/numéro de compte.
    brut = banques.to_texte(row.get("reference") or row.get("reference_operation") or "")
    return _RE_REF.sub("", brut).upper()[:24]


def _sens(row: dict[str, Any]) -> str:
    s = banques.to_texte(row.get("sens")).upper()
    if s in ("DEBIT", "CREDIT"):
        return s
    montant = banques.to_nombre(row.get("montant"))
    if montant is None:
        return ""
    return "DEBIT" if montant < 0 else "CREDIT"


def _contrat(row: dict[str, Any]) -> MouvementContrat | None:
    mid = banques.to_texte(row.get("mouvement_id"))
    if not mid:
        return None
    date = banques.to_date(row.get("date_operation"))
    montant = banques.to_nombre(row.get("montant"))
    sens = _sens(row)
    libelle = banques.to_texte(row.get("libelle"))   # déjà nettoyé côté service ; jamais libelle_brut
    return MouvementContrat(
        mouvement_opaque=id_opaque_mouvement(mid),
        date=date,
        mois=banques.to_mois(row.get("date_operation")),
        montant=montant,
        sens=sens,
        libelle_masque=_libelle_masque(libelle),
        reference_normalisee=_reference_normalisee(row),
        empreinte=_empreinte(mid, date, montant, sens, libelle),
    )


def source() -> SourceContrat:
    """Retourne l'état de la source bancaire + la liste masquée des mouvements (ou vide)."""
    src = banques.mouvements()
    etat = src.etat.etat
    if etat != ETAT_OK:
        return SourceContrat(etat=etat, disponible=False, mouvements=[])
    mvts = [m for m in (_contrat(r) for r in src.lignes) if m is not None]
    return SourceContrat(etat=etat, disponible=True, mouvements=mvts)


def charger_mouvement(mouvement_opaque: str) -> MouvementContrat | None:
    """Résout un MVT-opaque en mouvement masqué courant (None si absent/disparu)."""
    for m in source().mouvements:
        if m.mouvement_opaque == mouvement_opaque:
            return m
    return None

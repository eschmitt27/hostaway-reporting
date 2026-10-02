"""Périmètre de la comptabilité applicative V1 — UN seul paramètre, lu partout ici.

DÉCISION (cutover V1, 2026-10-02, cf. `00_CADRAGE/CUTOVER_V1_2026-09.md`) : la comptabilité
applicative V1 de Chouette Patrimoine démarre au 1er septembre 2026. Aucune facture propriétaire,
aucune écriture comptable, aucune clôture ne peut porter sur une période antérieure.

OÙ VIT LA DATE. Dans `parametres_societe_facturation`, clé `V1_ACCOUNTING_START_DATE` — la table
des paramètres société, administrée et historisée (migration 0111). Elle n'est PAS dans la liste des
champs de l'écran « Paramètres société » : on ne la modifie pas d'un clic, et la migration 0120 la
rend immuable. Elle est posée UNE fois, par la transaction de cutover
(`cutover_v1_service.executer`).

TANT QU'ELLE N'EST PAS POSÉE, rien n'est restreint (installation neuve, bases de test) : la règle
naît avec le cutover, elle ne le précède pas.

CETTE DATE N'EST PAS le début de l'historique Hostaway, ni de l'historique bancaire, ni la date de
création des logements, des propriétaires ou des référentiels. Les séjours anciens restent
consultables comme historique métier ; seule la COMPTABILITÉ V1 (factures émises, écritures,
clôtures) est bornée.

AUCUN CACHE. La date est relue en base à chaque appel : le résultat ne dépend jamais d'un état
mémoire, et survit au redémarrage par construction.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db

CLE_PARAMETRE = "V1_ACCOUNTING_START_DATE"
# Valeur DÉCIDÉE par l'utilisateur pour le cutover. Elle ne sert qu'à POSER le paramètre ; après le
# cutover, seule la base fait foi (`debut()`).
DATE_V1_DECIDEE = "2026-09-01"

E_FACTURATION_AVANT_V1 = "FACTURATION_AVANT_V1"
E_COMPTABILITE_AVANT_V1 = "COMPTABILITE_AVANT_V1"
E_CLOTURE_AVANT_V1 = "CLOTURE_AVANT_V1"

_MOIS_FR = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
            "octobre", "novembre", "décembre")
_RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RE_MOIS = re.compile(r"^\d{4}-\d{2}")


def _base_presente(db_path) -> bool:
    return Path(db_path or cfg.DB_PATH).exists()


def debut(*, db_path=None) -> str | None:
    """Date de début de la comptabilité V1 (`AAAA-MM-JJ`), ou None si le cutover n'est pas
    appliqué. Base ou table absente : None — jamais une erreur d'écran."""
    if not _base_presente(db_path):
        return None
    conn = get_db(db_path)
    try:
        r = conn.execute(
            "SELECT valeur FROM parametres_societe_facturation WHERE cle = ?",
            (CLE_PARAMETRE,)).fetchone()
    except Exception:          # noqa: BLE001 — base antérieure à 0111
        return None
    finally:
        conn.close()
    valeur = str(r[0] or "").strip() if r else ""
    return valeur if _RE_DATE.match(valeur) else None


def premier_mois(*, db_path=None) -> str | None:
    """`AAAA-MM` du premier mois V1, ou None si le cutover n'est pas appliqué."""
    d = debut(db_path=db_path)
    return d[:7] if d else None


def libelle_mois(mois: str) -> str:
    """« 2026-09 » → « septembre 2026 »."""
    try:
        return f"{_MOIS_FR[int(mois[5:7]) - 1]} {mois[:4]}"
    except (ValueError, IndexError):
        return mois


def _mois_de(valeur: Any) -> str:
    texte = str(valeur or "").strip()
    return texte[:7] if _RE_MOIS.match(texte) else ""


def est_anterieur(periode: Any, *, db_path=None) -> bool:
    """La période (mois `AAAA-MM` ou date `AAAA-MM-JJ`) précède-t-elle la comptabilité V1 ?

    Faux tant que le cutover n'est pas appliqué, et pour une valeur illisible (son refus relève de
    la validation de format de chaque parcours, pas de celle-ci)."""
    mois = _mois_de(periode)
    v1 = premier_mois(db_path=db_path)
    return bool(mois and v1 and mois < v1)


def message_facturation(*, db_path=None) -> str:
    v1 = premier_mois(db_path=db_path) or DATE_V1_DECIDEE[:7]
    return f"La facturation V1 débute en {libelle_mois(v1)}."


def message_comptabilite(*, db_path=None) -> str:
    v1 = premier_mois(db_path=db_path) or DATE_V1_DECIDEE[:7]
    return (f"La comptabilité V1 débute en {libelle_mois(v1)} : aucune écriture ne peut porter "
            "sur une période antérieure.")


def message_cloture(mois: str, *, db_path=None) -> str:
    v1 = premier_mois(db_path=db_path) or DATE_V1_DECIDEE[:7]
    return (f"La comptabilité V1 débute en {libelle_mois(v1)} : {libelle_mois(mois)} n'est pas "
            "une période clôturable.")


def contexte(*, db_path=None) -> dict[str, str] | None:
    """Ce que les écrans affichent (bornes des sélecteurs, mention) — None sans cutover."""
    d = debut(db_path=db_path)
    if not d:
        return None
    return {"debut": d, "mois": d[:7], "libelle": libelle_mois(d[:7]),
            "message_facturation": f"La facturation V1 débute en {libelle_mois(d[:7])}."}

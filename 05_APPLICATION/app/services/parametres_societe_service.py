"""Paramètres société & facturation — source canonique : SQLite (migration 0111).

Identité légale, contact et conditions de facturation ne sont pas des secrets : ils s'impriment
sur chaque facture. Ils sont donc administrables depuis l'écran « Administration › Paramètres
société & facturation », historisés, et lus ici par `facturation_config_service` — qui reste le
seul module que le reste de l'application interroge.

UNE SEULE SOURCE PAR DONNÉE. Un paramètre enregistré en base fait foi, définitivement. Tant qu'un
paramètre n'a JAMAIS été enregistré, l'ancienne source (variable d'environnement) est encore lue,
et l'écran le signale comme tel : c'est une transition, pas une seconde vérité. La reprise
(`reprendre_depuis_environnement`) copie ces valeurs en base une fois pour toutes.

RIEN N'EST INVENTÉ. Un champ vide reste vide (`NULL` = non configuré). Le délai de paiement
distingue `0` (paiement à réception) de `NULL` (non configuré). Les identifiants sont vérifiés dans
leur forme (9 chiffres pour un SIREN, 14 pour un SIRET qui commence par ce SIREN), jamais complétés.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import app.config as cfg
from app.db.connection import get_db

TABLE = "parametres_societe_facturation"
TABLE_HISTORIQUE = "parametres_societe_facturation_historique"

ORIGINE_SAISIE = "SAISIE"
ORIGINE_ENVIRONNEMENT = "REPRISE_ENVIRONNEMENT"
ORIGINE_DOCUMENT = "REPRISE_DOCUMENT"

SECTION_IDENTITE = "Identité"
SECTION_CONTACT = "Contact"
SECTION_FACTURATION = "Facturation"

REGIMES_TVA = ("FRANCHISE_TVA", "ASSUJETTI_TVA", "EXONERATION_AUTRE", "A_CONTROLER")
LIBELLES_REGIMES = {
    "FRANCHISE_TVA": "Franchise en base de TVA",
    "ASSUJETTI_TVA": "Assujetti à la TVA",
    "EXONERATION_AUTRE": "Autre exonération",
    "A_CONTROLER": "Non confirmé",
}


@dataclass(frozen=True)
class Champ:
    cle: str
    section: str
    libelle: str
    aide: str = ""
    type: str = "texte"          # texte | multiligne | siren | siret | tva_intra | entier | nombre | regime


CHAMPS: tuple[Champ, ...] = (
    Champ("SOCIETE_NOM", SECTION_IDENTITE, "Dénomination sociale"),
    Champ("SOCIETE_FORME_JURIDIQUE", SECTION_IDENTITE, "Forme juridique", "Ex. SAS"),
    Champ("SOCIETE_CAPITAL", SECTION_IDENTITE, "Capital social", "Tel qu'imprimé, ex. 200,00 €"),
    Champ("SOCIETE_ADRESSE", SECTION_IDENTITE, "Siège social", "Une ligne d'adresse par ligne",
          "multiligne"),
    Champ("SOCIETE_SIREN", SECTION_IDENTITE, "SIREN", "9 chiffres", "siren"),
    Champ("SOCIETE_SIRET", SECTION_IDENTITE, "SIRET",
          "14 chiffres, si connu — jamais déduit du SIREN", "siret"),
    Champ("SOCIETE_RCS", SECTION_IDENTITE, "Immatriculation RCS", "Ex. R.C.S. Bordeaux"),
    Champ("SOCIETE_TVA_INTRA", SECTION_IDENTITE, "N° de TVA intracommunautaire",
          "Si attribué — facultatif en franchise en base", "tva_intra"),
    Champ("SOCIETE_REPRESENTANTS", SECTION_CONTACT, "Représentants",
          "Imprimé « Représentée par … », sans titre juridique"),
    Champ("SOCIETE_CONTACT", SECTION_CONTACT, "Contact (e-mail ou téléphone)"),
    Champ("SOCIETE_COORDONNEES_PAIEMENT", SECTION_CONTACT, "Coordonnées de paiement",
          "Imprimées sur la facture (ex. IBAN)", "multiligne"),
    Champ("FACTURATION_REGIME_TVA", SECTION_FACTURATION, "Régime de TVA", "", "regime"),
    Champ("FACTURATION_MENTION_FRANCHISE_TVA", SECTION_FACTURATION, "Mention — franchise en base",
          "Ex. TVA non applicable, art. 293 B du CGI"),
    Champ("FACTURATION_MENTION_EXONERATION", SECTION_FACTURATION, "Mention — autre exonération"),
    Champ("FACTURATION_MENTION_ASSUJETTI", SECTION_FACTURATION, "Mention — assujetti"),
    Champ("FACTURATION_TAUX_TVA", SECTION_FACTURATION, "Taux de TVA (%)",
          "Uniquement si assujetti", "nombre"),
    Champ("FACTURATION_DELAI_PAIEMENT_JOURS", SECTION_FACTURATION, "Délai de paiement (jours)",
          "0 = paiement à réception · vide = non configuré", "entier"),
    Champ("FACTURATION_CONDITIONS_ESCOMPTE", SECTION_FACTURATION, "Escompte",
          "Vide : « Escompte pour paiement anticipé : néant »"),
    Champ("FACTURATION_TAUX_PENALITES_RETARD", SECTION_FACTURATION, "Pénalités de retard",
          "Exigé pour un client professionnel"),
    Champ("FACTURATION_INDEMNITE_RECOUVREMENT", SECTION_FACTURATION,
          "Indemnité forfaitaire de recouvrement", "Exigée pour un client professionnel"),
)
CLES = tuple(c.cle for c in CHAMPS)
PAR_CLE = {c.cle: c for c in CHAMPS}


class ParametreInvalide(ValueError):
    def __init__(self, erreurs: dict[str, str]):
        self.erreurs = erreurs
        super().__init__("; ".join(f"{PAR_CLE[k].libelle} : {v}" for k, v in erreurs.items()))


# ── Lecture ─────────────────────────────────────────────────────────────────────────────────────

def _base_disponible(db_path) -> bool:
    chemin = Path(db_path) if db_path else Path(cfg.DB_PATH)
    return chemin.exists()


def enregistres(*, db_path=None) -> dict[str, dict[str, Any]]:
    """Paramètres présents en base : clé → {valeur, maj_le, maj_par, origine}. Base ou table
    absente : aucun paramètre enregistré (jamais une erreur)."""
    if not _base_disponible(db_path):
        return {}
    conn = get_db(db_path)
    try:
        return {r["cle"]: dict(r) for r in conn.execute(f"SELECT * FROM {TABLE}")}
    except Exception:          # noqa: BLE001 — base antérieure à 0111
        return {}
    finally:
        conn.close()


def lire(cle: str, *, db_path=None) -> tuple[bool, str | None]:
    """(enregistré ?, valeur). Enregistré avec une valeur NULL = « non configuré » explicitement."""
    ligne = enregistres(db_path=db_path).get(cle)
    return (True, ligne["valeur"]) if ligne is not None else (False, None)


def _valeur_environnement(cle: str) -> str:
    return str(os.environ.get(cle, getattr(cfg, cle, "")) or "").strip()


def etat(*, db_path=None) -> list[dict[str, Any]]:
    """Chaque champ avec sa valeur effective et d'où elle vient — pour l'écran."""
    en_base = enregistres(db_path=db_path)
    lignes = []
    for c in CHAMPS:
        if c.cle in en_base:
            valeur, origine = en_base[c.cle]["valeur"], "BASE"
            maj = en_base[c.cle]
        else:
            env = _valeur_environnement(c.cle)
            valeur, origine, maj = (env or None), ("ENVIRONNEMENT" if env else "NON_CONFIGURE"), {}
        lignes.append({"cle": c.cle, "section": c.section, "libelle": c.libelle, "aide": c.aide,
                       "type": c.type, "valeur": valeur, "origine": origine,
                       "maj_le": maj.get("maj_le"), "maj_par": maj.get("maj_par")})
    return lignes


def historique(*, limite: int = 30, db_path=None) -> list[dict[str, Any]]:
    if not _base_disponible(db_path):
        return []
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(
            f"SELECT * FROM {TABLE_HISTORIQUE} ORDER BY id DESC LIMIT ?", (limite,))]
    except Exception:          # noqa: BLE001
        return []
    finally:
        conn.close()


# ── Validation ──────────────────────────────────────────────────────────────────────────────────

def _chiffres(v: str) -> str:
    return re.sub(r"[\s.]", "", v)


def normaliser(valeurs: dict[str, Any]) -> dict[str, str | None]:
    """Valeurs du formulaire → valeurs stockées. Lève `ParametreInvalide` avec tous les défauts."""
    sortie: dict[str, str | None] = {}
    erreurs: dict[str, str] = {}
    for cle in CLES:
        if cle not in valeurs:
            continue
        brut = str(valeurs.get(cle) or "").replace("\r\n", "\n").strip()
        c = PAR_CLE[cle]
        if brut == "":
            sortie[cle] = None
            continue
        if c.type == "siren":
            d = _chiffres(brut)
            if not re.fullmatch(r"\d{9}", d):
                erreurs[cle] = "9 chiffres attendus"
            brut = d
        elif c.type == "siret":
            d = _chiffres(brut)
            if not re.fullmatch(r"\d{14}", d):
                erreurs[cle] = "14 chiffres attendus"
            brut = d
        elif c.type == "tva_intra":
            d = re.sub(r"\s", "", brut).upper()
            if not re.fullmatch(r"[A-Z]{2}[0-9A-Z]{2,13}", d):
                erreurs[cle] = "format inattendu (ex. FR12345678901)"
            brut = d
        elif c.type == "entier":
            if not re.fullmatch(r"\d{1,3}", brut):
                erreurs[cle] = "nombre entier de jours attendu (0 = à réception)"
        elif c.type == "nombre":
            try:
                float(brut.replace(",", "."))
                brut = brut.replace(",", ".")
            except ValueError:
                erreurs[cle] = "nombre attendu"
        elif c.type == "regime":
            brut = brut.upper()
            if brut not in REGIMES_TVA:
                erreurs[cle] = "régime inconnu"
        sortie[cle] = brut
    siren = sortie.get("SOCIETE_SIREN")
    siret = sortie.get("SOCIETE_SIRET")
    if siren and siret and "SOCIETE_SIRET" not in erreurs and not siret.startswith(siren):
        erreurs["SOCIETE_SIRET"] = "un SIRET commence par le SIREN de la société"
    if erreurs:
        raise ParametreInvalide(erreurs)
    return sortie


# ── Écriture ────────────────────────────────────────────────────────────────────────────────────

def enregistrer(valeurs: dict[str, Any], *, acteur: str, motif: str = "",
                origine: str = ORIGINE_SAISIE, db_path=None) -> dict[str, Any]:
    """Enregistre les champs fournis. Seuls les champs RÉELLEMENT modifiés sont écrits et
    historisés ; une facture déjà émise n'est jamais touchée (elle porte son propre instantané)."""
    if not (acteur or "").strip():
        raise ParametreInvalide({"SOCIETE_NOM": "auteur de la modification requis"})
    nouvelles = normaliser(valeurs)
    en_base = enregistres(db_path=db_path)
    modifiees = []
    conn = get_db(db_path)
    try:
        for cle, valeur in nouvelles.items():
            present = cle in en_base
            ancienne = en_base[cle]["valeur"] if present else None
            if present and ancienne == valeur:
                continue
            conn.execute(
                f"INSERT INTO {TABLE} (cle, valeur, maj_le, maj_par, origine) "
                "VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'), ?, ?) "
                "ON CONFLICT(cle) DO UPDATE SET valeur=excluded.valeur, maj_le=excluded.maj_le, "
                "maj_par=excluded.maj_par, origine=excluded.origine",
                (cle, valeur, acteur, origine))
            conn.execute(
                f"INSERT INTO {TABLE_HISTORIQUE} (cle, ancienne_valeur, nouvelle_valeur, "
                "modifie_par, motif) VALUES (?,?,?,?,?)",
                (cle, ancienne, valeur, acteur, (motif or "").strip() or None))
            modifiees.append(cle)
        conn.commit()
    finally:
        conn.close()
    if modifiees:
        from app.services import audit_service
        audit_service.log_event("PARAMETRES_SOCIETE_MODIFIES",
                                {"champs": modifiees, "origine": origine,
                                 "motif": (motif or "").strip()},
                                user_label=acteur, db_path=db_path)
    return {"ok": True, "modifiees": modifiees}


def reprendre_depuis_environnement(*, acteur: str, db_path=None) -> dict[str, Any]:
    """Copie en base, UNE FOIS, les paramètres encore lus depuis l'environnement. Les paramètres
    déjà enregistrés ne sont jamais écrasés ; un paramètre absent partout est enregistré NULL
    (« non configuré »), ce qui retire définitivement l'environnement du jeu pour cette donnée."""
    en_base = enregistres(db_path=db_path)
    a_reprendre = {c: _valeur_environnement(c) for c in CLES if c not in en_base}
    if not a_reprendre:
        return {"ok": True, "modifiees": []}
    return enregistrer(a_reprendre, acteur=acteur, origine=ORIGINE_ENVIRONNEMENT,
                       motif="Reprise de la configuration d'environnement", db_path=db_path)

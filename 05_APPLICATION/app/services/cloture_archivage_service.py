"""Archivage économique d'un mois clôturé (mission 15, migration 0064).

Règle cible : VALIDEE → ARCHIVEE (clotures_service) doit, dans UNE SEULE transaction :
  1. figer l'état économique du mois (réservations + résultats Lot10 pertinents) ;
  2. vérifier l'archive ;
  3. seulement alors marquer `ref_cloture_mensuelle.statut_mois = 'CLOTURE'`.

Si l'archivage échoue : rollback complet — ni CLOTURE, ni ARCHIVEE (jamais un mois CLOTURE sans
archive, jamais une archive orpheline sans mois clos).

Réutilise `reservations_historique_cloture` (migration 0034, déjà construite par lot4ter) pour le
niveau réservation — mêmes colonnes, même contrat d'upsert-sans-suppression. Une nouvelle table
légère (`mois_archive_reglement`) couvre le niveau agrégé (Lot10 net règlement) qu'aucune table
existante ne portait.

IMPORTANT (mission 15, correction métier) : cette fonction fige les valeurs DÉJÀ CALCULÉES au
moment de l'appel — elle ne relance rien, ne réextrait rien depuis Hostaway. Appelée au moment
réel de la clôture (désormais), figer maintenant EST figer "à la clôture" : ce n'est plus une
reconstruction a posteriori (cf. mission 14g/audit — le risque ne concernait que les mois legacy
déjà clos AVANT que cette mécanique existe).
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db

_COLS_HIST = (
    "archive_id", "cle_historisation", "reservation_calc_id", "reservation_id_hostaway",
    "reservation_hh_id", "canal", "logement_id", "proprietaire_id", "mois", "date_arrivee",
    "date_depart", "nuits", "montant_retenu", "payout_calcule", "menage_retenu",
    "assiette_commission", "code_impact", "impact_resultat_reel", "impact_resultat_comptable",
    "statut_controle", "niveau_anomalie", "code_anomalie", "origine_initiale", "source_ligne",
    "source_montant", "methode", "mois_cloture", "fige_le", "row_hash",
)

E_DEJA_ARCHIVE = "CLOTURE_ARCHIVE_DEJA_PRESENT"
E_VERIFICATION_ECHOUEE = "CLOTURE_ARCHIVE_VERIFICATION_ECHOUEE"


class ArchivageRefuse(Exception):
    """Archivage refusé — jamais levée pour un cas nominal, jamais silencieuse."""


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _biz_hash(values) -> str:
    s = "|".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _table_presente(conn, table: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def deja_archive(mois: str, *, db_path=None) -> bool:
    conn = get_db(db_path)
    try:
        if not _table_presente(conn, "reservations_historique_cloture"):
            return False
        return conn.execute(
            "SELECT 1 FROM reservations_historique_cloture WHERE mois_cloture = ? LIMIT 1",
            (mois,)).fetchone() is not None
    finally:
        conn.close()


def classifier_legacy(mois: str, *, motif: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """Marque explicitement un mois comme LEGACY_SANS_ARCHIVE_ORIGINE — mission 15, partie E.

    JAMAIS déduit automatiquement d'une absence d'archive (un mois tout juste clos, pas encore
    archivé, n'est pas legacy) : un motif est obligatoire, une décision à la fois. Ce statut
    signifie : mois considéré clôturé historiquement, aucune archive économique authentique
    disponible, aucune reconstruction automatique, aucune dépendance future à Hostaway pour ce
    passé — et ne doit plus produire d'anomalie opérationnelle par réservation.
    """
    if not (motif or "").strip():
        raise ArchivageRefuse("Motif obligatoire pour une classification LEGACY_SANS_ARCHIVE_ORIGINE.")
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO mois_classification_legacy (mois, classification, motif, acteur) "
            "VALUES (?,?,?,?) "
            "ON CONFLICT(mois) DO UPDATE SET motif=excluded.motif, acteur=excluded.acteur, "
            "date_classification=strftime('%Y-%m-%dT%H:%M:%SZ','now')",
            (mois, "LEGACY_SANS_ARCHIVE_ORIGINE", motif, acteur or None))
        conn.commit()
        return {"ok": True, "mois": mois, "classification": "LEGACY_SANS_ARCHIVE_ORIGINE"}
    finally:
        conn.close()


def mois_legacy(*, db_path=None) -> set[str]:
    conn = get_db(db_path)
    try:
        if not _table_presente(conn, "mois_classification_legacy"):
            return set()
        return {r[0] for r in conn.execute(
            "SELECT mois FROM mois_classification_legacy "
            "WHERE classification = 'LEGACY_SANS_ARCHIVE_ORIGINE'")}
    finally:
        conn.close()


def correction_historique(cle_historisation: str, mois: str, apres: dict[str, Any], *,
                          justification: str, acteur: str = "", db_path=None) -> dict[str, Any]:
    """CORRECTION_HISTORIQUE explicite (mission 15, partie D) — seule voie de modification d'une
    ligne déjà figée. Jamais silencieuse : justification obligatoire, ancien état ET nouvel état
    conservés (`reservations_historique_corrections`), avant d'UPDATE la ligne figée elle-même
    (le contrat upsert-sans-suppression de `reservations_historique_cloture` porte sur l'ajout
    normal — une correction explicite reste une opération distincte et tracée, pas une réécriture
    silencieuse)."""
    if not (justification or "").strip():
        raise ArchivageRefuse("Justification obligatoire pour une CORRECTION_HISTORIQUE.")
    conn = get_db(db_path)
    try:
        avant = conn.execute(
            "SELECT * FROM reservations_historique_cloture WHERE cle_historisation = ?",
            (cle_historisation,)).fetchone()
        if avant is None:
            raise ArchivageRefuse(f"Ligne figée introuvable : {cle_historisation}.")
        avant = dict(avant)
        import json
        conn.execute(
            "INSERT INTO reservations_historique_corrections (cle_historisation, mois, "
            "avant_json, apres_json, justification, acteur) VALUES (?,?,?,?,?,?)",
            (cle_historisation, mois, json.dumps(avant, default=str),
             json.dumps(apres, default=str), justification, acteur or None))
        colonnes = [c for c in apres if c in _COLS_HIST and c != "cle_historisation"]
        if colonnes:
            conn.execute(
                f"UPDATE reservations_historique_cloture SET "
                f"{', '.join(f'{c} = ?' for c in colonnes)} WHERE cle_historisation = ?",
                [apres[c] for c in colonnes] + [cle_historisation])
        conn.commit()
        return {"ok": True, "cle_historisation": cle_historisation}
    finally:
        conn.close()


def archiver_mois(mois: str, *, acteur: str = "", conn=None, db_path=None) -> dict[str, Any]:
    """Fige l'état économique du mois — réservations résolues VALIDE + agrégat Lot10 règlement.

    Appelée par `clotures_service.archiver()` dans SA transaction (`conn` partagé). Lève
    `ArchivageRefuse` (jamais silencieuse) si le mois est déjà archivé (une correction passe par
    `CORRECTION_HISTORIQUE`, jamais une réécriture ici) ou si la vérification post-écriture échoue
    — l'appelant doit alors rollback et NE JAMAIS marquer le mois CLOTURE.
    """
    connexion_locale = conn is None
    if connexion_locale:
        conn = get_db(db_path)
    try:
        if conn.execute(
            "SELECT 1 FROM reservations_historique_cloture WHERE mois_cloture = ? LIMIT 1",
            (mois,)
        ).fetchone():
            raise ArchivageRefuse(
                f"{E_DEJA_ARCHIVE}: le mois {mois} porte déjà une archive — utiliser une "
                "CORRECTION_HISTORIQUE explicite, jamais une réécriture.")

        reservations = [dict(r) for r in conn.execute(
            "SELECT reservation_calc_id, reservation_id_hostaway, reservation_hh_id, source, "
            "logement_id, proprietaire_id, mois, date_arrivee, date_depart, nuits, "
            "montant_retenu, code_impact, impact_resultat_reel, impact_resultat_comptable, "
            "statut_controle, niveau_anomalie, code_anomalie "
            "FROM reservations_resolues WHERE mois = ? AND statut_controle = 'VALIDE'", (mois,))]

        pay_par_res = {}
        if _table_presente(conn, "hostaway_payouts"):
            for r in conn.execute(
                "SELECT reservation_id, payout_calcule, menage_retenu, assiette_commission "
                "FROM hostaway_payouts"):
                pay_par_res[str(r["reservation_id"])] = dict(r)

        fige_le = _maintenant()
        archive_id = "ARC-" + uuid.uuid4().hex[:12].upper()
        nb_archivees = 0
        for r in reservations:
            rid_ha = r.get("reservation_id_hostaway")
            cle = str(rid_ha) if rid_ha else (
                str(r.get("reservation_hh_id")) or str(r["reservation_calc_id"]))
            p = pay_par_res.get(str(rid_ha), {}) if rid_ha else {}
            valeurs = {
                "archive_id": archive_id,
                "cle_historisation": cle, "reservation_calc_id": r["reservation_calc_id"],
                "reservation_id_hostaway": rid_ha, "reservation_hh_id": r.get("reservation_hh_id"),
                "canal": r.get("source"), "logement_id": r.get("logement_id"),
                "proprietaire_id": r.get("proprietaire_id"), "mois": r["mois"],
                "date_arrivee": r.get("date_arrivee"), "date_depart": r.get("date_depart"),
                "nuits": r.get("nuits"), "montant_retenu": r.get("montant_retenu"),
                "payout_calcule": p.get("payout_calcule"), "menage_retenu": p.get("menage_retenu"),
                "assiette_commission": p.get("assiette_commission"),
                "code_impact": r.get("code_impact"),
                "impact_resultat_reel": r.get("impact_resultat_reel"),
                "impact_resultat_comptable": r.get("impact_resultat_comptable"),
                "statut_controle": r.get("statut_controle"),
                "niveau_anomalie": r.get("niveau_anomalie"), "code_anomalie": r.get("code_anomalie"),
                "origine_initiale": "API_HOSTAWAY" if rid_ha else "SAISIE_HH",
                "source_ligne": "CLOTURE_ARCHIVAGE", "source_montant": "CLOTURE_ARCHIVAGE",
                "methode": "ARCHIVAGE_A_LA_CLOTURE", "mois_cloture": mois, "fige_le": fige_le,
            }
            valeurs["row_hash"] = _biz_hash(
                [valeurs[c] for c in _COLS_HIST if c not in ("fige_le", "row_hash")])
            conn.execute(
                f"INSERT OR IGNORE INTO reservations_historique_cloture "
                f"({', '.join(_COLS_HIST)}) VALUES ({', '.join(['?'] * len(_COLS_HIST))})",
                [valeurs[c] for c in _COLS_HIST])
            nb_archivees += 1

        if _table_presente(conn, "lot10_net_reglement"):
            lignes_reglement = [dict(r) for r in conn.execute(
                "SELECT * FROM lot10_net_reglement WHERE mois = ?", (mois,))]
        else:
            lignes_reglement = []
        nb_reglement = 0
        for r in lignes_reglement:
            conn.execute(
                "INSERT OR IGNORE INTO mois_archive_reglement (mois, logement_id, "
                "proprietaire_id, total_commission_mois, total_menage_mois, "
                "total_preparation_canape_mois, charges_exceptionnelles_refacturees, "
                "montant_du_conciergerie, reste_a_payer_conciergerie, fige_le) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (mois, r.get("logement_id"), r.get("proprietaire_id"),
                 r.get("total_commission_mois"), r.get("total_menage_mois"),
                 r.get("total_preparation_canape_mois"),
                 r.get("charges_exceptionnelles_refacturees"), r.get("montant_du_conciergerie"),
                 r.get("reste_a_payer_conciergerie"), fige_le))
            nb_reglement += 1

        # Vérification avant de rendre la main : le nombre de lignes réellement présentes doit
        # correspondre à ce qu'on vient d'écrire — jamais une archive silencieusement incomplète.
        nb_verif = conn.execute(
            "SELECT COUNT(*) FROM reservations_historique_cloture WHERE mois_cloture = ?",
            (mois,)).fetchone()[0]
        if nb_verif != nb_archivees:
            raise ArchivageRefuse(
                f"{E_VERIFICATION_ECHOUEE}: {nb_archivees} lignes ecrites mais {nb_verif} "
                "relues — archivage incomplet.")

        if _table_presente(conn, "reservations_archives") and nb_archivees > 0:
            conn.execute(
                "INSERT INTO reservations_archives (archive_id, mois_traites, nb_conservees, "
                "nb_ajoutees, motif, acteur) VALUES (?,?,?,?,?,?)",
                (archive_id, mois, 0, nb_archivees, "ARCHIVAGE_CLOTURE_MISSION15", acteur or None))

        if connexion_locale:
            conn.commit()
        return {"ok": True, "mois": mois, "nb_reservations_archivees": nb_archivees,
               "nb_lignes_reglement_archivees": nb_reglement}
    except Exception:
        if connexion_locale:
            conn.rollback()
        raise
    finally:
        if connexion_locale:
            conn.close()

"""Lignes d'une facture prestataire ménage externe (`facture_lignes_menage`, migration 0037).

Satellite de `factures` (0017) — pas un en-tête concurrent — mais qui ne passe PAS par
`facture_lignes` (0022) : cette table impose un `charge_id` déjà créé par le module Charges
existant, qui reste un écrivain Excel (`saisie_charges_writer.py`), hors périmètre de cette mission.
Une ligne ménage porte donc directement `logement_id`/`montant_ttc`.

CONTRÔLE TOTAL (mission §7)
`controler_total` compare la somme des lignes (ménages affectés + frais non affectés, AVANT
ventilation — la ventilation n'ajoute jamais de montant, elle explique une répartition) au montant
TTC de la facture. Un écart signale une facture à revoir ; il ne la fait jamais passer VALIDEE tout
seul (§31 : décision humaine via `factures_service.changer_statut`).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from app.db.connection import get_db

TOLERANCE = 0.005

TYPE_MENAGE_INTERNE = "MENAGE_INTERNE"
TYPE_MENAGE_EXTERNE = "MENAGE_EXTERNE"
TYPE_FRAIS_NON_AFFECTE = "FRAIS_NON_AFFECTE"
TYPE_AUTRE = "AUTRE"
TYPES = (TYPE_MENAGE_INTERNE, TYPE_MENAGE_EXTERNE, TYPE_FRAIS_NON_AFFECTE, TYPE_AUTRE)

SOURCE_PDF = "PDF_EXTRACTION"
SOURCE_SAISIE = "SAISIE"


def _maintenant() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _refus(code: str, detail: str = "") -> dict[str, Any]:
    return {"ok": False, "code": code, "detail": detail}


def ajouter_ligne(facture_id_opaque: str, *, type_ligne: str, montant_ttc: float,
                  logement_id: str = "", menage_id_opaque: str = "", description: str = "",
                  montant_ht: float | None = None, montant_tva: float | None = None,
                  quantite: int | None = None, prix_unitaire: float | None = None,
                  source: str = SOURCE_PDF, commentaire: str = "", acteur: str = "",
                  db_path=None) -> dict[str, Any]:
    if type_ligne not in TYPES:
        return _refus("TYPE_LIGNE_INVALIDE", type_ligne)
    montant_ttc = round(float(montant_ttc or 0), 2)
    if montant_ttc == 0:
        return _refus("MONTANT_INVALIDE", str(montant_ttc))
    if type_ligne != TYPE_FRAIS_NON_AFFECTE and not logement_id:
        return _refus("LOGEMENT_MANQUANT", type_ligne)

    ligne_id = "FLM-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, menage_id_opaque, description, montant_ht, montant_tva, montant_ttc, "
            "source, commentaire, acteur) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (ligne_id, facture_id_opaque, type_ligne, logement_id or None,
             menage_id_opaque or None, description or None, montant_ht, montant_tva, montant_ttc,
             source, commentaire or None, acteur or None))
        if quantite is not None or prix_unitaire is not None:
            conn.execute(
                "INSERT INTO facture_lignes_menage_detail (ligne_id_opaque, quantite, "
                "prix_unitaire) VALUES (?,?,?)", (ligne_id, quantite, prix_unitaire))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "ligne_id_opaque": ligne_id, "facture_id_opaque": facture_id_opaque}


def lignes(facture_id_opaque: str, db_path=None) -> list[dict[str, Any]]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT l.*, d.quantite, d.prix_unitaire FROM facture_lignes_menage l "
            "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
            "WHERE l.facture_id_opaque = ? ORDER BY l.id", (facture_id_opaque,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def cout_menages_par_logement(facture_id_opaque: str, db_path=None) -> dict[str, float]:
    """Coût des ménages de chaque logement DANS cette facture — la base de pondération de la
    ventilation (§11) : uniquement les lignes MENAGE_EXTERNE, jamais les autres factures du même
    prestataire."""
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT logement_id, SUM(montant_ttc) AS total FROM facture_lignes_menage "
            "WHERE facture_id_opaque = ? AND type_ligne = ? AND logement_id IS NOT NULL "
            "GROUP BY logement_id", (facture_id_opaque, TYPE_MENAGE_EXTERNE)).fetchall()
        return {r["logement_id"]: round(r["total"], 2) for r in rows}
    finally:
        conn.close()


def controler_total(facture_id_opaque: str, db_path=None) -> dict[str, Any]:
    """Somme des lignes ménage vs montant_ttc de la facture. Écart attendu : 0,00€."""
    conn = get_db(db_path)
    try:
        f = conn.execute("SELECT montant_ttc FROM factures WHERE facture_id_opaque = ?",
                         (facture_id_opaque,)).fetchone()
        if f is None:
            return _refus("FACTURE_INTROUVABLE", facture_id_opaque)
        total_lignes = conn.execute(
            "SELECT COALESCE(SUM(montant_ttc), 0) FROM facture_lignes_menage "
            "WHERE facture_id_opaque = ?", (facture_id_opaque,)).fetchone()[0]
    finally:
        conn.close()
    montant_facture = round(f["montant_ttc"] or 0, 2)
    total_lignes = round(total_lignes, 2)
    ecart = round(montant_facture - total_lignes, 2)
    return {"ok": True, "montant_facture": montant_facture, "montant_lignes": total_lignes,
            "ecart": ecart, "coherent": abs(ecart) <= TOLERANCE}

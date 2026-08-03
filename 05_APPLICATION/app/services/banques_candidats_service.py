"""Collecte des objets métier réellement rapprochables avec un mouvement bancaire.

Aucun objet fictif n'est fabriqué ici : chaque candidat vient d'une source existante du dépôt
(charges Lot3, propriétaires, réservations résolues...). Quand une source est absente ou illisible,
elle est simplement ignorée — jamais remplacée par des données inventées, jamais une exception qui
ferait échouer la page.

Ce service ne score rien : il fournit la matière à `banques_suggestions_service.evaluer()`.
"""
from __future__ import annotations

from typing import Any

import app.config as cfg
from app.readers import charges_reader
from app.readers.banques_reader import to_texte, to_nombre, to_date


def _charges() -> list[dict[str, Any]]:
    """Charges Lot3 disponibles → candidats CHARGE_FOURNISSEUR / REGLEMENT_CHARGE."""
    try:
        if not charges_reader.master_available():
            return []
        lignes = charges_reader.read_charges()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in lignes:
        cid = to_texte(r.get("charge_id"))
        montant = to_nombre(r.get("montant"))
        if not cid or montant is None:
            continue
        out.append({
            "type_objet": "CHARGE_FOURNISSEUR",
            "objet_id": cid,
            "montant": abs(montant),
            "date": to_date(r.get("date_charge")),
            "reference": to_texte(r.get("facture_ref")) or "",
            "fournisseur": to_texte(r.get("fournisseur")) or to_texte(r.get("tiers")),
            "proprietaire_id": to_texte(r.get("proprietaire_id")),
            "libelle": f"Charge {cid} — {to_texte(r.get('categorie_charge_id'))}",
        })
    return out


def _reservations() -> list[dict[str, Any]]:
    """Réservations résolues (Lot4quater) → candidats RESERVATION / PAYOUT_PLATEFORME."""
    p = getattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES", None)
    if not p or not p.exists():
        return []
    try:
        import openpyxl
        wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            rows = list(ws.iter_rows(values_only=True))
        finally:
            wb.close()
    except Exception:
        return []
    if len(rows) <= 1:
        return []
    hdr = [to_texte(c) for c in rows[0]]
    out: list[dict[str, Any]] = []
    for r in rows[1:]:
        d = dict(zip(hdr, r))
        rid = to_texte(d.get("reservation_calc_id"))
        montant = to_nombre(d.get("montant_retenu"))
        if not rid or montant is None:
            continue
        out.append({
            "type_objet": "RESERVATION",
            "objet_id": rid,
            "montant": abs(montant),
            "date": to_date(d.get("date_arrivee")),
            "reference": rid,
            "plateforme": to_texte(d.get("canal")),
            "proprietaire_id": to_texte(d.get("proprietaire_id")),
            "libelle": f"Réservation {rid}",
        })
    return out


def candidats_pour(mouvement: dict[str, Any]) -> list[dict[str, Any]]:
    """Candidats pertinents selon le SENS du mouvement — un débit ne peut pas être un encaissement
    de réservation, un crédit ne peut pas être un paiement de charge : filtrer ici évite d'exposer
    des suggestions absurdes à l'utilisateur."""
    sens = to_texte(mouvement.get("sens")).upper()
    if sens == "CREDIT":
        return _reservations()
    if sens == "DEBIT":
        return _charges()
    return _charges() + _reservations()


def compter_sources() -> dict[str, int]:
    """Diagnostic honnête pour l'interface : combien de candidats sont réellement disponibles."""
    return {"charges": len(_charges()), "reservations": len(_reservations())}

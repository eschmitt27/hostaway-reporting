"""Reversements Airbnb — création applicative dans `imputations_airbnb` (migration 0054).

POURQUOI CE MODULE EXISTE
La table `imputations_airbnb` était jusqu'ici uniquement LUE : par
`lot10_calculer_resultats.charger_imputations_airbnb_sqlite` côté moteur, et par
`proprietaires_extras_reader.imputations_airbnb` côté application. Aucun service ne savait en créer
une ligne — l'alimentation était donc hors application. Ce module fournit le SEUL point d'écriture
applicatif, en respectant exactement le contrat de lecture existant (colonnes de 0054, vocabulaire
de `lib_settlements.validated_airbnb_imputation`).

CE QU'UN REVERSEMENT AIRBNB N'EST PAS
Ce n'est ni une réservation, ni une charge, ni un mouvement bancaire, ni une écriture comptable.
C'est un PAYOUT PLATEFORME déjà encaissé, imputé sur une créance qui existe déjà. Ce module n'écrit
donc QUE dans `imputations_airbnb`, et dans aucune autre table.
"""
from __future__ import annotations

import uuid
from typing import Any

from app.db.connection import get_db

# Statut aligné sur ce que le lecteur Lot10 attend d'une imputation prise en compte.
STATUT_VALIDE = "VALIDE"

E_MONTANT_INVALIDE = "IMPUTATION_AIRBNB_MONTANT_INVALIDE"
E_DATE_MANQUANTE = "IMPUTATION_AIRBNB_DATE_MANQUANTE"
E_PROPRIETAIRE_MANQUANT = "IMPUTATION_AIRBNB_PROPRIETAIRE_MANQUANT"


def _refus(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "code": code, "message": message}


def _nombre(v: Any) -> float | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return round(float(str(v).replace(",", ".").replace(" ", "")), 2)
    except (TypeError, ValueError):
        return None


def creer(*, proprietaire_id: str, montant_impute: Any, date_imputation: str,
          logement_id: str = "", mois: str = "", document_id: str = "",
          reference_airbnb: str = "", transaction_banque_id: str = "",
          justificatif: str = "", commentaire: str = "", statut: str = STATUT_VALIDE,
          db_path=None) -> dict[str, Any]:
    """Insère un reversement Airbnb. Refus métier lisible, jamais une exception.

    Les valeurs par défaut reproduisent ce que le lecteur Lot10 attend : un montant positif, un
    propriétaire, une date, et un statut retenu. Rien n'est déduit ni inventé au-delà.
    """
    pid = str(proprietaire_id or "").strip()
    if not pid:
        return _refus(E_PROPRIETAIRE_MANQUANT, "Le propriétaire est obligatoire.")
    montant = _nombre(montant_impute)
    if montant is None or montant <= 0:
        return _refus(E_MONTANT_INVALIDE, "Le montant doit être un nombre strictement positif.")
    if not str(date_imputation or "").strip():
        return _refus(E_DATE_MANQUANTE, "La date d'imputation est obligatoire.")

    imputation_id = "IMPA-" + uuid.uuid4().hex[:12].upper()
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO imputations_airbnb "
            "(imputation_airbnb_id, transaction_banque_id, reference_airbnb, proprietaire_id, "
            " logement_id, mois, document_id, montant_impute, date_imputation, justificatif, "
            " statut, commentaire) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (imputation_id, str(transaction_banque_id).strip() or None,
             str(reference_airbnb).strip() or None, pid, str(logement_id).strip() or None,
             str(mois).strip() or None, str(document_id).strip() or None, montant,
             str(date_imputation).strip(), str(justificatif).strip() or None,
             statut, str(commentaire).strip() or None))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "imputation_airbnb_id": imputation_id, "montant_impute": montant}


def lister(*, proprietaire_id: str = "", mois: str = "", document_id: str = "",
           db_path=None) -> list[dict[str, Any]]:
    sql = "SELECT * FROM imputations_airbnb WHERE 1=1"
    params: list[Any] = []
    for champ, valeur in (("proprietaire_id", proprietaire_id), ("mois", mois),
                          ("document_id", document_id)):
        if valeur:
            sql += f" AND {champ}=?"
            params.append(valeur)
    sql += " ORDER BY date_imputation, imputation_airbnb_id"
    conn = get_db(db_path)
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()

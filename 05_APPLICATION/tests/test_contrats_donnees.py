"""Tests des contrats de données typés (app/contrats_donnees.py) — mission durcissement Phase 1.

Ces contrats ne sont pas encore câblés dans les services de saisie (voir
DURCISSEMENT_SQLITE_CONTRATS_DONNEES.md) : `valider()` dans `charges_saisie_service.py` accepte
des formats de date plus permissifs que le contrat structurel ci-dessous. Les câbler sans audit
préalable des données réelles aurait risqué une régression. Ce fichier teste le contrat seul.
"""
from __future__ import annotations

import pytest

from app.contrats_donnees import (
    Charge,
    ContratInvalideError,
    MouvementBanque,
    MouvementTresorerieProprietaire,
    ReservationHH,
)


# ── Charge ────────────────────────────────────────────────────────────────────

def test_charge_valide_acceptee():
    c = Charge.from_dict({"date_charge": "2026-06-12", "montant": 149.90,
                          "categorie_charge_id": "CAT_ENTRETIEN"})
    assert c.montant == 149.90


def test_charge_montant_non_numerique_refuse():
    with pytest.raises(ContratInvalideError, match="montant"):
        Charge.from_dict({"date_charge": "2026-06-12", "montant": "abc",
                          "categorie_charge_id": "CAT_ENTRETIEN"})


def test_charge_date_invalide_refuse():
    with pytest.raises(ContratInvalideError, match="date_charge"):
        Charge.from_dict({"date_charge": "12/06/2026", "montant": 10.0,
                          "categorie_charge_id": "CAT_ENTRETIEN"})


def test_charge_date_calendaire_impossible_refuse():
    with pytest.raises(ContratInvalideError, match="date_charge"):
        Charge.from_dict({"date_charge": "2026-02-30", "montant": 10.0,
                          "categorie_charge_id": "CAT_ENTRETIEN"})


def test_charge_categorie_absente_refuse():
    with pytest.raises(ContratInvalideError, match="categorie_charge_id"):
        Charge.from_dict({"date_charge": "2026-06-12", "montant": 10.0})


# ── ReservationHH ─────────────────────────────────────────────────────────────

def test_reservation_hh_valide_acceptee():
    r = ReservationHH.from_dict({
        "mois": "2026-06", "logement_id": "LOG_0001", "date_arrivee": "2026-06-02",
        "date_depart": "2026-06-06", "montant_retenu": 420.0})
    assert r.logement_id == "LOG_0001"


def test_reservation_hh_depart_avant_arrivee_refuse():
    with pytest.raises(ContratInvalideError, match="date_depart"):
        ReservationHH.from_dict({
            "mois": "2026-06", "logement_id": "LOG_0001", "date_arrivee": "2026-06-06",
            "date_depart": "2026-06-02", "montant_retenu": 420.0})


def test_reservation_hh_mois_invalide_refuse():
    with pytest.raises(ContratInvalideError, match="mois"):
        ReservationHH.from_dict({
            "mois": "juin", "logement_id": "LOG_0001", "date_arrivee": "2026-06-02",
            "date_depart": "2026-06-06", "montant_retenu": 420.0})


# ── MouvementBanque ───────────────────────────────────────────────────────────

def test_mouvement_banque_valide_accepte():
    m = MouvementBanque.from_dict({
        "bank_account_id": "CPT-0001", "date_operation": "2026-06-01", "sens": "DEBIT",
        "montant": 10.0, "libelle_brut": "PRELEVEMENT"})
    assert m.sens == "DEBIT"


def test_mouvement_banque_sens_invalide_refuse():
    with pytest.raises(ContratInvalideError, match="sens"):
        MouvementBanque.from_dict({
            "bank_account_id": "CPT-0001", "date_operation": "2026-06-01", "sens": "AUTRE",
            "montant": 10.0, "libelle_brut": "x"})


# ── MouvementTresorerieProprietaire ──────────────────────────────────────────

def test_mouvement_tresorerie_valide_accepte():
    m = MouvementTresorerieProprietaire.from_dict({
        "proprietaire_id": "PROP_A", "sens": "PROPRIETAIRE_VERS_SOCIETE",
        "nature": "ACOMPTE_PROPRIETAIRE", "montant": 100.0, "date_mouvement": "2026-06-01"})
    assert m.proprietaire_id == "PROP_A"


def test_mouvement_tresorerie_sens_invalide_refuse():
    with pytest.raises(ContratInvalideError, match="sens"):
        MouvementTresorerieProprietaire.from_dict({
            "proprietaire_id": "PROP_A", "sens": "AUTRE",
            "nature": "ACOMPTE_PROPRIETAIRE", "montant": 100.0, "date_mouvement": "2026-06-01"})


def test_mouvement_tresorerie_id_vide_refuse():
    with pytest.raises(ContratInvalideError, match="proprietaire_id"):
        MouvementTresorerieProprietaire.from_dict({
            "proprietaire_id": "  ", "sens": "PROPRIETAIRE_VERS_SOCIETE",
            "nature": "ACOMPTE_PROPRIETAIRE", "montant": 100.0, "date_mouvement": "2026-06-01"})

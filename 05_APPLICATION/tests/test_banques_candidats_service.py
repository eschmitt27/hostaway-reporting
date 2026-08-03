"""APP — candidats de rapprochement bancaire (banques_candidats_service).

Fixtures synthétiques uniquement, jamais de données bancaires réelles. Le fichier
MASTER_CALC_Reservations_Resolues.xlsx est monkeypatché via `cfg.MASTER_CALC_RESERVATIONS_RESOLUES`.

Le schéma réel de ce fichier (Lot4quater) porte la colonne `montant_retenu` (et `date_arrivee`),
jamais `montant_paye`/`montant_total`/`date_checkin`/`date_reservation` — colonnes qui n'existent
dans aucune version connue de ce fichier.
"""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.services import banques_candidats_service as candidats

RESA_HDR = [
    "reservation_calc_id", "ROW_HASH", "source", "reservation_id_hostaway", "reservation_hh_id",
    "mois", "logement_id", "proprietaire_id", "date_arrivee", "date_depart", "nuits",
    "guestCount", "source_guestCount", "montant_retenu", "source_montant", "code_impact",
    "impact_resultat_reel", "impact_resultat_comptable", "statut_controle", "niveau_anomalie",
    "code_anomalie", "commentaire", "source_module", "source_table", "source_pk",
    "date_integration", "canal", "etat_mois", "origine_initiale", "source_ligne", "methode",
    "payout_calcule", "menage_retenu", "assiette_commission",
]


def _write_reservations_resolues(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MASTER"
    ws.append(RESA_HDR)
    for r in rows:
        ws.append([r.get(h) for h in RESA_HDR])
    wb.create_sheet("VUE_FLUX")
    wb.save(path)


@pytest.fixture
def resa_file(tmp_path, monkeypatch):
    p = tmp_path / "MASTER_CALC_Reservations_Resolues.xlsx"
    monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES", p)
    return p


def test_reservations_candidat_lu_avec_le_schema_reel(resa_file):
    _write_reservations_resolues(resa_file, [
        {"reservation_calc_id": "RESA-0001", "montant_retenu": 361.92,
         "date_arrivee": "2025-11-03", "proprietaire_id": "PROP_0001", "canal": "AIRBNB"},
    ])
    out = candidats._reservations()
    assert len(out) == 1, "le schema reel (montant_retenu/date_arrivee) doit produire un candidat"
    c = out[0]
    assert c["type_objet"] == "RESERVATION"
    assert c["objet_id"] == "RESA-0001"
    assert c["montant"] == 361.92
    assert c["date"] == "2025-11-03"


def test_reservations_plusieurs_lignes(resa_file):
    _write_reservations_resolues(resa_file, [
        {"reservation_calc_id": "RESA-0001", "montant_retenu": 100.0, "date_arrivee": "2025-11-01"},
        {"reservation_calc_id": "RESA-0002", "montant_retenu": 200.0, "date_arrivee": "2025-11-02"},
    ])
    out = candidats._reservations()
    assert len(out) == 2


def test_reservations_ligne_sans_id_ignoree(resa_file):
    _write_reservations_resolues(resa_file, [
        {"reservation_calc_id": None, "montant_retenu": 100.0, "date_arrivee": "2025-11-01"},
    ])
    assert candidats._reservations() == []


def test_reservations_ligne_sans_montant_ignoree(resa_file):
    _write_reservations_resolues(resa_file, [
        {"reservation_calc_id": "RESA-0001", "montant_retenu": None, "date_arrivee": "2025-11-01"},
    ])
    assert candidats._reservations() == []


def test_reservations_fichier_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES", tmp_path / "absent.xlsx")
    assert candidats._reservations() == []


def test_candidats_pour_credit_utilise_reservations(resa_file):
    _write_reservations_resolues(resa_file, [
        {"reservation_calc_id": "RESA-0001", "montant_retenu": 361.92, "date_arrivee": "2025-11-03"},
    ])
    r = candidats.candidats_pour({"sens": "CREDIT"})
    assert len(r) == 1 and r[0]["type_objet"] == "RESERVATION"


def test_compter_sources_reservations_honnete(resa_file):
    _write_reservations_resolues(resa_file, [
        {"reservation_calc_id": "RESA-0001", "montant_retenu": 361.92, "date_arrivee": "2025-11-03"},
    ])
    assert candidats.compter_sources()["reservations"] == 1

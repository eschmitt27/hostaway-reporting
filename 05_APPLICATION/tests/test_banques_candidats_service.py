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


# ── Trésorerie propriétaires (migration 0025) ────────────────────────────────

@pytest.fixture(autouse=True)
def _proprietaire_connu(monkeypatch):
    from app.services import proprietaires_tresorerie_service as tresorerie
    monkeypatch.setattr(tresorerie, "find_proprietaire",
                        lambda pid: {"proprietaire_id": pid} if pid == "PROP_TEST01" else None)


def _mouvement_valide(tmp_db, **overrides):
    from app.services import proprietaires_tresorerie_service as tresorerie
    base = dict(proprietaire_id="PROP_TEST01", sens="PROPRIETAIRE_VERS_SOCIETE",
               nature="ACOMPTE_PROPRIETAIRE", montant=300.0, date_mouvement="2026-01-10")
    base.update(overrides)
    r = tresorerie.creer(db_path=tmp_db, **base)
    tresorerie.valider(r["mouvement_opaque"], db_path=tmp_db)
    return r["mouvement_opaque"]


def test_reversements_proprietaires_mouvement_valide_devient_candidat(tmp_db, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    mid = _mouvement_valide(tmp_db)
    out = candidats._reversements_proprietaires()
    assert len(out) == 1
    assert out[0]["objet_id"] == mid and out[0]["montant"] == 300.0
    assert out[0]["type_objet"] == "REVERSEMENT_PROPRIETAIRE"


def test_reversements_proprietaires_brouillon_absent(tmp_db, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    from app.services import proprietaires_tresorerie_service as tresorerie
    tresorerie.creer(db_path=tmp_db, proprietaire_id="PROP_TEST01", sens="PROPRIETAIRE_VERS_SOCIETE",
                     nature="ACOMPTE_PROPRIETAIRE", montant=100.0, date_mouvement="2026-01-10")
    assert candidats._reversements_proprietaires() == []


def test_candidats_pour_credit_filtre_sens_objet(tmp_db, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    _mouvement_valide(tmp_db, sens="PROPRIETAIRE_VERS_SOCIETE")
    _mouvement_valide(tmp_db, sens="SOCIETE_VERS_PROPRIETAIRE")
    r = candidats.candidats_pour({"sens": "CREDIT"})
    props = [c for c in r if c["type_objet"] == "REVERSEMENT_PROPRIETAIRE"]
    assert len(props) == 1 and props[0]["sens_objet"] == "PROPRIETAIRE_VERS_SOCIETE"


def test_candidats_pour_debit_filtre_sens_objet(tmp_db, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    _mouvement_valide(tmp_db, sens="PROPRIETAIRE_VERS_SOCIETE")
    _mouvement_valide(tmp_db, sens="SOCIETE_VERS_PROPRIETAIRE")
    r = candidats.candidats_pour({"sens": "DEBIT"})
    props = [c for c in r if c["type_objet"] == "REVERSEMENT_PROPRIETAIRE"]
    assert len(props) == 1 and props[0]["sens_objet"] == "SOCIETE_VERS_PROPRIETAIRE"


def test_reversement_partiellement_rapproche_reste_candidat_avec_solde_reduit(tmp_db, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    mid = _mouvement_valide(tmp_db, montant=1000.0)
    from app.services import banques_rapprochement_service as rappro
    rappro.enregistrer("MVT-FAKE-002", "REVERSEMENT_PROPRIETAIRE", mid, 400.0,
                       montant_mouvement=400.0, db_path=tmp_db)
    out = candidats._reversements_proprietaires()
    assert len(out) == 1 and out[0]["montant"] == 600.0


def test_reversement_solde_integralement_disparait_des_candidats(tmp_db, monkeypatch):
    import app.config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    mid = _mouvement_valide(tmp_db, montant=400.0)
    from app.services import banques_rapprochement_service as rappro
    rappro.enregistrer("MVT-FAKE-003", "REVERSEMENT_PROPRIETAIRE", mid, 400.0,
                       montant_mouvement=400.0, db_path=tmp_db)
    assert candidats._reversements_proprietaires() == []

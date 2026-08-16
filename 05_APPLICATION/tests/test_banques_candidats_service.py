"""APP — candidats de rapprochement bancaire (banques_candidats_service).

Fixtures synthétiques uniquement, jamais de données bancaires réelles.

RÈGLE MÉTIER (2026-08-08, définitive) : un virement bancaire entrant n'est JAMAIS rapproché d'une
réservation individuelle (Hostaway ou hors Hostaway) — le montant reçu en banque n'a pas de
correspondance fiable avec une réservation. `banques_candidats_service` ne contient et ne doit
jamais contenir de générateur de candidats de type RESERVATION. Ce fichier teste explicitement
cette absence (voir tests ci-dessous), en plus des candidats légitimes (charges, trésorerie
propriétaires).
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.services import banques_candidats_service as candidats


def test_aucun_generateur_de_candidats_reservation_dans_le_module():
    """Garantie structurelle : aucune fonction `_reservations` ne doit exister dans ce module —
    empêche la réintroduction accidentelle du rapprochement virement -> réservation."""
    assert not hasattr(candidats, "_reservations")


def test_candidats_pour_credit_ne_contient_jamais_de_type_reservation(monkeypatch, tmp_path):
    """Même si une source de réservations résolues existe et est peuplée, aucun candidat de type
    RESERVATION ne doit jamais apparaître pour un mouvement CREDIT (virement entrant plateforme)."""
    monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES", tmp_path / "absent.xlsx")
    r = candidats.candidats_pour({"sens": "CREDIT"})
    assert all(c["type_objet"] != "RESERVATION" for c in r)


def test_candidats_pour_debit_ne_contient_jamais_de_type_reservation(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES", tmp_path / "absent.xlsx")
    r = candidats.candidats_pour({"sens": "DEBIT"})
    assert all(c["type_objet"] != "RESERVATION" for c in r)


def test_candidats_pour_sens_absent_ne_contient_jamais_de_type_reservation(monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "MASTER_CALC_RESERVATIONS_RESOLUES", tmp_path / "absent.xlsx")
    r = candidats.candidats_pour({})
    assert all(c["type_objet"] != "RESERVATION" for c in r)


def test_compter_sources_ne_reference_plus_les_reservations():
    """`compter_sources()` ne doit plus exposer de clé `reservations` — supprimée avec le
    générateur (elle serait un diagnostic mensonger si elle restait à 0 en permanence)."""
    assert "reservations" not in candidats.compter_sources()


# ── Trésorerie propriétaires (migration 0025) ────────────────────────────────

@pytest.fixture(autouse=True)
def _proprietaire_connu(monkeypatch):
    from app.services import proprietaires_tresorerie_service as tresorerie
    # Le service appelle le lecteur par son module : c'est lui qu'il faut patcher.
    from app.readers import proprietaires_reader
    monkeypatch.setattr(proprietaires_reader, "ref_available", lambda: True)
    monkeypatch.setattr(proprietaires_reader, "find_proprietaire",
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

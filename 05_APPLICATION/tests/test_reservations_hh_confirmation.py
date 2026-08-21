"""APP-2b/2c — Prévisualisation et confirmation d'une réservation HH, entièrement SQLite.

Remplace l'ancienne suite Excel (test_saisie_hh_app2c/app2d/app2e : copie de travail du classeur,
comparaison Lot4A en sous-processus, verrou, restauration de snapshot) : l'écriture est désormais une
transaction SQLite (`reservations_hh_saisie_service.creer`), atomique par construction. Ces tests
couvrent : prévisualisation, confirmation, idempotence, expiration, revalidation métier sur l'état
actuel (mois clôturé entre-temps), et les dérogations menage/commission.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.db.connection import get_db
from app.services import reservations_hh_confirmation_service as confirmation


@pytest.fixture(autouse=True)
def _referentiel(tmp_db, tmp_path, monkeypatch):
    import openpyxl

    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "REF_SETUP", tmp_path / "REF_Setup_ABSENT.xlsm")
    monkeypatch.setattr(cfg, "SAISIE_RESERVATIONS_HH", tmp_path / "SAISIE_ABSENTE.xlsx")

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_modes_paiement (mode_paiement_id, mode_paiement, actif, import_id) "
            "VALUES (?,?,?,?)", ("PAY_001", "BANQUE_PRO", "OUI", fx.IMPORT_TEST))
        conn.execute(
            "INSERT INTO ref_codes_impact (code_impact, impact_resultat_comptable, actif, "
            "import_id) VALUES (?,?,?,?)", ("HC", "OUI", "OUI", fx.IMPORT_TEST))
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?,?,?)",
            ("2026-06", "OUVERT", fx.IMPORT_TEST))
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?,?,?)",
            ("2026-01", "CLOTURE", fx.IMPORT_TEST))
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, date_debut_validite, date_fin_validite, actif, import_id) "
            "VALUES (?,?,?,?,?,?,?)",
            ("COUT_001", "TYPE_001", "50.00", "2026-01-01", "", "OUI", fx.IMPORT_TEST))
        conn.commit()
    finally:
        conn.close()

    original = openpyxl.load_workbook

    def garde(chemin, *a, **kw):
        raise AssertionError(f"Classeur ouvert au runtime HH : {chemin}")

    monkeypatch.setattr(openpyxl, "load_workbook", garde)
    return tmp_db


@pytest.fixture
def dryruns(tmp_path: Path) -> Path:
    return tmp_path / "dryruns"


def _form(**overrides) -> dict:
    base = {
        "canal_id": "CANAL_001", "source_financiere": "SAISIE_MANUELLE",
        "logement_id": "LOG_A1", "date_arrivee": "2026-06-10", "date_depart": "2026-06-15",
        "total_percu": "450.00", "mode_paiement_id": "PAY_001", "code_impact": "HC",
    }
    base.update(overrides)
    return base


def _previsualiser(db_path, dryruns_root, **overrides) -> str:
    res = confirmation.previsualiser(_form(**overrides), db_path=db_path, dryruns_root=dryruns_root)
    assert res["ok"], res["manifest"].get("errors")
    return res["token"]


def test_previsualisation_ok_sans_classeur(tmp_db, dryruns):
    res = confirmation.previsualiser(_form(), db_path=tmp_db, dryruns_root=dryruns)
    assert res["ok"], res["manifest"].get("errors")
    manifest = res["manifest"]
    assert manifest["status"] == "OK"
    assert manifest["pk"].startswith("RESHH-2026-06-")
    assert manifest["preview"]["proprietaire_id"] == "PROP_A"


def test_previsualisation_refuse_logement_inconnu(tmp_db, dryruns):
    res = confirmation.previsualiser(_form(logement_id="LOG_INEXISTANT"),
                                     db_path=tmp_db, dryruns_root=dryruns)
    assert not res["ok"]


def test_confirmation_ecrit_en_sqlite(tmp_db, dryruns):
    token = _previsualiser(tmp_db, dryruns)
    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns, acteur="test")

    assert res.statut == confirmation.SUCCES, res.message
    assert res.reservation_hh_id.startswith("RESHH-")

    conn = get_db(tmp_db)
    try:
        row = conn.execute("SELECT * FROM reservations_hors_hostaway WHERE reservation_hh_id = ?",
                           (res.reservation_hh_id,)).fetchone()
        overrides = conn.execute(
            "SELECT * FROM reservation_hh_overrides WHERE reservation_hh_id = ?",
            (res.reservation_hh_id,)).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["statut"] == "ACTIVE"
    assert row["montant_percu"] == 450.0
    assert row["proprietaire_id"] == "PROP_A"
    assert overrides is not None


def test_confirmation_est_idempotente(tmp_db, dryruns):
    token = _previsualiser(tmp_db, dryruns)
    premier = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    second = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)

    assert premier.reservation_hh_id == second.reservation_hh_id
    conn = get_db(tmp_db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM reservations_hors_hostaway WHERE reservation_hh_id = ?",
            (premier.reservation_hh_id,)).fetchone()["n"]
    finally:
        conn.close()
    assert n == 1


def test_confirmation_token_inconnu(tmp_db, dryruns):
    res = confirmation.confirmer("jamais-vu", db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_TOKEN_INCONNU


def test_confirmation_manifest_refuse_nest_pas_confirmable(tmp_db, dryruns):
    token_res = confirmation.previsualiser(_form(logement_id="LOG_INEXISTANT"),
                                           db_path=tmp_db, dryruns_root=dryruns)
    res = confirmation.confirmer(token_res["token"], db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_MANIFEST_INVALIDE


def test_confirmation_manifest_expire_refuse(tmp_db, dryruns):
    token = _previsualiser(tmp_db, dryruns)
    manifest_path = Path(dryruns) / token / confirmation.MANIFEST_NAME
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["created_at_utc"] = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    manifest_path.write_text(json.dumps(data), encoding="utf-8")

    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_MANIFEST_EXPIRE


def test_confirmation_revalide_metier_mois_cloture_entre_temps(tmp_db, dryruns):
    """Une prévisualisation faite AVANT une clôture ne doit plus être confirmable APRÈS."""
    token = _previsualiser(tmp_db, dryruns)

    conn = get_db(tmp_db)
    try:
        conn.execute("UPDATE ref_cloture_mensuelle SET statut_mois = 'CLOTURE' WHERE mois = ?",
                     ("2026-06",))
        conn.commit()
    finally:
        conn.close()

    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_VALIDATION_PERIMEE


def test_confirmation_persiste_derogation_commission(tmp_db, dryruns):
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, logement_id, "
            "taux_commission, date_debut, date_fin, actif, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("TX_1", "PROP_A", "", "0.15", "2026-01-01", "", "OUI", fx.IMPORT_TEST))
        conn.commit()
    finally:
        conn.close()

    token = _previsualiser(
        tmp_db, dryruns,
        taux_commission_override_pct="20", motif_override_taux_commission="negociation",
        confirmation_override_taux_commission="oui")
    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.SUCCES, res.message

    conn = get_db(tmp_db)
    try:
        row = conn.execute(
            "SELECT * FROM reservation_hh_overrides WHERE reservation_hh_id = ?",
            (res.reservation_hh_id,)).fetchone()
    finally:
        conn.close()
    # LOG_A1 a déjà un taux spécifique au logement (0.19, via semer_parc_standard) : il prime sur
    # le taux propriétaire (0.15) inséré ci-dessus, exactement comme `resolve_taux_commission`
    # (logement > propriétaire).
    assert row["taux_commission_standard"] == 0.19
    assert row["taux_commission_override"] == 0.20
    assert row["motif_override_taux_commission"] == "negociation"

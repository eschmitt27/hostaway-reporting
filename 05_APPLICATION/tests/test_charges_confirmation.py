"""APP-3b — Confirmation d'une charge : token de prévisualisation → écriture SQLite.

Remplace l'ancienne suite (transaction/verrou/journal Excel, sauvegarde/rollback de fichier) :
l'écriture est désormais une transaction SQLite (`charges_saisie_service.creer`), atomique par
construction. Ces tests couvrent les gardes qui restent pertinentes : token inconnu, manifest altéré
ou expiré, idempotence (double confirmation), et revalidation métier sur l'état ACTUEL (mois clôturé
entre-temps).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import charges_confirmation_service as confirmation
from app.services import charges_preview_service as prev


@pytest.fixture(autouse=True)
def _referentiel(tmp_db, tmp_path, monkeypatch):
    import openpyxl

    fx.semer_parc_standard(tmp_db)
    fx.semer_referentiel_charges(tmp_db)
    monkeypatch.setattr(cfg, "REF_SETUP", tmp_path / "REF_Setup_ABSENT.xlsm")

    original = openpyxl.load_workbook

    def garde(chemin, *a, **kw):
        raise AssertionError(f"Classeur ouvert au runtime de la confirmation : {chemin}")

    monkeypatch.setattr(openpyxl, "load_workbook", garde)
    return tmp_db


@pytest.fixture
def dryruns(tmp_path: Path) -> Path:
    return tmp_path / "dryruns"


def _form(**overrides) -> dict:
    base = {
        "date_charge": "2026-06-15", "montant": "100.00", "categorie_charge_id": "CHG_017",
        "code_impact": "IC", "mode_paiement_id": "PAY_001", "refacturable": "NON",
    }
    base.update(overrides)
    return base


def _previsualiser(db_path, dryruns_root, **overrides) -> str:
    res = prev.previsualiser(_form(**overrides), db_path=db_path, dryruns_root=dryruns_root)
    assert res["ok"], res["manifest"].get("errors")
    return res["token"]


def test_confirmation_ecrit_en_sqlite(tmp_db, dryruns):
    token = _previsualiser(tmp_db, dryruns)
    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns, acteur="test")

    assert res.statut == confirmation.SUCCES, res.message
    assert res.charge_id.startswith("CHG-")

    from app.db.connection import get_db
    conn_check = get_db(tmp_db)
    try:
        row = conn_check.execute("SELECT * FROM charges WHERE charge_id = ?",
                                 (res.charge_id,)).fetchone()
    finally:
        conn_check.close()
    assert row is not None
    assert row["statut"] == "ACTIVE"
    assert row["statut_controle"] == "A_CONTROLER"
    assert row["montant"] == 100.0


def test_confirmation_est_idempotente(tmp_db, dryruns):
    """Rejouer la confirmation du même token ne crée pas une seconde charge."""
    token = _previsualiser(tmp_db, dryruns)
    premier = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    second = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)

    assert premier.charge_id == second.charge_id
    from app.db.connection import get_db
    conn = get_db(tmp_db)
    try:
        n = conn.execute("SELECT COUNT(*) AS n FROM charges WHERE charge_id = ?",
                         (premier.charge_id,)).fetchone()["n"]
    finally:
        conn.close()
    assert n == 1


def test_confirmation_token_inconnu(tmp_db, dryruns):
    res = confirmation.confirmer("jamais-vu", db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_TOKEN_INCONNU


def test_confirmation_manifest_refuse_nest_pas_confirmable(tmp_db, dryruns):
    token_res = prev.previsualiser(_form(montant=""), db_path=tmp_db, dryruns_root=dryruns)
    res = confirmation.confirmer(token_res["token"], db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_MANIFEST_INVALIDE


def test_confirmation_manifest_altere_refuse(tmp_db, dryruns):
    token = _previsualiser(tmp_db, dryruns)
    manifest_path = Path(dryruns) / token / prev.MANIFEST_NAME
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["form_data"]["montant"] = "999999"          # falsifié après le sceau
    manifest_path.write_text(json.dumps(data), encoding="utf-8")

    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_MANIFEST_ALTERE


def test_confirmation_manifest_expire_refuse(tmp_db, dryruns):
    token = _previsualiser(tmp_db, dryruns)
    manifest_path = Path(dryruns) / token / prev.MANIFEST_NAME
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    vieux = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    data["created_at_utc"] = vieux
    data["integrite"] = prev.sceller_manifest(data)
    manifest_path.write_text(json.dumps(data), encoding="utf-8")

    res = confirmation.confirmer(token, db_path=tmp_db, dryruns_root=dryruns)
    assert res.statut == confirmation.REFUSE
    assert res.code == confirmation.E_MANIFEST_EXPIRE


def test_confirmation_revalide_metier_mois_cloture_entre_temps(tmp_db, dryruns):
    """Une prévisualisation faite AVANT une clôture ne doit plus être confirmable APRÈS."""
    token = _previsualiser(tmp_db, dryruns)

    from app.db.connection import get_db
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

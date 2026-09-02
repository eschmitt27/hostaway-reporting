"""Garde-fou anti-Excel — `hostaway_cleaning_tasks_actualisation_service.actualiser()` doit rester
API → SQLite direct, jamais API → Excel → SQLite (mission « supprimer le dernier effet de bord
Excel »). Un `.xlsx` créé/modifié par ce chemin serait une régression vers l'ancien pont.

Aucun appel réseau : `_extract_cleaning_tasks` (lot1_hostaway_extract) est remplacé par un double.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))

LOT1 = _TRAVAIL / "lot1_hostaway_extract.py"
pytestmark = pytest.mark.skipif(not LOT1.exists(), reason="lot1 absent")

from app.services import hostaway_cleaning_tasks_actualisation_service as svc  # noqa: E402


def _hash_ou_absent(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_actualiser_ne_cree_ni_ne_modifie_aucun_xlsx(tmp_db, monkeypatch, tmp_path):
    """Le cas central : après un `actualiser()` réussi, tout `.xlsx` déjà présent sous
    02_TRAVAIL/Lot1_Hostaway reste hash-identique, et aucun nouveau `.xlsx` n'apparaît."""
    pytest.importorskip("pandas")
    import lot1_hostaway_extract as lot1

    monkeypatch.setattr(svc, "_credentials",
                        lambda: ("https://exemple.test", "cid", "csec", "aid"))

    def faux_extract(client, date_from, detector, log):
        return ([
            {"task_id": 1, "reservation_id": 10, "listingMapId": 20, "task_type": None,
             "status": "completed", "scheduled_date": "2026-03-15", "assignee": None,
             "title": "Ménage Test", "canStartFrom": "2026-03-15 10:00:00",
             "assigneeUserId": None, "cost": None, "h6_note": "x",
             "extrait_le": "2026-03-15T00:00:00Z", "ROW_HASH": "abc123"},
        ], "OK")

    monkeypatch.setattr(lot1, "_extract_cleaning_tasks", faux_extract)

    dossier_lot1 = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "Lot1_Hostaway"
    xlsx_avant = {p: _hash_ou_absent(p) for p in dossier_lot1.glob("*.xlsx")} if dossier_lot1.exists() else {}

    res = svc.actualiser(db_path=tmp_db)

    assert res["ok"] is True
    assert res["nb_taches"] == 1

    xlsx_apres = {p: _hash_ou_absent(p) for p in dossier_lot1.glob("*.xlsx")} if dossier_lot1.exists() else {}
    assert xlsx_apres == xlsx_avant, "aucun .xlsx ne doit être créé, supprimé ou modifié"

    from app.db.connection import get_db
    conn = get_db(tmp_db)
    try:
        row = conn.execute(
            "SELECT task_id, title, can_start_from FROM hostaway_cleaning_tasks LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row["task_id"] == "1"
    assert row["title"] == "Ménage Test"
    assert row["can_start_from"] == "2026-03-15 10:00:00"


def test_credentials_absentes_refuse_proprement_sans_toucher_excel(tmp_db, monkeypatch):
    monkeypatch.setattr(svc, "_credentials", lambda: None)
    dossier_lot1 = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "Lot1_Hostaway"
    xlsx_avant = {p: _hash_ou_absent(p) for p in dossier_lot1.glob("*.xlsx")} if dossier_lot1.exists() else {}

    res = svc.actualiser(db_path=tmp_db)

    assert res["ok"] is False
    assert res["code"] == svc.E_CREDENTIALS_ABSENTES
    xlsx_apres = {p: _hash_ou_absent(p) for p in dossier_lot1.glob("*.xlsx")} if dossier_lot1.exists() else {}
    assert xlsx_apres == xlsx_avant


def test_aucun_appel_runtime_a_master_excel_hors_export_explicite():
    """Garde-fou textuel : `actualiser()` ne doit référencer ni `subprocess` ni le nom du fichier
    Discovery — seule `exporter_cleaning_tasks_excel()` (jamais appelée automatiquement) le fait."""
    source = (Path(cfg.APP_ROOT) / "app" / "services"
              / "hostaway_cleaning_tasks_actualisation_service.py").read_text(encoding="utf-8")
    corps_actualiser = source[source.index("def actualiser("):source.index("class _LogRelais")]
    assert "subprocess" not in corps_actualiser
    assert "MASTER_FACT_HA_CleaningTasks_Discovery" not in corps_actualiser
    assert ".xlsx" not in corps_actualiser


def test_exporter_est_le_seul_point_qui_ecrit_le_xlsx():
    """Hors docstring de module (documentation), le nom du fichier Discovery ne doit apparaître
    que dans le corps de `exporter_cleaning_tasks_excel()`."""
    source = (Path(cfg.APP_ROOT) / "app" / "services"
              / "hostaway_cleaning_tasks_actualisation_service.py").read_text(encoding="utf-8")
    corps_export = source[source.index("def exporter_cleaning_tasks_excel("):]
    reste = source[:source.index("def exporter_cleaning_tasks_excel(")]
    corps_module_docstring_fin = reste.index('"""', reste.index('"""') + 3) + 3
    reste_hors_docstring = reste[corps_module_docstring_fin:]
    assert "MASTER_FACT_HA_CleaningTasks_Discovery" not in reste_hors_docstring
    assert "MASTER_FACT_HA_CleaningTasks_Discovery" in corps_export

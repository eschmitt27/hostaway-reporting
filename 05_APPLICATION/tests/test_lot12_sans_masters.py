"""Lot12 — test bloquant : `lot12_prefactures_service.construire()` n'ouvre AUCUN classeur Excel,
et les lecteurs applicatifs (`factures_entetes`/`dashboard_facturation`/`controles_factures`) ne
lisent plus `MASTER_FACT_Proprietaires.xlsx`. Même principe que `test_lot11_sans_masters.py`.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _interdire_tout_excel(monkeypatch):
    import openpyxl

    def garde(chemin, *a, **kw):
        raise AssertionError(f"Lot12 ne doit ouvrir aucun classeur Excel : {chemin}")

    monkeypatch.setattr(openpyxl, "load_workbook", garde)


def test_construire_sans_excel_base_vide(tmp_db):
    from app.services import lot12_prefactures_service as svc

    res = svc.construire(db_path=tmp_db)
    assert res["ok"], res


def test_construire_sans_excel_avec_lot10_et_referentiel(tmp_db):
    from app.db.connection import get_db
    from app.services import lot12_prefactures_service as svc

    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('L10-T','SUCCES',1)")
        conn.execute(
            "INSERT INTO lot10_net_reglement (run_id, mois, logement_id, proprietaire_id, "
            "montant_du_conciergerie, reste_a_payer_conciergerie, nb_reservations) "
            "VALUES ('L10-T','2026-06','LOG_A','PROP_A', 100, 100, 2)")
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES ('IMP','2026-01-01T00:00:00Z','t','t','IMPORTE')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, mode_facturation, actif, import_id) "
            "VALUES ('PROP_A', 'MENSUEL', 'OUI', 'IMP')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, actif, statut_parc, import_id) "
            "VALUES ('LOG_A', 'OUI', 'GERE', 'IMP')")
        conn.commit()
    finally:
        conn.close()

    res = svc.construire(db_path=tmp_db)
    assert res["ok"], res
    assert res["nb_entetes"] == 1


def test_reader_proprietaires_reglements_lot12_sans_excel(tmp_db):
    """Les 3 lecteurs Lot12 lisent la table alimentée par le moteur SQLite natif, jamais un
    classeur, même après un `construire()` réel."""
    from app.readers import proprietaires_reglements_reader as reader
    from app.services import lot12_prefactures_service as svc

    svc.construire(db_path=tmp_db)
    reader.vider_cache()
    assert reader.factures_entetes(db_path=tmp_db) is not None
    assert reader.dashboard_facturation(db_path=tmp_db) is not None
    assert reader.controles_factures(db_path=tmp_db) is not None
    reader.vider_cache()

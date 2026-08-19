"""Lot11 — test bloquant : `controles_lot11_service.construire()` n'ouvre AUCUN classeur Excel.

Contrairement à Lot9/Lot10 (qui interceptaient une liste fermée de masters), `controles_lot11_
service` ne dépend plus d'AUCUN fichier Excel pour les groupes de contrôle qu'il couvre (réservations,
payouts/anomalies Hostaway, flux Lot9, résultats Lot10, référentiel REF_Setup, banque) : toutes ces
chaînes sont déjà SQLite. Preuve active par interception globale d'`openpyxl.load_workbook` — même
principe que `test_menages_sans_excel.py`/`test_lot9_sans_master_calc_flux.py`/
`test_lot10_sans_masters.py`, mais sans liste de fichiers autorisés puisqu'aucun n'est légitime ici.

Les groupes non portés (AirCover, ajustements post-clôture, sources vides Charges/M04/Acomptes/IK,
Lot7C avantages, ménages externes 6f, caisse théorique) restent hors périmètre de ce service — cf.
le docstring de `controles_lot11_service.py` — et continuent de passer par le moteur legacy
(`controles_runner_service`/`controles_lot11_adapter`), non concerné par ce test.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _interdire_tout_excel(monkeypatch):
    import openpyxl

    def garde(chemin, *a, **kw):
        raise AssertionError(f"controles_lot11_service ne doit ouvrir aucun classeur Excel : {chemin}")

    monkeypatch.setattr(openpyxl, "load_workbook", garde)


def test_construire_sans_excel_base_vide(tmp_db):
    from app.services import controles_lot11_service as svc

    res = svc.construire(db_path=tmp_db)
    assert res["ok"], res
    assert res["nb_constats"] >= 0


def test_construire_sans_excel_avec_referentiel(tmp_db):
    """Un référentiel REF_Setup importé (SQLite, migration 0029) ne déclenche aucune lecture Excel."""
    from app.db.connection import get_db
    from app.services import controles_lot11_service as svc

    conn = get_db(tmp_db)
    try:
        if _table_existe(conn, "ref_setup_imports"):
            conn.execute(
                "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
                "empreinte_source, statut) VALUES ('IMP-TEST', '2026-01-01T00:00:00Z', 'test', "
                "'test', 'IMPORTE')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, mode_facturation, actif, import_id) "
            "VALUES ('PROP_TEST', 'MENSUEL', 'OUI', 'IMP-TEST')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, actif, statut_parc, import_id) "
            "VALUES ('LOG_TEST', 'OUI', 'GERE', 'IMP-TEST')")
        conn.commit()
    finally:
        conn.close()

    res = svc.construire(db_path=tmp_db)
    assert res["ok"], res


def _table_existe(conn, nom: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nom,)
    ).fetchone() is not None


def test_reader_menages_controles_lot11_sans_excel(tmp_db):
    """`menages_reader.controles_lot11()` lit la table alimentée par le moteur SQLite natif,
    jamais un classeur, même après un `construire()` réel. `tmp_db` patche déjà `cfg.DB_PATH`."""
    from app.readers import menages_reader as reader
    from app.services import controles_lot11_service as svc

    svc.construire(db_path=tmp_db)
    assert reader.controles_lot11() is not None

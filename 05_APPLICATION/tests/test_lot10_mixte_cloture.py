"""Lot10 — ne calcule que les mois entièrement terminés (mission « exclure mois en cours et
futurs de Lot10 »).

Étend la mission précédente (« arrêter le backfill historique ménages », qui excluait déjà les
mois CLOTURE du périmètre courant) : un mois qui n'est ni clôturé ni strictement antérieur au mois
courant n'est pas non plus économiquement définitif — les factures et déclarations arrivent en fin
de mois. Seul un mois strictement < mois courant ET non clôturé est recalculé. Le mois courant et
tout mois futur sont exclus au même titre qu'un mois clôturé, avec un motif distinct.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

_TRAVAIL = Path(__file__).resolve().parents[2] / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pytest.importorskip("pandas")

import lot10_calculer_resultats as l10  # noqa: E402

from app.db.connection import get_db

DATE_REF = datetime(2026, 9, 4)


def _cloturer(db_path, mois):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'CLOTURE', 'TEST')", (mois,))
        conn.commit()
    finally:
        conn.close()


def _df(mois, logement_id="LOG_0001", proprietaire_id="PROP_0001", **kw):
    ligne = {"mois": mois, "logement_id": logement_id, "proprietaire_id": proprietaire_id}
    ligne.update(kw)
    return ligne


def _vides():
    return (pd.DataFrame(columns=list(l10._T_COMMISSIONS)),
           pd.DataFrame(columns=list(l10._T_RESULTATS)),
           pd.DataFrame(columns=list(l10._T_RESULTATS)),
           pd.DataFrame(columns=list(l10._T_RESULTATS)),
           pd.DataFrame(columns=list(l10._T_EXPLOITATION)),
           pd.DataFrame(columns=list(l10._T_REGLEMENT)),
           pd.DataFrame(columns=list(l10._T_VUE_MOIS)))


def _classifier(conn, mois_list, date_reference=DATE_REF):
    return l10.classifier_mois_lot10(conn, mois_list, date_reference=date_reference)


# ── Classification (mission §5, tests 1-6) — date de référence = 2026-09-04 ─────────────────────

def test_1_mois_termine_non_cloture_classe_recalcule(tmp_db):
    conn = get_db(tmp_db)
    try:
        r = _classifier(conn, ["2026-08"])
    finally:
        conn.close()
    assert r["2026-08"] == {"classification": l10.CLASS_MOIS_TERMINE_OUVERT,
                            "mode": l10.MODE_RECALCULE}


def test_2_mois_courant_exclu(tmp_db):
    conn = get_db(tmp_db)
    try:
        r = _classifier(conn, ["2026-09"])
    finally:
        conn.close()
    assert r["2026-09"] == {"classification": l10.CLASS_MOIS_EN_COURS,
                            "mode": l10.MODE_EXCLU_EN_COURS}


def test_3_mois_futur_proche_exclu(tmp_db):
    conn = get_db(tmp_db)
    try:
        r = _classifier(conn, ["2026-10"])
    finally:
        conn.close()
    assert r["2026-10"] == {"classification": l10.CLASS_FUTUR, "mode": l10.MODE_EXCLU_FUTUR}


def test_4_mois_futur_lointain_exclu(tmp_db):
    conn = get_db(tmp_db)
    try:
        r = _classifier(conn, ["2027-05"])
    finally:
        conn.close()
    assert r["2027-05"] == {"classification": l10.CLASS_FUTUR, "mode": l10.MODE_EXCLU_FUTUR}


def test_5_mois_cloture_exclu_prioritaire_sur_la_date(tmp_db):
    """La clôture prime sur la comparaison de date — un mois clôturé reste exclu même s'il est
    strictement antérieur au mois courant."""
    _cloturer(tmp_db, "2026-05")
    conn = get_db(tmp_db)
    try:
        r = _classifier(conn, ["2026-05"])
    finally:
        conn.close()
    assert r["2026-05"] == {"classification": l10.CLASS_CLOTURE, "mode": l10.MODE_EXCLU_CLOTURE}


def test_6_date_reference_posterieure_rend_septembre_recalculable(tmp_db):
    """Testabilité de la date de référence (§2) : au 2026-10-01, septembre devient un mois
    terminé, donc recalculable — rien de figé en dur."""
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-09"], date_reference=datetime(2026, 10, 1))
    finally:
        conn.close()
    assert r["2026-09"] == {"classification": l10.CLASS_MOIS_TERMINE_OUVERT,
                            "mode": l10.MODE_RECALCULE}


# ── Exclusion (mission §5, tests 7-8) ────────────────────────────────────────────────────────────

def test_7_aucune_ligne_septembre_ou_future_dans_resultats(tmp_db):
    df_reel = pd.DataFrame([
        _df("2026-08", vision="REEL", total_produits=100.0, total_charges=0.0, resultat=100.0,
           nb_flux=1, commentaire=""),
        _df("2026-09", vision="REEL", total_produits=200.0, total_charges=0.0, resultat=200.0,
           nb_flux=1, commentaire=""),
        _df("2026-10", vision="REEL", total_produits=300.0, total_charges=0.0, resultat=300.0,
           nb_flux=1, commentaire=""),
        _df("2027-05", vision="REEL", total_produits=400.0, total_charges=0.0, resultat=400.0,
           nb_flux=1, commentaire=""),
    ])
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    df_comm, _, _, _, df_exploit, df_reg, df_vue = _vides()

    conn = get_db(tmp_db)
    try:
        classifications = _classifier(conn, ["2026-08", "2026-09", "2026-10", "2027-05"])
    finally:
        conn.close()

    (_c, df_reel_out, *_rest, prov) = l10.exclure_mois_clotures(
        classifications, df_comm, df_reel, df_vide, df_vide, df_exploit, df_reg, df_vue)

    assert list(df_reel_out["mois"]) == ["2026-08"]


def test_8_aucune_ligne_septembre_ou_future_dans_les_autres_tables(tmp_db):
    df_comm = pd.DataFrame([_df("2026-09", flux_source_pk="X", reservation_calc_id="R1")])
    df_exploit = pd.DataFrame([_df("2026-10", revenu_net_exploitation=1.0)])
    df_reg = pd.DataFrame([_df("2027-05", montant_du_conciergerie=1.0, nb_reservations=1)])
    df_vue = pd.DataFrame([{"mois": "2026-09", "proprietaire_id": "PROP_0001",
                          "montant_du_conciergerie": 1.0}])
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))

    conn = get_db(tmp_db)
    try:
        classifications = _classifier(conn, ["2026-09", "2026-10", "2027-05"])
    finally:
        conn.close()

    (df_comm_out, _r, _cpt, _hc, df_exploit_out, df_reg_out, df_vue_out, prov) = \
        l10.exclure_mois_clotures(classifications, df_comm, df_vide, df_vide, df_vide, df_exploit,
                                  df_reg, df_vue)

    assert len(df_comm_out) == 0
    assert len(df_exploit_out) == 0
    assert len(df_reg_out) == 0
    assert len(df_vue_out) == 0


# ── Provenance ────────────────────────────────────────────────────────────────────────────────

def test_provenance_distingue_les_trois_motifs_d_exclusion(tmp_db):
    _cloturer(tmp_db, "2026-05")
    conn = get_db(tmp_db)
    try:
        classifications = _classifier(conn, ["2026-05", "2026-08", "2026-09", "2026-10"])
    finally:
        conn.close()
    df_comm, df_reel, df_vide, _, df_exploit, df_reg, df_vue = _vides()

    (*_frames, prov) = l10.exclure_mois_clotures(
        classifications, df_comm, df_reel, df_vide, df_vide, df_exploit, df_reg, df_vue)

    par_mois = {r["mois"]: r["mode_traitement"] for r in prov}
    assert par_mois["2026-05"] == l10.MODE_EXCLU_CLOTURE
    assert par_mois["2026-08"] == l10.MODE_RECALCULE
    assert par_mois["2026-09"] == l10.MODE_EXCLU_EN_COURS
    assert par_mois["2026-10"] == l10.MODE_EXCLU_FUTUR


def test_provenance_enregistree_via_ecrire_sqlite(tmp_db):
    provenance_rows = [
        {"mois": "2026-08", "classification": l10.CLASS_MOIS_TERMINE_OUVERT,
         "mode_traitement": l10.MODE_RECALCULE, "source_run_id": None, "source_archive_id": None},
        {"mois": "2026-09", "classification": l10.CLASS_MOIS_EN_COURS,
         "mode_traitement": l10.MODE_EXCLU_EN_COURS, "source_run_id": None,
         "source_archive_id": None},
        {"mois": "2026-10", "classification": l10.CLASS_FUTUR,
         "mode_traitement": l10.MODE_EXCLU_FUTUR, "source_run_id": None,
         "source_archive_id": None},
    ]
    df_com, df_res, df_exp, df_reg, df_vue = (
        pd.DataFrame(columns=list(l10._T_COMM_AC)), pd.DataFrame(columns=list(l10._T_RESULTATS)),
        pd.DataFrame(columns=list(l10._T_EXPLOITATION)), pd.DataFrame(columns=list(l10._T_REGLEMENT)),
        pd.DataFrame(columns=list(l10._T_VUE_MOIS)))
    df_comm = pd.DataFrame(columns=list(l10._T_COMMISSIONS))

    resultat = l10.ecrire_sqlite(
        tmp_db, df_comm, df_com, df_res, df_res, df_res, df_exp, df_reg, df_vue,
        provenance_rows=provenance_rows)

    assert resultat["ecrit"] is True
    conn = get_db(tmp_db)
    try:
        rows = conn.execute(
            "SELECT mois, mode_traitement FROM lot10_run_mois_provenance "
            "WHERE run_id = ? ORDER BY mois", (resultat["run_id"],)).fetchall()
    finally:
        conn.close()
    assert [tuple(r) for r in rows] == [
        ("2026-08", l10.MODE_RECALCULE),
        ("2026-09", l10.MODE_EXCLU_EN_COURS),
        ("2026-10", l10.MODE_EXCLU_FUTUR),
    ]


def test_zero_excel(tmp_db, monkeypatch):
    import openpyxl

    def _piege(*a, **k):
        raise AssertionError("OUVERTURE EXCEL DETECTEE : " + str(a))

    monkeypatch.setattr(openpyxl, "load_workbook", _piege)

    conn = get_db(tmp_db)
    try:
        classifications = _classifier(conn, ["2026-08", "2026-09", "2026-10"])
    finally:
        conn.close()

    df_comm, df_reel, df_vide, _, df_exploit, df_reg, df_vue = _vides()
    l10.exclure_mois_clotures(classifications, df_comm, df_reel, df_vide, df_vide, df_exploit,
                              df_reg, df_vue)

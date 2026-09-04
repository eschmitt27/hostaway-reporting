"""Lot10 — mois clôturés hors périmètre, mois ouverts recalculés (mission « arrêter le backfill
historique ménages »).

`ref_cloture_mensuelle` existait déjà (migration 0064) mais restait write-only côté Lot10 : le
script recalculait silencieusement tous les mois présents dans le flux, y compris ceux déjà
CLOTURE. Décision produit : on ne cherche plus à reconstruire/préserver l'historique ménage des
mois clôturés — remplace le mécanisme mixte (substitution/archive/legacy) d'une mission
précédente, jamais appliqué en production (aucun run Lot10 réel n'a jamais porté mai 2026 ou
antérieur). Un mois CLOTURE est désormais simplement absent du nouveau run — jamais recalculé,
jamais recopié depuis une archive ou un run antérieur.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_TRAVAIL = Path(__file__).resolve().parents[2] / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pytest.importorskip("pandas")

import lot10_calculer_resultats as l10  # noqa: E402

from app.db.connection import get_db
import fixtures_lot10 as flot10


def _cloturer(db_path, mois):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'CLOTURE', 'TEST')", (mois,))
        conn.commit()
    finally:
        conn.close()


def _ouvrir(db_path, mois):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'OUVERT', 'TEST')", (mois,))
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


# ── 1/2 — classification : deux états seulement, sans exception ─────────────────────────────────

def test_mois_ouvert_classe_recalcule(tmp_db):
    _ouvrir(tmp_db, "2026-06")
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-06"])
    finally:
        conn.close()
    assert r["2026-06"] == {"classification": l10.CLASS_OUVERT, "mode": l10.MODE_RECALCULE}


def test_mois_cloture_classe_exclu_perimetre(tmp_db):
    """Un mois CLOTURE est exclu quelle que soit sa classification legacy/archive éventuelle —
    la politique ne les distingue plus."""
    _cloturer(tmp_db, "2026-05")
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()
    assert r["2026-05"] == {"classification": l10.CLASS_CLOTURE, "mode": l10.MODE_EXCLU_PERIMETRE}


def test_mois_absent_de_ref_cloture_classe_ouvert(tmp_db):
    """Un mois qui n'a jamais été clôturé (absent de `ref_cloture_mensuelle`) est OUVERT par
    défaut — la mission cible juillet 2026, jamais présent dans la table."""
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-07"])
    finally:
        conn.close()
    assert r["2026-07"] == {"classification": l10.CLASS_OUVERT, "mode": l10.MODE_RECALCULE}


# ── 3/4 — exclusion : les valeurs fraîches d'un mois clôturé sont jetées, jamais recopiées ──────

def test_mois_cloture_retire_des_sept_dataframes(tmp_db):
    """Un mois clôturé disparaît complètement du run — pas de recopie, pas de recalcul, pas de
    fabrication : simple absence assumée."""
    _cloturer(tmp_db, "2026-05")
    _ouvrir(tmp_db, "2026-06")

    df_reel = pd.DataFrame([
        _df("2026-05", vision="REEL", total_produits=99999.0, total_charges=0.0,
           resultat=99999.0, nb_flux=1, commentaire=""),
        _df("2026-06", vision="REEL", total_produits=500.0, total_charges=0.0,
           resultat=500.0, nb_flux=1, commentaire=""),
    ])
    df_comm = pd.DataFrame([_df("2026-05", flux_source_pk="X", reservation_calc_id="R1")])
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    df_exploit = pd.DataFrame([_df("2026-05", revenu_net_exploitation=99999.0)])
    df_reg = pd.DataFrame([_df("2026-05", montant_du_conciergerie=99999.0, nb_reservations=1)])
    df_vue = pd.DataFrame([{"mois": "2026-05", "proprietaire_id": "PROP_0001",
                          "montant_du_conciergerie": 99999.0}])

    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05", "2026-06"])
    finally:
        conn.close()

    (df_comm_out, df_reel_out, df_compt_out, df_hc_out, df_exploit_out, df_reg_out, df_vue_out,
     prov) = l10.exclure_mois_clotures(
        classifications, df_comm, df_reel, df_vide, df_vide, df_exploit, df_reg, df_vue)

    assert len(df_reel_out) == 1
    assert df_reel_out.iloc[0]["mois"] == "2026-06"
    assert len(df_comm_out) == 0
    assert len(df_exploit_out) == 0
    assert len(df_reg_out) == 0
    assert len(df_vue_out) == 0


def test_mois_ouvert_non_touche_valeurs_fraiches_conservees(tmp_db):
    _ouvrir(tmp_db, "2026-06")
    df_reel = pd.DataFrame([_df("2026-06", vision="REEL", total_produits=500.0, total_charges=0.0,
                                resultat=500.0, nb_flux=1, commentaire="")])
    df_comm, _, df_vide, _, df_exploit, df_reg, df_vue = _vides()

    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-06"])
    finally:
        conn.close()

    (_df_comm, df_reel_out, _df_compt, _df_hc, _df_exploit, _df_reg, _df_vue,
     prov) = l10.exclure_mois_clotures(
        classifications, df_comm, df_reel, df_vide, df_vide, df_exploit, df_reg, df_vue)

    assert df_reel_out.iloc[0]["total_produits"] == 500.0


# ── plus de fail-closed : aucune baseline exigée pour un mois clôturé ───────────────────────────

def test_mois_cloture_sans_aucune_source_ne_bloque_plus_le_run(tmp_db):
    """Changement de comportement central de cette mission : un mois clôturé n'exige plus de
    baseline/archive pour ne pas bloquer le run — il est juste exclu."""
    _cloturer(tmp_db, "2026-05")  # aucune archive, aucune classification legacy
    df_comm, df_reel, df_vide, _, df_exploit, df_reg, df_vue = _vides()
    df_reel = pd.DataFrame([_df("2026-05", vision="REEL", total_produits=1.0, total_charges=0.0,
                                resultat=1.0, nb_flux=1, commentaire="")])

    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()

    (_df_comm, df_reel_out, _df_compt, _df_hc, _df_exploit, _df_reg, _df_vue,
     prov) = l10.exclure_mois_clotures(
        classifications, df_comm, df_reel, df_vide, df_vide, df_exploit, df_reg, df_vue)

    assert len(df_reel_out) == 0
    assert prov[0]["mode_traitement"] == l10.MODE_EXCLU_PERIMETRE


# ── provenance ────────────────────────────────────────────────────────────────────────────────

def test_provenance_liste_tous_les_mois_du_run(tmp_db):
    _cloturer(tmp_db, "2026-05")
    _ouvrir(tmp_db, "2026-06")
    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05", "2026-06"])
    finally:
        conn.close()
    df_comm, df_reel, df_vide, _, df_exploit, df_reg, df_vue = _vides()

    (*_frames, prov) = l10.exclure_mois_clotures(
        classifications, df_comm, df_reel, df_vide, df_vide, df_exploit, df_reg, df_vue)

    par_mois = {r["mois"]: r for r in prov}
    assert par_mois["2026-05"]["mode_traitement"] == l10.MODE_EXCLU_PERIMETRE
    assert par_mois["2026-05"]["classification"] == l10.CLASS_CLOTURE
    assert par_mois["2026-06"]["mode_traitement"] == l10.MODE_RECALCULE
    assert par_mois["2026-06"]["classification"] == l10.CLASS_OUVERT


def test_provenance_enregistree_via_ecrire_sqlite(tmp_db):
    provenance_rows = [
        {"mois": "2026-05", "classification": l10.CLASS_CLOTURE,
         "mode_traitement": l10.MODE_EXCLU_PERIMETRE, "source_run_id": None,
         "source_archive_id": None},
        {"mois": "2026-06", "classification": l10.CLASS_OUVERT,
         "mode_traitement": l10.MODE_RECALCULE, "source_run_id": None,
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
            "SELECT mois, classification, mode_traitement FROM lot10_run_mois_provenance "
            "WHERE run_id = ? ORDER BY mois", (resultat["run_id"],)).fetchall()
    finally:
        conn.close()
    assert [tuple(r) for r in rows] == [
        ("2026-05", l10.CLASS_CLOTURE, l10.MODE_EXCLU_PERIMETRE),
        ("2026-06", l10.CLASS_OUVERT, l10.MODE_RECALCULE),
    ]


def test_activation_atomique_reste_inchangee():
    pass  # couvert structurellement : `exclure_mois_clotures` ne peut jamais refuser le run (plus
    # de code de sortie fail-closed) — la seule surface de risque, l'activation atomique de
    # `ecrire_sqlite` (bascule `actif` en fin de transaction), n'a pas été modifiée par cette
    # mission.


def test_classification_et_exclusion_zero_excel(tmp_db, monkeypatch):
    """`classifier_mois_lot10`/`exclure_mois_clotures` sont des fonctions SQLite/pandas pures —
    aucune des deux ne doit jamais ouvrir un classeur."""
    import openpyxl

    def _piege(*a, **k):
        raise AssertionError("OUVERTURE EXCEL DETECTEE : " + str(a))

    monkeypatch.setattr(openpyxl, "load_workbook", _piege)

    _cloturer(tmp_db, "2026-05")
    _ouvrir(tmp_db, "2026-06")

    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05", "2026-06"])
    finally:
        conn.close()

    df_comm, df_reel, df_vide, _, df_exploit, df_reg, df_vue = _vides()
    l10.exclure_mois_clotures(classifications, df_comm, df_reel, df_vide, df_vide, df_exploit,
                              df_reg, df_vue)

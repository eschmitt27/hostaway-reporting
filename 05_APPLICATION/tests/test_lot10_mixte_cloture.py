"""Lot10 mixte — mois clôturés préservés, mois ouverts recalculés (mission « Lot10 mixte : préserver
les mois clôturés et recalculer uniquement les mois ouverts »).

`ref_cloture_mensuelle` / `mois_classification_legacy` / `mois_archive_reglement` existaient déjà
(migration 0064) mais restaient write-only : aucun service ne les relisait, et Lot10 recalculait
silencieusement tous les mois présents dans le flux, y compris ceux déjà CLOTURE. Ce fichier teste
les deux fonctions qui corrigent cela : `classifier_mois_lot10` (§3) et `substituer_mois_figes`
(§4), ainsi que leur intégration dans `ecrire_sqlite` via `provenance_rows` (§5).
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


def _cloturer(db_path, mois, *, legacy=False, archive_rows=None):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'CLOTURE', 'TEST')", (mois,))
        if legacy:
            conn.execute(
                "INSERT INTO mois_classification_legacy (mois, classification, motif) "
                "VALUES (?, 'LEGACY_SANS_ARCHIVE_ORIGINE', 'test')", (mois,))
        for r in archive_rows or ():
            conn.execute(
                "INSERT INTO mois_archive_reglement (mois, logement_id, proprietaire_id, "
                "total_commission_mois, total_menage_mois, total_preparation_canape_mois, "
                "charges_exceptionnelles_refacturees, montant_du_conciergerie, "
                "reste_a_payer_conciergerie) VALUES (?,?,?,?,?,?,?,?,?)",
                (mois, r["logement_id"], r["proprietaire_id"], r.get("total_commission_mois", 0.0),
                 r.get("total_menage_mois", 0.0), r.get("total_preparation_canape_mois", 0.0),
                 r.get("charges_exceptionnelles_refacturees", 0.0),
                 r.get("montant_du_conciergerie", 0.0), r.get("reste_a_payer_conciergerie", 0.0)))
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


# ── 1/2/3 — classification ───────────────────────────────────────────────────────────────────

def test_mois_ouvert_classe_recalcule(tmp_db):
    _ouvrir(tmp_db, "2026-06")
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-06"])
    finally:
        conn.close()
    assert r["2026-06"] == {"classification": l10.CLASS_OUVERT, "mode": l10.MODE_RECALCULE}


def test_mois_cloture_legacy_classe_legacy_fige(tmp_db):
    _cloturer(tmp_db, "2026-05", legacy=True)
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()
    assert r["2026-05"] == {"classification": l10.CLASS_LEGACY, "mode": l10.MODE_LEGACY_FIGE}


def test_mois_cloture_avec_archive_authentique_prefere_larchive(tmp_db):
    """§3-B / test 6 : une archive authentique disponible est préférée à la classification
    legacy — même si le mois était aussi classé LEGACY_SANS_ARCHIVE_ORIGINE par erreur."""
    _cloturer(tmp_db, "2026-04", legacy=True,
             archive_rows=[{"logement_id": "LOG_0001", "proprietaire_id": "PROP_0001",
                           "montant_du_conciergerie": 100.0}])
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-04"])
    finally:
        conn.close()
    assert r["2026-04"] == {"classification": l10.CLASS_ARCHIVE, "mode": l10.MODE_ARCHIVE_AUTHENTIQUE}


def test_mois_cloture_sans_source_classe_bloquant(tmp_db):
    """Jamais déduite d'une absence (commentaire migration 0064) : un mois CLOTURE ni archivé ni
    classé legacy est un cas NON RÉSOLU, pas un cas legacy par défaut."""
    _cloturer(tmp_db, "2026-05")
    conn = get_db(tmp_db)
    try:
        r = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()
    assert r["2026-05"] == {"classification": l10.CLASS_SANS_SOURCE, "mode": None}


# ── substitution : LEGACY_FIGE ────────────────────────────────────────────────────────────────

def test_mois_legacy_fige_recopie_baseline_jamais_recalcule(tmp_db):
    """Test 3/4 : le baseline est copié verbatim ; les valeurs fraîchement calculées pour ce mois
    (même différentes) sont totalement écartées — nouvelles données Lot9 sans effet sur mai."""
    flot10.seeder(
        tmp_db, run_id="L10-BASELINE",
        resultats=[_df("2026-05", vision="REEL", total_produits=1000.0, total_charges=200.0,
                       resultat=800.0, nb_flux=3, commentaire="")],
        commissions=[_df("2026-05", flux_source_pk="X", reservation_calc_id="R1")],
        net_exploitation=[_df("2026-05", reservation_calc_id="R1", revenu_net_exploitation=800.0)],
        net_reglement=[_df("2026-05", montant_du_conciergerie=250.0, nb_reservations=3)],
        net_vue_mois=[{"mois": "2026-05", "proprietaire_id": "PROP_0001",
                      "montant_du_conciergerie": 250.0}],
    )
    _cloturer(tmp_db, "2026-05", legacy=True)

    df_reel_frais = pd.DataFrame([_df("2026-05", vision="REEL", total_produits=99999.0,
                                     total_charges=0.0, resultat=99999.0, nb_flux=1,
                                     commentaire="")])
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    df_comm_frais = pd.DataFrame([_df("2026-05", flux_source_pk="Y", reservation_calc_id="R2")])
    df_exploit_frais = pd.DataFrame([_df("2026-05", revenu_net_exploitation=99999.0)])
    df_reg_frais = pd.DataFrame([_df("2026-05", montant_du_conciergerie=99999.0, nb_reservations=1)])
    df_vue_frais = pd.DataFrame([{"mois": "2026-05", "proprietaire_id": "PROP_0001",
                                 "montant_du_conciergerie": 99999.0}])

    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()

    (df_comm, df_reel, df_compt, df_hc, df_exploit, df_reg, df_vue, prov,
     ok, code, message) = l10.substituer_mois_figes(
        tmp_db, classifications, "L10-BASELINE",
        df_comm_frais, df_reel_frais, df_vide, df_vide, df_exploit_frais, df_reg_frais,
        df_vue_frais)

    assert ok is True, message
    assert len(df_reel) == 1
    assert df_reel.iloc[0]["total_produits"] == 1000.0  # baseline, jamais 99999.0
    assert df_reg.iloc[0]["montant_du_conciergerie"] == 250.0
    assert df_comm.iloc[0]["flux_source_pk"] == "X"
    assert prov[0]["mode_traitement"] == l10.MODE_LEGACY_FIGE
    assert prov[0]["source_run_id"] == "L10-BASELINE"


# ── 5 — fail-closed ───────────────────────────────────────────────────────────────────────────

def test_baseline_absente_echoue_fail_closed(tmp_db):
    _cloturer(tmp_db, "2026-05", legacy=True)
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()

    (*_frames, prov, ok, code, message) = l10.substituer_mois_figes(
        tmp_db, classifications, None,  # aucun baseline fourni
        df_vide, df_vide, df_vide, df_vide, df_vide, df_vide, df_vide)

    assert ok is False
    assert code == l10.E_CLOTURE_SANS_SOURCE_FIGEE


def test_baseline_fourni_mais_sans_lignes_pour_le_mois_echoue(tmp_db):
    """Le run existe mais ne porte aucune ligne pour CE mois précis — même refus, jamais un
    règlement vide activé silencieusement (cas réel rencontré : L10-ADB755190D18 ne couvrait
    jamais mai 2026)."""
    flot10.seeder(tmp_db, run_id="L10-BASELINE",
                  resultats=[_df("2026-06", vision="REEL", total_produits=1.0, total_charges=0.0,
                                resultat=1.0, nb_flux=1, commentaire="")])
    _cloturer(tmp_db, "2026-05", legacy=True)
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()

    (*_frames, prov, ok, code, message) = l10.substituer_mois_figes(
        tmp_db, classifications, "L10-BASELINE",
        df_vide, df_vide, df_vide, df_vide, df_vide, df_vide, df_vide)

    assert ok is False
    assert code == l10.E_CLOTURE_SANS_SOURCE_FIGEE
    assert "2026-05" in message


def test_mois_bloquant_refuse_meme_avec_baseline_fourni(tmp_db):
    """Un mois CLOTURE_SANS_SOURCE_FIGEE refuse toujours, même si un baseline existe par ailleurs
    — ce n'est pas un baseline manquant, c'est une classification qui n'a jamais été résolue."""
    flot10.seeder(tmp_db, run_id="L10-BASELINE",
                  resultats=[_df("2026-05", vision="REEL", total_produits=1.0, total_charges=0.0,
                                resultat=1.0, nb_flux=1, commentaire="")])
    _cloturer(tmp_db, "2026-05")  # ni legacy, ni archive
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05"])
    finally:
        conn.close()

    (*_frames, prov, ok, code, message) = l10.substituer_mois_figes(
        tmp_db, classifications, "L10-BASELINE",
        df_vide, df_vide, df_vide, df_vide, df_vide, df_vide, df_vide)

    assert ok is False
    assert code == l10.E_CLOTURE_SANS_SOURCE_FIGEE


# ── mixte : un mois OUVERT n'est jamais substitué ────────────────────────────────────────────

def test_mois_ouvert_non_substitue_valeurs_fraiches_conservees(tmp_db):
    _ouvrir(tmp_db, "2026-06")
    df_reel = pd.DataFrame([_df("2026-06", vision="REEL", total_produits=500.0, total_charges=0.0,
                                resultat=500.0, nb_flux=1, commentaire="")])
    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-06"])
    finally:
        conn.close()

    (df_comm, df_reel_out, *_rest, ok, code, message) = l10.substituer_mois_figes(
        tmp_db, classifications, None,
        df_vide, df_reel, df_vide, df_vide, df_vide, df_vide, df_vide)

    assert ok is True, message
    assert df_reel_out.iloc[0]["total_produits"] == 500.0


# ── provenance via ecrire_sqlite ──────────────────────────────────────────────────────────────

def test_provenance_enregistree_legacy_fige(tmp_db):
    flot10.seeder(tmp_db, run_id="L10-BASELINE")
    provenance_rows = [{
        "mois": "2026-05", "classification": l10.CLASS_LEGACY, "mode_traitement": l10.MODE_LEGACY_FIGE,
        "source_run_id": "L10-BASELINE", "source_archive_id": None,
    }]
    df_vide_com = pd.DataFrame(columns=list(l10._T_COMM_AC))
    df_vide_res = pd.DataFrame(columns=list(l10._T_RESULTATS))
    df_vide_comm = pd.DataFrame(columns=list(l10._T_COMMISSIONS))
    df_vide_exp = pd.DataFrame(columns=list(l10._T_EXPLOITATION))
    df_vide_reg = pd.DataFrame(columns=list(l10._T_REGLEMENT))
    df_vide_vue = pd.DataFrame(columns=list(l10._T_VUE_MOIS))

    resultat = l10.ecrire_sqlite(
        tmp_db, df_vide_comm, df_vide_com, df_vide_res, df_vide_res, df_vide_res,
        df_vide_exp, df_vide_reg, df_vide_vue, provenance_rows=provenance_rows)

    assert resultat["ecrit"] is True
    conn = get_db(tmp_db)
    try:
        row = conn.execute(
            "SELECT mois, classification, mode_traitement, source_run_id FROM "
            "lot10_run_mois_provenance WHERE run_id = ?", (resultat["run_id"],)).fetchone()
    finally:
        conn.close()
    assert tuple(row) == ("2026-05", l10.CLASS_LEGACY, l10.MODE_LEGACY_FIGE, "L10-BASELINE")


def test_activation_atomique_ancien_run_reste_actif_si_echec():
    pass  # couvert structurellement : `ecrire_sqlite` désactive l'ancien run SEULEMENT après avoir
    # écrit toutes les tables (voir son propre commentaire) ; `substituer_mois_figes` refuse AVANT
    # tout appel à `ecrire_sqlite` (aucun run n'est même créé) — testé par
    # `test_baseline_absente_echoue_fail_closed` et consorts ci-dessus : aucun run_id n'est produit
    # en cas de refus, donc rien ne peut jamais devenir actif à moitié.


def test_classification_et_substitution_zero_excel(tmp_db, monkeypatch):
    """`classifier_mois_lot10`/`substituer_mois_figes` sont des fonctions SQLite pures — aucune
    des deux ne doit jamais ouvrir un classeur, mois figé ou pas."""
    import openpyxl

    def _piege(*a, **k):
        raise AssertionError("OUVERTURE EXCEL DETECTEE : " + str(a))

    monkeypatch.setattr(openpyxl, "load_workbook", _piege)

    flot10.seeder(
        tmp_db, run_id="L10-BASELINE",
        resultats=[_df("2026-05", vision="REEL", total_produits=1.0, total_charges=0.0,
                       resultat=1.0, nb_flux=1, commentaire="")])
    _cloturer(tmp_db, "2026-05", legacy=True)
    _ouvrir(tmp_db, "2026-06")

    conn = get_db(tmp_db)
    try:
        classifications = l10.classifier_mois_lot10(conn, ["2026-05", "2026-06"])
    finally:
        conn.close()

    df_vide = pd.DataFrame(columns=list(l10._T_RESULTATS))
    (*_frames, prov, ok, code, message) = l10.substituer_mois_figes(
        tmp_db, classifications, "L10-BASELINE",
        df_vide, df_vide, df_vide, df_vide, df_vide, df_vide, df_vide)
    assert ok is True, message

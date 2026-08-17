"""Lot4quater — résolution de la source des réservations (contrat global, jamais mono-mois).

Documente le contrat exact après l'anomalie apparente « `/calculs` ne produit que 104 lignes au
lieu de 1349 » observée lors d'une mission de recette globale : ce n'était PAS un bug
d'orchestration mensuelle — `lot4quater_resoudre_source_reservations.py` ne prend AUCUN paramètre
mois et reconstruit toujours l'intégralité de l'historique (mois ouverts depuis le live, mois
clôturés depuis HIST). La cause réelle était un environnement de copies incomplet
(`HIST_Reservations_Cloturees.xlsx` pas encore copié à ce moment précis) : lot4quater a alors
appliqué son propre mécanisme de repli déjà documenté (`CLOTURE_SANS_HIST` → `A_CONTROLER`), pas
un défaut de code. Ce fichier fixe ce contrat par des tests, sur fixtures entièrement fictives,
exécutant le script réel via `runpy.run_path` (aucune donnée réelle, aucune duplication de moteur).
"""
from __future__ import annotations

import runpy
import sys
import shutil
from datetime import date
from pathlib import Path

import openpyxl
import pytest

REAL_SCRIPT = (Path(__file__).resolve().parents[1] / "02_TRAVAIL" /
               "lot4quater_resoudre_source_reservations.py")

LIVE_COLS = ["reservation_calc_id", "ROW_HASH", "source", "reservation_id_hostaway",
             "reservation_hh_id", "mois", "logement_id", "proprietaire_id",
             "date_arrivee", "date_depart", "nuits", "guestCount", "source_guestCount",
             "montant_retenu", "source_montant", "code_impact", "impact_resultat_reel",
             "impact_resultat_comptable", "statut_controle", "niveau_anomalie",
             "code_anomalie", "commentaire", "source_module", "source_table", "source_pk",
             "date_integration"]

HIST_COLS = LIVE_COLS + ["canal", "etat_mois", "origine_initiale", "cle_historisation",
                          "payout_calcule", "menage_retenu", "assiette_commission"]


def _live_row(rid, mois, montant=100.0, statut="VALIDE", impact="OUI", source="HOSTAWAY_AIRBNB"):
    return [f"RES-{rid}", f"HASH-{rid}", source, rid, None, mois, "LOG_A1", "PROP_A",
            date(2026, 1, 1), date(2026, 1, 3), 2, 2, "HOSTAWAY", montant, "PAYOUT",
            None, impact, impact, statut, "INFO", None, "", "lot1", "MASTER", rid,
            "2026-01-01T00:00:00"]


def _hist_row(rid, mois, montant=100.0, statut="VALIDE", impact="OUI"):
    return [f"RES-{rid}", f"HASH-{rid}", "HOSTAWAY_AIRBNB", rid, None, mois, "LOG_A1",
            "PROP_A", date(2026, 1, 1), date(2026, 1, 3), 2, 2, "HIST", montant,
            "HIST_RESERVATIONS_CLOTUREES", None, impact, impact, statut, "INFO", None,
            "", "lot4quater", "HIST_Reservations_Cloturees", rid, "2026-01-01T00:00:00",
            "AIRBNB", "CLOTURE", "API_HOSTAWAY", f"HIST-{rid}", None, None, None]


def _make_env(tmp_path: Path, *, live_rows, hist_rows=None, closed_months=()):
    """Reproduit l'arborescence attendue par `ROOT = dirname(dirname(__file__))` : le script réel
    est copié dans <tmp>/02_TRAVAIL/, ses chemins se résolvent donc naturellement dans <tmp>."""
    travail = tmp_path / "02_TRAVAIL"
    travail.mkdir(parents=True)
    script_copy = travail / REAL_SCRIPT.name
    shutil.copy2(REAL_SCRIPT, script_copy)
    # Les bibliotheques voisines que le script importe doivent l'accompagner : copier le seul
    # fichier de lot laisserait un import manquant, et le test echouerait pour une raison qui n'a
    # rien a voir avec ce qu'il verifie.
    for lib in ("lib_db_moteur.py",):
        source = REAL_SCRIPT.parent / lib
        if source.exists():
            shutil.copy2(source, travail / lib)

    live_dir = travail / "Lot4bis_TableCommune"
    live_dir.mkdir()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MASTER"
    ws.append(LIVE_COLS)
    for r in live_rows:
        ws.append(r)
    wb.save(live_dir / "MASTER_CALC_Reservations.xlsx")
    wb.close()

    if hist_rows is not None:
        hist_dir = tmp_path / "02_DONNEES_NORMALISEES" / "historique_reservations"
        hist_dir.mkdir(parents=True)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "HIST_Reservations_Cloturees"
        ws.append(HIST_COLS)
        for r in hist_rows:
            ws.append(r)
        wb.save(hist_dir / "HIST_Reservations_Cloturees.xlsx")
        wb.close()

    ref_dir = tmp_path / "01_SOURCES_BRUTES" / "REF_Setup"
    ref_dir.mkdir(parents=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "REF_Cloture_Mensuelle"
    ws.append(["mois", "statut_mois"])
    for m in closed_months:
        ws.append([m, "CLOTURE"])
    wb.save(ref_dir / "REF_Setup.xlsm")
    wb.close()

    return script_copy


def _run(script_copy: Path):
    # `lot4quater_resoudre_source_reservations.py` protège son entrée par
    # `if __name__ == "__main__": main()` (contrairement à lot8a) : run_name doit être "__main__"
    # sinon `main()` n'est jamais appelé et le test échoue silencieusement (fichier jamais écrit).
    # `sys.argv` doit etre celui du SCRIPT, pas celui de pytest : le lot analyse desormais ses
    # arguments (--source, --db, --sans-excel), et il recevrait sinon les options de pytest, qu'il
    # rejetterait avec un code 2.
    argv_pytest = sys.argv
    sys.argv = [str(script_copy)]
    try:
        return runpy.run_path(str(script_copy), run_name="__main__")
    finally:
        sys.argv = argv_pytest


def _read_master(tmp_path: Path):
    out = tmp_path / "02_TRAVAIL" / "Lot4quater_SourceResolue" / "MASTER_CALC_Reservations_Resolues.xlsx"
    wb = openpyxl.load_workbook(out, data_only=True)
    hdr = [c.value for c in wb["MASTER"][1]]
    master = [dict(zip(hdr, r)) for r in wb["MASTER"].iter_rows(min_row=2, values_only=True)]
    hdr_v = [c.value for c in wb["VUE_FLUX"][1]]
    vue = [dict(zip(hdr_v, r)) for r in wb["VUE_FLUX"].iter_rows(min_row=2, values_only=True)]
    wb.close()
    return master, vue


# ── Contrat : aucun paramètre mois, reconstruction toujours globale ──────────

def test_aucun_filtre_mois_tous_les_mois_ouverts_sont_repris(tmp_path):
    """Le script ne prend aucun argument mois : chaque mois du live non clôturé doit apparaître,
    quel que soit le nombre de mois — la notion de « mois demandé » n'existe pas dans ce contrat."""
    live = [_live_row(1, "2025-01"), _live_row(2, "2025-06"), _live_row(3, "2026-06"),
            _live_row(4, "2026-12")]
    script = _make_env(tmp_path, live_rows=live, hist_rows=[], closed_months=[])
    _run(script)
    master, vue = _read_master(tmp_path)
    mois_presents = {r["mois"] for r in master}
    assert mois_presents == {"2025-01", "2025-06", "2026-06", "2026-12"}
    assert len(vue) == 4


def test_mois_clotures_utilisent_hist_pas_le_live(tmp_path):
    live = [_live_row(1, "2025-01", montant=999.0)]  # valeur live périmée, HIST doit primer
    hist = [_hist_row(1, "2025-01", montant=123.45)]
    script = _make_env(tmp_path, live_rows=live, hist_rows=hist, closed_months=["2025-01"])
    _run(script)
    master, vue = _read_master(tmp_path)
    assert len(master) == 1
    assert master[0]["montant_retenu"] == 123.45  # HIST prime, jamais le live périmé
    assert master[0]["etat_mois"] == "CLOTURE"


def test_hist_absent_replie_sur_live_et_signale_a_controler(tmp_path):
    """LA CAUSE RÉELLE de l'anomalie observée : quand HIST_Reservations_Cloturees.xlsx est absent
    (fichier non copié dans l'environnement, pas un défaut de code), lot4quater applique son
    repli documenté sur le live pour les mois CLOTURE — jamais un crash, jamais des données
    fabriquées, toujours signalé A_CONTROLER/CLOTURE_SANS_HIST."""
    live = [_live_row(1, "2025-01", montant=50.0), _live_row(2, "2026-06", montant=80.0)]
    script = _make_env(tmp_path, live_rows=live, hist_rows=None, closed_months=["2025-01"])
    _run(script)
    master, vue = _read_master(tmp_path)
    row_2025_01 = next(r for r in master if r["mois"] == "2025-01")
    assert row_2025_01["etat_mois"] == "CLOTURE_SANS_HIST"
    assert row_2025_01["statut_controle"] == "A_CONTROLER"
    assert row_2025_01["code_anomalie"] == "MOIS_CLOTURE_SANS_HISTORIQUE"
    # le mois ouvert n'est jamais affecté par l'absence de HIST
    row_ouvert = next(r for r in master if r["mois"] == "2026-06")
    assert row_ouvert["etat_mois"] == "OUVERT"


def test_reinjection_reservation_disparue_du_live_apres_cloture(tmp_path):
    """Une réservation présente en HIST mais disparue du live après clôture doit être conservée
    (jamais silencieusement perdue)."""
    live = []  # rien dans le live pour ce mois clôturé
    hist = [_hist_row(1, "2025-03", montant=200.0)]
    script = _make_env(tmp_path, live_rows=live, hist_rows=hist, closed_months=["2025-03"])
    _run(script)
    master, vue = _read_master(tmp_path)
    assert len(master) == 1
    assert master[0]["montant_retenu"] == 200.0


def test_idempotent_deux_executions_memes_totaux(tmp_path):
    live = [_live_row(1, "2025-01"), _live_row(2, "2026-06")]
    hist = [_hist_row(1, "2025-01", montant=111.11)]
    script = _make_env(tmp_path, live_rows=live, hist_rows=hist, closed_months=["2025-01"])
    _run(script)
    master1, vue1 = _read_master(tmp_path)
    _run(script)
    master2, vue2 = _read_master(tmp_path)
    assert len(master1) == len(master2) == 2
    assert sorted(r["montant_retenu"] for r in master1) == sorted(r["montant_retenu"] for r in master2)


def test_vue_flux_exclut_statut_non_valide_et_montant_nul(tmp_path):
    live = [
        _live_row(1, "2026-06", montant=100.0, statut="VALIDE", impact="OUI"),
        _live_row(2, "2026-06", montant=0.0, statut="VALIDE", impact="OUI"),
        _live_row(3, "2026-06", montant=50.0, statut="A_CONTROLER", impact="OUI"),
    ]
    script = _make_env(tmp_path, live_rows=live, hist_rows=[], closed_months=[])
    _run(script)
    master, vue = _read_master(tmp_path)
    assert len(master) == 3
    assert len(vue) == 1
    assert vue[0]["reservation_calc_id"] == "RES-1"

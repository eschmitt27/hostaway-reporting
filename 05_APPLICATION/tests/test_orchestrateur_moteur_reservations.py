"""RESERVATIONS branché au DAG (mission 14e) — `executer_reservations()` exécute réellement
lot4bis puis lot4quater en sous-processus, tout SQLite, sur une base isolée.

Test réel (pas de mock de subprocess) : preuve que le chemin qu'emprunte l'orchestrateur en
production fonctionne effectivement de bout en bout, pas seulement que le DAG le déclare.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import orchestrateur_moteur as om


@pytest.fixture
def db_avec_hostaway_et_ref(tmp_path):
    """Base réelle (migrations applicatives) + une extraction Hostaway et un référentiel
    minimaux — un cas S1 (Airbnb, payout normal), résolvable de bout en bout."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-T1','R1','API','2026-01-01T00:00:00Z','SUCCES',1,1,1)")
        conn.execute(
            "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
            "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
            "inclure_resultat, check_in_date, check_out_date, nights) "
            "VALUES ('HAX-T1','70001','480136','HOSTAWAY','AIRBNB','AIRBNB','new',150.0,'false',"
            "'OUI','2026-06-01','2026-06-03',2)")
        conn.execute(
            "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
            "statut_calcul_payout, payout_calcule) "
            "VALUES ('HAX-T1','70001','480136','NORMAL',130.0)")
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-1','Hostaway','listingMapId','480136','LOG_A1','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_A1','480136','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-1','LOG_A1','PROP_A','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES ('2026-05','CLOTURE','IMP-1')")
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_executer_reservations_produit_reservations_resolues(db_avec_hostaway_et_ref):
    resultat = om.executer_reservations(db_path=db_avec_hostaway_et_ref)

    assert resultat["ok"] is True, resultat

    conn = sqlite3.connect(str(db_avec_hostaway_et_ref))
    conn.row_factory = sqlite3.Row
    calc = conn.execute("SELECT COUNT(*) FROM reservations_calculees").fetchone()[0]
    resolues = [dict(r) for r in conn.execute(
        "SELECT reservation_calc_id, source, logement_id, proprietaire_id, statut_controle, "
        "montant_retenu FROM reservations_resolues")]
    conn.close()

    assert calc == 1
    assert len(resolues) == 1
    assert resolues[0]["source"] == "HOSTAWAY_AIRBNB"
    assert resolues[0]["logement_id"] == "LOG_A1"
    assert resolues[0]["proprietaire_id"] == "PROP_A"
    assert resolues[0]["statut_controle"] == "VALIDE"
    assert resolues[0]["montant_retenu"] == 130.0


def test_executer_reservations_echoue_proprement_sans_referentiel(tmp_path):
    """Base avec Hostaway mais sans référentiel jamais importé : fail-closed, pas de résultat
    partiel présenté comme complet."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-T1','R1','API','2026-01-01T00:00:00Z','SUCCES',0,0,0)")
        conn.commit()
    finally:
        conn.close()

    resultat = om.executer_reservations(db_path=db_path)
    assert resultat["ok"] is False
    assert "lot4bis" in resultat["message"]

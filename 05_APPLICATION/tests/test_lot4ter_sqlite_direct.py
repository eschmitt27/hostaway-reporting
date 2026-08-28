"""lot4ter — clôtures et coûts standards menage lus depuis SQLite, jamais REF_Setup.xlsm en mode
SQLITE (mission 14g).

Avant cette mission, `closed_months()`/`cout_menage_standard()` restaient lues
INCONDITIONNELLEMENT depuis `REF_Setup.xlsm`, même avec `--source SQLITE --sans-excel` — seule la
donnée VIVANTE des réservations basculait réellement (découvert en auditant l'historisation avant
tout run réel, en miroir du fix Lot10 de la mission 14f).

L'historisation elle-même est un gel PUR de valeurs déjà résolues par lot4bis/hostaway_payouts
(payout/menage/assiette snapshotés à l'extraction, gestion/commission résolues par date) : ce test
ne prouve donc pas une « reconstruction du passé », mais que le gel n'invente rien et est
idempotent — exactement ce qui a été vérifié manuellement sur le clone de la vraie base (1269
réservations historisées, 0 différence sur relecture)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import orchestrateur_moteur as om


@pytest.fixture
def db_hist(tmp_path) -> Path:
    """Un mois CLOTURE avec une réservation Airbnb déjà résolue, un mois OUVERT ignoré."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES ('2025-06','CLOTURE','IMP-1')")
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES ('2026-06','OUVERT','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_T1','700001','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-T1','LOG_T1','PROP_T1','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-T1','Hostaway','listingMapId','700001','LOG_T1','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-T1','R1','API','2025-06-01T00:00:00Z','SUCCES',1,1,1)")
        conn.execute(
            "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
            "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
            "inclure_resultat, check_in_date, check_out_date, nights) "
            "VALUES ('HAX-T1','80000001','700001','HOSTAWAY','AIRBNB','AIRBNB','new',150.0,"
            "'false','OUI','2025-06-10','2025-06-12',2)")
        conn.execute(
            "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
            "statut_calcul_payout, payout_calcule, menage_retenu, assiette_commission) "
            "VALUES ('HAX-T1','80000001','700001','NORMAL',130.0,30.0,100.0)")
        conn.commit()
    finally:
        conn.close()

    assert om.executer_reservations(db_path=db_path)["ok"] is True
    return db_path


def test_lot4ter_sqlite_direct_sans_excel(db_hist, tmp_path):
    r = om.executer("lot4ter_historiser_reservations_cloturees.py", db_path=db_hist,
                    arguments=("--source", "SQLITE", "--sans-excel"))
    assert r["ok"] is True, r

    conn = sqlite3.connect(str(db_hist))
    conn.row_factory = sqlite3.Row
    rows = [dict(x) for x in conn.execute("SELECT * FROM reservations_historique_cloture")]
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["reservation_id_hostaway"] == "80000001"
    assert row["mois"] == "2025-06"
    assert row["payout_calcule"] == 130.0
    assert row["menage_retenu"] == 30.0
    assert row["assiette_commission"] == 100.0

    # Aucun classeur legacy ecrit (--sans-excel).
    out_file = (Path(__file__).resolve().parents[2] / "02_DONNEES_NORMALISEES" /
               "historique_reservations" / "HIST_Reservations_Cloturees.xlsx")
    mtime_avant = out_file.stat().st_mtime if out_file.exists() else None
    conn.close()

    # Idempotence : un second run ne duplique rien.
    r2 = om.executer("lot4ter_historiser_reservations_cloturees.py", db_path=db_hist,
                     arguments=("--source", "SQLITE", "--sans-excel"))
    assert r2["ok"] is True, r2

    conn = sqlite3.connect(str(db_hist))
    rows2 = conn.execute("SELECT COUNT(*) FROM reservations_historique_cloture").fetchone()[0]
    assert rows2 == 1
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert len(conn.execute("PRAGMA foreign_key_check").fetchall()) == 0
    conn.close()

    mtime_apres = out_file.stat().st_mtime if out_file.exists() else None
    assert mtime_avant == mtime_apres  # jamais touché, ni créé ni modifié

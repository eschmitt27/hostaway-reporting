"""Archivage économique à la clôture (mission 15, partie 15-17).

VALIDEE -> ARCHIVEE fige l'état économique du mois, ATOMIQUEMENT avec la transition de workflow.
Après archivage, une donnée Hostaway live modifiée ne doit jamais changer le mois clôturé.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import cloture_archivage_service as arch
from app.services import clotures_service as cs
from app.services import orchestrateur_moteur as om


@pytest.fixture
def db_avec_reservation(tmp_path) -> Path:
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_A1','700101','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-A1','LOG_A1','PROP_A1','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-A1','Hostaway','listingMapId','700101','LOG_A1','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES ('2026-06','OUVERT','IMP-1')")
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-A1','R1','API','2026-06-01T00:00:00Z','SUCCES',1,1,1)")
        conn.execute(
            "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
            "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
            "inclure_resultat, check_in_date, check_out_date, nights) "
            "VALUES ('HAX-A1','82000001','700101','HOSTAWAY','AIRBNB','AIRBNB','new',150.0,"
            "'false','OUI','2026-06-10','2026-06-12',2)")
        conn.execute(
            "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
            "statut_calcul_payout, payout_calcule, menage_retenu, assiette_commission) "
            "VALUES ('HAX-A1','82000001','700101','NORMAL',130.0,30.0,100.0)")
        conn.commit()
    finally:
        conn.close()
    assert om.executer_reservations(db_path=db_path)["ok"] is True
    return db_path


def _amener_a_valide(db_path, mois="2026-06", acteur="TEST"):
    c = cs.creer_ou_charger(mois, acteur=acteur, db_path=db_path)
    c = cs.demarrer_preparation(c, acteur=acteur, db_path=db_path)
    c = cs.passer_a_valider(c, acteur=acteur, db_path=db_path)
    return cs.valider(c, acteur=acteur, commentaire="Controles revus, RAS", db_path=db_path)


def test_mois_ouvert_aucune_archive_definitive(db_avec_reservation):
    assert arch.deja_archive("2026-06", db_path=db_avec_reservation) is False


def test_validee_encore_modifiable(db_avec_reservation):
    c = _amener_a_valide(db_avec_reservation)
    assert c["statut"] == cs.ST_VALIDEE
    assert arch.deja_archive("2026-06", db_path=db_avec_reservation) is False
    # VALIDEE peut encore etre rouverte (workflow existant, pas touche par cette mission).
    assert cs.ST_ROUVERTE in cs.TRANSITIONS[cs.ST_VALIDEE]


def test_archivee_cree_snapshot_puis_cloture(db_avec_reservation):
    c = _amener_a_valide(db_avec_reservation)
    c = cs.archiver(c, acteur="TEST", db_path=db_avec_reservation)
    assert c["statut"] == cs.ST_ARCHIVEE
    assert arch.deja_archive("2026-06", db_path=db_avec_reservation) is True

    conn = sqlite3.connect(str(db_avec_reservation))
    conn.row_factory = sqlite3.Row
    statut_mois = conn.execute(
        "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois='2026-06'").fetchone()[0]
    assert statut_mois == "CLOTURE"
    row = conn.execute(
        "SELECT * FROM reservations_historique_cloture WHERE reservation_id_hostaway='82000001'"
    ).fetchone()
    assert row is not None
    assert row["payout_calcule"] == 130.0
    assert row["menage_retenu"] == 30.0
    assert row["assiette_commission"] == 100.0
    conn.close()


def test_double_archivage_refuse(db_avec_reservation):
    c = _amener_a_valide(db_avec_reservation)
    c = cs.archiver(c, acteur="TEST", db_path=db_avec_reservation)
    with pytest.raises(arch.ArchivageRefuse, match=arch.E_DEJA_ARCHIVE):
        arch.archiver_mois("2026-06", acteur="TEST", db_path=db_avec_reservation)


def test_apres_archivage_changement_hostaway_live_nimpacte_pas_larchive(db_avec_reservation):
    c = _amener_a_valide(db_avec_reservation)
    cs.archiver(c, acteur="TEST", db_path=db_avec_reservation)

    # Modification forte de la donnee Hostaway live APRES archivage (simule une reextraction qui
    # changerait le payout connu aujourd'hui).
    conn = get_db(db_avec_reservation)
    conn.execute(
        "UPDATE hostaway_payouts SET payout_calcule=999.99, menage_retenu=999.99, "
        "assiette_commission=0.0 WHERE reservation_id='82000001'")
    conn.commit()
    conn.close()

    # Un recalcul global (RESERVATIONS) peut retourner sur la table live, mais l'ARCHIVE reste
    # inchangee : c'est elle qui fait foi pour un mois cloture, jamais un nouvel appel Hostaway.
    om.executer_reservations(db_path=db_avec_reservation)

    conn = sqlite3.connect(str(db_avec_reservation))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM reservations_historique_cloture WHERE reservation_id_hostaway='82000001'"
    ).fetchone()
    assert row["payout_calcule"] == 130.0  # inchange, malgre la donnee live modifiee a 999.99
    assert row["menage_retenu"] == 30.0
    assert row["assiette_commission"] == 100.0
    conn.close()


def test_correction_historique_explicite_conserve_avant_apres(db_avec_reservation):
    c = _amener_a_valide(db_avec_reservation)
    cs.archiver(c, acteur="TEST", db_path=db_avec_reservation)
    conn = sqlite3.connect(str(db_avec_reservation))
    conn.row_factory = sqlite3.Row
    cle = conn.execute(
        "SELECT cle_historisation FROM reservations_historique_cloture "
        "WHERE reservation_id_hostaway='82000001'").fetchone()["cle_historisation"]
    conn.close()

    with pytest.raises(arch.ArchivageRefuse):
        arch.correction_historique(cle, "2026-06", {"payout_calcule": 140.0}, justification="",
                                   db_path=db_avec_reservation)

    r = arch.correction_historique(cle, "2026-06", {"payout_calcule": 140.0},
                                   justification="Erreur de saisie corrigee, piece jointe X",
                                   acteur="TEST", db_path=db_avec_reservation)
    assert r["ok"] is True
    conn = sqlite3.connect(str(db_avec_reservation))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT payout_calcule FROM reservations_historique_cloture WHERE cle_historisation=?",
        (cle,)).fetchone()
    assert row["payout_calcule"] == 140.0
    correction = conn.execute(
        "SELECT * FROM reservations_historique_corrections WHERE cle_historisation=?",
        (cle,)).fetchone()
    assert correction is not None
    assert "130" in correction["avant_json"]
    assert correction["justification"]
    conn.close()


def test_legacy_sans_archive_origine_pas_de_reconstruction_et_ne_bloque_pas(db_avec_reservation,
                                                                            tmp_path):
    """Un mois CLOTURE sans HIST, classe LEGACY_SANS_ARCHIVE_ORIGINE, sort de la file
    operationnelle (INFO, pas A_CONTROLER) et ne bloque jamais la facturation courante."""
    import subprocess
    import sys

    db_path = tmp_path / "legacy.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    conn.execute(
        "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
        "VALUES ('2025-03','CLOTURE','IMP-1')")
    conn.execute(
        "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
        "import_id) VALUES ('LOG_L1','700201','GERE','OUI','IMP-1')")
    conn.execute(
        "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
        "date_debut, date_fin, statut_gestion, import_id) "
        "VALUES ('GST-L1','LOG_L1','PROP_L1','2025-01-01','','ACTIF','IMP-1')")
    conn.execute(
        "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
        "valeur_source, logement_id, actif, import_id) "
        "VALUES ('MAP-L1','Hostaway','listingMapId','700201','LOG_L1','OUI','IMP-1')")
    conn.execute(
        "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
        "nb_listings, nb_reservations, nb_payouts) "
        "VALUES ('HAX-L1','R1','API','2025-03-01T00:00:00Z','SUCCES',1,1,1)")
    conn.execute(
        "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
        "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
        "inclure_resultat, check_in_date, check_out_date, nights) "
        "VALUES ('HAX-L1','83000001','700201','HOSTAWAY','AIRBNB','AIRBNB','new',150.0,'false',"
        "'OUI','2025-03-10','2025-03-12',2)")
    conn.commit()
    conn.close()

    def _resolue_active(conn):
        actif = conn.execute(
            "SELECT dataset_id FROM reservations_datasets WHERE etape='RESOLUES' AND actif=1"
        ).fetchone()[0]
        return conn.execute(
            "SELECT statut_controle, niveau_anomalie, code_anomalie FROM reservations_resolues "
            "WHERE dataset_id=?", (actif,)).fetchone()

    assert om.executer_reservations(db_path=db_path)["ok"] is True
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    avant = _resolue_active(conn)
    assert avant["code_anomalie"] == "MOIS_CLOTURE_SANS_HISTORIQUE"
    assert avant["niveau_anomalie"] == "A_CONTROLER"
    conn.close()

    r = arch.classifier_legacy("2025-03", motif="Cutover legacy identifie mission 14g/15",
                               acteur="TEST", db_path=db_path)
    assert r["ok"] is True

    assert om.executer_reservations(db_path=db_path)["ok"] is True
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    apres = _resolue_active(conn)
    assert apres["code_anomalie"] == "LEGACY_SANS_ARCHIVE_ORIGINE"
    assert apres["niveau_anomalie"] == "INFO"  # ne bloque plus la file operationnelle
    assert apres["statut_controle"] != "VALIDE"  # exclu du calcul (pas reconstruit depuis Hostaway)
    conn.close()

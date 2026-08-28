"""HH -> Lot12, chaîne complète SQLite (mission 14f).

Découverte cette mission : `lot10_calculer_resultats.py` ne basculait réellement en SQLite que
pour `flux_unifies` — réservations résolues, payouts, référentiels logements/propriétaires/taux et
saisie HH restaient lus depuis les classeurs Excel `MASTER_*`, MÊME en `--source SQLITE`, y compris
pour une base isolée (ces classeurs de production existent physiquement dans le worktree). Une
réservation HH créée uniquement en SQLite (via le service applicatif, jamais présente dans ces
classeurs) était donc silencieusement exclue du calcul de commission — aucune erreur, juste un
résultat manquant. Corrigé par les nouveaux chargeurs `charger_reservations_sqlite`,
`charger_payout_sqlite`, `charger_logements_sqlite`, `charger_proprietaires_sqlite`,
`charger_taux_commission_sqlite`, `charger_gestion_sqlite`, `charger_hh_sqlite`.

Ce test crée une réservation HH via le VRAI service applicatif (`reservations_hh_saisie_service.
creer`, celui de l'écran de saisie) sur une base isolée, puis fait traverser la chaîne canonique
complète RESERVATIONS -> FLUX_LOT9 -> LOT10 -> LOT11 -> LOT12 en n'appelant que des services réels
(subprocess réel pour lot4bis/lot4quater/lot10, pas de mock), et vérifie les montants à chaque
frontière.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import (
    controles_lot11_service,
    flux_unifie_service,
    lot12_prefactures_service,
    orchestrateur_moteur as om,
    reservations_hh_saisie_service as saisie,
)


@pytest.fixture
def db_hh(tmp_path) -> Path:
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_HH1','999001','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-HH1','LOG_HH1','PROP_HH','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-HH1','Hostaway','listingMapId','999001','LOG_HH1','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-HHTEST','R1','API','2026-06-01T00:00:00Z','SUCCES',0,0,0)")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) "
            "VALUES ('PROP_HH','HH','Proprio','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, logement_id, "
            "taux_commission, date_debut, actif, import_id) "
            "VALUES ('TXC-1','PROP_HH','LOG_HH1',0.20,'2025-01-01','OUI','IMP-1')")
        conn.commit()
    finally:
        conn.close()

    r = saisie.creer({
        "mois": "2026-06", "canal_id": "DIRECT", "source_financiere": "VIREMENT",
        "proprietaire_id": "PROP_HH", "logement_id": "LOG_HH1",
        "date_arrivee": "2026-06-10", "date_depart": "2026-06-12", "nuits": 2, "guest_count": 2,
        "montant_percu": 200.0, "montant_retenu": 200.0, "mode_paiement_id": "DIRECT",
        "code_impact": "HC", "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON",
        "statut_controle": "VALIDE", "niveau_anomalie": "INFO",
    }, acteur="TEST_HH_LOT12", db_path=db_path)
    assert r["ok"] is True, r
    return db_path


def test_hh_traverse_jusqua_lot12_avec_montants_corrects(db_hh):
    r_res = om.executer_reservations(db_path=db_hh)
    assert r_res["ok"] is True, r_res

    conn = sqlite3.connect(str(db_hh))
    conn.row_factory = sqlite3.Row
    resolues = [dict(x) for x in conn.execute(
        "SELECT reservation_calc_id, source, montant_retenu, statut_controle "
        "FROM reservations_resolues")]
    assert len(resolues) == 1
    assert resolues[0]["source"] == "MANUEL_HORS_HOSTAWAY"
    assert resolues[0]["statut_controle"] == "VALIDE"

    r_flux = flux_unifie_service.construire(db_path=db_hh)
    assert r_flux["ok"] is True, r_flux
    assert r_flux["nb_res"] == 1
    flux = [dict(x) for x in conn.execute("SELECT montant, code_impact FROM flux_unifies")]
    assert len(flux) == 1
    assert flux[0]["montant"] == 200.0

    r_lot10 = om.executer_lot10(db_path=db_hh)
    assert r_lot10["ok"] is True, r_lot10
    commissions = [dict(x) for x in conn.execute(
        "SELECT payout_calcule, taux_commission, commission_conciergerie, net_proprietaire "
        "FROM lot10_commissions")]
    assert len(commissions) == 1
    assert commissions[0]["payout_calcule"] == 200.0
    assert commissions[0]["taux_commission"] == pytest.approx(0.20)
    assert commissions[0]["commission_conciergerie"] == pytest.approx(40.0)
    assert commissions[0]["net_proprietaire"] == pytest.approx(160.0)

    r_lot11 = controles_lot11_service.construire(db_path=db_hh)
    assert r_lot11["ok"] is True, r_lot11
    assert r_lot11["nb_bloquants"] == 0

    r_lot12 = lot12_prefactures_service.construire(db_path=db_hh)
    assert r_lot12["ok"] is True, r_lot12
    assert r_lot12["nb_entetes"] == 1
    entete = dict(conn.execute("SELECT * FROM lot12_prefactures_entete").fetchone())
    assert entete["total_reglement_du"] == pytest.approx(40.0)
    assert entete["total_exploitation_net"] == pytest.approx(160.0)
    conn.close()

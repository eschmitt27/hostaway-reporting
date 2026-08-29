"""E2E synthétique complet — Hostaway + HH + ménage interne + canapé + charges (mission 14g).

Scénario déterministe, entièrement isolé, SQLite direct (zéro Excel), qui n'existait pas encore :

  - 1 réservation Hostaway (Airbnb, LOG_E1, PROP_E1) : payout 300€, ménage standard 40€,
    5 voyageurs → seuil canapé (4) dépassé → préparation canapé 30€.
  - 1 réservation hors Hostaway (HH, LOG_E2, PROP_E2), créée via le vrai service de saisie :
    payout 500€, pas de ménage.
  - 1 déclaration ménage interne (LOG_E2) → chaîne GPM (lot6d→6e→6f) → Lot9.
  - 1 charge DIRECTE non refacturable (LOG_E1, 50€).
  - 1 facture fournisseur MULTI-LOGEMENTS (3 lignes partageant `justificatif`, PAS de groupe
    permanent de logements — le périmètre vient de la facture elle-même) : 3.34€ (LOG_E1) +
    3.33€ (LOG_E2) + 3.33€ (LOG_E3, qui n'a AUCUNE réservation — seulement cette charge) =
    10.00€ exactement, aucune clé de répartition inventée (montants décidés en amont, saisis
    tels quels, comme le ferait un opérateur humain).

Chaîne réelle traversée (subprocess réels pour lot4bis/lot4quater/lot6d-e-f/lot10, pas de mock) :
RESERVATIONS → MENAGES → FLUX_LOT9 → LOT10 → LOT11 → LOT12.

Tous les montants sont vérifiés au centime, pas seulement la présence des lignes.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import (
    charges_refacturation_service as refac,
    charges_saisie_service as chs,
    controles_lot11_service,
    factures_proprietaires_service as fps,
    flux_unifie_service,
    lot12_prefactures_service,
    orchestrateur_moteur as om,
    reservations_hh_saisie_service as saisie,
)

MOIS = "2026-06"


@pytest.fixture
def db_e2e(tmp_path) -> Path:
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, type_logement_id, "
            "statut_parc, actif, import_id) VALUES ('LOG_E1','999201','TYPE_E1','GERE','OUI',"
            "'IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, type_logement_id, "
            "statut_parc, actif, import_id) VALUES ('LOG_E2',NULL,'TYPE_E1','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, type_logement_id, "
            "statut_parc, actif, import_id) VALUES ('LOG_E3',NULL,'TYPE_E1','GERE','OUI','IMP-1')")
        for gid, log, prop in (("GST-E1", "LOG_E1", "PROP_E1"), ("GST-E2", "LOG_E2", "PROP_E2"),
                               ("GST-E3", "LOG_E3", "PROP_E2")):
            conn.execute(
                "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, "
                "proprietaire_id, date_debut, date_fin, statut_gestion, import_id) "
                "VALUES (?,?,?,'2025-01-01','','ACTIF','IMP-1')", (gid, log, prop))
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES ('PROP_E1','Un','Proprio','OUI',"
            "'IMP-1')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES ('PROP_E2','Deux','Proprio','OUI',"
            "'IMP-1')")
        for tid, prop, log in (("TXC-E1", "PROP_E1", "LOG_E1"), ("TXC-E2", "PROP_E2", "LOG_E2")):
            conn.execute(
                "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, "
                "logement_id, taux_commission, date_debut, actif, import_id) "
                "VALUES (?,?,?,0.20,'2025-01-01','OUI','IMP-1')", (tid, prop, log))
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-E1','Hostaway','listingMapId','999201','LOG_E1','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'OUVERT', 'IMP-1')", (MOIS,))
        conn.execute(
            "INSERT INTO ref_canape_parametres (canape_parametre_id, logement_id, "
            "seuil_voyageurs_preparation_canape, montant_preparation_canape, date_debut, actif, "
            "import_id) VALUES ('CAN-E1','LOG_E1',4,30.0,'2025-01-01','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, date_debut_validite, actif, import_id) "
            "VALUES ('CSM-E1','TYPE_E1',40.0,'2025-01-01','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_couts_menage_interne (cout_menage_interne_id, type_logement_id, "
            "montant_interne_standard, date_debut_validite, actif, cout_fixe_par_menage, "
            "priorite, import_id) VALUES ('CMI-E1','TYPE_E1',40.0,'2025-01-01','OUI','OUI',1,"
            "'IMP-1')")
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "actif, nom_normalise, import_id) VALUES ('INT_E1','Femme de menage E','INTERNE',"
            "'OUI','FEMMEMENAGEE','IMP-1')")

        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-E2E','R1','API','2026-06-01T00:00:00Z','SUCCES',1,1,1)")
        conn.execute(
            "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
            "source, channel_type, source_financiere, status, total_price, number_of_guests, "
            "guest_count, source_guest_count, is_owner_stay, inclure_resultat, check_in_date, "
            "check_out_date, nights) VALUES ('HAX-E2E','90000001','999201','HOSTAWAY','AIRBNB',"
            "'AIRBNB','new',350.0,5,5,'API_LIST','false','OUI','2026-06-10','2026-06-13',3)")
        conn.execute(
            "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
            "statut_calcul_payout, payout_calcule, menage_retenu, assiette_commission, "
            "menage_retenu_source, cout_standard_id, logement_id_snapshot, "
            "type_logement_id_snapshot) VALUES ('HAX-E2E','90000001','999201','NORMAL',300.0,"
            "40.0,260.0,'REF_COUT_STANDARD_MENAGE','CSM-E1','LOG_E1','TYPE_E1')")
        # Le mois traité par lot6d/e/f (sans --mois explicite) est déduit de MAX(mois) de cette
        # table (import Hostaway cleaning tasks, optionnel) — sans elle, repli sur le mois
        # calendaire courant, pas 2026-06. Cf. mission 14e/14g : limite connue, documentée.
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, proprietaire_id, "
            "status, statut_menage, compte_comme_menage) "
            "VALUES ('TASK-E2E','2026-06','LOG_E2','PROP_E2','completed','réalisé','OUI')")
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle, run_id) "
            "VALUES ('2026-06','LOG_E2','INT_E1',1,'VALIDE','SEED-E2E')")
        conn.commit()
    finally:
        conn.close()

    assert saisie.creer({
        "mois": MOIS, "canal_id": "DIRECT", "source_financiere": "VIREMENT",
        "proprietaire_id": "PROP_E2", "logement_id": "LOG_E2",
        "date_arrivee": "2026-06-15", "date_depart": "2026-06-18", "nuits": 3, "guest_count": 2,
        "montant_percu": 500.0, "montant_retenu": 500.0, "mode_paiement_id": "DIRECT",
        "code_impact": "HC", "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON",
        "statut_controle": "VALIDE", "niveau_anomalie": "INFO",
    }, acteur="TEST_E2E", db_path=db_path)["ok"] is True

    assert chs.creer({
        "date_charge": "2026-06-05", "mois": MOIS, "montant": 50.0, "sens_flux": "SORTIE",
        "sens": "CHARGE", "categorie_charge_id": "ENTRETIEN", "code_impact": "IC",
        "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI",
        "affectation_type": "LOGEMENT", "logement_id": "LOG_E1", "proprietaire_id": "PROP_E1",
        "refacturable": "NON", "statut_controle": "VALIDE", "justificatif": "FACT-DIRECTE-001",
        "commentaire": "Charge directe E2E",
    }, acteur="TEST_E2E", db_path=db_path)["ok"] is True

    for log, prop, montant in (("LOG_E1", "PROP_E1", 3.34), ("LOG_E2", "PROP_E2", 3.33),
                               ("LOG_E3", "PROP_E2", 3.33)):
        assert chs.creer({
            "date_charge": "2026-06-06", "mois": MOIS, "montant": montant, "sens_flux": "SORTIE",
            "sens": "CHARGE", "categorie_charge_id": "FOURNITURE", "code_impact": "IC",
            "impact_resultat_reel": "OUI", "impact_resultat_comptable": "OUI",
            "affectation_type": "LOGEMENT", "logement_id": log, "proprietaire_id": prop,
            "refacturable": "OUI", "statut_controle": "VALIDE",
            "justificatif": "FACT-MULTI-001",
            "commentaire": "Ventilation facture multi-logements, montants decides en amont",
        }, acteur="TEST_E2E", db_path=db_path)["ok"] is True

    return db_path


def test_e2e_synthetique_centime_par_centime(db_e2e):
    db = db_e2e

    assert om.executer_reservations(db_path=db)["ok"] is True
    assert om.executer_menages(db_path=db)["ok"] is True
    r_flux = flux_unifie_service.construire(db_path=db)
    assert r_flux["ok"] is True, r_flux
    assert r_flux["nb_res"] == 2 and r_flux["nb_chg"] == 4 and r_flux["nb_gpm"] == 1

    r_lot10 = om.executer_lot10(db_path=db)
    assert r_lot10["ok"] is True, r_lot10

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    com = {r["logement_id"]: dict(r) for r in conn.execute(
        "SELECT logement_id, payout_calcule, menage_retenu, assiette_commission, "
        "taux_commission, commission_conciergerie, preparation_canape_voyageurs, "
        "net_proprietaire FROM lot10_commissions")}

    # LOG_E1 — Hostaway/Airbnb : payout 300, menage standard 40, assiette 260, commission 20%=52,
    # canape 5 voyageurs >= seuil 4 -> 30, net = 300-40-52 = 178.
    e1 = com["LOG_E1"]
    assert e1["payout_calcule"] == 300.0
    assert e1["menage_retenu"] == 40.0
    assert e1["assiette_commission"] == 260.0
    assert e1["commission_conciergerie"] == 52.0
    assert e1["preparation_canape_voyageurs"] == 30.0
    assert e1["net_proprietaire"] == 178.0

    # LOG_E2 — HH : payout 500, pas de menage, commission 20%=100, net=400. Pas de canape
    # (parametre uniquement configure pour LOG_E1).
    e2 = com["LOG_E2"]
    assert e2["payout_calcule"] == 500.0
    assert e2["menage_retenu"] == 0.0
    assert e2["commission_conciergerie"] == 100.0
    assert e2["preparation_canape_voyageurs"] == 0.0
    assert e2["net_proprietaire"] == 400.0

    # Ventilation multi-logements de la facture unique : 3.34 + 3.33 + 3.33 = 10.00 exactement.
    lignes_refac = [dict(r) for r in conn.execute(
        "SELECT logement_id, montant FROM charges WHERE justificatif='FACT-MULTI-001'")]
    assert round(sum(r["montant"] for r in lignes_refac), 2) == 10.00

    r_lot11 = controles_lot11_service.construire(db_path=db)
    assert r_lot11["ok"] is True, r_lot11
    assert r_lot11["nb_bloquants"] == 0

    r_lot12 = lot12_prefactures_service.construire(db_path=db)
    assert r_lot12["ok"] is True, r_lot12

    entetes = {r["logement_id"]: dict(r) for r in conn.execute(
        "SELECT logement_id, proprietaire_id, total_exploitation_net, total_reglement_du, "
        "reste_a_payer FROM lot12_prefactures_entete")}
    # Mission 15 : une charge refacturable='OUI' n'est plus automatiquement facturee — sans
    # decision d'imputation, LOG_E1/LOG_E2 ne portent PAS encore le montant des charges (seulement
    # menage+commission+canape), et LOG_E3 (aucune reservation, seulement la charge JAMAIS
    # imputee) n'apparait meme plus dans le reglement.
    assert "LOG_E3" not in entetes
    assert entetes["LOG_E1"]["total_exploitation_net"] == 178.0
    assert entetes["LOG_E1"]["reste_a_payer"] == pytest.approx(122.00, abs=0.005)  # 40+52+30
    assert entetes["LOG_E2"]["total_exploitation_net"] == 400.0
    assert entetes["LOG_E2"]["reste_a_payer"] == pytest.approx(100.00, abs=0.005)  # commission seule

    # Les 3 charges refacturables restent visibles comme POSITIONS disponibles (jamais perdues).
    positions_e1 = refac.proposer_pour_facture("PROP_E1", "LOG_E1", db_path=db)
    positions_e2 = refac.proposer_pour_facture("PROP_E2", "LOG_E2", db_path=db)
    positions_e3 = refac.proposer_pour_facture("PROP_E2", "LOG_E3", db_path=db)
    assert [p["montant_restant"] for p in positions_e1] == [3.34]
    assert [p["montant_restant"] for p in positions_e2] == [3.33]
    assert [p["montant_restant"] for p in positions_e3] == [3.33]

    # Decision humaine : imputation complete des 3 positions sur la facture du mois.
    for prop, log, positions, montant_du in (
        ("PROP_E1", "LOG_E1", positions_e1, 122.00 + 3.34),
        ("PROP_E2", "LOG_E2", positions_e2, 100.00 + 3.33),
    ):
        decisions = [{"position_id": positions[0]["position_id"],
                     "montant": positions[0]["montant_restant"], "libelle": "Charge refacturee"}]
        source = {"proprietaire_id": prop, "logement_id": log, "mois": MOIS,
                  "source_calcul": "TEST", "COMMISSION_CONCIERGERIE": 0, "MENAGE_FACTURE": 0,
                  "PREPARATION_CANAPE": 0, "CHARGE_FIXE": 0, "CHARGES_EXCEPT_REFAC": 0,
                  "montant_du_conciergerie": 0.0}
        f = fps.creer(source, acteur="TEST", db_path=db, decisions_charges=decisions)
        fv = fps.valider(f["facture_id_opaque"],
                        emetteur={"nom": "Conciergerie", "adresse": "1 rue X", "siret": "123"},
                        destinataire={"nom": prop}, acteur="TEST", decisions_charges=decisions,
                        db_path=db)
        assert fv["statut"] == fps.ST_VALIDE

    # LOG_E3 : jamais imputee (aucune facture ne la couvre dans ce scenario) -> reste disponible.
    assert refac.proposer_pour_facture("PROP_E2", "LOG_E3", db_path=db)[0]["montant_restant"] == 3.33

    # Rejouer Lot10 : les charges REALISEES (imputees) apparaissent desormais.
    assert om.executer_lot10(db_path=db)["ok"] is True
    conn2 = sqlite3.connect(str(db))
    conn2.row_factory = sqlite3.Row
    reg = {r["logement_id"]: dict(r) for r in conn2.execute(
        "SELECT logement_id, charges_exceptionnelles_refacturees, montant_du_conciergerie "
        "FROM lot10_net_reglement")}
    assert reg["LOG_E1"]["charges_exceptionnelles_refacturees"] == pytest.approx(3.34, abs=0.005)
    assert reg["LOG_E1"]["montant_du_conciergerie"] == pytest.approx(125.34, abs=0.005)
    assert reg["LOG_E2"]["charges_exceptionnelles_refacturees"] == pytest.approx(3.33, abs=0.005)
    assert reg["LOG_E2"]["montant_du_conciergerie"] == pytest.approx(103.33, abs=0.005)
    conn2.close()
    conn.close()

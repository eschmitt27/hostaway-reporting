"""Assiette négative -> commission plafonnée à 0 (mission 14f-bis).

Reproduit le cas réel découvert sur le clone de la vraie base (audit lecture-seule uniquement,
aucune donnée réelle modifiée) : réservation Hostaway `reservation_id=65060946`, payout=11.86 €,
ménage retenu=29.00 € (coût standard), assiette brute=-17.14 €. Avant cette mission, Lot10
s'arrêtait (BLOQUANT ASSIETTE_NEGATIVE). Règle validée : l'assiette BRUTE reste inchangée (preuve),
seule la commission qui en découle est plafonnée à 0 — jamais une commission négative — et un
contrôle Lot11 visible et non bloquant est généré.

Test réel : subprocess réel pour lot4bis/lot4quater/lot10 (via `orchestrateur_moteur`), pas de mock,
et `controles_lot11_service.construire()` réel.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import controles_lot11_service, flux_unifie_service
from app.services import orchestrateur_moteur as om


@pytest.fixture
def db_assiette_negative(tmp_path) -> Path:
    """Réservation Airbnb réelle rejouée : 1 nuit, payout=11.86, coût ménage standard=29.00."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, statut, "
            "nb_listings, nb_reservations, nb_payouts) "
            "VALUES ('HAX-T1','R1','API','2026-08-01T00:00:00Z','SUCCES',1,1,1)")
        conn.execute(
            "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
            "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
            "inclure_resultat, check_in_date, check_out_date, nights) "
            "VALUES ('HAX-T1','65060946','487144','HOSTAWAY','AIRBNB','AIRBNB','modified',28.59,"
            "'false','OUI','2026-08-25','2026-08-26',1)")
        conn.execute(
            "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
            "statut_calcul_payout, payout_calcule, menage_retenu, assiette_commission) "
            "VALUES ('HAX-T1','65060946','487144','NORMAL',11.86,29.00,-17.14)")
        conn.execute(
            "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
            "valeur_source, logement_id, actif, import_id) "
            "VALUES ('MAP-1','Hostaway','listingMapId','487144','LOG_0006','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_0006','487144','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-1','LOG_0006','PROP_A','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) "
            "VALUES ('PROP_A','Dupont','Jean','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, logement_id, "
            "taux_commission, date_debut, actif, import_id) "
            "VALUES ('TXC-1','PROP_A','LOG_0006',0.20,'2025-01-01','OUI','IMP-1')")
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_assiette_negative_ne_bloque_plus_et_produit_un_controle(db_assiette_negative):
    db = db_assiette_negative

    assert om.executer_reservations(db_path=db)["ok"] is True
    assert flux_unifie_service.construire(db_path=db)["ok"] is True

    r_lot10 = om.executer_lot10(db_path=db)
    assert r_lot10["ok"] is True, r_lot10  # ne bloque plus (avant : sys.exit ASSIETTE_NEGATIVE)

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    com = dict(conn.execute(
        "SELECT assiette_commission, commission_conciergerie, net_proprietaire, payout_calcule, "
        "menage_retenu FROM lot10_commissions WHERE reservation_id_hostaway='65060946'"
    ).fetchone())
    # Assiette BRUTE conservée telle quelle (jamais réécrite) — c'est la preuve du cas.
    assert com["assiette_commission"] == pytest.approx(-17.14, abs=0.01)
    # Commission jamais négative : plafonnée à 0.
    assert com["commission_conciergerie"] == 0.0
    # net_proprietaire reflète le vrai résultat économique négatif (payout - menage - 0 commission).
    assert com["net_proprietaire"] == pytest.approx(-17.14, abs=0.01)

    r_lot11 = controles_lot11_service.construire(db_path=db)
    assert r_lot11["ok"] is True, r_lot11
    constats = [dict(x) for x in conn.execute(
        "SELECT k.*, c.reservation_id, c.logement_id FROM controles_lot11_constats k "
        "JOIN controles_lot11_constats_champs c ON c.ctrl_pk = k.ctrl_pk "
        "WHERE k.code_controle='ASSIETTE_NEGATIVE_RAMENEE_ZERO'")]
    assert len(constats) == 1, constats
    c = constats[0]
    assert c["severity"] == "A_CONTROLER"
    assert c["reservation_id"] == "65060946"
    assert "-17.14" in c["message"]
    assert "11.86" in c["message"]
    assert "29.0" in c["message"]
    conn.close()

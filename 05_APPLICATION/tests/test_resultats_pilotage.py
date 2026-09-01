"""Mission « comptabilité + résultats + graphiques » — non-régression.

TEST A — dataset/extraction/run actifs uniquement (pas de ×N sur plusieurs générations).
TEST B — Status New/Modified inclus, autre statut exclu (périmètre Résultats identique à Lot10).
TEST C — Cerdine-like : 2773.28 / 414.00 / 6 réservations, jamais 11093.12 / 1656.00 / 24.
TEST D — filtres propriétaire / logement / plateforme / période.
TEST E — cohérence Résultats ↔ Lot10 (mêmes chiffres, même source).
TEST F — cohérence Résultats ↔ Facturation (lot12_prefactures_entete, quand comparable).
TEST G — état sans données / calcul non disponible / comptabilité non alimentée.
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.db.connection import apply_migrations, get_db
from app.services import orchestrateur_moteur as om
from app.services import flux_unifie_service
from app.services import resultats_pilotage_service as pilot


def _ref_minimal(conn, *, logement_id="LOG_A1", proprietaire_id="PROP_A", listing="480136"):
    conn.execute(
        "INSERT INTO ref_mapping_logements (mapping_logement_id, source, champ_source, "
        "valeur_source, logement_id, actif, import_id) "
        "VALUES (?, 'Hostaway', 'listingMapId', ?, ?, 'OUI', 'IMP-1')",
        (f"MAP-{listing}", listing, logement_id))
    conn.execute(
        "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
        "import_id, nom_court) VALUES (?, ?, 'GERE', 'OUI', 'IMP-1', ?)",
        (logement_id, listing, f"Logement {logement_id}"))
    conn.execute(
        "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
        "date_debut, date_fin, statut_gestion, import_id) "
        "VALUES (?, ?, ?, '2025-01-01', '', 'ACTIF', 'IMP-1')",
        (f"GST-{logement_id}", logement_id, proprietaire_id))
    conn.execute(
        "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, prenom_proprietaire, "
        "import_id) VALUES (?, 'Delrieu', 'Cédrine', 'IMP-1')", (proprietaire_id,))
    conn.execute(
        "INSERT INTO ref_taux_commission (taux_commission_id, proprietaire_id, logement_id, "
        "taux_commission, date_debut, date_fin, actif, import_id) "
        "VALUES (?, ?, '', 0.15, '2025-01-01', '', 'OUI', 'IMP-1')",
        (f"TX-{proprietaire_id}", proprietaire_id))


def _reservation(conn, *, extraction_id, reservation_id, status, listing="480136",
                 payout=222.22, menage=69.0, channel="AIRBNB", mois_arrivee="2026-07-05"):
    conn.execute(
        "INSERT OR IGNORE INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, "
        "statut, nb_listings, nb_reservations, nb_payouts) "
        "VALUES (?, ?, 'API', '2026-07-01T00:00:00Z', 'SUCCES', 1, 1, 1)",
        (extraction_id, f"R-{extraction_id}"))
    conn.execute(
        "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
        "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
        "inclure_resultat, check_in_date, check_out_date, nights) "
        "VALUES (?, ?, ?, 'HOSTAWAY', ?, ?, ?, 300.0, 'false', 'OUI', ?, ?, 2)",
        (extraction_id, reservation_id, listing, channel, channel, status,
         mois_arrivee, mois_arrivee[:8] + str(int(mois_arrivee[-2:]) + 2).zfill(2)))
    conn.execute(
        "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
        "channel_type, statut_calcul_payout, payout_calcule, menage_retenu, assiette_commission) "
        "VALUES (?, ?, ?, ?, 'NORMAL', ?, ?, ?)",
        (extraction_id, reservation_id, listing, channel, payout, menage, round(payout - menage, 2)))


def _cloture_ouverte(conn, mois="2026-07"):
    conn.execute(
        "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?, 'OUVERT', 'IMP-1')",
        (mois,))


def _construire_cerdine_like(db_path):
    conn = get_db(db_path)
    _ref_minimal(conn)
    _cloture_ouverte(conn)
    reservations = [
        ("70001", 222.22, 69.0), ("70002", 341.88, 69.0), ("70003", 732.60, 69.0),
        ("70004", 367.11, 69.0), ("70005", 443.62, 69.0), ("70006", 665.85, 69.0),
    ]
    for rid, payout, menage in reservations:
        _reservation(conn, extraction_id="HAX-T1", reservation_id=rid, status="new",
                    payout=payout, menage=menage)
    conn.commit()
    conn.close()
    assert om.executer_reservations(db_path=db_path)["ok"] is True
    assert flux_unifie_service.construire(db_path=db_path)["ok"] is True
    assert om.executer_lot10(db_path=db_path)["ok"] is True


# ── TEST A — dataset/extraction/run actifs uniquement ────────────────────────────────────────────

def test_pas_de_duplication_multi_generations(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    _ref_minimal(conn)
    _cloture_ouverte(conn)
    _reservation(conn, extraction_id="HAX-T1", reservation_id="70001", status="new",
                payout=222.22, menage=69.0)
    conn.commit()
    conn.close()
    assert om.executer_reservations(db_path=db_path)["ok"] is True

    conn = get_db(db_path)
    _reservation(conn, extraction_id="HAX-T2", reservation_id="70001", status="new",
                payout=222.22, menage=69.0)
    conn.commit()
    conn.close()
    assert om.executer_reservations(db_path=db_path)["ok"] is True
    assert flux_unifie_service.construire(db_path=db_path)["ok"] is True
    assert om.executer_lot10(db_path=db_path)["ok"] is True

    v = pilot.vue(mois="2026-07", proprietaire_id="PROP_A", db_path=str(db_path))
    assert v["statut"] == "OK"
    assert v["kpi"]["total_payout"] == 222.22   # jamais 444.44 (×2)
    assert v["kpi"]["nb_reservations"] == 1


# ── TEST B — Status New/Modified inclus, autre exclu ─────────────────────────────────────────────

def test_statut_hostaway_perimetre_identique_lot10(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    _ref_minimal(conn)
    _cloture_ouverte(conn)
    _reservation(conn, extraction_id="HAX-T1", reservation_id="70001", status="new")
    _reservation(conn, extraction_id="HAX-T1", reservation_id="70002", status="modified")
    _reservation(conn, extraction_id="HAX-T1", reservation_id="70003", status="cancelled")
    conn.commit()
    conn.close()
    assert om.executer_reservations(db_path=db_path)["ok"] is True
    assert flux_unifie_service.construire(db_path=db_path)["ok"] is True
    assert om.executer_lot10(db_path=db_path)["ok"] is True

    v = pilot.vue(mois="2026-07", proprietaire_id="PROP_A", db_path=str(db_path))
    assert v["kpi"]["nb_reservations"] == 2   # cancelled exclue


# ── TEST C — Cerdine-like ─────────────────────────────────────────────────────────────────────────

def test_cerdine_like_2773_28_414_6(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _construire_cerdine_like(db_path)

    v = pilot.vue(mois="2026-07", proprietaire_id="PROP_A", db_path=str(db_path))
    assert v["statut"] == "OK"
    assert v["kpi"]["total_payout"] == 2773.28
    assert v["kpi"]["menage"] == 414.00
    assert v["kpi"]["nb_reservations"] == 6
    assert v["kpi"]["total_payout"] != 11093.12
    assert v["kpi"]["menage"] != 1656.00
    assert v["kpi"]["nb_reservations"] != 24


# ── TEST D — filtres propriétaire / logement / plateforme / période ─────────────────────────────

def test_filtres_proprietaire_logement_plateforme_periode(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _construire_cerdine_like(db_path)

    par_prop = pilot.vue(proprietaire_id="PROP_A", db_path=str(db_path))
    assert par_prop["kpi"]["nb_reservations"] == 6

    par_log = pilot.vue(logement_id="LOG_A1", db_path=str(db_path))
    assert par_log["kpi"]["nb_reservations"] == 6

    par_canal = pilot.vue(canal="AIRBNB", db_path=str(db_path))
    assert par_canal["kpi"]["nb_reservations"] == 6
    assert par_canal["kpi"]["ca_conciergerie"] is None   # jamais ventilé au grain réservation

    par_canal_absent = pilot.vue(canal="BOOKING", db_path=str(db_path))
    assert par_canal_absent["statut"] == "VIDE"

    par_mois = pilot.vue(mois="2026-07", db_path=str(db_path))
    assert par_mois["kpi"]["nb_reservations"] == 6
    par_autre_mois = pilot.vue(mois="2025-01", db_path=str(db_path))
    assert par_autre_mois["statut"] == "VIDE"

    logs = pilot.logements_du_proprietaire("PROP_A", db_path=str(db_path))
    assert {l["id"] for l in logs} == {"LOG_A1"}


# ── TEST E — cohérence Résultats ↔ Lot10 ────────────────────────────────────────────────────────

def test_coherence_resultats_lot10(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _construire_cerdine_like(db_path)

    v = pilot.vue(mois="2026-07", proprietaire_id="PROP_A", logement_id="LOG_A1", db_path=str(db_path))
    conn = sqlite3.connect(str(db_path))
    run_id = conn.execute("SELECT run_id FROM lot10_runs WHERE actif=1").fetchone()[0]
    row = conn.execute(
        "SELECT total_payout_mois, total_menage_mois, nb_reservations FROM lot10_net_reglement "
        "WHERE run_id=? AND logement_id='LOG_A1' AND mois='2026-07'", (run_id,)).fetchone()
    conn.close()
    assert v["kpi"]["total_payout"] == row[0]
    assert v["kpi"]["menage"] == row[1]
    assert v["kpi"]["nb_reservations"] == row[2]


# ── TEST F — cohérence Résultats ↔ Facturation (Lot12, quand comparable) ────────────────────────

def test_coherence_resultats_lot12(tmp_path):
    from app.services import controles_lot11_service
    from app.services import lot12_prefactures_service

    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    _construire_cerdine_like(db_path)
    assert controles_lot11_service.construire(db_path=db_path)["ok"] is True
    assert lot12_prefactures_service.construire(db_path=db_path)["ok"] is True

    v = pilot.vue(mois="2026-07", proprietaire_id="PROP_A", logement_id="LOG_A1", db_path=str(db_path))
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    run12 = conn.execute("SELECT run_id FROM lot12_runs WHERE actif=1").fetchone()[0]
    # `lot12_prefactures_entete` ne porte pas de colonne payout brute (agrégat facture, pas
    # règlement) : le champ réellement comparable entre Résultats et Lot12 est nb_reservations.
    entete = conn.execute(
        "SELECT nb_reservations FROM lot12_prefactures_entete WHERE run_id=? "
        "AND logement_id='LOG_A1' AND mois='2026-07'", (run12,)).fetchone()
    conn.close()
    if entete is not None and entete["nb_reservations"] is not None:
        assert v["kpi"]["nb_reservations"] == entete["nb_reservations"]


# ── TEST G — état sans données / calcul non disponible / comptabilité non alimentée ─────────────

def test_etat_sans_donnees_et_calcul_non_disponible(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    _ref_minimal(conn)
    conn.commit()
    conn.close()

    v = pilot.vue(mois="2026-07", db_path=str(db_path))
    assert v["statut"] == "NON_DISPONIBLE"   # Lot10 jamais exécuté


def test_comptabilite_non_alimentee(tmp_db):
    from fastapi.testclient import TestClient
    from app.main import app
    with TestClient(app) as client:
        r = client.get("/comptabilite")
    assert r.status_code == 200
    assert "non encore alimentée" in r.text.lower()

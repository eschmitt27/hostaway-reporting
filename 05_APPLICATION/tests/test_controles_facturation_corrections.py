"""Mission « corriger ensemble contrôles / facturation » — non-régression.

TEST A — statut Hostaway : New/Modified inclus, tout autre statut exclu (jamais compté).
TEST B — pas de duplication : plusieurs générations Hostaway/dataset ne multiplient jamais une
         même réservation économique dans l'agrégation Lot10.
TEST C — cas Cerdine-like (payout/ménage attendus exacts, jamais ×4) — jamais hardcodé.
TEST D — noms UI : referentiel_service résout PROP/LOG en vrais noms, fallback explicite sinon.
TEST E — assiette : négative → 0 automatique → correction manuelle possible → justification
         obligatoire → recalcul → brute conservée → jamais masqué (ASSIETTE_CORRIGEE_MANUELLEMENT).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import orchestrateur_moteur as om
from app.services import flux_unifie_service
from app.services import referentiel_service as ref_svc
from app.services import assiette_correction_service as assiette_svc
from app.services import controles_actionnable_service as act


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
                 payout=222.22, menage=69.0):
    conn.execute(
        "INSERT OR IGNORE INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, "
        "statut, nb_listings, nb_reservations, nb_payouts) "
        "VALUES (?, ?, 'API', '2026-07-01T00:00:00Z', 'SUCCES', 1, 1, 1)",
        (extraction_id, f"R-{extraction_id}"))
    conn.execute(
        "INSERT INTO hostaway_reservations (extraction_id, reservation_id, listing_map_id, "
        "source, channel_type, source_financiere, status, total_price, is_owner_stay, "
        "inclure_resultat, check_in_date, check_out_date, nights) "
        "VALUES (?, ?, ?, 'HOSTAWAY', 'AIRBNB', 'AIRBNB', ?, 300.0, 'false', 'OUI', "
        "'2026-07-05', '2026-07-07', 2)",
        (extraction_id, reservation_id, listing, status))
    conn.execute(
        "INSERT INTO hostaway_payouts (extraction_id, reservation_id, listing_map_id, "
        "statut_calcul_payout, payout_calcule, menage_retenu, assiette_commission) "
        "VALUES (?, ?, ?, 'NORMAL', ?, ?, ?)",
        (extraction_id, reservation_id, listing, payout, menage, round(payout - menage, 2)))


def _cloture_ouverte(conn, mois="2026-07"):
    conn.execute(
        "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) VALUES (?, 'OUVERT', 'IMP-1')",
        (mois,))


# ── TEST A — Status ∈ {new, modified} inclus, tout autre statut exclu ───────────────────────────

def test_statut_new_modified_inclus_autre_exclu(tmp_path):
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

    resultat = om.executer_reservations(db_path=db_path)
    assert resultat["ok"] is True, resultat

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = {r["reservation_id_hostaway"]: dict(r) for r in conn.execute(
        "SELECT reservation_id_hostaway, source, statut_controle, code_anomalie, montant_retenu "
        "FROM reservations_resolues")}
    conn.close()

    assert rows["70001"]["statut_controle"] == "VALIDE"
    assert rows["70001"]["montant_retenu"] == 222.22
    assert rows["70002"]["statut_controle"] == "VALIDE"
    assert rows["70002"]["montant_retenu"] == 222.22
    assert rows["70003"]["source"] == "STATUT_HOSTAWAY_HORS_PERIMETRE"
    assert rows["70003"]["code_anomalie"] == "STATUT_HOSTAWAY_HORS_PERIMETRE"
    assert rows["70003"]["statut_controle"] == "EXCLU_RESULTAT"
    assert rows["70003"]["montant_retenu"] in (0, 0.0, None)


# ── TEST B / C — pas de ×N malgré plusieurs générations Hostaway/dataset ────────────────────────

def test_pas_de_duplication_multi_generations_cerdine_like(tmp_path):
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    _ref_minimal(conn)
    _cloture_ouverte(conn)
    _reservation(conn, extraction_id="HAX-T1", reservation_id="70001", status="new",
                payout=222.22, menage=69.0)
    conn.commit()
    conn.close()

    # Première génération (comme Mission 14, HAX-44B3AA50E576).
    assert om.executer_reservations(db_path=db_path)["ok"] is True

    # Deuxième extraction Hostaway réelle de la MÊME réservation (comme Mission 17,
    # HAX-97F6E34A7D92) — append-only, l'ancienne extraction reste en base.
    conn = get_db(db_path)
    _reservation(conn, extraction_id="HAX-T2", reservation_id="70001", status="new",
                payout=222.22, menage=69.0)
    conn.commit()
    conn.close()
    assert om.executer_reservations(db_path=db_path)["ok"] is True

    assert flux_unifie_service.construire(db_path=db_path)["ok"] is True
    lot10 = om.executer_lot10(db_path=db_path)
    assert lot10["ok"] is True, lot10

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    run_id = conn.execute("SELECT run_id FROM lot10_runs WHERE actif=1").fetchone()[0]
    commissions = [dict(r) for r in conn.execute(
        "SELECT reservation_calc_id, payout_calcule, menage_retenu FROM lot10_commissions "
        "WHERE run_id=?", (run_id,))]
    reglement = conn.execute(
        "SELECT total_payout_mois, total_menage_mois, nb_reservations FROM lot10_net_reglement "
        "WHERE run_id=? AND logement_id='LOG_A1' AND mois='2026-07'", (run_id,)).fetchone()
    conn.close()

    assert len(commissions) == 1, f"réservation dupliquée : {commissions}"
    assert commissions[0]["payout_calcule"] == 222.22
    assert commissions[0]["menage_retenu"] == 69.0
    assert dict(reglement)["total_payout_mois"] == 222.22   # jamais 444.44 (×2) ni ×4
    assert dict(reglement)["total_menage_mois"] == 69.0
    assert dict(reglement)["nb_reservations"] == 1


# ── TEST D — vrais noms, jamais un ID brut silencieux ────────────────────────────────────────────

def test_libelle_proprietaire_et_logement_resolus_et_fallback(tmp_db):
    conn = get_db(tmp_db)
    _ref_minimal(conn, logement_id="LOG_A1", proprietaire_id="PROP_A")
    conn.commit()
    conn.close()

    assert ref_svc.libelle_proprietaire("PROP_A", db_path=tmp_db) == "Cédrine Delrieu"
    assert ref_svc.libelle_logement("LOG_A1", db_path=tmp_db) == "Logement LOG_A1"
    assert ref_svc.libelle_proprietaire("PROP_INCONNU", db_path=tmp_db) == \
        "Propriétaire non résolu (PROP_INCONNU)"
    assert ref_svc.libelle_logement("LOG_INCONNU", db_path=tmp_db) == \
        "Logement non résolu (LOG_INCONNU)"


# ── TEST E — assiette négative → 0 auto → correction manuelle → justification → recalcul ────────

def test_correction_assiette_manuelle_tracee_et_recalcul(tmp_path, monkeypatch):
    import app.config as cfg

    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    _ref_minimal(conn, logement_id="LOG_A1", proprietaire_id="PROP_A")
    _cloture_ouverte(conn)
    # Assiette brute négative : ménage (69) > payout (29) — cas réel audité Mission 14f-bis.
    _reservation(conn, extraction_id="HAX-T1", reservation_id="70001", status="new",
                payout=29.0, menage=69.0)
    conn.commit()
    conn.close()
    monkeypatch.setattr(cfg, "DB_PATH", db_path)

    assert om.executer_reservations(db_path=db_path)["ok"] is True
    assert flux_unifie_service.construire(db_path=db_path)["ok"] is True
    assert om.executer_lot10(db_path=db_path)["ok"] is True
    from app.services import controles_lot11_service
    assert controles_lot11_service.construire(db_path=db_path)["ok"] is True

    data = act.load_dashboard(vue="tous", code="ASSIETTE_NEGATIVE_RAMENEE_ZERO", page=1, db_path=str(db_path))
    assert data["rows"], "constat ASSIETTE_NEGATIVE_RAMENEE_ZERO absent"
    ctrl = data["rows"][0]["ctrl_opaque"]

    prep = assiette_svc.preparer_formulaire(ctrl, db_path=str(db_path))
    assert prep["assiette_brute"] == -40.0
    assert prep["assiette_automatique"] == 0.0
    assert prep["commission_actuelle"] == 0.0
    assert "montant_percu" not in prep  # jamais une valeur d'un autre écran qui s'y glisse

    # Justification obligatoire.
    refus = assiette_svc.corriger(ctrl, nouvelle_assiette="12.00", justification="", db_path=str(db_path))
    assert refus["ok"] is False
    assert refus["code"] == "JUSTIFICATION_OBLIGATOIRE"

    resultat = assiette_svc.corriger(
        ctrl, nouvelle_assiette="12.00", justification="Nuit compensée hors Hostaway",
        acteur="test", db_path=str(db_path))
    assert resultat["ok"] is True
    assert resultat["ancienne_commission"] == 0.0
    assert resultat["nouvelle_commission"] == round(12.00 * prep["taux_commission"], 2)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT assiette_brute, assiette_automatique, assiette_manuelle, justification "
        "FROM assiette_corrections_manuelles WHERE actif=1").fetchone()
    conn.close()
    assert row["assiette_brute"] == -40.0       # jamais transformée
    assert row["assiette_automatique"] == 0.0    # jamais transformée
    assert row["assiette_manuelle"] == 12.0
    assert row["justification"] == "Nuit compensée hors Hostaway"

    recalc = assiette_svc.recalculer(db_path=str(db_path))
    assert recalc["ok"] is True, recalc

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    run_id = conn.execute("SELECT run_id FROM lot10_runs WHERE actif=1").fetchone()[0]
    com = conn.execute(
        "SELECT assiette_commission, commission_conciergerie FROM lot10_commissions "
        "WHERE run_id=? AND reservation_calc_id=?", (run_id, prep["reservation_calc_id"])).fetchone()
    conn.close()
    assert com["assiette_commission"] == -40.0    # brute jamais réécrite, même après recalcul
    assert com["commission_conciergerie"] == resultat["nouvelle_commission"]

    # Le contrôle ne disparaît pas silencieusement : il devient traçable, jamais masqué.
    data2 = act.load_dashboard(vue="tous", code="ASSIETTE_CORRIGEE_MANUELLEMENT", page=1, db_path=str(db_path))
    assert data2["rows"], "ASSIETTE_CORRIGEE_MANUELLEMENT doit rester visible après correction"

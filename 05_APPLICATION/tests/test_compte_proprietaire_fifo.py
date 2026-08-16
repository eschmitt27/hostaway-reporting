"""Compte global propriétaire — allocations FIFO, crédits, compensations.

Les scénarios chiffrés reprennent exactement ceux du cahier des charges, pour qu'un écart se lise
comme un écart au métier et non comme un désaccord d'interprétation.

Identifiants entièrement synthétiques (PROP_9xxx, FPR-TEST-xxx).
"""
import uuid

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import compte_proprietaire_service as cpt

PROP = "PROP_9001"
AUTRE = "PROP_9002"


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "compte.db"
    apply_migrations(p)
    return p


def _facture(db, proprietaire, montant, date_emission, *, numero="", logement="LOG_9001",
             fid=None, type_document="FACTURE", statut="EMIS"):
    fid = fid or "FPR-" + uuid.uuid4().hex[:8].upper()
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, numero_facture, type_document, "
            "proprietaire_id, logement_id, mois, montant_total, statut, date_emission) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (fid, numero or fid, type_document, proprietaire, logement,
             date_emission[:7], montant, statut, date_emission))
        conn.commit()
    finally:
        conn.close()
    return fid


def _mouvement(db, proprietaire, montant, date_mouvement, *, sens="PROPRIETAIRE_VERS_SOCIETE",
               nature="ACOMPTE_PROPRIETAIRE", statut="VALIDE", mid=None):
    mid = mid or "MTP-" + uuid.uuid4().hex[:8].upper()
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
            "date_mouvement, montant, sens, nature, statut) VALUES (?,?,?,?,?,?,?)",
            (mid, proprietaire, date_mouvement, montant, sens, nature, statut))
        conn.commit()
    finally:
        conn.close()
    return mid


# ── Moteur pur ──────────────────────────────────────────────────────────────────────────────────

def test_fifo_solde_de_la_plus_ancienne_a_la_plus_recente():
    """§22 — F1=200, F2=300, F3=400, paiement 750 → 200 / 300 / 250, reste 150 sur F3."""
    factures = [
        {"facture_id_opaque": "F1", "montant_total": 200.0},
        {"facture_id_opaque": "F2", "montant_total": 300.0},
        {"facture_id_opaque": "F3", "montant_total": 400.0},
    ]
    sources = [{"source_type": cpt.SRC_PAIEMENT, "source_ref": "P1",
                "source_date": "2026-03-01", "montant": 750.0}]
    alloc = cpt.calculer_fifo(factures, sources)
    assert [(a["facture_id_opaque"], a["montant_alloue"]) for a in alloc] == [
        ("F1", 200.0), ("F2", 300.0), ("F3", 250.0)]
    assert [a["rang_fifo"] for a in alloc] == [1, 2, 3]


def test_fifo_n_alloue_jamais_plus_que_le_du():
    factures = [{"facture_id_opaque": "F1", "montant_total": 100.0}]
    sources = [{"source_type": cpt.SRC_PAIEMENT, "source_ref": "P1",
                "source_date": "2026-03-01", "montant": 500.0}]
    alloc = cpt.calculer_fifo(factures, sources)
    assert sum(a["montant_alloue"] for a in alloc) == 100.0


def test_fifo_sans_facture_n_alloue_rien():
    alloc = cpt.calculer_fifo([], [{"source_type": cpt.SRC_PAIEMENT, "source_ref": "P1",
                                    "source_date": "2026-03-01", "montant": 100.0}])
    assert alloc == []


def test_fifo_deterministe():
    factures = [{"facture_id_opaque": f"F{i}", "montant_total": 100.0} for i in range(1, 6)]
    sources = [{"source_type": cpt.SRC_PAIEMENT, "source_ref": f"P{i}",
                "source_date": f"2026-0{i}-01", "montant": 130.0} for i in range(1, 4)]
    assert cpt.calculer_fifo(factures, sources) == cpt.calculer_fifo(factures, sources)


# ── Scénarios du cahier des charges ─────────────────────────────────────────────────────────────

def test_scenario_22_paiement_partiel_sur_trois_factures(db):
    _facture(db, PROP, 200, "2026-01-31", numero="F-2026-000001")
    _facture(db, PROP, 300, "2026-02-28", numero="F-2026-000002")
    f3 = _facture(db, PROP, 400, "2026-03-31", numero="F-2026-000003")
    _mouvement(db, PROP, 750, "2026-04-05")

    p = cpt.position(PROP, db_path=db)
    par_id = {f["facture_id_opaque"]: f for f in p["factures"]}
    assert par_id[f3]["solde"] == 150.0
    assert par_id[f3]["statut_reglement"] == cpt.ST_PARTIELLE
    assert [f["statut_reglement"] for f in p["factures"][:2]] == [cpt.ST_REGLEE, cpt.ST_REGLEE]
    assert p["creance_restante"] == 150.0
    assert p["credit_disponible"] == 0.0
    assert p["position_nette"] == 150.0


def test_scenario_23_surpaiement_devient_credit(db):
    """§23 — 900 € de factures, 1 000 € payés : tout soldé, 100 € de crédit. Aucune facture fictive."""
    _facture(db, PROP, 200, "2026-01-31")
    _facture(db, PROP, 300, "2026-02-28")
    _facture(db, PROP, 400, "2026-03-31")
    _mouvement(db, PROP, 1000, "2026-04-05")

    p = cpt.position(PROP, db_path=db)
    assert all(f["statut_reglement"] == cpt.ST_REGLEE for f in p["factures"])
    assert p["creance_restante"] == 0.0
    assert p["credit_disponible"] == 100.0
    assert p["position_nette"] == -100.0, "un crédit est une créance négative pour la conciergerie"
    assert len(p["factures"]) == 3, "aucune facture fictive n'a été créée"


def test_scenario_24_credit_anterieur_solde_une_facture_ulterieure(db):
    """§24 — 100 € de crédit, puis facture de 80 € : soldée, crédit restant 20 €."""
    _mouvement(db, PROP, 100, "2026-01-10")
    p = cpt.position(PROP, db_path=db)
    assert p["credit_disponible"] == 100.0 and p["creance_restante"] == 0.0

    _facture(db, PROP, 80, "2026-02-28")
    p = cpt.position(PROP, db_path=db)
    assert p["factures"][0]["statut_reglement"] == cpt.ST_REGLEE
    assert p["credit_disponible"] == 20.0
    assert p["creance_restante"] == 0.0


def test_scenario_26_compensation_avec_reversement(db):
    """§26 — 1 000 € à reverser, 200 € de factures : compensation 200, virement net 800.

    Les quatre montants doivent rester lisibles séparément — aucun écrasement économique.
    """
    _facture(db, PROP, 200, "2026-01-31")
    _mouvement(db, PROP, 1000, "2026-02-05", sens="SOCIETE_VERS_PROPRIETAIRE",
               nature="REMBOURSEMENT_PROPRIETAIRE")

    p = cpt.position(PROP, db_path=db)
    assert p["factures_a_recevoir"] == 200.0
    assert p["reversements_dus"] == 1000.0
    assert p["compensations"] == 200.0
    assert p["virement_net"] == 800.0
    assert p["creance_restante"] == 0.0
    assert p["position_nette"] == -800.0


def test_scenario_27_compensation_utilise_aussi_le_fifo(db):
    """§27 — plusieurs factures ouvertes : les plus anciennes sont compensées d'abord."""
    f1 = _facture(db, PROP, 100, "2026-01-31", numero="F-2026-000001")
    f2 = _facture(db, PROP, 100, "2026-02-28", numero="F-2026-000002")
    f3 = _facture(db, PROP, 100, "2026-03-31", numero="F-2026-000003")
    _mouvement(db, PROP, 250, "2026-04-05", sens="SOCIETE_VERS_PROPRIETAIRE",
               nature="REMBOURSEMENT_PROPRIETAIRE")

    p = cpt.position(PROP, db_path=db)
    par_id = {f["facture_id_opaque"]: f for f in p["factures"]}
    assert par_id[f1]["solde"] == 0.0
    assert par_id[f2]["solde"] == 0.0
    assert par_id[f3]["solde"] == 50.0
    assert p["compensations"] == 250.0
    assert p["virement_net"] == 0.0


# ── Cas critiques §51 ───────────────────────────────────────────────────────────────────────────

def test_paiement_exact(db):
    _facture(db, PROP, 250, "2026-01-31")
    _mouvement(db, PROP, 250, "2026-02-05")
    p = cpt.position(PROP, db_path=db)
    assert p["creance_restante"] == 0.0 and p["credit_disponible"] == 0.0
    assert p["position_nette"] == 0.0


def test_paiement_partiel_unique(db):
    _facture(db, PROP, 250, "2026-01-31")
    _mouvement(db, PROP, 100, "2026-02-05")
    p = cpt.position(PROP, db_path=db)
    assert p["factures"][0]["statut_reglement"] == cpt.ST_PARTIELLE
    assert p["creance_restante"] == 150.0


def test_douze_factures_un_paiement_global(db):
    for i in range(1, 13):
        _facture(db, PROP, 100, f"2026-{i:02d}-28", numero=f"F-2026-{i:06d}")
    _mouvement(db, PROP, 1000, "2027-01-05")

    p = cpt.position(PROP, db_path=db)
    soldees = [f for f in p["factures"] if f["statut_reglement"] == cpt.ST_REGLEE]
    assert len(soldees) == 10, "les 10 plus anciennes doivent être soldées, pas dix quelconques"
    assert [f["numero_facture"] for f in soldees] == [f"F-2026-{i:06d}" for i in range(1, 11)]
    assert p["creance_restante"] == 200.0


def test_plusieurs_logements_un_seul_compte(db):
    """Le compte est global : un paiement solde les factures de tous les logements."""
    _facture(db, PROP, 100, "2026-01-31", logement="LOG_9001")
    _facture(db, PROP, 100, "2026-02-28", logement="LOG_9002")
    _mouvement(db, PROP, 150, "2026-03-05")

    p = cpt.position(PROP, db_path=db)
    assert p["creance_restante"] == 50.0
    # Le détail par logement reste visible : le compte global ne l'efface pas.
    assert {f["logement_id"] for f in p["factures"]} == {"LOG_9001", "LOG_9002"}


def test_cloisonnement_entre_proprietaires(db):
    _facture(db, PROP, 100, "2026-01-31")
    _mouvement(db, AUTRE, 500, "2026-02-05")
    p = cpt.position(PROP, db_path=db)
    assert p["creance_restante"] == 100.0, "le paiement d'un autre propriétaire ne solde rien ici"
    assert cpt.position(AUTRE, db_path=db)["credit_disponible"] == 500.0


def test_idempotence_du_recalcul(db):
    _facture(db, PROP, 200, "2026-01-31")
    _facture(db, PROP, 300, "2026-02-28")
    _mouvement(db, PROP, 400, "2026-03-05")

    a = cpt.recalculer(PROP, db_path=db)
    b = cpt.recalculer(PROP, db_path=db)
    assert a["empreinte_entrees"] == b["empreinte_entrees"]
    assert a["empreinte_allocations"] == b["empreinte_allocations"]
    conn = get_db(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM proprietaire_allocations").fetchone()[0] == \
               a["nb_allocations"], "un recalcul remplace, il n'empile pas"
        # Le journal des recalculs, lui, conserve les deux passages.
        assert conn.execute("SELECT COUNT(*) FROM proprietaire_recalculs").fetchone()[0] == 2
    finally:
        conn.close()


def test_position_survit_a_un_redemarrage(db):
    """La position se reconstruit depuis la base, sans état en mémoire."""
    _facture(db, PROP, 300, "2026-01-31")
    _mouvement(db, PROP, 120, "2026-02-05")
    avant = cpt.position(PROP, db_path=db)

    import importlib
    importlib.reload(cpt)
    apres = cpt.position(PROP, db_path=db)
    assert apres["creance_restante"] == avant["creance_restante"] == 180.0


# ── Ce qui ne doit PAS compter ──────────────────────────────────────────────────────────────────

def test_facture_brouillon_n_est_pas_une_creance(db):
    _facture(db, PROP, 500, "2026-01-31", statut="BROUILLON")
    assert cpt.position(PROP, db_path=db)["factures_a_recevoir"] == 0.0


def test_facture_annulee_n_est_pas_une_creance(db):
    _facture(db, PROP, 500, "2026-01-31", statut="ANNULE")
    assert cpt.position(PROP, db_path=db)["factures_a_recevoir"] == 0.0


def test_mouvement_non_valide_ne_solde_rien(db):
    _facture(db, PROP, 100, "2026-01-31")
    _mouvement(db, PROP, 100, "2026-02-05", statut="BROUILLON")
    p = cpt.position(PROP, db_path=db)
    assert p["creance_restante"] == 100.0
    assert p["paiements_recus"] == 0.0


def test_avoir_n_est_pas_une_source_fifo(db):
    """Un avoir réduit déjà la créance ailleurs (créance négative) : l'utiliser ici le compterait
    deux fois. Régression constatée en test, pas supposée."""
    _facture(db, PROP, 300, "2026-01-31")
    _facture(db, PROP, 100, "2026-02-10", type_document="AVOIR")
    p = cpt.position(PROP, db_path=db)
    assert p["sources"] == [], "l'avoir ne doit pas devenir une source de paiement"
    assert p["creance_restante"] == 300.0
    assert p["factures_a_recevoir"] == 300.0, "l'avoir n'est pas une facture à recevoir non plus"


# ── Traçabilité ─────────────────────────────────────────────────────────────────────────────────

def test_chaque_allocation_est_tracee(db):
    """§25 — source, facture, montant : le solde doit être explicable ligne à ligne."""
    f1 = _facture(db, PROP, 200, "2026-01-31")
    m1 = _mouvement(db, PROP, 250, "2026-02-05")
    p = cpt.position(PROP, db_path=db)
    assert len(p["allocations"]) == 1
    a = p["allocations"][0]
    assert a["source_ref"] == m1 and a["facture_id_opaque"] == f1
    assert a["montant_alloue"] == 200.0 and a["source_type"] == cpt.SRC_PAIEMENT


def test_imputations_facture_expose_le_montant_impute(db):
    f1 = _facture(db, PROP, 200, "2026-01-31")
    _mouvement(db, PROP, 120, "2026-02-05")
    cpt.recalculer(PROP, db_path=db)
    assert cpt.imputations_facture(f1, db_path=db) == 120.0


def test_historique_des_recalculs_conserve_les_empreintes(db):
    _facture(db, PROP, 100, "2026-01-31")
    cpt.recalculer(PROP, db_path=db)
    _mouvement(db, PROP, 100, "2026-02-05")
    cpt.recalculer(PROP, db_path=db)

    histo = cpt.historique_recalculs(PROP, db_path=db)
    assert len(histo) == 2
    assert histo[0]["empreinte_entrees"] != histo[1]["empreinte_entrees"], (
        "l'ajout d'un paiement doit changer l'empreinte des entrées")
    assert histo[0]["creance_restante"] == 0.0


def test_proprietaires_concernes(db):
    _facture(db, PROP, 100, "2026-01-31")
    _mouvement(db, AUTRE, 50, "2026-02-05")
    _facture(db, "PROP_9003", 100, "2026-01-31", statut="BROUILLON")
    assert cpt.proprietaires_concernes(db_path=db) == [PROP, AUTRE]

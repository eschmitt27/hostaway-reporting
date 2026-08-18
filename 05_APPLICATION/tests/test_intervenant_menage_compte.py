"""Compte FIFO intervenant ménage interne — mission §16-20/§36.

Le test 36 de la mission fixe l'exemple : 3 ménages validés à 30€ = dette 90€, paiement 50€ →
reste 40€, dette suivante 60€, paiement 80€ → ancienne dette (40) soldée + 40 sur la nouvelle,
nouvelle dette restante 20€. Testé ici directement sur `intervenant_menage_dettes`/`recalculer`
(le calcul du tarif lui-même — `tarif_menage` — est testé séparément, sur `ref_couts_menage_interne`
en SQLite, comme au runtime réel — plus de lecture REF_Setup.xlsm à ce niveau).
"""
from __future__ import annotations

import uuid

import pytest

from app.db.connection import get_db
from app.services import intervenant_menage_compte_service as compte

INTERVENANT = "FRS-INT-0001"


def _inserer_dette(db_path, montant, date_validation, *, menage_id=None):
    menage_id = menage_id or ("MEN-" + uuid.uuid4().hex[:8].upper())
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO intervenant_menage_dettes (dette_id_opaque, intervenant_id, "
            "menage_id_opaque, date_validation, tarif_unitaire, montant) VALUES (?,?,?,?,?,?)",
            ("DIM-" + uuid.uuid4().hex[:12].upper(), INTERVENANT, menage_id, date_validation,
             montant, montant))
        conn.commit()
    finally:
        conn.close()


def test_exemple_metier_fifo_deux_vagues_de_dettes(tmp_db):
    # Vague 1 : 3 ménages validés à 30€ = 90€ de dette.
    for i in range(3):
        _inserer_dette(tmp_db, 30.0, f"2026-05-0{i+1}")

    compte.enregistrer_paiement(INTERVENANT, 50.0, "2026-05-10", db_path=tmp_db)
    pos = compte.position(INTERVENANT, db_path=tmp_db)
    assert pos["dette_totale"] == 90.0
    assert pos["dette_restante"] == 40.0

    # Vague 2 : 2 ménages supplémentaires à 30€ = 60€ de dette.
    for i in range(2):
        _inserer_dette(tmp_db, 30.0, f"2026-06-0{i+1}")

    compte.enregistrer_paiement(INTERVENANT, 80.0, "2026-06-10", db_path=tmp_db)
    pos = compte.position(INTERVENANT, db_path=tmp_db)
    assert pos["dette_totale"] == 150.0
    assert pos["dette_restante"] == 20.0


def test_fifo_dette_la_plus_ancienne_payee_en_premier(tmp_db):
    _inserer_dette(tmp_db, 30.0, "2026-05-01")
    _inserer_dette(tmp_db, 30.0, "2026-05-15")
    compte.enregistrer_paiement(INTERVENANT, 30.0, "2026-05-20", db_path=tmp_db)

    pos = compte.position(INTERVENANT, db_path=tmp_db)
    soldes = {d["date_validation"]: d["solde"] for d in pos["dettes"]}
    assert soldes["2026-05-01"] == 0.0
    assert soldes["2026-05-15"] == 30.0


def test_un_paiement_peut_couvrir_plusieurs_dettes(tmp_db):
    _inserer_dette(tmp_db, 30.0, "2026-05-01")
    _inserer_dette(tmp_db, 30.0, "2026-05-02")
    compte.enregistrer_paiement(INTERVENANT, 60.0, "2026-05-10", db_path=tmp_db)

    pos = compte.position(INTERVENANT, db_path=tmp_db)
    assert pos["dette_restante"] == 0.0
    assert len(pos["allocations"]) == 2


def test_recalcul_idempotent(tmp_db):
    _inserer_dette(tmp_db, 30.0, "2026-05-01")
    compte.enregistrer_paiement(INTERVENANT, 30.0, "2026-05-10", db_path=tmp_db)
    a = compte.position(INTERVENANT, db_path=tmp_db)
    b = compte.position(INTERVENANT, db_path=tmp_db)
    assert a["dette_restante"] == b["dette_restante"] == 0.0
    assert len(a["allocations"]) == len(b["allocations"]) == 1


# ── Tarif : résolu via ref_couts_menage_interne (SQLite), jamais inventé ────────────────────────────

def _importer_referentiel(db_path, *, avec_tarif=True):
    """Simule un import REF_Setup abouti : une ligne `ref_setup_imports` IMPORTE, et si demandé une
    ligne de tarif interne standard (30€, valide depuis 2026-01-01, sans restriction)."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES (?,?,?,?,?)",
            ("IMP-TEST-0001", "2026-08-18T00:00:00Z", "REF_Setup.xlsm", "TEST", "IMPORTE"))
        if avec_tarif:
            conn.execute(
                "INSERT INTO ref_couts_menage_interne (cout_menage_interne_id, actif, "
                "date_debut_validite, montant_interne_standard, import_id) VALUES (?,?,?,?,?)",
                ("CIM_0001", "OUI", "2026-01-01", "30.0", "IMP-TEST-0001"))
        conn.commit()
    finally:
        conn.close()


def test_tarif_menage_resolu_depuis_ref_couts_menage_interne(tmp_db):
    _importer_referentiel(tmp_db)

    resultat = compte.tarif_menage(INTERVENANT, "LOG_0001", "2026-07-01", db_path=tmp_db)
    assert resultat["statut"] == "OK"
    assert resultat["montant"] == 30.0


def test_tarif_absent_ne_devine_rien(tmp_db):
    _importer_referentiel(tmp_db, avec_tarif=False)

    resultat = compte.tarif_menage(INTERVENANT, "LOG_0001", "2026-07-01", db_path=tmp_db)
    assert resultat["statut"] == "MISSING"
    assert resultat["montant"] is None


def test_generer_dettes_marque_a_controler_sans_tarif(tmp_db):
    _importer_referentiel(tmp_db, avec_tarif=False)

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages (menage_id_opaque, mois, logement_id, proprietaire_id, "
            "type_menage, fournisseur_id_opaque, statut) VALUES (?,?,?,?,?,?,?)",
            ("MEN-TEST-001", "2026-07", "LOG_0001", "PROP_0001", "INTERNE", INTERVENANT, "VALIDE"))
        conn.commit()
    finally:
        conn.close()

    resultat = compte.generer_dettes(INTERVENANT, db_path=tmp_db)
    assert resultat["nb_creees"] == 0
    assert len(resultat["a_controler"]) == 1
    assert resultat["a_controler"][0]["code"] == compte.A_CONTROLER_METIER
    assert compte.dettes(INTERVENANT, db_path=tmp_db) == []


def test_generer_dettes_idempotent_pas_de_doublon(tmp_db):
    _importer_referentiel(tmp_db)

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO menages (menage_id_opaque, mois, logement_id, proprietaire_id, "
            "type_menage, fournisseur_id_opaque, statut, date_realisation) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("MEN-TEST-002", "2026-07", "LOG_0001", "PROP_0001", "INTERNE", INTERVENANT, "VALIDE",
             "2026-07-05"))
        conn.commit()
    finally:
        conn.close()

    premiere = compte.generer_dettes(INTERVENANT, db_path=tmp_db)
    seconde = compte.generer_dettes(INTERVENANT, db_path=tmp_db)
    assert premiere["nb_creees"] == 1
    assert seconde["nb_creees"] == 0
    assert len(compte.dettes(INTERVENANT, db_path=tmp_db)) == 1

"""§37/§38 — une charge VALIDE peut revenir À CONTRÔLER, mais pas n'importe quand.

Le module Charges est jugé satisfaisant par l'utilisateur : la seule demande est ce retour en
arrière. Il n'est légitime que tant qu'il reste RÉVERSIBLE — sinon on réécrirait, en amont, le
contenu d'un document déjà remis ou d'une période déjà arrêtée.

Deux verrous, testés ici :
  · mois clôturé                                  → refus
  · refacturation déjà portée par une facture ÉMISE → refus
Et un troisième, non négociable : le motif est obligatoire.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import charges_saisie_service as saisie


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "charges.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def _charge_validee(db, *, mois="2026-08") -> str:
    charge_id = "CHG-TEST-" + mois.replace("-", "")
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO charges (charge_id, date_charge, mois, montant, categorie_charge_id, "
            "statut, statut_controle) VALUES (?,?,?,?,?,?,?)",
            (charge_id, f"{mois}-15", mois, 120.0, "CHG_001", "ACTIVE", "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    return charge_id


def _statut(db, charge_id: str) -> str:
    conn = get_db(db)
    try:
        return conn.execute("SELECT statut_controle FROM charges WHERE charge_id=?",
                            (charge_id,)).fetchone()["statut_controle"]
    finally:
        conn.close()


def test_retour_a_controler_exige_un_motif(db):
    charge_id = _charge_validee(db)
    res = saisie.rouvrir_controle(charge_id, motif="  ", db_path=db)
    assert res["ok"] is False and res["code"] == "MOTIF_OBLIGATOIRE"
    assert _statut(db, charge_id) == "VALIDE"


def test_retour_a_controler_autorise_si_mois_ouvert(db):
    charge_id = _charge_validee(db)
    res = saisie.rouvrir_controle(charge_id, motif="Montant à corriger : TTC saisi au lieu du HT",
                                  acteur="test", db_path=db)
    assert res["ok"] is True
    assert res["statut_controle"] == "A_CONTROLER"
    assert _statut(db, charge_id) == "A_CONTROLER"

    # Le geste est journalisé : une réouverture laisse une trace, comme la validation.
    conn = get_db(db)
    try:
        evt = conn.execute(
            "SELECT COUNT(*) c FROM charge_evenements WHERE charge_id=?", (charge_id,)).fetchone()
    finally:
        conn.close()
    assert evt["c"] >= 1


def test_retour_a_controler_refuse_si_mois_cloture(db):
    charge_id = _charge_validee(db, mois="2026-05")
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
                     "VALUES ('2026-05','CLOTURE','IMP-TEST')")
        conn.commit()
    finally:
        conn.close()

    res = saisie.rouvrir_controle(charge_id, motif="correction", db_path=db)
    assert res["ok"] is False
    assert res["code"] == saisie.E_MOIS_CLOTURE
    assert "clôturé" in res["message"]
    assert _statut(db, charge_id) == "VALIDE"


def test_retour_a_controler_refuse_si_refacturation_sur_facture_emise(db):
    """Le montant refacturé vit déjà dans un document remis : on ne le corrige pas en amont."""
    charge_id = _charge_validee(db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, numero_facture, "
            "proprietaire_id, logement_id, mois, montant_total, statut) "
            "VALUES ('FPR-TEST','2026-08-009','PROP_0001','LOG_0001','2026-08',120.0,'EMIS')")
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes_charge (ligne_id_opaque, "
            "facture_id_opaque, charge_id, code_impact) VALUES ('FPRL-TEST','FPR-TEST',?,'IC')",
            (charge_id,))
        conn.commit()
    finally:
        conn.close()

    res = saisie.rouvrir_controle(charge_id, motif="correction", db_path=db)
    assert res["ok"] is False
    assert res["code"] == saisie.E_REFACTURATION_UTILISEE
    assert "2026-08-009" in res["message"]
    assert _statut(db, charge_id) == "VALIDE"


def test_retour_sur_charge_deja_a_controler_est_sans_effet(db):
    """Idempotence : rejouer le geste ne journalise pas une réouverture qui n'a pas lieu."""
    charge_id = _charge_validee(db)
    saisie.rouvrir_controle(charge_id, motif="première correction", db_path=db)
    res = saisie.rouvrir_controle(charge_id, motif="seconde tentative", db_path=db)
    assert res["ok"] is True and res["inchange"] is True
    assert _statut(db, charge_id) == "A_CONTROLER"

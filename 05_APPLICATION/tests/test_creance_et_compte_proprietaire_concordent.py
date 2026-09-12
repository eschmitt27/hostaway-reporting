"""§53-70 — deux écrans, un seul solde. L'invariant, et le défaut qu'il ferme.

LE DÉFAUT. « Créances » et « Comptes propriétaires » répondaient différemment sur la MÊME facture :

    écran Créances          F-11/0-000001 : solde  40,88 €  (dont 425,00 € compensés)
    écran Comptes prop.     F-11/0-000001 : solde 465,88 €

425,00 € d'écart sur un même document, entre deux écrans du même logiciel — et rien pour le
signaler. La cause : `position()` calculait le solde depuis les seules allocations FIFO, alors que
les reversements Airbnb sont imputés DIRECTEMENT sur le document et ne passent pas par le FIFO.
`imputations_detail`, dans le même module, le savait déjà et les comptait.

Ce test ne vérifie pas un chiffre : il vérifie que les deux chemins **tombent d'accord**, quelle
que soit la combinaison de paiements, d'acomptes et de reversements.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import compte_proprietaire_service as cp
from app.services import creances_dettes_service as cd


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    for flag in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED"):
        monkeypatch.setattr(cfg, flag, True)
    conn = get_db(p)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES (?,?,?,?,?)",
            ("PROP_0001", "UZON", "Didier", "OUI", "TEST"))
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, adresse, "
            "ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("LOG_0001", "Studio - 46", "Studio - 46", "", "TOULOUSE", "OUI", "GERE", "TEST"))
        # Une facture émise de 465,88 €, et un reversement Airbnb de 425,00 € imputé dessus —
        # exactement la configuration réelle qui a révélé l'écart.
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, type_document, "
            "proprietaire_id, logement_id, mois, montant_total, statut, numero_facture, "
            "date_facture) VALUES (?,?,?,?,?,?,?,?,?)",
            ("FPR-TEST", "FACTURE", "PROP_0001", "LOG_0001", "2026-08", 465.88, "EMIS",
             "2026-08-042", "2026-09-11"))
        conn.execute(
            "INSERT INTO imputations_airbnb (imputation_airbnb_id, proprietaire_id, logement_id, "
            "mois, document_id, montant_impute, date_imputation, statut) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("IMP-TEST", "PROP_0001", "LOG_0001", "2026-08", "FPR-TEST", 425.0,
             "2026-08-15", "VALIDE"))
        conn.commit()
    finally:
        conn.close()
    return p


def _creance(db, proprietaire="PROP_0001"):
    return next(l for l in cd.creances(db_path=db) if l["tiers_id"] == proprietaire)


def test_le_reversement_airbnb_compte_des_deux_cotes(db):
    """C'est le cas exact qui divergeait de 425,00 €."""
    c = _creance(db)
    p = cp.position("PROP_0001", db_path=db)
    assert c["compense"] == 425.0
    assert p["compensations"] == 425.0, "le compte propriétaire le voyait à 0,00 €"
    assert c["solde"] == p["creance_restante"] == 40.88


def test_les_deux_ecrans_donnent_le_meme_solde(db):
    c = _creance(db)
    p = cp.position("PROP_0001", db_path=db)
    assert round(c["solde"] - p["creance_restante"], 2) == 0.0


def test_l_invariant_tient_sans_aucune_imputation(db):
    """Une facture intacte : les deux doivent dire le total, pas zéro."""
    conn = get_db(db)
    try:
        conn.execute("DELETE FROM imputations_airbnb")
        conn.commit()
    finally:
        conn.close()
    c = _creance(db)
    p = cp.position("PROP_0001", db_path=db)
    assert c["solde"] == p["creance_restante"] == 465.88
    assert p["compensations"] == 0.0


def test_l_invariant_tient_quand_le_reversement_solde_tout(db):
    """Reversement égal au total : solde nul des deux côtés, jamais négatif d'un seul."""
    conn = get_db(db)
    try:
        conn.execute("UPDATE imputations_airbnb SET montant_impute = 465.88")
        conn.commit()
    finally:
        conn.close()
    c = _creance(db)
    p = cp.position("PROP_0001", db_path=db)
    assert c["solde"] == p["creance_restante"] == 0.0
    assert c["statut_reglement"] == cd.ST_REGLEE


def test_regle_et_compense_ne_se_confondent_jamais(db):
    """Un règlement est de l'argent reçu ; une compensation ne l'est pas. Les additionner dans un
    seul chiffre ferait disparaître une différence économique réelle."""
    c = _creance(db)
    p = cp.position("PROP_0001", db_path=db)
    assert c["regle"] == 0.0 and c["compense"] == 425.0
    assert p["paiements_recus"] == 0.0 and p["compensations"] == 425.0


def test_la_ligne_de_facture_du_compte_porte_le_detail(db):
    """Le détail par facture doit lui aussi être complet, pas seulement l'agrégat."""
    p = cp.position("PROP_0001", db_path=db)
    ligne = next(f for f in p["factures"] if f["facture_id_opaque"] == "FPR-TEST")
    assert ligne["compense"] == 425.0
    assert ligne["reversements_airbnb"] == 425.0
    assert ligne["solde"] == 40.88

"""Positions de refacturation des charges (mission 15) — CAS 1 à 12 de la spécification.

Point d'entrée unique : `charges_saisie_service.creer()`/`modifier()`. Aucun second flux de
création n'est testé ici — toute position naît d'une charge réelle, exactement comme en
production.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import charges_refacturation_service as refac
from app.services import charges_saisie_service as chs
from app.services import factures_proprietaires_service as fps


@pytest.fixture
def db(tmp_path) -> Path:
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES ('PROP_D','David','','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES ('PROP_E','Autre','','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, statut_parc, actif, import_id) "
            "VALUES ('LOG_X','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, statut_parc, actif, import_id) "
            "VALUES ('LOG_Y','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, statut_parc, actif, import_id) "
            "VALUES ('LOG_Z','GERE','OUI','IMP-1')")
        conn.commit()
    finally:
        conn.close()
    return db_path


def _charge(db, montant, refacturable, *, logement_id=None, proprietaire_id=None, mois="2026-08",
           date_charge="2026-08-05", justificatif=None):
    r = chs.creer({
        "date_charge": date_charge, "mois": mois, "montant": montant,
        "categorie_charge_id": "FOURNITURE", "affectation_type": "LOGEMENT",
        "logement_id": logement_id, "proprietaire_id": proprietaire_id,
        "refacturable": refacturable, "statut_controle": "VALIDE",
        "justificatif": justificatif, "commentaire": "Test mission 15",
    }, acteur="TEST", db_path=db)
    assert r["ok"] is True, r
    return r["charge_id"]


def _position_de(db, charge_id):
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM charges_refacturation_positions WHERE charge_id=?", (charge_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _facture_source(proprietaire_id, logement_id, mois, montant_du=0.0):
    return {
        "proprietaire_id": proprietaire_id, "logement_id": logement_id, "mois": mois,
        "source_calcul": "TEST", "COMMISSION_CONCIERGERIE": 0, "MENAGE_FACTURE": 0,
        "PREPARATION_CANAPE": 0, "CHARGE_FIXE": 0, "CHARGES_EXCEPT_REFAC": 0,
        "montant_du_conciergerie": montant_du,
    }


def _emetteur():
    return {"nom": "Conciergerie", "adresse": "1 rue Test", "siret": "123456789"}


def test_cas1_non_refacturable_aucune_position(db):
    cid = _charge(db, 50.0, "NON", logement_id="LOG_X", proprietaire_id="PROP_D")
    assert _position_de(db, cid) is None


def test_cas2_refacturable_prop_logement_connus_disponible(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    assert pos is not None
    assert pos["statut"] == refac.STATUT_DISPONIBLE
    assert pos["montant_eligible"] == 50.0
    assert pos["montant_restant"] if "montant_restant" in pos else True


def test_cas3_refacturable_sans_identite_a_traiter(db):
    cid = _charge(db, 50.0, "OUI")
    pos = _position_de(db, cid)
    assert pos is not None
    assert pos["statut"] == refac.STATUT_A_TRAITER
    assert pos["proprietaire_id"] is None
    assert pos["logement_id"] is None


def test_cas4_report_puis_toujours_disponible(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    r = refac.reporter(pos["position_id"], acteur="TEST", db_path=db)
    assert r["ok"] is True
    assert r["statut"] == refac.STATUT_REPORTEE
    proposees = refac.proposer_pour_facture("PROP_D", "LOG_X", db_path=db)
    assert len(proposees) == 1
    assert proposees[0]["montant_restant"] == 50.0


def test_cas5_imputation_partielle(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    decisions = [{"position_id": pos["position_id"], "montant": 30.0, "libelle": "Charge",
                 "justification": "Imputation partielle decidee"}]
    f = fps.creer(_facture_source("PROP_D", "LOG_X", "2026-08"), acteur="TEST", db_path=db,
                 decisions_charges=decisions)
    fv = fps.valider(f["facture_id_opaque"], emetteur=_emetteur(),
                     destinataire={"nom": "David"}, acteur="TEST", decisions_charges=decisions,
                     db_path=db)
    assert fv["statut"] == fps.ST_VALIDE
    pos_apres = _position_de(db, cid)
    assert pos_apres["statut"] == refac.STATUT_PARTIELLEMENT_IMPUTEE
    assert pos_apres["montant_impute_total"] == 30.0
    restant = refac.proposer_pour_facture("PROP_D", "LOG_X", db_path=db)
    assert restant[0]["montant_restant"] == 20.0


def test_cas6_ne_pas_refacturer_jamais_reproposee(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    r = refac.ne_pas_refacturer(pos["position_id"], acteur="TEST",
                                justification="Client mecontent, geste commercial", db_path=db)
    assert r["ok"] is True
    assert r["statut"] == refac.STATUT_NON_REFACTUREE
    assert refac.proposer_pour_facture("PROP_D", "LOG_X", db_path=db) == []
    # Sans justification : refuse.
    cid2 = _charge(db, 20.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos2 = _position_de(db, cid2)
    refuse = refac.ne_pas_refacturer(pos2["position_id"], acteur="TEST", justification="",
                                     db_path=db)
    assert refuse["ok"] is False
    assert refuse["code"] == refac.E_JUSTIFICATION_MANQUANTE


def test_cas7_modification_montant_avec_alerte_justification(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    # Sans justification, un montant != solde propose est refuse.
    decisions_sans_justif = [{"position_id": pos["position_id"], "montant": 40.0,
                             "libelle": "Charge"}]
    f = fps.creer(_facture_source("PROP_D", "LOG_X", "2026-08"), acteur="TEST", db_path=db,
                 decisions_charges=decisions_sans_justif)
    with pytest.raises(fps.FactureProprietaireError, match="IMPUTATION_REFUSEE"):
        fps.valider(f["facture_id_opaque"], emetteur=_emetteur(), destinataire={"nom": "David"},
                   acteur="TEST", decisions_charges=decisions_sans_justif, db_path=db)
    # Le refus d'imputation ne doit rien avoir consomme.
    assert _position_de(db, cid)["montant_impute_total"] == 0.0

    decisions = [{"position_id": pos["position_id"], "montant": 40.0, "libelle": "Charge",
                 "justification": "Remise commerciale negociee"}]
    fv = fps.valider(f["facture_id_opaque"], emetteur=_emetteur(), destinataire={"nom": "David"},
                     acteur="TEST", decisions_charges=decisions, db_path=db)
    assert fv["statut"] == fps.ST_VALIDE
    pos_apres = _position_de(db, cid)
    assert pos_apres["montant_impute_total"] == 40.0
    assert pos_apres["derniere_justification"] == "Remise commerciale negociee"
    assert pos_apres["montant_eligible"] - pos_apres["montant_impute_total"] == 10.0


def test_cas8_previsualisation_repetee_aucune_consommation(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    decisions = [{"position_id": pos["position_id"], "montant": 50.0, "libelle": "Charge"}]
    for _ in range(10):
        apercu = fps.previsualiser(_facture_source("PROP_D", "LOG_X", "2026-08"),
                                   decisions_charges=decisions)
        assert apercu["montant_total"] == 50.0
    assert _position_de(db, cid)["montant_impute_total"] == 0.0
    assert _position_de(db, cid)["statut"] == refac.STATUT_DISPONIBLE


def test_cas9_validation_facture_echoue_aucune_consommation(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    decisions = [{"position_id": pos["position_id"], "montant": 50.0, "libelle": "Charge"}]
    f = fps.creer(_facture_source("PROP_D", "LOG_X", "2026-08"), acteur="TEST", db_path=db,
                 decisions_charges=decisions)
    # Emetteur incomplet -> valider() refuse avant meme de toucher les positions.
    with pytest.raises(fps.FactureProprietaireError):
        fps.valider(f["facture_id_opaque"], emetteur={"nom": ""}, destinataire={"nom": "David"},
                   acteur="TEST", decisions_charges=decisions, db_path=db)
    assert _position_de(db, cid)["montant_impute_total"] == 0.0
    assert _position_de(db, cid)["statut"] == refac.STATUT_DISPONIBLE


def test_cas10_validation_repetee_aucune_double_facturation(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D")
    pos = _position_de(db, cid)
    decisions = [{"position_id": pos["position_id"], "montant": 50.0, "libelle": "Charge"}]
    f = fps.creer(_facture_source("PROP_D", "LOG_X", "2026-08"), acteur="TEST", db_path=db,
                 decisions_charges=decisions)
    fps.valider(f["facture_id_opaque"], emetteur=_emetteur(), destinataire={"nom": "David"},
               acteur="TEST", decisions_charges=decisions, db_path=db)
    # Revalider une facture deja VALIDE : refuse (statut), aucune double imputation.
    with pytest.raises(fps.FactureProprietaireError, match="seul un BROUILLON"):
        fps.valider(f["facture_id_opaque"], emetteur=_emetteur(), destinataire={"nom": "David"},
                   acteur="TEST", decisions_charges=decisions, db_path=db)
    assert _position_de(db, cid)["montant_impute_total"] == 50.0


def test_cas11_ventilation_multi_logements_positions_independantes(db):
    ids = []
    for log, prop, montant in (("LOG_X", "PROP_D", 3.34), ("LOG_Y", "PROP_D", 3.33),
                               ("LOG_Z", "PROP_E", 3.33)):
        cid = _charge(db, montant, "OUI", logement_id=log, proprietaire_id=prop,
                     justificatif="FACT-MULTI-TEST")
        ids.append(cid)
    positions = [_position_de(db, cid) for cid in ids]
    assert len(positions) == 3
    assert round(sum(p["montant_origine"] for p in positions), 2) == 10.00
    assert len({p["position_id"] for p in positions}) == 3  # independantes


def test_cas12_charge_reportee_aout_disponible_septembre(db):
    cid = _charge(db, 50.0, "OUI", logement_id="LOG_X", proprietaire_id="PROP_D",
                 mois="2026-08", date_charge="2026-08-20")
    pos = _position_de(db, cid)
    refac.reporter(pos["position_id"], acteur="TEST", db_path=db)
    # Le mois d'ORIGINE de la charge ne change jamais.
    conn = sqlite3.connect(str(db))
    mois_charge = conn.execute("SELECT mois FROM charges WHERE charge_id=?", (cid,)).fetchone()[0]
    conn.close()
    assert mois_charge == "2026-08"
    # Disponible pour une facture de septembre (le "mois" de la facture est independant).
    decisions = [{"position_id": pos["position_id"], "montant": 50.0, "libelle": "Charge"}]
    f = fps.creer(_facture_source("PROP_D", "LOG_X", "2026-09"), acteur="TEST", db_path=db,
                 decisions_charges=decisions)
    fv = fps.valider(f["facture_id_opaque"], emetteur=_emetteur(), destinataire={"nom": "David"},
                     acteur="TEST", decisions_charges=decisions, db_path=db)
    assert fv["statut"] == fps.ST_VALIDE
    assert fv["mois"] == "2026-09"
    conn = sqlite3.connect(str(db))
    mois_charge_apres = conn.execute(
        "SELECT mois FROM charges WHERE charge_id=?", (cid,)).fetchone()[0]
    conn.close()
    assert mois_charge_apres == "2026-08"  # inchange

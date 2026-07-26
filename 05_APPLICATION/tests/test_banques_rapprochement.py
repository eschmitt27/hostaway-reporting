"""Rapprochement mouvement bancaire <-> objet métier : persistance, partiel/multiple,
dépassement refusé, double rapprochement refusé, propositions automatiques."""
from __future__ import annotations

import pytest

from app.db.connection import apply_migrations
from app.services import banques_rapprochement_service as svc


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


# ── Rapprochement simple par type d'objet ────────────────────────────────────

def test_rapprocher_reservation(db):
    res = svc.enregistrer("MVT-1", "RESERVATION", "RES_001", 850.0, montant_mouvement=850.0,
                          acteur="recette", db_path=db)
    assert res["ok"], res
    liens = svc.lister("MVT-1", db_path=db)
    assert len(liens) == 1 and liens[0]["type_objet"] == "RESERVATION"


def test_rapprocher_charge(db):
    res = svc.enregistrer("MVT-2", "CHARGE_FOURNISSEUR", "CHG_010", 120.0, montant_mouvement=120.0,
                          acteur="recette", db_path=db)
    assert res["ok"], res


def test_rapprocher_proprietaire(db):
    res = svc.enregistrer("MVT-3", "REVERSEMENT_PROPRIETAIRE", "PROP_A", 400.0,
                          montant_mouvement=400.0, acteur="recette", db_path=db)
    assert res["ok"], res


def test_rapprocher_associe(db):
    res = svc.enregistrer("MVT-4", "REMBOURSEMENT_ASSOCIE", "PERS_X", 75.0,
                          montant_mouvement=75.0, acteur="recette", db_path=db)
    assert res["ok"], res


def test_type_objet_inconnu_refuse(db):
    res = svc.enregistrer("MVT-5", "TYPE_INEXISTANT", "X", 10.0, montant_mouvement=10.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_TYPE_OBJET_INCONNU


def test_montant_invalide_refuse(db):
    res = svc.enregistrer("MVT-6", "RESERVATION", "RES_001", -10.0, montant_mouvement=10.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_MONTANT_INVALIDE
    res2 = svc.enregistrer("MVT-6", "RESERVATION", "RES_001", 0, montant_mouvement=10.0, db_path=db)
    assert res2["ok"] is False and res2["code"] == svc.E_MONTANT_INVALIDE


# ── Partiel / multiple ────────────────────────────────────────────────────────

def test_rapprochement_partiel_puis_complement(db):
    r1 = svc.enregistrer("MVT-7", "RESERVATION", "RES_A", 300.0, montant_mouvement=850.0, db_path=db)
    assert r1["ok"] and r1["montant_restant"] == 550.0
    etat = svc.etat_rapprochement("MVT-7", 850.0, db_path=db)
    assert etat["statut"] == "PARTIEL" and etat["montant_restant"] == 550.0

    r2 = svc.enregistrer("MVT-7", "RESERVATION", "RES_B", 550.0, montant_mouvement=850.0, db_path=db)
    assert r2["ok"] and r2["montant_restant"] == 0.0
    etat2 = svc.etat_rapprochement("MVT-7", 850.0, db_path=db)
    assert etat2["statut"] == "RAPPROCHE"
    liens = svc.lister("MVT-7", db_path=db)
    assert len(liens) == 2                       # un mouvement couvrant deux objets


def test_un_objet_regle_par_plusieurs_mouvements(db):
    """Le même objet (une charge de 500) réglé par deux mouvements distincts (300 + 200)."""
    r1 = svc.enregistrer("MVT-8", "CHARGE_FOURNISSEUR", "CHG_099", 300.0,
                         montant_mouvement=300.0, db_path=db)
    r2 = svc.enregistrer("MVT-9", "CHARGE_FOURNISSEUR", "CHG_099", 200.0,
                         montant_mouvement=200.0, db_path=db)
    assert r1["ok"] and r2["ok"]
    # Les deux mouvements référencent le même objet_id sans conflit.


def test_depassement_refuse(db):
    svc.enregistrer("MVT-10", "RESERVATION", "RES_C", 600.0, montant_mouvement=850.0, db_path=db)
    res = svc.enregistrer("MVT-10", "RESERVATION", "RES_D", 300.0, montant_mouvement=850.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_DEPASSEMENT


def test_double_rapprochement_refuse_si_deja_complet(db):
    svc.enregistrer("MVT-11", "RESERVATION", "RES_E", 850.0, montant_mouvement=850.0, db_path=db)
    res = svc.enregistrer("MVT-11", "RESERVATION", "RES_F", 10.0, montant_mouvement=850.0, db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_DEJA_RAPPROCHE


def test_annulation_libere_le_montant(db):
    r1 = svc.enregistrer("MVT-12", "RESERVATION", "RES_G", 850.0, montant_mouvement=850.0, db_path=db)
    svc.annuler(r1["rapprochement_id_opaque"], acteur="recette", db_path=db)
    # Le lien annulé ne compte plus dans le montant déjà rapproché : un nouveau lien est possible.
    r2 = svc.enregistrer("MVT-12", "RESERVATION", "RES_H", 850.0, montant_mouvement=850.0, db_path=db)
    assert r2["ok"], r2


# ── Transitions et historique ─────────────────────────────────────────────────

def test_confirmer_puis_historique(db):
    r = svc.enregistrer("MVT-13", "RESERVATION", "RES_I", 850.0, montant_mouvement=850.0,
                        statut=svc.ST_PROPOSE, acteur="auto", db_path=db)
    opaque = r["rapprochement_id_opaque"]
    conf = svc.confirmer(opaque, acteur="recette", commentaire="ok", db_path=db)
    assert conf["ok"] and conf["statut"] == svc.ST_CONFIRME
    hist = svc.historique_evenements(opaque, db_path=db)
    assert len(hist) == 2                        # CREATION + CONFIRMATION
    assert hist[0]["type_evenement"] == "CONFIRMATION"


def test_refuser_transition(db):
    r = svc.enregistrer("MVT-14", "NON_IDENTIFIE", None, 45.0, montant_mouvement=45.0, db_path=db)
    res = svc.refuser(r["rapprochement_id_opaque"], commentaire="pas le bon objet", db_path=db)
    assert res["ok"] and res["statut"] == svc.ST_REFUSE
    # Un rapprochement REFUSE ne compte plus comme actif : le montant redevient disponible.
    etat = svc.etat_rapprochement("MVT-14", 45.0, db_path=db)
    assert etat["statut"] == "NON_RAPPROCHE"


def test_transition_sur_opaque_inconnu_refuse(db):
    res = svc.confirmer("BRP-INEXISTANT", db_path=db)
    assert res["ok"] is False and res["code"] == svc.E_INTROUVABLE


# ── Propositions automatiques ─────────────────────────────────────────────────

def test_proposer_correspondance_exacte():
    mouvement = {"montant": 850.0, "date_operation": "2026-06-05", "libelle": "VIR HOSTAWAY RES_A1"}
    candidats = [
        {"type_objet": "RESERVATION", "objet_id": "RES_A1", "montant": 850.0,
         "date": "2026-06-04", "reference": "RES_A1"},
        {"type_objet": "RESERVATION", "objet_id": "RES_A2", "montant": 120.0, "date": "2026-01-01"},
    ]
    props = svc.proposer(mouvement, candidats)
    assert len(props) == 1
    assert props[0]["objet_id"] == "RES_A1"
    assert props[0]["raison"] in ("Correspondance exacte", "Correspondance probable")


def test_proposer_aucune_correspondance():
    mouvement = {"montant": 999.0, "date_operation": "2026-06-05", "libelle": "INCONNU"}
    candidats = [{"type_objet": "RESERVATION", "objet_id": "RES_X", "montant": 12.0,
                 "date": "2020-01-01"}]
    assert svc.proposer(mouvement, candidats) == []

"""Cutover comptable — remise à zéro explicite du domaine Banque/Comptabilité (mission 14d).

Aucune écriture réelle : `tmp_db` isole `cfg.DB_PATH` (conftest.py).
"""
from __future__ import annotations

import sqlite3

import pytest

from app.db.connection import get_db
from app.services import cutover_service as cutover


def _inserer_fixture_bancaire(db_path):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO banque_imports (import_id, nom_fichier_origine, compte_id_opaque, "
            "nb_lignes_lues, nb_valides, nb_doublons_certains, nb_doublons_probables, "
            "nb_invalides, total_debit, total_credit, statut, acteur) "
            "VALUES ('IMP-1','x.csv','CM_1',1,1,0,0,0,10.0,0.0,'SUCCES','test')")
        conn.commit()
    finally:
        conn.close()


def _inserer_reservation_hors_hostaway(db_path):
    """Table HORS domaine du cutover : doit survivre intacte (preuve que reset ne déborde pas)."""
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO reservations_hors_hostaway (reservation_hh_id, logement_id, "
            "date_arrivee, date_depart, statut) "
            "VALUES ('HH-1', 'LOG_0001', '2026-06-01', '2026-06-03', 'ACTIVE')")
        conn.commit()
    finally:
        conn.close()


def test_previsualiser_compte_sans_rien_modifier(tmp_db):
    _inserer_fixture_bancaire(tmp_db)
    avant = cutover.previsualiser(db_path=tmp_db)
    assert avant["tables"]["banque_imports"] == 1
    assert avant["total_lignes_a_supprimer"] >= 1
    # Rejouer ne doit rien avoir changé.
    apres = cutover.previsualiser(db_path=tmp_db)
    assert apres == avant


def test_reset_refuse_sans_confirmation(tmp_db):
    _inserer_fixture_bancaire(tmp_db)
    res = cutover.reset_domaine_bancaire(db_path=tmp_db)
    assert res["ok"] is False
    assert res["code"] == cutover.E_CONFIRMATION_REQUISE
    assert cutover.previsualiser(db_path=tmp_db)["tables"]["banque_imports"] == 1


def test_reset_vide_toutes_les_tables_du_domaine(tmp_db):
    _inserer_fixture_bancaire(tmp_db)
    res = cutover.reset_domaine_bancaire(confirmer=True, db_path=tmp_db)
    assert res["ok"] is True, res
    assert res["supprimes"]["banque_imports"] == 1
    apres = cutover.previsualiser(db_path=tmp_db)
    assert apres["total_lignes_a_supprimer"] == 0
    assert res["integrity_check"] == "ok"
    assert res["foreign_key_check"] == []


def test_reset_ne_touche_pas_aux_donnees_hors_domaine(tmp_db):
    """Réservations/Hostaway/référentiels doivent survivre intacts — preuve que le domaine du
    reset reste strictement celui déclaré, jamais un DELETE dispersé plus large."""
    _inserer_fixture_bancaire(tmp_db)
    _inserer_reservation_hors_hostaway(tmp_db)

    cutover.reset_domaine_bancaire(confirmer=True, db_path=tmp_db)

    conn = get_db(tmp_db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM reservations_hors_hostaway WHERE reservation_hh_id='HH-1'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n == 1, "le reset ne doit jamais toucher aux réservations"


def test_reset_est_une_seule_transaction_tout_ou_rien(tmp_db, monkeypatch):
    """Une panne au milieu de la liste ne doit laisser aucune table partiellement vidée."""
    _inserer_fixture_bancaire(tmp_db)

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
            "type_objet, objet_id, montant_rapproche, statut, source) "
            "VALUES ('BRP-1','MVT-1','RESERVATION','RES-1',10.0,'CONFIRME','MANUEL')")
        conn.commit()
    finally:
        conn.close()

    # `sqlite3.Connection.execute` est un attribut en lecture seule (type C) : on ne peut pas le
    # remplacer sur l'instance. On simule la panne au niveau de `_table_existe`, appelée juste
    # avant chaque DELETE dans la boucle — la lever à mi-parcours de `TABLES_DOMAINE` reproduit
    # exactement une panne survenant APRÈS que certaines tables ont déjà été vidées dans la même
    # transaction, ce qui est le scénario réel à couvrir (rollback total ou rien).
    reelle = cutover._table_existe
    appels = {"n": 0}

    def _table_existe_qui_panne(conn, nom):
        if nom == "banque_controles":
            appels["n"] += 1
            if appels["n"] > 1:
                # 1er appel : dans `previsualiser()` initial (avant tout DELETE) — laissé passer.
                # 2e appel : dans la boucle de suppression elle-même — c'est celui-ci qui panne,
                # après que des tables précédentes de la liste ont déjà été vidées dans CETTE
                # transaction (banque_classement_decisions, banque_suggestion_decisions, etc.).
                raise sqlite3.OperationalError("panne simulée")
        return reelle(conn, nom)

    monkeypatch.setattr(cutover, "_table_existe", _table_existe_qui_panne)
    with pytest.raises(sqlite3.OperationalError):
        cutover.reset_domaine_bancaire(confirmer=True, db_path=tmp_db)

    monkeypatch.undo()
    # Rien n'a été supprimé : le rollback a annulé les DELETE déjà exécutés dans la transaction.
    apres = cutover.previsualiser(db_path=tmp_db)
    assert apres["tables"]["banque_imports"] == 1
    assert apres["tables"]["banque_rapprochements"] == 1

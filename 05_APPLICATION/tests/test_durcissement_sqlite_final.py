"""Tests négatifs — dernier durcissement SQLite ciblé (migration 0060, Mission 12).

Contrairement à la migration 0055 (domaines RAW/importés, l'un a dû être partiellement retiré en
0056), les 4 CHECK ajoutés ici portent uniquement sur des tables ENTIÈREMENT APPLICATION-GÉNÉRÉES
(charges, réservations HH, trésorerie propriétaire, factures fournisseurs) — jamais alimentées par
un import brut externe. Chaque test prouve que la contrainte est réellement active côté SQLite
(`sqlite3.IntegrityError`), pas seulement documentée, ET que les cas métier légitimes (stop-gates
§31/§32/§33 de la mission) restent acceptés.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.db.connection import apply_migrations, get_db


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "app.db"
    apply_migrations(p)
    return p


# ── charges.statut ────────────────────────────────────────────────────────────

def test_charge_statut_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO charges (charge_id, montant, categorie_charge_id, statut) "
                "VALUES ('CHG-T1', 10.0, 'CAT_X', 'SUPPRIMEE')")
            conn.commit()
    finally:
        conn.close()


def test_charge_statuts_valides_acceptes(db):
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO charges (charge_id, montant, categorie_charge_id, statut) "
            "VALUES ('CHG-T2', 10.0, 'CAT_X', 'ACTIVE')")
        conn.execute(
            "INSERT INTO charges (charge_id, montant, categorie_charge_id, statut) "
            "VALUES ('CHG-T3', 10.0, 'CAT_X', 'ANNULEE')")
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0] == 2
    finally:
        conn.close()


def test_stop_gate_charge_commune_sans_logement_direct_toujours_valide(db):
    """§33 : une charge commune sans `logement_id` direct reste un état métier valide (périmètre
    défini autrement, Mission 8) — PAS de NOT NULL introduit sur `logement_id`."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO charges (charge_id, montant, categorie_charge_id, logement_id) "
            "VALUES ('CHG-T4', 90.0, 'CAT_X', NULL)")
        conn.commit()
        assert conn.execute(
            "SELECT logement_id FROM charges WHERE charge_id='CHG-T4'").fetchone()[0] is None
    finally:
        conn.close()


# ── reservations_hors_hostaway.statut ────────────────────────────────────────

def test_reservation_hh_statut_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO reservations_hors_hostaway (reservation_hh_id, statut) "
                "VALUES ('RESHH-T1', 'BROUILLON')")
            conn.commit()
    finally:
        conn.close()


def test_stop_gate_reservation_hh_sans_montant_retenu_toujours_valide(db):
    """§32 : un placeholder HH sans `montant_retenu` reste un cas valide (Mission 11) — PAS de
    NOT NULL introduit sur `montant_retenu`."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO reservations_hors_hostaway (reservation_hh_id, montant_retenu) "
            "VALUES ('RESHH-T2', NULL)")
        conn.commit()
        assert conn.execute(
            "SELECT montant_retenu FROM reservations_hors_hostaway WHERE reservation_hh_id="
            "'RESHH-T2'").fetchone()[0] is None
    finally:
        conn.close()


# ── mouvements_tresorerie_proprietaires.sens/nature/statut ───────────────────

def _insert_mtp(conn, **kw):
    base = dict(mouvement_opaque="MTP-T1", proprietaire_id="PROP_A", date_mouvement="2026-06-01",
               montant=100.0, sens="PROPRIETAIRE_VERS_SOCIETE", nature="ACOMPTE_PROPRIETAIRE")
    base.update(kw)
    conn.execute(
        "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, proprietaire_id, "
        "date_mouvement, montant, sens, nature) VALUES (?,?,?,?,?,?)",
        (base["mouvement_opaque"], base["proprietaire_id"], base["date_mouvement"],
         base["montant"], base["sens"], base["nature"]))


def test_tresorerie_sens_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            _insert_mtp(conn, sens="AUTRE")
            conn.commit()
    finally:
        conn.close()


def test_tresorerie_nature_invalide_refusee(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            _insert_mtp(conn, mouvement_opaque="MTP-T2", nature="AUTRE_INCONNUE")
            conn.commit()
    finally:
        conn.close()


def test_tresorerie_statut_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, "
                "proprietaire_id, date_mouvement, montant, sens, nature, statut) VALUES "
                "('MTP-T3','PROP_A','2026-06-01',100.0,'PROPRIETAIRE_VERS_SOCIETE',"
                "'ACOMPTE_PROPRIETAIRE','SUPPRIME')")
            conn.commit()
    finally:
        conn.close()


def test_tresorerie_toutes_valeurs_valides_acceptees(db):
    conn = get_db(db)
    try:
        for nature in ("ACOMPTE_PROPRIETAIRE", "REMBOURSEMENT_PROPRIETAIRE",
                      "REGULARISATION_PROPRIETAIRE", "COMPENSATION_PROPRIETAIRE",
                      "AVANCE_PROPRIETAIRE", "RESTITUTION_PROPRIETAIRE", "AUTRE_A_CONTROLER"):
            conn.execute(
                "INSERT INTO mouvements_tresorerie_proprietaires (mouvement_opaque, "
                "proprietaire_id, date_mouvement, montant, sens, nature) VALUES "
                "(?,'PROP_A','2026-06-01',10.0,'SOCIETE_VERS_PROPRIETAIRE',?)",
                (f"MTP-{nature}", nature))
        conn.commit()
        assert conn.execute(
            "SELECT COUNT(*) FROM mouvements_tresorerie_proprietaires").fetchone()[0] == 7
    finally:
        conn.close()


# ── factures.statut ───────────────────────────────────────────────────────────

def test_facture_fournisseur_statut_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                "montant_ttc, statut) VALUES ('FAC-T1','FRS-A','REF-1',100.0,'SUPPRIMEE')")
            conn.commit()
    finally:
        conn.close()


def test_facture_fournisseur_tous_statuts_valides_acceptes(db):
    conn = get_db(db)
    try:
        for i, statut in enumerate(("BROUILLON", "A_CONTROLER", "VALIDEE",
                                    "PARTIELLEMENT_REGLEE", "REGLEE", "ANNULEE", "LITIGE")):
            conn.execute(
                "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
                "montant_ttc, statut) VALUES (?,'FRS-A',?,100.0,?)",
                (f"FAC-T{i}", f"REF-{i}", statut))
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0] == 7
    finally:
        conn.close()


# ── Stop-gate banque (§31) — non-régression, aucun CHECK ajouté sur `sens` ────

def test_stop_gate_banque_sens_hors_domaine_toujours_accepte(db):
    """Confirme, après ce durcissement, que le correctif 0056 tient toujours : aucun CHECK n'a été
    réintroduit sur `banque_mouvements.sens` par cette mission."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, bank_account_id, "
            "date_operation, sens, montant, libelle_brut, fingerprint) "
            "VALUES ('MVT-T1', 'IMP-T1', 'CPT-T1', '2026-06-01', 'INCONNU', 10.0, 'x', 'fpT1')")
        conn.commit()
        assert conn.execute(
            "SELECT sens FROM banque_mouvements WHERE mouvement_id_opaque='MVT-T1'"
        ).fetchone()[0] == "INCONNU"
    finally:
        conn.close()


# ── PRAGMA foreign_keys actif au runtime ──────────────────────────────────────

def test_pragma_foreign_keys_actif_sur_connexion_applicative(db):
    conn = get_db(db)
    try:
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        conn.close()

"""Tests négatifs — durcissement SQLite Phase 1 (migration 0055).

Ces tests ESSAIENT de casser la base : chaque cas doit être refusé par SQLite lui-même
(`sqlite3.IntegrityError`), pas seulement par une validation applicative en amont. Prouve que les
contraintes ajoutées (FK, CHECK) sont réellement actives — pas juste documentées en commentaire.
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


# ── FK réelles ajoutées par la migration 0055 ────────────────────────────────

def test_classification_orpheline_refusee(db):
    """Une classification sans mouvement bancaire existant doit être refusée (FK)."""
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            conn.execute(
                "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id) "
                "VALUES ('MVT-INEXISTANT', 'RUN-001')")
            conn.commit()
    finally:
        conn.close()


def test_ligne_facture_orpheline_refusee(db):
    """Une ligne de facture sans facture existante doit être refusée (FK)."""
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes "
                "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant) "
                "VALUES ('FPRL-TEST', 'FPR-INEXISTANTE', 1, 'CHARGE_FIXE', 'x', 10.0)")
            conn.commit()
    finally:
        conn.close()


def test_ligne_facture_rattachee_a_une_facture_reelle_acceptee(db):
    """Le chemin normal (facture réelle puis ligne rattachée) doit rester accepté."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, logement_id, "
            "mois) VALUES ('FPR-0001', 'PROP_A', 'LOG_A1', '2026-06')")
        conn.execute(
            "INSERT INTO factures_proprietaires_lignes "
            "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant) "
            "VALUES ('FPRL-0001', 'FPR-0001', 1, 'CHARGE_FIXE', 'x', 10.0)")
        conn.commit()
        assert conn.execute(
            "SELECT COUNT(*) FROM factures_proprietaires_lignes").fetchone()[0] == 1
    finally:
        conn.close()


# ── CHECK réels ajoutés par la migration 0055 ────────────────────────────────
#
# Le CHECK(sens IN ('DEBIT','CREDIT')) initialement ajouté en 0055 a été retiré par la migration
# 0056 : `test_banques_controles_catalogue.py::test_sens_incoherent_detecte` insère volontairement
# un `sens` hors domaine ("INCONNU") pour prouver qu'un mouvement bancaire brut corrompu est
# CONTRÔLÉ (détecté, signalé à l'écran), jamais rejeté à l'insertion — le CHECK cassait ce mécanisme
# existant. Voir 0056 pour le détail. Aucun test négatif sur `sens` ici en conséquence.


def test_sens_mouvement_bancaire_hors_domaine_toujours_accepte(db):
    """Non-régression du correctif 0056 : un `sens` hors domaine reste insérable (anomalie
    contrôlée en aval, pas un rejet SQL)."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, bank_account_id, "
            "date_operation, sens, montant, libelle_brut, fingerprint) "
            "VALUES ('MVT-0001', 'IMP-0001', 'CPT-0001', '2026-06-01', 'INCONNU', 10.0, 'x', 'fp1')")
        conn.commit()
        assert conn.execute(
            "SELECT sens FROM banque_mouvements WHERE mouvement_id_opaque='MVT-0001'"
        ).fetchone()[0] == "INCONNU"
    finally:
        conn.close()


def test_statut_facture_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, "
                "logement_id, mois, statut) VALUES ('FPR-0002', 'PROP_A', 'LOG_A1', '2026-06', "
                "'STATUT_INCONNU')")
            conn.commit()
    finally:
        conn.close()


def test_type_document_facture_invalide_refuse(db):
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, "
                "logement_id, mois, type_document) VALUES ('FPR-0003', 'PROP_A', 'LOG_A1', "
                "'2026-06', 'DEVIS')")
            conn.commit()
    finally:
        conn.close()


def test_type_ligne_facture_invalide_refuse(db):
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, logement_id, "
            "mois) VALUES ('FPR-0004', 'PROP_A', 'LOG_A1', '2026-06')")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            conn.execute(
                "INSERT INTO factures_proprietaires_lignes "
                "(ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant) "
                "VALUES ('FPRL-0002', 'FPR-0004', 1, 'TYPE_INVENTE', 'x', 10.0)")
            conn.commit()
    finally:
        conn.close()


# ── Non-régression : les mécanismes déjà en place restent actifs ────────────

def test_doublon_facture_meme_grain_toujours_refuse(db):
    """Anti-doublon déjà en place (index unique partiel 0027) — non touché par 0055."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, logement_id, "
            "mois) VALUES ('FPR-A', 'PROP_A', 'LOG_A1', '2026-06')")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            conn.execute(
                "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, "
                "logement_id, mois) VALUES ('FPR-B', 'PROP_A', 'LOG_A1', '2026-06')")
            conn.commit()
    finally:
        conn.close()


def test_double_dataset_lot10_actif_toujours_refuse(db):
    """Anti-doublon dataset actif déjà en place (index unique partiel 0044) — non touché par 0055."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('RUN-A', 'SUCCES', 1)")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            conn.execute(
                "INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('RUN-B', 'SUCCES', 1)")
            conn.commit()
    finally:
        conn.close()


def test_classification_doublon_meme_run_toujours_refuse(db):
    """Anti-doublon classification déjà en place (index unique 0032) — non touché par 0055."""
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO banque_mouvements (mouvement_id_opaque, import_id, bank_account_id, "
            "date_operation, sens, montant, libelle_brut, fingerprint) "
            "VALUES ('MVT-0002', 'IMP-0001', 'CPT-0001', '2026-06-01', 'DEBIT', 10.0, 'x', 'fp2')")
        conn.execute(
            "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id) "
            "VALUES ('MVT-0002', 'RUN-001')")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            conn.execute(
                "INSERT INTO banque_classifications (mouvement_id_opaque, classification_run_id) "
                "VALUES ('MVT-0002', 'RUN-001')")
            conn.commit()
    finally:
        conn.close()

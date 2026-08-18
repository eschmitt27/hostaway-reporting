"""Lot6d — rapprochement Tasks Hostaway <-> déclarations/factures, en SQLite.

Source Tasks : menages_taches_enrichies (Lot6a, 0038) + hostaway_cleaning_tasks (assignee/title).
Source externe : facture_lignes_menage (0037/0039), type_ligne=MENAGE_EXTERNE uniquement.
Source interne : menages_declarations_internes (Lot6b, 0038).

Deux régressions trouvées et corrigées pendant la migration : deux blocs (ménages externes, ménages
internes) relisaient encore inconditionnellement les classeurs Excel legacy après le branchement
SQLite, écrasant silencieusement les données SQLite avant le calcul — invisibles tant qu'aucun test
ne comparait le résultat réel à un cas construit.
"""
from __future__ import annotations

import runpy
import sqlite3
import sys
from pathlib import Path

REAL_SCRIPT = (Path(__file__).resolve().parents[1] / "02_TRAVAIL" /
               "lot6d_rapprochement_menages.py")


def _appliquer_migrations(db_path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "05_APPLICATION"))
    import app.config as cfg
    from app.db.connection import apply_migrations
    orig = cfg.DB_PATH
    cfg.DB_PATH = db_path
    try:
        apply_migrations(db_path)
    finally:
        cfg.DB_PATH = orig


def _peupler(db_path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, actif, import_id) "
            "VALUES ('LOG_0001','480001','OUI','IMP1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, import_id) VALUES ('GEST1','LOG_0001','PROP_0001',"
            "'2026-01-01','','IMP1')")
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "nom_normalise, hostaway_assigneeUserId, hostaway_mapping_actif, import_id) "
            "VALUES ('INT_0001','Kheira','INTERNE','kheira','1001','OUI','IMP1')")
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "nom_normalise, hostaway_mapping_actif, import_id) "
            "VALUES ('FRS-EXT-0001','Aissata','EXTERNE','aissata','NON','IMP1')")

        conn.execute(
            "INSERT INTO hostaway_cleaning_tasks_extractions (extraction_id, mode, date_debut, "
            "statut) VALUES ('HCT-1','FIXTURE','2026-08-18T00:00:00Z','SUCCES')")
        for i in (1, 2):
            conn.execute(
                "INSERT INTO hostaway_cleaning_tasks (extraction_id, task_id, reservation_id, "
                "listing_map_id, title, status, can_start_from, assignee_user_id) "
                "VALUES ('HCT-1', ?, ?, '480001', 'Menage Kheira', 'completed', ?, '1001')",
                (f"T{i}", f"R{i}", f"2026-07-0{i}"))
            conn.execute(
                "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, "
                "proprietaire_id, status, statut_menage, statut_controle) "
                "VALUES (?, '2026-07', 'LOG_0001', 'PROP_0001', 'completed', 'réalisé', 'OK')",
                (f"T{i}",))

        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages) VALUES ('2026-07', 'LOG_0001', 'INT_0001', 2)")

        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, statut) VALUES "
            "('FAC-1','FRS-EXT-0001','R1','2026-07-15',55.0,'A_CONTROLER')")
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, montant_ttc, source) VALUES "
            "('FLM-1','FAC-1','MENAGE_EXTERNE','LOG_0001',55.0,'PDF_EXTRACTION')")
        conn.execute(
            "INSERT INTO facture_lignes_menage_detail (ligne_id_opaque, quantite) "
            "VALUES ('FLM-1', 2)")
        conn.commit()
    finally:
        conn.close()


def _run(db_path, *extra):
    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel", "--mois", "2026-07",
                "--db", str(db_path), *extra]
    try:
        runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    except SystemExit as exc:
        assert exc.code in (0, None), f"lot6d a quitte avec code {exc.code}"
    finally:
        sys.argv = argv_pytest


def test_taches_declarations_factures_coherentes_valide(tmp_path):
    """2 Tasks internes réalisées = 2 déclarations M04 : aucun écart, VALIDE."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)
    # Retire le prestataire externe pour isoler le cas "coherent" (seul l'interne compte ici).
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM facture_lignes_menage")
    conn.commit()
    conn.close()

    _run(db)

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT nb_menages_tasks_hostaway_completed, nb_menages_declares_interne_m04, "
            "statut_controle FROM menages_rapprochement WHERE intervenant_id='INT_0001'"
        ).fetchone()
    finally:
        conn.close()
    assert row == (2, 2, "VALIDE")


def test_ecart_externe_declare_sans_task_hostaway_a_controler(tmp_path):
    """Facture externe déclare 1 ménage sur un logement sans Task Hostaway correspondante côté
    prestataire externe : écart détecté, A_CONTROLER — jamais corrigé automatiquement (mission §33)."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    _run(db)

    conn = sqlite3.connect(db)
    try:
        rows = {r[0]: r for r in conn.execute(
            "SELECT intervenant_id, nb_menages_tasks_hostaway_completed, "
            "nb_menages_declares_externe, statut_controle, code_controle "
            "FROM menages_rapprochement")}
    finally:
        conn.close()
    assert rows["FRS-EXT-0001"][3] == "A_CONTROLER"
    assert rows["FRS-EXT-0001"][4] == "MENAGE_PRESTATAIRE_ECART_HOSTAWAY"
    # Task Hostaway interne toujours visible séparément, jamais fusionnée avec l'externe.
    assert rows["INT_0001"][1] == 2


def test_rejeu_meme_mois_pas_de_doublon(tmp_path):
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    _run(db)
    _run(db)

    conn = sqlite3.connect(db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM menages_rapprochement WHERE mois='2026-07'").fetchone()[0]
    finally:
        conn.close()
    assert n == 2


def test_mois_par_defaut_derive_des_taches_pas_code_en_dur(tmp_path):
    """Sans --mois, le mois vient du dataset (MAX(mois) des tâches), jamais d'une constante."""
    db = tmp_path / "app.db"
    _appliquer_migrations(db)
    _peupler(db)

    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel", "--db", str(db)]
    try:
        runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    except SystemExit as exc:
        assert exc.code in (0, None)
    finally:
        sys.argv = argv_pytest

    conn = sqlite3.connect(db)
    try:
        mois = {r[0] for r in conn.execute("SELECT DISTINCT mois FROM menages_rapprochement")}
    finally:
        conn.close()
    assert mois == {"2026-07"}


def test_sans_base_designee_refuse_proprement(tmp_path, monkeypatch):
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    import pytest
    argv_pytest = sys.argv
    sys.argv = [str(REAL_SCRIPT), "--source", "SQLITE", "--sans-excel"]
    try:
        with pytest.raises(SystemExit):
            runpy.run_path(str(REAL_SCRIPT), run_name="__main__")
    finally:
        sys.argv = argv_pytest

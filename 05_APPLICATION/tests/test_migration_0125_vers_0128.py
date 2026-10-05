"""Migration 0125 → 0128 sur une base qui porte déjà des données : la RÉPÉTITION de ce qui se passera au premier
démarrage de l'application sur la vraie base (schéma 0125).

Au démarrage (`migration_service.migrer_au_demarrage`) : des migrations sont en attente → sauvegarde
AVANT_MIGRATION vérifiée → migrations → contrôle d'intégrité et des clés étrangères → ou restauration. Sont éprouvés
ici, sur une base montée jusqu'à 0125 puis migrée pour de vrai (les VRAIS fichiers 0126, 0127, 0128) :

  · une sauvegarde est faite AVANT, et contient bien l'état d'avant (aucune des nouvelles tables) ;
  · les trois migrations sont additives : aucune donnée existante ne bouge ;
  · l'intégrité et les clés étrangères sont intactes ;
  · un redémarrage suivant ne refait ni migration ni sauvegarde ;
  · la clôture est FONCTIONNELLE sur la base migrée : un module se clôture, une décision d'exclusion s'enregistre,
    et les déclencheurs qui protègent l'historique sont en place.

Isolation complète : `APP_DATA_DIR`, base et sauvegardes sous `tmp_path` — la vraie base n'est jamais ouverte.
"""
from __future__ import annotations

import hashlib
import shutil
import sqlite3
from datetime import date
from pathlib import Path

import pytest

import app.config as cfg
from app.db import connection as dbconn
from app.db.connection import apply_migrations, get_db
from app.services import backup_service as bk
from app.services import cloture_modules_service as cm
from app.services import clotures_service as cs
from app.services import migration_service as mig
from app.services import ordonnanceur_service as ordo
from app.services import perimetre_gestion_service as pg

NOUVELLES_TABLES = ("cloture_modules", "cloture_modules_evenements", "reservation_perimetre_decisions",
                    "reservation_perimetre_evenements", "cloture_archives_retirees")
DONNEES = ("clotures_mensuelles", "cloture_evenements", "ref_cloture_mensuelle", "parametres_societe_facturation",
           "ref_logements", "ref_gestion_logements_hist", "reservations_resolues", "factures_proprietaires")


@pytest.fixture(autouse=True)
def isolation(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setenv("APP_DATA_DIR", str(data))
    monkeypatch.setattr(cfg, "DATA_DIR", data)
    monkeypatch.setattr(cfg, "DB_PATH", data / "app.db")
    monkeypatch.setattr(cfg, "BACKUPS_DIR", data / "backups")
    monkeypatch.setattr(cfg, "BACKUP_SECONDARY_DIR", None)
    monkeypatch.setattr(cfg, "BACKUP_DAILY_ENABLED", False)
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", False)
    monkeypatch.setattr(cs, "aujourdhui", lambda: date(2026, 10, 5))
    yield data
    ordo.arreter()


def _lignes(db, sql, *params):
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def _versions(db):
    return {r["version"] for r in _lignes(db, "SELECT version FROM schema_migrations")}


def _tables(db):
    return {r["name"] for r in _lignes(db, "SELECT name FROM sqlite_master WHERE type='table'")}


def _empreinte(db, tables=DONNEES):
    h = hashlib.sha256()
    conn = sqlite3.connect(str(db))
    try:
        for t in tables:
            for r in conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2"):
                h.update(repr(tuple(r)).encode())
    finally:
        conn.close()
    return h.hexdigest()


@pytest.fixture
def base_en_0125(tmp_path, monkeypatch, isolation):
    """Une base montée avec les VRAIES migrations jusqu'à 0125 — comme la base réelle aujourd'hui — et des données :
    une clôture de septembre déjà en préparation, l'historique de clôture, un logement retiré, un séjour à trancher."""
    vraies = dbconn.MIGRATIONS_DIR
    anciennes = tmp_path / "migrations_0125"
    anciennes.mkdir()
    for f in sorted(vraies.glob("*.sql")):
        if f.stem.split("_", 1)[0] <= "0125":
            shutil.copy(f, anciennes / f.name)
    monkeypatch.setattr(dbconn, "MIGRATIONS_DIR", anciennes)
    apply_migrations(cfg.DB_PATH)
    conn = get_db(cfg.DB_PATH)
    try:
        conn.execute("INSERT INTO parametres_societe_facturation (cle, valeur, maj_par) VALUES "
                     "('V1_ACCOUNTING_START_DATE', '2026-09-01', 'test')")
        conn.execute("INSERT INTO clotures_mensuelles (cloture_id_opaque, mois, statut, cree_par) "
                     "VALUES ('CLO-HERITEE', '2026-09', 'EN_PREPARATION', 'utilisateur')")
        conn.execute("INSERT INTO cloture_evenements (cloture_id_opaque, type_evenement, nouveau_statut, acteur) "
                     "VALUES ('CLO-HERITEE', 'CREATION', 'NON_DEMARREE', 'utilisateur')")
        for mois in ("2025-12", "2026-01", "2026-05"):
            conn.execute("INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
                         "VALUES (?, 'CLOTURE', 'IMP-TEST')", (mois,))
        conn.execute("INSERT INTO ref_logements (logement_id, nom_court, actif, statut_parc, import_id) "
                     "VALUES ('LOG_M', 'Logement migré', 'NON', 'RETIRE', 'IMP-TEST')")
        conn.execute("INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, date_debut, "
                     "date_fin, statut_gestion, import_id) VALUES ('G_M', 'LOG_M', 'PROP_M', '2026-01-01', "
                     "'2026-09-01', 'RETIRE', 'IMP-TEST')")
        conn.execute("INSERT INTO reservations_datasets (dataset_id, etape, nb_lignes, statut, actif) "
                     "VALUES ('RDS-M', 'RESOLUES', 1, 'SUCCES', 1)")
        conn.execute("INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, row_hash, source, "
                     "reservation_id_hostaway, mois, logement_id, date_arrivee, date_depart, nuits, montant_retenu, "
                     "statut_controle, code_anomalie, canal) VALUES ('RDS-M','RES-HA-95001','h','HOSTAWAY_BOOKING',"
                     "'95001','2026-10','LOG_M','2026-10-30','2026-11-01',2,346.39,'A_CONTROLER',"
                     "'GESTION_LOGEMENT_MISSING','BOOKING')")
        conn.execute("INSERT INTO factures_proprietaires (facture_id_opaque, type_document, proprietaire_id, "
                     "logement_id, mois, montant_total, statut, numero_facture, date_facture) VALUES "
                     "('FPR-M','FACTURE','PROP_M','LOG_M','2026-09',300.0,'EMIS','2026-09-001','2026-09-30')")
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setattr(dbconn, "MIGRATIONS_DIR", vraies)          # l'application, elle, voit les vraies migrations
    return cfg.DB_PATH


def test_01_la_base_de_depart_est_bien_en_0125_sans_les_nouvelles_tables(base_en_0125):
    assert max(_versions(base_en_0125)) == "0125"
    assert not set(NOUVELLES_TABLES) & _tables(base_en_0125)
    attente = dbconn.migrations_en_attente(base_en_0125)
    assert attente["a_jouer"] == ["0126", "0127", "0128"] and not attente["base_neuve"]


def test_02_le_demarrage_sauvegarde_puis_migre_sans_toucher_aux_donnees(base_en_0125):
    avant = _empreinte(base_en_0125)
    res = mig.migrer_au_demarrage()
    assert res["ok"] is True and res["migrations"] == ["0126", "0127", "0128"]
    assert res["version_avant"] == "0125" and res["version_apres"] == "0128"
    assert res["sauvegarde_id"], "une sauvegarde est faite AVANT la migration"
    meta = next(e["meta"] for e in bk.catalogue() if e["sauvegarde_id"] == res["sauvegarde_id"])
    assert meta["categorie"] == bk.CAT_AVANT_MIGRATION and meta["raison"] == "AVANT_MIGRATION"
    assert (meta["migration_avant"], meta["migration_cible"]) == ("0125", "0128")
    assert meta["integrity_check"] == "ok" and meta["foreign_key_check"] == 0
    # La copie est bien l'état D'AVANT : sans les nouvelles tables, avec les données.
    copie = meta["chemin"]
    assert not set(NOUVELLES_TABLES) & _tables(copie) and max(_versions(copie)) == "0125"
    assert _empreinte(copie) == avant
    # Après : les trois migrations sont enregistrées, les nouvelles tables existent, les données n'ont pas bougé.
    assert {"0126", "0127", "0128"} <= _versions(base_en_0125)
    assert set(NOUVELLES_TABLES) <= _tables(base_en_0125)
    assert _empreinte(base_en_0125) == avant, "migrations additives : aucune donnée existante ne bouge"


def test_03_l_integrite_et_les_cles_etrangeres_sont_intactes_apres_migration(base_en_0125):
    assert mig.migrer_au_demarrage()["ok"] is True
    conn = sqlite3.connect(str(base_en_0125))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        declencheurs = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
    finally:
        conn.close()
    for attendu in ("trg_cloture_modules_evt_no_update", "trg_cloture_modules_evt_no_delete",
                    "trg_cloture_modules_mois_v1", "trg_perimetre_evt_no_update", "trg_perimetre_evt_no_delete",
                    "trg_perimetre_decision_no_delete", "trg_perimetre_decision_annulation_seule",
                    "trg_cloture_archives_retirees_no_update", "trg_cloture_archives_retirees_no_delete"):
        assert attendu in declencheurs, attendu


def test_04_un_redemarrage_ne_refait_ni_migration_ni_sauvegarde(base_en_0125):
    premier = mig.migrer_au_demarrage()
    assert premier["ok"] and len(bk.catalogue()) == 1
    suivant = mig.migrer_au_demarrage()
    assert suivant == {"ok": True, "migrations": [], "sauvegarde_id": None}
    assert len(bk.catalogue()) == 1


def test_05_la_cloture_est_fonctionnelle_sur_la_base_migree(base_en_0125):
    assert mig.migrer_au_demarrage()["ok"] is True
    db = base_en_0125
    # La clôture de septembre, déjà en préparation avant la migration, est reprise telle quelle.
    c = cs.demarrer("2026-09", acteur="utilisateur", db_path=db)
    assert c["cloture_id_opaque"] == "CLO-HERITEE" and c["statut"] == cs.ST_EN_PREPARATION
    t = cm.tableau_de_bord(c, db_path=db)
    assert t["nb_total"] == 7 and t["nb_clos"] == 0
    # Un module se clôture ; son état et sa trace sont enregistrés ; son historique est protégé.
    cm.cloturer_module(c, "BANQUE", acteur="utilisateur", commentaire="après migration", db_path=db)
    assert _lignes(db, "SELECT statut FROM cloture_modules WHERE module='BANQUE'") == [{"statut": "CLOS"}]
    conn = sqlite3.connect(str(db))
    try:
        with pytest.raises(sqlite3.IntegrityError, match="CLOTURE_MODULE_TRACE"):
            conn.execute("DELETE FROM cloture_modules_evenements")
    finally:
        conn.close()
    # Une décision d'exclusion s'écrit et se lit ; le séjour hors gestion est celui de la base d'avant.
    s = pg.sejour("RES-HA-95001", db_path=db)
    assert s is not None and s["code"] == pg.MISSING and s["decidable"]
    assert pg.exclure("RES-HA-95001", justification="Logement retiré", acteur="utilisateur", db_path=db)["ok"]
    assert pg.decision_active("95001", db_path=db)["acteur"] == "utilisateur"
    # Un mois antérieur à la comptabilité V1 ne se clôture toujours pas (déclencheur de la migration 0126).
    conn = sqlite3.connect(str(db))
    try:
        with pytest.raises(sqlite3.DatabaseError, match="CLOTURE_AVANT_V1"):
            conn.execute("INSERT INTO cloture_modules (mois, module, cloture_id_opaque, statut) "
                         "VALUES ('2026-08', 'CHARGES', 'CLO-x', 'CLOS')")
    finally:
        conn.close()


def test_06_une_migration_qui_echoue_restaure_la_base_d_avant(base_en_0125, monkeypatch):
    avant = _empreinte(base_en_0125)
    reelles = dbconn.MIGRATIONS_DIR
    cassee = base_en_0125.parent.parent / "migrations_cassees"
    cassee.mkdir()
    for f in reelles.glob("*.sql"):
        shutil.copy(f, cassee / f.name)
    (cassee / "0127_reservation_perimetre_decisions.sql").write_text("CREATE TABLE tout_va_casser (", encoding="utf-8")
    monkeypatch.setattr(dbconn, "MIGRATIONS_DIR", cassee)
    res = mig.migrer_au_demarrage()
    assert res["ok"] is False and res["code"] == "E_MIGRATION_ECHOUEE" and res["rollback"]["ok"] is True
    assert max(_versions(base_en_0125)) == "0125", "la base est revenue à son état d'avant"
    assert not set(NOUVELLES_TABLES) & _tables(base_en_0125)
    assert _empreinte(base_en_0125) == avant

"""Journal des runs moteur — un run partiellement réussi ne doit jamais disparaître.

Le run Lot 1 du 2026-08-17 a téléchargé 1 542 réservations, écrit sept masters, puis a été
interrompu pendant les tâches ménage. `MASTER_RUN_Log` n'en porte aucune trace, parce qu'il n'est
écrit qu'à la toute fin. Un run qui réussit l'essentiel puis échoue devient invisible — et rien
n'indique qu'il faut vérifier quoi que ce soit.

Ces tests verrouillent le comportement inverse : la ligne de run existe avant le premier appel
réseau, chaque étape est écrite dès qu'elle se termine, et le statut global est **déduit** des
étapes plutôt qu'affirmé.

Aucun appel Hostaway : tout passe par des bases temporaires et des doubles.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

RACINE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RACINE / "02_TRAVAIL"))

from lib_run_journal import (  # noqa: E402
    ECHEC, EN_COURS, ETAPE_ECHEC, ETAPE_IGNOREE, ETAPE_SUCCES, INTERROMPU, PARTIEL, SUCCES,
    RunJournal, marquer_runs_interrompus, resoudre_base,
)

SCHEMA = (RACINE / "05_APPLICATION" / "app" / "db" / "migrations" / "0031_moteur_runs.sql")


@pytest.fixture
def db(tmp_path) -> Path:
    """Base portant uniquement les tables du journal — pas besoin de tout le schéma."""
    p = tmp_path / "journal.db"
    conn = sqlite3.connect(p)
    try:
        sql = SCHEMA.read_text(encoding="utf-8")
        sql = sql.replace("INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0031');", "")
        conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()
    return p


def _runs(db: Path) -> list[dict]:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM moteur_runs ORDER BY id")]
    finally:
        conn.close()


def _etapes(db: Path) -> list[dict]:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM moteur_run_etapes ORDER BY ordre")]
    finally:
        conn.close()


# ── I. Le journal existe dès le démarrage ───────────────────────────────────────────────────────

def test_le_run_est_ecrit_avant_tout_travail(db, tmp_path):
    """Le cœur du correctif : la ligne existe avant le premier appel réseau."""
    RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    runs = _runs(db)
    assert len(runs) == 1
    assert runs[0]["statut"] == EN_COURS
    assert runs[0]["started_at"]
    assert runs[0]["ended_at"] is None
    assert runs[0]["pid"], "le PID doit être enregistré pour repérer un run orphelin"


def test_chaque_etape_est_ecrite_immediatement(db, tmp_path):
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    j.etape("RESERVATIONS", ETAPE_SUCCES, nb_lus=1542, nb_ecrits=1542, position="1800")
    # Volontairement AVANT toute clôture : c'est l'état qu'un process tué laisserait.
    etapes = _etapes(db)
    assert len(etapes) == 1
    assert etapes[0]["etape"] == "RESERVATIONS"
    assert etapes[0]["nb_ecrits"] == 1542
    assert etapes[0]["position"] == "1800"


# ── A/B/C. Succès, échec, run partiel ───────────────────────────────────────────────────────────

def test_succes_complet(db, tmp_path):
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    for nom in ("LISTINGS", "RESERVATIONS", "PAYOUTS"):
        j.etape(nom, ETAPE_SUCCES, nb_ecrits=10)
    assert j.fermer() == SUCCES
    r = _runs(db)[0]
    assert r["statut"] == SUCCES and r["nb_etapes_ok"] == 3 and r["nb_etapes_ko"] == 0


def test_echec_avant_toute_donnee(db, tmp_path):
    """Aucune étape aboutie : ECHEC, mais le run EXISTE."""
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    j.etape("LISTINGS", ETAPE_ECHEC, http_status=401, erreur="Token invalide")
    assert j.fermer() == ECHEC
    r = _runs(db)[0]
    assert r["statut"] == ECHEC
    assert _etapes(db)[0]["http_status"] == 401


def test_run_partiel_le_cas_du_2026_08_17(db, tmp_path):
    """Le scénario réel : étapes principales réussies, tâches ménage en échec 429."""
    j = RunJournal("lot1_hostaway_extract", racine=tmp_path, db_path=str(db))
    for nom, n in (("LISTINGS", 17), ("RESERVATIONS", 1542), ("PAYOUTS", 1518)):
        j.etape(nom, ETAPE_SUCCES, nb_ecrits=n)
    j.etape("CLEANING_TASKS", ETAPE_ECHEC, http_status=429, tentatives=3,
            erreur="Rate limit non résorbé")

    assert j.fermer() == PARTIEL, "ni SUCCES, ni ECHEC : le run a fait une partie du travail"
    r = _runs(db)[0]
    assert r["statut"] == PARTIEL
    assert r["nb_etapes_ok"] == 3 and r["nb_etapes_ko"] == 1
    ko = [e for e in _etapes(db) if e["statut"] == ETAPE_ECHEC]
    assert ko[0]["etape"] == "CLEANING_TASKS" and ko[0]["http_status"] == 429


def test_le_statut_est_deduit_jamais_affirme(db, tmp_path):
    """On ne peut pas déclarer un run réussi si une étape a échoué."""
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    j.etape("A", ETAPE_SUCCES)
    j.etape("B", ETAPE_ECHEC, erreur="boum")
    assert j.statut_global() == PARTIEL
    j.etape("C", ETAPE_SUCCES)
    assert j.statut_global() == PARTIEL


def test_etape_ignoree_ne_compte_ni_pour_ni_contre(db, tmp_path):
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    j.etape("RESERVATIONS", ETAPE_SUCCES)
    j.etape("CLEANING_TASKS", ETAPE_IGNOREE, erreur="--skip-cleaning-tasks")
    assert j.fermer() == SUCCES


# ── F. Process interrompu ───────────────────────────────────────────────────────────────────────

def test_run_non_ferme_reste_en_cours(db, tmp_path):
    """Un process tué ne clôture rien : la ligne reste EN_COURS, et c'est visible."""
    RunJournal("lot_test", racine=tmp_path, db_path=str(db)).etape("RESERVATIONS", ETAPE_SUCCES)
    r = _runs(db)[0]
    assert r["statut"] == EN_COURS and r["ended_at"] is None
    assert len(_etapes(db)) == 1, "le travail déjà fait reste enregistré"


def test_run_orphelin_devient_interrompu(db, tmp_path):
    """Au redémarrage, un run EN_COURS sans processus vivant n'est pas en cours."""
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    j.etape("RESERVATIONS", ETAPE_SUCCES)
    # PID impossible : simule un processus disparu.
    conn = sqlite3.connect(db)
    conn.execute("UPDATE moteur_runs SET pid=? WHERE run_id=?", (0, j.run_id))
    conn.commit()
    conn.close()

    assert marquer_runs_interrompus(db) == 1
    r = _runs(db)[0]
    assert r["statut"] == INTERROMPU
    assert "jamais cloture" in (r["erreur_resume"] or "").lower()


def test_run_vivant_n_est_pas_marque_interrompu(db, tmp_path):
    """Le PID courant existe : ne jamais marquer INTERROMPU un run bien vivant."""
    RunJournal("lot_test", racine=tmp_path, db_path=str(db))
    assert marquer_runs_interrompus(db) == 0
    assert _runs(db)[0]["statut"] == EN_COURS


# ── Repli JSON et robustesse ────────────────────────────────────────────────────────────────────

def test_repli_json_si_aucune_base(tmp_path, monkeypatch):
    """Sans base désignée, le run laisse quand même une trace exploitable."""
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    j = RunJournal("lot_test", racine=tmp_path, fallback_dir=tmp_path / "runs")
    j.etape("RESERVATIONS", ETAPE_SUCCES, nb_ecrits=5)
    j.fermer()

    fichier = tmp_path / "runs" / f"{j.run_id}.json"
    assert fichier.exists()
    donnees = json.loads(fichier.read_text(encoding="utf-8"))
    assert donnees["statut"] == SUCCES
    assert donnees["etapes"][0]["nb_ecrits"] == 5


def test_journal_indisponible_n_interrompt_pas_le_run(tmp_path, monkeypatch):
    """Journaliser est une observation. Un journal cassé ne doit pas empêcher le lot de tourner."""
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    j = RunJournal("lot_test", racine=tmp_path, db_path=str(tmp_path / "inexistante.db"),
                   fallback_dir=tmp_path / "runs")
    j.etape("RESERVATIONS", ETAPE_SUCCES)
    assert j.fermer() == SUCCES


def test_aucune_base_devinee(tmp_path, monkeypatch):
    """`resoudre_base` ne doit jamais tomber sur une base de production par défaut."""
    monkeypatch.delenv("PILOTAGE_DB_PATH", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)
    assert resoudre_base() is None

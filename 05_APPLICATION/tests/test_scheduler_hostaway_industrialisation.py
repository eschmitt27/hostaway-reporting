"""Scheduler Hostaway — mission industrialisation 3 : câblage app, cadence configurable,
run_history, non-doublement du minuteur.

Complète `test_ordonnanceur.py` (cadence, propagation, H6) sans dupliquer sa couverture. Aucun
appel API réel, aucune écriture réelle (`tmp_db` isole `cfg.DB_PATH`/`cfg.BACKUPS_DIR`).
"""
from __future__ import annotations

import app.config as cfg
from app.services import hostaway_actualisation_service as hostaway
from app.services import ordonnanceur_service as ordo
from app.services import run_history_service as history


# ── Cadence configurable par variable d'environnement (§4/§19) ──────────────

def test_cadence_hostaway_configurable(monkeypatch):
    monkeypatch.setattr(cfg, "HOSTAWAY_REFRESH_INTERVAL_HOURS", 7)
    assert ordo.cadences()[ordo.TACHE_HOSTAWAY] == 7


def test_cadence_cleaning_tasks_configurable(monkeypatch):
    monkeypatch.setattr(cfg, "HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS", 48)
    assert ordo.cadences()[ordo.TACHE_CLEANING_TASKS] == 48


def test_cadence_par_defaut_dans_la_configuration():
    """Aucune activation implicite, aucun 5h codé ailleurs que dans cfg."""
    assert cfg.HOSTAWAY_REFRESH_INTERVAL_HOURS == 5
    assert cfg.HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS == 24


# ── Un seul minuteur, jamais doublé (§5) ─────────────────────────────────────

def test_demarrer_deux_fois_ne_double_pas_le_minuteur(monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", True)
    try:
        r1 = ordo.demarrer(intervalle_s=3600)
        assert r1["ok"] is True and "deja_demarre" not in r1
        r2 = ordo.demarrer(intervalle_s=3600)
        assert r2 == {"ok": True, "deja_demarre": True}
    finally:
        ordo.arreter()


def test_arreter_puis_demarrer_recree_un_minuteur(monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", True)
    try:
        ordo.demarrer(intervalle_s=3600)
        ordo.arreter()
        r = ordo.demarrer(intervalle_s=3600)
        assert r["ok"] is True and "deja_demarre" not in r
    finally:
        ordo.arreter()


def test_demarrer_refuse_si_inactif():
    assert cfg.ORDONNANCEUR_ACTIF is False
    r = ordo.demarrer()
    assert r["ok"] is False and r["code"] == ordo.E_INACTIF


# ── main.py démarre/arrête l'ordonnanceur avec l'application (§5) ───────────

def test_lifespan_demarre_et_arrete_ordonnanceur(tmp_db, monkeypatch):
    appels = []
    monkeypatch.setattr(ordo, "demarrer", lambda **kw: appels.append("demarrer") or {"ok": False})
    monkeypatch.setattr(ordo, "arreter", lambda: appels.append("arreter"))

    import importlib
    main_module = importlib.import_module("app.main")

    import asyncio

    async def _cycle():
        async with main_module.lifespan(main_module.app):
            pass

    asyncio.run(_cycle())
    assert appels == ["demarrer", "arreter"]


# ── run_history pour Hostaway (§7) ────────────────────────────────────────────

def test_hostaway_actualiser_journalise_dans_run_history_si_attendre(tmp_db, tmp_path, monkeypatch):
    """Le chemin synchrone (celui de l'orchestrateur/ordonnanceur) journalise dans run_history."""
    monkeypatch.setattr(hostaway, "_racine_moteur", lambda: tmp_path)
    (tmp_path / hostaway.SCRIPT).write_text("# double de test", encoding="utf-8")
    monkeypatch.setattr(hostaway, "_interpreteur", lambda: "python")

    class _Proc:
        returncode = 0
        pid = 4242

    monkeypatch.setattr(hostaway.subprocess, "run", lambda *a, **k: _Proc())

    res = hostaway.actualiser(declencheur=hostaway.DECLENCHEUR_AUTO, db_path=tmp_db, attendre=True)
    assert res["ok"] is True and res["history_run_id"] is not None
    entree = next(r for r in history.derniers(db_path=tmp_db)
                  if r["run_id_opaque"] == res["history_run_id"])
    assert entree["statut"] == "SUCCESS" and entree["operation"] == "HOSTAWAY"
    assert entree["acteur"] == hostaway.DECLENCHEUR_AUTO


def test_hostaway_actualiser_echec_journalise_dans_run_history(tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(hostaway, "_racine_moteur", lambda: tmp_path)
    (tmp_path / hostaway.SCRIPT).write_text("# double de test", encoding="utf-8")
    monkeypatch.setattr(hostaway, "_interpreteur", lambda: "python")

    class _ProcEchec:
        returncode = 1
        pid = 4243

    monkeypatch.setattr(hostaway.subprocess, "run", lambda *a, **k: _ProcEchec())

    res = hostaway.actualiser(declencheur=hostaway.DECLENCHEUR_AUTO, db_path=tmp_db, attendre=True)
    entree = next(r for r in history.derniers(db_path=tmp_db)
                  if r["run_id_opaque"] == res["history_run_id"])
    assert entree["statut"] == "FAILED"


def test_hostaway_actualiser_non_bloquant_ne_cree_pas_de_run_history(tmp_db, monkeypatch):
    """Le bouton fire-and-forget (attendre=False) reste suivi par moteur_runs, pas run_history :
    l'issue n'est pas connue avant que la fonction ne réponde."""
    monkeypatch.setattr(hostaway, "_interpreteur", lambda: None)
    res = hostaway.actualiser(declencheur=hostaway.DECLENCHEUR_MANUEL, db_path=tmp_db,
                              attendre=False)
    assert res["ok"] is False  # script absent de toute façon sur cet environnement de test
    assert history.derniers(db_path=tmp_db) == []

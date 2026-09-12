"""Scheduler Hostaway sécurisé 5 h — cycle de vie, périmètre, concurrence, aval, observabilité.

Complète `test_ordonnanceur.py` (décision de cadence) et `test_scheduler_hostaway_industrialisation.py`
(câblage de base) sans les dupliquer. Les numéros de test reprennent la liste de la mission ; la
correspondance complète, y compris les tests qui existaient déjà ailleurs, est dans
`SCHEDULER_HOSTAWAY.md` §13.

AUCUN APPEL RÉEL : ni API Hostaway, ni `git fetch`, ni vraie base (`tmp_db`), ni horloge réelle.
"""
from __future__ import annotations

import ast
import asyncio
import importlib
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

import app.config as cfg
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch
from app.services import ordonnanceur_service as ordo

T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _fils_scheduler() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == ordo.NOM_FIL and t.is_alive()]


def _attendre(condition, delai_s: float = 5.0) -> bool:
    fin = time.monotonic() + delai_s
    while time.monotonic() < fin:
        if condition():
            return True
        time.sleep(0.01)
    return bool(condition())


@pytest.fixture(autouse=True)
def _scheduler_toujours_arrete():
    """Aucun minuteur ne survit à un test, quoi qu'il s'y passe."""
    yield
    ordo.arreter()
    _attendre(lambda: not _fils_scheduler(), 2.0)


def _espion_services(monkeypatch) -> list[str]:
    appels: list[str] = []

    def faux_service(chemin, db_path):
        appels.append(chemin)
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", faux_service)
    return appels


def _battement_bloquant(monkeypatch) -> tuple[threading.Event, threading.Event]:
    entre, libere = threading.Event(), threading.Event()

    def tick_bloquant(**_):
        entre.set()
        libere.wait(5)
        return {}

    monkeypatch.setattr(ordo, "tick", tick_bloquant)
    return entre, libere


# ── 1-4, 19, 26 : un processus, un scheduler, jamais implicite ─────────────────────────────────

def test_01_desactive_par_defaut_aucun_fil_dans_l_application(client):
    assert cfg.ORDONNANCEUR_ACTIF is False
    assert client.get("/actualisation").status_code == 200   # l'application a démarré (lifespan)
    assert ordo.en_marche() is False
    assert _fils_scheduler() == []


def test_02_04_activation_explicite_un_seul_fil_meme_apres_plusieurs_demarrages(monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", True)
    assert ordo.demarrer(intervalle_s=3600)["ok"] is True
    for _ in range(3):
        assert ordo.demarrer(intervalle_s=3600) == {"ok": True, "deja_demarre": True}
    assert len(_fils_scheduler()) == 1
    ordo.arreter()
    assert _attendre(lambda: not _fils_scheduler())
    assert ordo.en_marche() is False


@pytest.mark.parametrize("valeur, attendu", [("7", "7"), ("0", "5"), ("-3", "5"), ("abc", "5"),
                                             ("", "5")])
def test_03_frequence_configurable_par_environnement_et_bornee(tmp_path, valeur, attendu):
    """La cadence vient de l'environnement ; une valeur nulle ou illisible ne devient jamais « à
    chaque battement » et n'empêche pas l'application de démarrer."""
    env = {**os.environ, "HOSTAWAY_REFRESH_INTERVAL_HOURS": valeur, "APP_DATA_DIR": str(tmp_path)}
    sortie = subprocess.run(
        [sys.executable, "-c",
         "import app.config as c; print(c.HOSTAWAY_REFRESH_INTERVAL_HOURS)"],
        cwd=str(cfg.APP_ROOT), env=env, capture_output=True, text=True, timeout=120)
    assert sortie.returncode == 0, sortie.stderr
    assert sortie.stdout.strip() == attendu


def test_19_26_redemarrage_de_l_application_un_seul_scheduler_a_chaque_cycle(tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", True)
    main_module = importlib.import_module("app.main")
    vus: list[int] = []

    async def _cycle():
        async with main_module.lifespan(main_module.app):
            vus.append(len(_fils_scheduler()))

    for _ in range(2):
        asyncio.run(_cycle())
        assert _attendre(lambda: not _fils_scheduler()), "fil orphelin après l'arrêt de l'application"
    assert vus == [1, 1]


def test_19_arret_pendant_un_battement_ne_ressuscite_pas_le_minuteur(monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", True)
    entre, libere = _battement_bloquant(monkeypatch)
    ordo.demarrer(intervalle_s=0.01)
    assert entre.wait(5)
    ordo.arreter()
    libere.set()
    assert _attendre(lambda: not _fils_scheduler()), "un minuteur a été réarmé après l'arrêt"
    assert ordo.en_marche() is False


def test_26_redemarrage_pendant_un_battement_ne_double_pas_le_scheduler(monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", True)
    entre, libere = _battement_bloquant(monkeypatch)
    ordo.demarrer(intervalle_s=0.01)
    assert entre.wait(5)
    battement_en_cours = _fils_scheduler()
    ordo.arreter()
    ordo.demarrer(intervalle_s=3600)
    libere.set()
    for fil in battement_en_cours:
        fil.join(5)
    assert len(_fils_scheduler()) == 1


def test_22_battement_en_echec_trace_sans_chemin_ni_secret(monkeypatch):
    chemin = str(Path.home() / "dossier_prive" / "app.db")

    def tick_en_panne(**_):
        raise RuntimeError(f"base illisible : {chemin}")

    monkeypatch.setattr(ordo, "tick", tick_en_panne)
    messages: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())

    capture = _Capture()
    logger = ordo.get_logger()
    logger.addHandler(capture)
    try:
        ordo._battement_protege(None)          # ne lève jamais
    finally:
        logger.removeHandler(capture)
    assert messages and "RuntimeError" in messages[0]
    assert chemin not in messages[0] and str(Path.home()) not in messages[0]


# ── Architecture : le scheduler n'est qu'un déclencheur ────────────────────────────────────────

def test_scheduler_ne_connait_ni_lots_ni_api_ni_transport():
    """Aucun import de transport (API, sous-processus, dépôt) et aucun appel de Lot dans le module :
    il ne parle qu'à l'orchestrateur."""
    arbre = ast.parse(Path(ordo.__file__).read_text(encoding="utf-8"))
    modules = set()
    for noeud in ast.walk(arbre):
        if isinstance(noeud, ast.Import):
            modules.update(a.name for a in noeud.names)
        elif isinstance(noeud, ast.ImportFrom):
            modules.add(noeud.module or "")
            modules.update(f"{noeud.module}.{a.name}" for a in noeud.names)
    for interdit in ("requests", "subprocess", "hostaway_client", "hostaway_depot_service",
                     "hostaway_actualisation_service", "orchestrateur_moteur"):
        assert not any(interdit in m for m in modules), f"le scheduler importe {interdit}"
    noms = {n.attr for n in ast.walk(arbre) if isinstance(n, ast.Attribute)} | \
           {n.id for n in ast.walk(arbre) if isinstance(n, ast.Name)}
    assert not any(n.startswith(("run_lot", "executer_")) for n in noms)


# ── 27 : CleaningTasks n'est jamais embarqué par le job 5 h ───────────────────────────────────

def test_27_cleaning_tasks_jamais_embarque_par_le_job_5h(tmp_db, monkeypatch):
    appels = _espion_services(monkeypatch)
    resultat = ordo.tick(maintenant=T0, db_path=tmp_db)   # les deux sources « jamais actualisées »

    assert [lance["tache"] for lance in resultat["lances"]] == [ordo.TACHE_HOSTAWAY]
    h6 = next(d for d in resultat["decisions"] if d["tache"] == ordo.TACHE_CLEANING_TASKS)
    assert h6["declencher"] is False and h6["automatique"] is False
    assert h6["echeance_atteinte"] is True
    assert not any("cleaning_tasks" in a for a in appels), appels
    assert not any("executer_menages" in a for a in appels), appels

    run = orch.dernier_run(db_path=tmp_db)
    etapes = {e["etape"]: e for e in orch.etapes_run(run["run_id"], db_path=tmp_db)}
    assert etapes[dag.HOSTAWAY_CLEANING_TASKS]["statut"] == "IGNOREE"
    assert dag.MENAGES not in etapes


def test_27_la_propagation_ne_traverse_pas_un_import_externe_non_demande(amonts_calcul_ok,
                                                                          monkeypatch):
    _espion_services(monkeypatch)
    res = orch.actualiser(cibles=[dag.HOSTAWAY_RAW], inclure_imports_externes=True,
                          db_path=amonts_calcul_ok)
    assert [e["dataset"] for e in res["etapes"]] == [
        dag.HOSTAWAY_RAW, dag.HOSTAWAY_CLEANING_TASKS, dag.RESERVATIONS, dag.FLUX_LOT9,
        dag.LOT10, dag.LOT11, dag.LOT12]
    h6 = next(e for e in res["etapes"] if e["dataset"] == dag.HOSTAWAY_CLEANING_TASKS)
    assert h6["statut"] == "IGNOREE"


def test_27_cleaning_tasks_demande_explicitement_reste_declenche_avec_son_aval(amonts_calcul_ok,
                                                                              monkeypatch):
    """« Actualiser les ménages » garde son sens : H6 désigné lui-même part, et son aval suit."""
    appels = _espion_services(monkeypatch)
    res = orch.actualiser(cibles=[dag.HOSTAWAY_CLEANING_TASKS], inclure_imports_externes=True,
                          db_path=amonts_calcul_ok)
    assert any("cleaning_tasks" in a for a in appels)
    assert dag.MENAGES in [e["dataset"] for e in res["etapes"]]

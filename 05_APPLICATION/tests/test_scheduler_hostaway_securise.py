"""Scheduler Hostaway sécurisé 5 h — cycle de vie, périmètre, concurrence, aval, observabilité.

Complète `test_ordonnanceur.py` (décision de cadence) et `test_scheduler_hostaway_industrialisation.py`
(câblage de base) sans les dupliquer. Les numéros de test reprennent la liste de la mission ; la
correspondance complète, y compris les tests qui existaient déjà ailleurs, est dans
`SCHEDULER_HOSTAWAY.md` §13.

AUCUN APPEL RÉEL : ni API Hostaway, ni `git fetch`, ni vraie base (`tmp_db`), ni horloge réelle.
Le seul double côté Hostaway remplace le TRANSPORT (lecture du dépôt, sous-processus d'extraction) :
le service canonique, le verrou, `run_history` et l'orchestrateur sont les vrais.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

import app.config as cfg
import fixtures_hostaway as fx
from app.db.connection import get_db
from app.services import backup_service
from app.services import hostaway_actualisation_service as ha
from app.services import hostaway_depot_service as depot
from app.services import hostaway_raw_service as raw
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch
from app.services import ordonnanceur_service as ordo
from app.services import run_history_service as history

T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
COMMIT_A = "a" * 40


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


def _services_aval_neutralises(monkeypatch) -> list[str]:
    """Hostaway passe par le VRAI service canonique ; tout l'aval est un double qui réussit."""
    reel = orch._appeler_service
    appels: list[str] = []

    def aiguillage(chemin, db_path):
        appels.append(chemin)
        if chemin.endswith(":importer_hostaway"):
            return reel(chemin, db_path)
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", aiguillage)
    return appels


def _battement_bloquant(monkeypatch) -> tuple[threading.Event, threading.Event]:
    entre, libere = threading.Event(), threading.Event()

    def tick_bloquant(**_):
        entre.set()
        libere.wait(5)
        return {}

    monkeypatch.setattr(ordo, "tick", tick_bloquant)
    return entre, libere


def _depot_publie(monkeypatch, commit: str = COMMIT_A, *, disponible: bool = True,
                  erreur: str = "") -> None:
    """Ce que le pipeline GitHub a publié — sans `git fetch`."""
    etat = {"disponible": disponible, "commit": commit if disponible else "",
            "commit_court": commit[:8], "source_horodatage": "2026-09-12T14:35:42Z"}
    if erreur:
        etat["erreur"] = erreur
    monkeypatch.setattr(depot, "etat_publie", lambda **_: dict(etat))


def _moteur_simule(monkeypatch, tmp_path, *, code_retour: int = 0, ecrire=None, lever=None,
                   pendant=None) -> SimpleNamespace:
    """Remplace le SEUL sous-processus d'extraction. `ecrire(db_path)` joue le moteur quand il
    alimente la couche RAW ; `pendant()` s'exécute pendant l'import (concurrence) ; `lever` simule
    une interruption (timeout). Tout autre sous-processus reste réel, sauf `git`, interdit."""
    racine = tmp_path / "moteur"
    racine.mkdir(exist_ok=True)
    (racine / ha.SCRIPT).write_text("# double de test", encoding="utf-8")
    monkeypatch.setattr(ha, "_racine_moteur", lambda: racine)
    monkeypatch.setattr(ha, "_interpreteur", lambda: sys.executable)
    trace = SimpleNamespace(lancements=[], autres=[])
    reel = subprocess.run

    class _Proc:
        returncode = code_retour

    def faux_run(commande, *args, **kwargs):
        texte = " ".join(map(str, commande)) if isinstance(commande, (list, tuple)) else str(commande)
        if ha.SCRIPT not in texte:
            trace.autres.append(texte)
            assert not texte.startswith("git"), f"appel git réel pendant un test : {texte}"
            return reel(commande, *args, **kwargs)
        trace.lancements.append(list(commande))
        if pendant is not None:
            pendant()
        if lever is not None:
            raise lever
        if ecrire is not None:
            ecrire(Path(commande[commande.index("--db") + 1]))
        return _Proc()

    monkeypatch.setattr(ha.subprocess, "run", faux_run)
    return trace


def _ecrire_extraction(commit: str, *, ids=("91001", "91002"), statut=None):
    """Le moteur écrit une extraction du dépôt ; `statut=False` la laisse ouverte (moteur tué)."""
    def ecrire(db_path):
        eid = raw.ouvrir(mode=raw.MODE_DEPOT_GITHUB, db_path=db_path, source_ref=commit,
                         source_horodatage="2026-09-12T14:35:42Z")
        raw.enregistrer(eid, db_path=db_path, listings=[fx.listing()],
                        reservations=[fx.reservation(i, check_in="2026-07-01") for i in ids],
                        payouts=[fx.payout(i) for i in ids])
        if statut is not False:
            raw.cloturer(eid, statut=statut or raw.ST_SUCCES, db_path=db_path)
        return eid
    return ecrire


def _runs_hostaway(db_path) -> list[dict]:
    return [r for r in history.derniers(limit=100, db_path=db_path)
            if r["operation"] == depot.OPERATION_HISTORIQUE]


def _a_jour_il_y_a(db_path, dataset: str, heures: float) -> None:
    orch.marquer_dataset(dataset, orch.ST_A_JOUR, run_id="SEED-TEST", db_path=db_path)
    passe = (datetime.now(timezone.utc) - timedelta(hours=heures)).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE orchestrateur_datasets SET calcule_le = ?, maj_le = ? "
                     "WHERE dataset = ?", (passe, passe, dataset))
        conn.commit()
    finally:
        conn.close()


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


# ── 5-8 : un seul service, un déclencheur exact, un run tracé ─────────────────────────────────

def test_05_manuel_et_automatique_appellent_le_meme_service(client, tmp_db, monkeypatch):
    appels: list[dict] = []

    def espion(**kw):
        appels.append(kw)
        return {"ok": True, "importe": False, "code": depot.E_DEJA_SYNCHRONISE, "publie": {}}

    monkeypatch.setattr(depot, "synchroniser", espion)
    _services_aval_neutralises(monkeypatch)

    client.post("/hostaway/actualiser", follow_redirects=False)
    ordo.tick(maintenant=T0, db_path=tmp_db)

    assert [a["declencheur"] for a in appels] == ["MANUEL", "AUTO"]
    # Mêmes arguments au déclencheur près : aucun paramètre ne dépend de qui déclenche.
    identiques = [{k: v for k, v in a.items() if k not in ("declencheur", "db_path")}
                  for a in appels]
    assert identiques[0] == identiques[1]


def test_06_07_08_run_automatique_trace_avec_declencheur_duree_et_volumes(tmp_db, tmp_path,
                                                                          monkeypatch):
    _depot_publie(monkeypatch)
    _moteur_simule(monkeypatch, tmp_path, ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)

    resultat = ordo.tick(maintenant=T0, db_path=tmp_db)

    assert [lance["tache"] for lance in resultat["lances"]] == [ordo.TACHE_HOSTAWAY]
    runs = _runs_hostaway(tmp_db)
    assert len(runs) == 1
    run = runs[0]
    assert run["acteur"] == "AUTO" and run["statut"] == "SUCCESS" and run["erreur"] is None
    assert run["date_debut"] and run["date_fin"] and run["duree_s"] is not None
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.HOSTAWAY_RAW)
    assert etat["statut"] == orch.ST_A_JOUR and etat["declencheur"] == "AUTO"
    assert etat["nb_lignes"]
    assert orch.dernier_run(db_path=tmp_db)["declencheur"] == "AUTO"


def test_06_run_lance_depuis_l_ecran_reste_manuel_jusqu_au_journal(tmp_db, tmp_path, monkeypatch):
    _depot_publie(monkeypatch)
    _moteur_simule(monkeypatch, tmp_path, ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)

    orch.actualiser(cibles=[dag.HOSTAWAY_RAW], declencheur=orch.DECLENCHEUR_MANUEL,
                    inclure_imports_externes=True, db_path=tmp_db)

    assert [r["acteur"] for r in _runs_hostaway(tmp_db)] == ["MANUEL"]
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.HOSTAWAY_RAW)
    assert etat["declencheur"] == "MANUEL"


# ── 9, 12, 13 : panne, atomicité, dernier état valide ─────────────────────────────────────────

def test_13_succes_active_le_candidat(tmp_db, tmp_path, monkeypatch):
    ancienne = fx.jeu_minimal(tmp_db, nb=1)
    _depot_publie(monkeypatch)
    _moteur_simule(monkeypatch, tmp_path, ecrire=_ecrire_extraction(COMMIT_A))

    res = depot.synchroniser(declencheur="AUTO", db_path=tmp_db)

    assert res["ok"] is True and res["importe"] is True
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) != ancienne
    assert sorted(r["reservation_id"] for r in raw.reservations(db_path=tmp_db)) == ["91001",
                                                                                   "91002"]


def test_09_12_timeout_conserve_le_dernier_dataset_sans_melange_ni_fuite(tmp_db, tmp_path,
                                                                        monkeypatch):
    ancienne = fx.jeu_minimal(tmp_db, nb=2)
    avant = sorted(r["reservation_id"] for r in raw.reservations(db_path=tmp_db))
    _depot_publie(monkeypatch)
    commande = [sys.executable, str(Path.home() / "prive" / ha.SCRIPT), "--db", str(tmp_db)]
    _moteur_simule(
        monkeypatch, tmp_path,
        # Le moteur a ouvert son extraction et écrit une partie des lignes quand il est arrêté.
        pendant=lambda: _ecrire_extraction(COMMIT_A, ids=("99999",), statut=False)(tmp_db),
        lever=subprocess.TimeoutExpired(commande, 1800))

    res = depot.synchroniser(declencheur="AUTO", db_path=tmp_db)

    assert res["ok"] is False and res["importe"] is False and res["code"] == ha.E_DELAI_DEPASSE
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ancienne
    assert sorted(r["reservation_id"] for r in raw.reservations(db_path=tmp_db)) == avant
    run = _runs_hostaway(tmp_db)[0]
    assert run["statut"] == "FAILED" and ha.E_DELAI_DEPASSE in run["erreur"]
    utilisateur = os.environ.get("USERNAME") or "§§§"
    for fuite in (str(Path.home()), str(tmp_db), str(tmp_path), utilisateur):
        assert fuite not in run["erreur"] and fuite not in res["message"]
    assert backup_service.lister() == []                  # jamais de restauration pour une panne


def test_09_12_depot_injoignable_via_scheduler_sans_restauration_ni_boucle(tmp_db, tmp_path,
                                                                          monkeypatch):
    ancienne = fx.jeu_minimal(tmp_db, nb=1)
    _a_jour_il_y_a(tmp_db, dag.HOSTAWAY_RAW, heures=6)
    secret = "ghp_" + "X" * 24
    _depot_publie(monkeypatch, disponible=False,
                  erreur=("DepotIndisponible: git fetch a échoué : fatal: unable to access "
                          f"'https://x-access-token:{secret}@github.com/o/r.git/': "
                          "Could not resolve host: github.com"))
    trace = _moteur_simule(monkeypatch, tmp_path)
    _services_aval_neutralises(monkeypatch)

    premier = ordo.tick(db_path=tmp_db)
    second = ordo.tick(db_path=tmp_db)                    # battement suivant, quelques instants après

    assert [lance["tache"] for lance in premier["lances"]] == [ordo.TACHE_HOSTAWAY]
    assert second["lances"] == [], "un échec ne doit pas être retenté au battement suivant"
    assert trace.lancements == []                         # dépôt illisible : moteur jamais lancé
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ancienne
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.HOSTAWAY_RAW)
    assert etat["statut"] == orch.ST_ECHEC and etat["erreur_code"] == depot.E_DEPOT_INDISPONIBLE
    run = _runs_hostaway(tmp_db)[0]
    assert run["statut"] == "FAILED"
    for trace_texte in (run["erreur"], etat["erreur_message"]):
        assert secret not in trace_texte and "x-access-token" not in trace_texte
    assert backup_service.lister() == []


def test_import_en_echec_n_est_jamais_annonce_termine(client, tmp_db, tmp_path, monkeypatch):
    """Un moteur qui rend un code non nul : l'écran ne dit plus « Synchronisation terminée »."""
    ancienne = fx.jeu_minimal(tmp_db, nb=1)
    _depot_publie(monkeypatch)
    _moteur_simule(monkeypatch, tmp_path, code_retour=1)

    r = client.post("/hostaway/actualiser", follow_redirects=False)

    destination = unquote(r.headers["location"])
    assert "message_type=error" in destination
    assert "Synchronisation terminée" not in destination
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ancienne
    assert _runs_hostaway(tmp_db)[0]["statut"] == "FAILED"


def test_22_erreur_externe_sans_identifiant_d_url_ni_chemin():
    from app.services.path_sanitizer import sanitize_erreur_externe

    brut = ("fatal: unable to access 'https://x-access-token:ghp_ABCDEFGHIJ@github.com/o/r.git/' "
            f"depuis {Path.home() / 'depot'}")
    propre = sanitize_erreur_externe(brut)
    assert "ghp_ABCDEFGHIJ" not in propre and "x-access-token" not in propre
    assert str(Path.home()) not in propre


# ── 16, 17 : concurrence, dans les deux sens, sans attente ────────────────────────────────────

def test_16_17_prendre_verrou_est_atomique_sous_concurrence(tmp_db):
    gagnants: list[int] = []
    barriere = threading.Barrier(8)

    def candidat(i):
        barriere.wait()
        if orch.prendre_verrou(depot.PORTEE_VERROU, f"RUN-{i}", db_path=tmp_db)["ok"]:
            gagnants.append(i)

    fils = [threading.Thread(target=candidat, args=(i,)) for i in range(8)]
    for f in fils:
        f.start()
    for f in fils:
        f.join(30)
    assert len(gagnants) == 1


def _import_bloque() -> tuple[threading.Event, threading.Event, callable]:
    dans_l_import, liberer = threading.Event(), threading.Event()

    def pendant():
        dans_l_import.set()
        liberer.wait(15)

    return dans_l_import, liberer, pendant


def test_16_synchronisation_manuelle_en_cours_le_scheduler_est_refuse(amonts_calcul_ok, tmp_path,
                                                                      monkeypatch):
    db = amonts_calcul_ok
    _depot_publie(monkeypatch)
    dans_l_import, liberer, pendant = _import_bloque()
    trace = _moteur_simule(monkeypatch, tmp_path, pendant=pendant,
                           ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)
    resultats: dict = {}
    manuel = threading.Thread(target=lambda: resultats.update(
        manuel=depot.synchroniser(declencheur="MANUEL", db_path=db)))
    manuel.start()
    try:
        assert dans_l_import.wait(10)
        tick = ordo.tick(maintenant=T0, db_path=db)
        assert tick["lances"] == []
        decision = next(d for d in tick["decisions"] if d["tache"] == ordo.TACHE_HOSTAWAY)
        assert "en cours" in decision["motif"]
        # Même si l'appel passait entre la décision et le lancement : refus immédiat, sans attente.
        debut = time.monotonic()
        auto = depot.synchroniser(declencheur="AUTO", db_path=db)
        assert auto["ok"] is False and auto["code"] == ha.E_DEJA_EN_COURS
        assert time.monotonic() - debut < 5
    finally:
        liberer.set()
        manuel.join(15)
    assert resultats["manuel"]["ok"] is True
    assert len(trace.lancements) == 1


def test_17_run_automatique_en_cours_le_bouton_manuel_est_refuse(client, amonts_calcul_ok,
                                                                 tmp_path, monkeypatch):
    db = amonts_calcul_ok
    _depot_publie(monkeypatch)
    dans_l_import, liberer, pendant = _import_bloque()
    trace = _moteur_simule(monkeypatch, tmp_path, pendant=pendant,
                           ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)
    resultats: dict = {}
    auto = threading.Thread(target=lambda: resultats.update(auto=ordo.tick(maintenant=T0,
                                                                           db_path=db)))
    auto.start()
    try:
        assert dans_l_import.wait(10)
        r = client.post("/hostaway/actualiser", follow_redirects=False)
        destination = unquote(r.headers["location"])
        assert "déjà en cours" in destination and "message_type=error" in destination
        assert depot.synchroniser(declencheur="MANUEL", db_path=db)["code"] == ha.E_DEJA_EN_COURS
    finally:
        liberer.set()
        auto.join(15)
    assert [lance["tache"] for lance in resultats["auto"]["lances"]] == [ordo.TACHE_HOSTAWAY]
    assert len(trace.lancements) == 1


# ── 18 : crash, puis reprise normale ──────────────────────────────────────────────────────────

def test_18_crash_run_interrompu_puis_reprise_normale(amonts_calcul_ok, tmp_path, monkeypatch):
    db = amonts_calcul_ok
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur) "
            "VALUES ('ORCH-CRASH', ?, '2026-06-01T06:00:00Z', 'EN_COURS', 'AUTO')",
            (orch.LOT_ORCHESTRATEUR,))
        conn.commit()
    finally:
        conn.close()
    orch.marquer_dataset(dag.HOSTAWAY_RAW, orch.ST_EN_COURS, run_id="ORCH-CRASH",
                         declencheur="AUTO", db_path=db)
    orphelin = history.demarrer(depot.OPERATION_HISTORIQUE, acteur="AUTO", db_path=db)
    _depot_publie(monkeypatch)
    trace = _moteur_simule(monkeypatch, tmp_path, ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)

    resultat = ordo.tick(maintenant=T0, db_path=db)

    runs = {r["run_id"]: r for r in orch.historique(limite=10, db_path=db)}
    assert runs["ORCH-CRASH"]["statut"] == orch.RUN_INTERROMPU
    assert [lance["tache"] for lance in resultat["lances"]] == [ordo.TACHE_HOSTAWAY]
    hist = {r["run_id_opaque"]: r for r in _runs_hostaway(db)}
    assert hist[orphelin]["statut"] == "FAILED" and "RUN_INTERROMPU" in hist[orphelin]["erreur"]
    assert hist[orphelin]["duree_s"] is None
    nouveaux = [r for rid, r in hist.items() if rid != orphelin]
    assert [r["statut"] for r in nouveaux] == ["SUCCESS"]
    assert len(trace.lancements) == 1


# ── 20, 21 : aucun réseau, aucune vraie base ──────────────────────────────────────────────────

def test_20_21_un_battement_complet_sans_reseau_ni_vraie_base(tmp_db, tmp_path, monkeypatch):
    import socket
    import sqlite3

    import requests

    def interdit(*_a, **_k):
        raise AssertionError("appel réseau réel pendant un test")

    for cible, nom in ((requests, "get"), (requests, "post"), (requests.Session, "request"),
                       (socket, "create_connection")):
        monkeypatch.setattr(cible, nom, interdit)
    chemins: set[str] = set()
    connexion_reelle = sqlite3.connect

    def connexion_espionnee(chemin, *a, **k):
        chemins.add(str(Path(chemin).resolve()))
        return connexion_reelle(chemin, *a, **k)

    monkeypatch.setattr(sqlite3, "connect", connexion_espionnee)
    _depot_publie(monkeypatch)
    trace = _moteur_simule(monkeypatch, tmp_path, ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)

    ordo.tick(maintenant=T0, db_path=tmp_db)

    assert len(trace.lancements) == 1
    assert not any(c.startswith("git") for c in trace.autres)
    assert chemins == {str(Path(tmp_db).resolve())}


# ── 23-25 : idempotence, aucun doublon ────────────────────────────────────────────────────────

def test_23_24_25_deux_battements_sur_le_meme_etat_publie_ne_dupliquent_rien(amonts_calcul_ok,
                                                                            tmp_path,
                                                                            monkeypatch):
    db = amonts_calcul_ok
    _depot_publie(monkeypatch)
    trace = _moteur_simule(monkeypatch, tmp_path, ecrire=_ecrire_extraction(COMMIT_A))
    _services_aval_neutralises(monkeypatch)

    ordo.tick(maintenant=T0, db_path=db)
    ordo.tick(maintenant=datetime.now(timezone.utc) + timedelta(hours=6), db_path=db)

    assert len(trace.lancements) == 1                     # le moteur n'est pas relancé
    conn = get_db(db)
    try:
        compte = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ("hostaway_extractions", "hostaway_reservations", "hostaway_payouts")}
    finally:
        conn.close()
    assert compte == {"hostaway_extractions": 1, "hostaway_reservations": 2, "hostaway_payouts": 2}
    assert [r["statut"] for r in _runs_hostaway(db)] == ["SUCCESS", "SUCCESS"]


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

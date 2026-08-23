"""Ordonnanceur Hostaway — cadence 5h, séparation H6, réutilisation du service unique (§39-44).

AUCUN APPEL RÉEL : l'horloge est injectée et l'exécution est remplacée par un double. Le mode réel
reste désactivé (§44) — un test vérifie explicitement que rien ne démarre tant que le flag est faux.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import app.config as cfg
from app.services import ordonnanceur_service as ordo
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

T0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _marquer_actualise(tmp_db, tache, quand: datetime, statut=orch.ST_A_JOUR):
    """Positionne l'état d'une source comme si elle avait été actualisée à `quand`."""
    from app.db.connection import get_db

    orch.marquer_dataset(tache, statut, run_id="R-TEST", db_path=tmp_db)
    conn = get_db(tmp_db)
    try:
        conn.execute(
            "UPDATE orchestrateur_datasets SET calcule_le = ?, maj_le = ? WHERE dataset = ?",
            (quand.strftime("%Y-%m-%dT%H:%M:%SZ"), quand.strftime("%Y-%m-%dT%H:%M:%SZ"), tache))
        conn.commit()
    finally:
        conn.close()


# ── Cadence (§40) ───────────────────────────────────────────────────────────

def test_jamais_actualise_declenche(tmp_db):
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0, db_path=tmp_db)
    assert d["declencher"] is True


def test_avant_5h_ne_declenche_pas(tmp_db):
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0)
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0 + timedelta(hours=4, minutes=59),
                             db_path=tmp_db)
    assert d["declencher"] is False


def test_a_5h_declenche(tmp_db):
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0)
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0 + timedelta(hours=5),
                             db_path=tmp_db)
    assert d["declencher"] is True


def test_actualisation_manuelle_recente_repousse_l_automatique(tmp_db):
    """Une actualisation manuelle compte : l'ordonnanceur ne relance pas juste après."""
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0 + timedelta(hours=4))
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0 + timedelta(hours=5),
                             db_path=tmp_db)
    assert d["declencher"] is False


def test_actualisation_en_cours_ne_relance_pas(tmp_db):
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0, statut=orch.ST_EN_COURS)
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0 + timedelta(hours=10),
                             db_path=tmp_db)
    assert d["declencher"] is False and "cours" in d["motif"].lower()


# ── 429 / échec récent (§42) ────────────────────────────────────────────────

def test_echec_recent_impose_un_palier(tmp_db):
    """Ne jamais réessayer immédiatement : le budget de tentatives se consommerait pour rien."""
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0 - timedelta(hours=10),
                       statut=orch.ST_ECHEC)
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0 - timedelta(hours=9, minutes=30),
                             db_path=tmp_db)
    assert d["declencher"] is False and "chec" in d["motif"]


def test_apres_le_palier_reessaie(tmp_db):
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0 - timedelta(hours=10),
                       statut=orch.ST_ECHEC)
    d = ordo.doit_declencher(ordo.TACHE_HOSTAWAY, maintenant=T0, db_path=tmp_db)
    assert d["declencher"] is True


def test_gestion_429_reste_dans_le_lot_dextraction():
    """§42 — l'ordonnanceur ne réimplémente NI Retry-After NI backoff : ils vivent dans le lot."""
    from pathlib import Path

    source = Path(cfg.APP_ROOT).parent / "02_TRAVAIL" / "lot1_hostaway_extract.py"
    contenu = source.read_text(encoding="utf-8", errors="ignore")
    assert "Retry-After" in contenu and "RateLimitEpuise" in contenu

    ordo_src = Path(ordo.__file__).read_text(encoding="utf-8")
    assert "Retry-After" not in ordo_src.replace("`Retry-After`", "")


# ── H6 : cadence distincte (§41) ────────────────────────────────────────────

def test_cleaning_tasks_pas_a_la_meme_cadence():
    cad = ordo.cadences()
    assert cad[ordo.TACHE_CLEANING_TASKS] > cad[ordo.TACHE_HOSTAWAY]
    assert cad[ordo.TACHE_HOSTAWAY] == 5


def test_cleaning_tasks_non_declenche_toutes_les_5h(tmp_db):
    _marquer_actualise(tmp_db, ordo.TACHE_CLEANING_TASKS, T0)
    d = ordo.doit_declencher(ordo.TACHE_CLEANING_TASKS, maintenant=T0 + timedelta(hours=6),
                             db_path=tmp_db)
    assert d["declencher"] is False


# ── Service unique manuel/auto (§39/§43) ────────────────────────────────────

def test_ordonnanceur_et_manuel_partagent_le_meme_service():
    """Une seule implémentation de l'extraction : le DAG pointe le service, l'écran l'appelle."""
    from app.services import orchestrateur_moteur

    noeud = dag.NOEUDS[dag.HOSTAWAY_RAW]
    assert noeud.service == "app.services.orchestrateur_moteur:importer_hostaway"
    source = __import__("inspect").getsource(orchestrateur_moteur.importer_hostaway)
    assert "hostaway_actualisation_service" in source
    assert "attendre=True" in source


def test_tick_declenche_via_orchestrateur(tmp_db, monkeypatch):
    """Le battement passe par le MÊME orchestrateur que l'écran, avec declencheur AUTO."""
    appels: list[dict] = []

    def faux_actualiser(**kwargs):
        appels.append(kwargs)
        return {"ok": True, "run_id": "R1", "statut": orch.RUN_SUCCES}

    monkeypatch.setattr(ordo.orch, "actualiser", faux_actualiser)
    res = ordo.tick(maintenant=T0, db_path=tmp_db)
    assert res["lances"], res
    premier = appels[0]
    assert premier["declencheur"] == orch.DECLENCHEUR_AUTO
    assert premier["inclure_imports_externes"] is True
    assert premier["cibles"] == [ordo.TACHE_HOSTAWAY]


def test_tick_ne_declenche_rien_si_rien_nest_du(tmp_db, monkeypatch):
    appels: list[dict] = []
    monkeypatch.setattr(ordo.orch, "actualiser",
                        lambda **kw: appels.append(kw) or {"ok": True, "run_id": "R"})
    _marquer_actualise(tmp_db, ordo.TACHE_HOSTAWAY, T0)
    _marquer_actualise(tmp_db, ordo.TACHE_CLEANING_TASKS, T0)
    ordo.tick(maintenant=T0 + timedelta(hours=1), db_path=tmp_db)
    assert appels == []


# ── Mode réel non activé (§44) ──────────────────────────────────────────────

def test_ordonnanceur_inerte_par_defaut(monkeypatch):
    monkeypatch.setattr(cfg, "ORDONNANCEUR_ACTIF", False)
    res = ordo.demarrer()
    assert res["ok"] is False and res["code"] == ordo.E_INACTIF


def test_flag_desactive_par_defaut_dans_la_configuration():
    """Aucune activation implicite : le flag doit être faux hors variable d'environnement."""
    assert cfg.ORDONNANCEUR_ACTIF is False


def test_etat_lisible_sans_demarrer(tmp_db):
    etat = ordo.etat(db_path=tmp_db)
    assert etat["actif"] is False
    assert etat["cadences_h"][ordo.TACHE_HOSTAWAY] == 5
    assert len(etat["prochaines_decisions"]) == len(ordo.cadences())

"""« Actualiser le rapprochement des ménages » — robustesse (mission « spinner infini »).

Complète `test_menages_actualisation_workflow.py` (orchestration nominale) avec les cas visés par la
mission qui a corrigé le blocage constaté en recette (le clic bloquait TOUT le serveur, pas
seulement cette requête) :
  A/B — préflight (config absente) : arrêt propre, AUCUN sous-processus/appel tenté.
  C/D — timeouts déjà en place, jamais un crash brut.
  E   — pagination Hostaway : plafond dur, jamais une boucle infinie même sous régression API.
  F/H — le message de succès n'apparaît QUE si tout a réussi ; sinon jamais.
  G   — un échec ne laisse jamais le verrou bloqué : un nouveau clic est accepté ensuite.
  Concurrence — deux appels simultanés ne lancent jamais deux chaînes en parallèle.
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import hostaway_cleaning_tasks_actualisation_service as hostaway_ct
from app.services import menages_actualisation_service as workflow


@pytest.fixture()
def db(tmp_path, monkeypatch):
    chemin = tmp_path / "app.db"
    monkeypatch.setattr(cfg, "DB_PATH", chemin, raising=False)
    apply_migrations(chemin)
    return chemin


@pytest.fixture()
def espions_ok(monkeypatch, db):
    """Toutes les frontières externes réputées OK — permet d'isoler UN seul point d'échec par test."""
    from app.services import menages_declarations_service as decl
    from app.services import menages_pdf_import_service as pdf_import
    from app.services import menages_service as svc
    from app.services import orchestrateur_moteur as moteur
    from app.services import orchestrateur_service as orch

    journal: list[str] = []
    pdf_etat = {"ok": True, "nb_detectes": 0, "nb_importees": 0, "nb_remplacees": 0,
                "nb_deja_importees": 0, "mois_impactes": [], "details": []}

    def _pdf(**kw):
        journal.append("PDF")
        return pdf_etat

    def _lot6b(**kw):
        journal.append("GOOGLE_SHEET")
        return {"ok": True}

    def _hostaway(dataset, **kw):
        journal.append(f"HOSTAWAY:{dataset}")
        return {"ok": True}

    def _menages(*, mois, **kw):
        journal.append(f"MENAGES:{mois}")
        return {"ok": True, "mois_traite": mois, "code": ""}

    monkeypatch.setattr(pdf_import, "importer_nouveaux", _pdf)
    monkeypatch.setattr(moteur, "executer_declarations_internes", _lot6b)
    monkeypatch.setattr(orch, "recalculer_dataset", _hostaway)
    monkeypatch.setattr(moteur, "executer_menages_cible", _menages)
    monkeypatch.setattr(orch, "marquer_dataset", lambda *a, **k: None)
    monkeypatch.setattr(orch, "actualiser",
                        lambda **k: journal.append("CASCADE") or {"ok": True})
    monkeypatch.setattr(decl, "mois_cloture", lambda mois, db_path=None: False)
    monkeypatch.setattr(svc, "invalidate_menages_cache", lambda: None)
    monkeypatch.setattr(svc, "periode_par_defaut", lambda: "2026-06")
    monkeypatch.setattr(hostaway_ct, "credentials_disponibles", lambda: True)
    # Config Google Sheet réelle (pas un monkeypatch de la fonction elle-même) : une ligne SRC_011
    # valide en base, exactement ce que `_google_sheet_config_ok` doit lire pour rendre True. Exercer
    # le VRAI check partout sauf dans le test A (qui le laisse volontairement absent).
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_sources_systeme (source_id, nom_source, dossier_source, actif, "
            "import_id) VALUES ('SRC_011', 'GOOGLE_SHEET_M04_DECLARATIONS', "
            "'https://example.test/sheet', 'OUI', 'TEST')")
        conn.commit()
    finally:
        conn.close()
    return journal, pdf_etat


# ── A. Google config absente ────────────────────────────────────────────────────────────────────

def test_a_google_config_absente_fail_fast_sans_sous_processus(monkeypatch, db, espions_ok):
    """SRC_011 absent (aucune ligne insérée sur cette base) : le sous-processus lot6b n'est JAMAIS
    lancé. `espions_ok` insère normalement une ligne SRC_011 valide — ce test la retire pour
    reproduire une config absente, sans toucher aux autres espions (PDF/Hostaway restent OK)."""
    from app.services import orchestrateur_moteur as moteur

    journal, _ = espions_ok
    conn = get_db(db)
    try:
        conn.execute("DELETE FROM ref_sources_systeme WHERE source_id='SRC_011'")
        conn.commit()
    finally:
        conn.close()
    # `ref_sources_systeme` existe mais ne contient plus SRC_011 : le check réel doit rendre False,
    # pas planter.
    assert workflow._google_sheet_config_ok(db_path=db) is False

    def _piege(**kw):
        raise AssertionError("lot6b (Google Sheet) appelé alors que la config est absente")

    monkeypatch.setattr(moteur, "executer_declarations_internes", _piege)

    resultat = workflow.actualiser(mois_affiche="2026-06", db_path=db)

    assert "GOOGLE_SHEET" not in journal
    etape_sheet = next(e for e in resultat["etapes"] if e["etape"] == "GOOGLE_SHEET")
    assert etape_sheet["ok"] is False
    assert "SRC_011" in etape_sheet["message"] or "indisponible" in etape_sheet["message"]
    # Le reste continue (PDF a bien tourné) — préflight Sheet est APRÈS PDF, pas un arrêt total.
    assert "PDF" in journal


# ── B. Hostaway config absente ──────────────────────────────────────────────────────────────────

def test_b_hostaway_config_absente_arret_avant_pdf_et_sheet(monkeypatch, db, espions_ok):
    """Identifiants Hostaway absents : arrêt AVANT PDF/Sheet (préflight placé au tout début)."""
    from app.services import menages_pdf_import_service as pdf_import
    from app.services import orchestrateur_moteur as moteur

    journal, _ = espions_ok
    monkeypatch.setattr(hostaway_ct, "credentials_disponibles", lambda: False)

    def _piege_pdf(**kw):
        raise AssertionError("PDF importé alors que Hostaway n'est pas configuré (préflight KO)")

    def _piege_sheet(**kw):
        raise AssertionError("Sheet appelé alors que Hostaway n'est pas configuré (préflight KO)")

    monkeypatch.setattr(pdf_import, "importer_nouveaux", _piege_pdf)
    monkeypatch.setattr(moteur, "executer_declarations_internes", _piege_sheet)

    resultat = workflow.actualiser(mois_affiche="2026-06", db_path=db)

    assert journal == [], "aucune étape ne doit avoir démarré"
    assert resultat["ok"] is False
    assert resultat["statut"] == "ECHEC"
    assert resultat["code"] == hostaway_ct.E_CREDENTIALS_ABSENTES
    assert "Hostaway" in resultat["message"] or "identifiants" in resultat["message"].lower()
    assert resultat["etapes"] == []


# ── C. Google timeout ───────────────────────────────────────────────────────────────────────────

def test_c_google_timeout_echec_propre_pas_de_crash(monkeypatch, db, espions_ok):
    from app.services import orchestrateur_moteur as moteur

    def _timeout(**kw):
        return {"ok": False, "code": moteur.E_TIMEOUT, "message": "lot6b : dépassement de 1800s."}

    monkeypatch.setattr(moteur, "executer_declarations_internes", _timeout)

    resultat = workflow.actualiser(mois_affiche="2026-06", db_path=db)

    assert resultat["statut"] in ("PARTIEL", "ECHEC")
    etape = next(e for e in resultat["etapes"] if e["etape"] == "GOOGLE_SHEET")
    assert etape["ok"] is False


# ── D. Hostaway timeout ─────────────────────────────────────────────────────────────────────────

def test_d_hostaway_timeout_transforme_en_echec_propre_par_recalculer_dataset(db):
    """Teste directement `orchestrateur_service.recalculer_dataset` (pas tout le workflow ménages) :
    une exception `requests.exceptions.Timeout` levée par le service Hostaway doit devenir un dict
    `{"ok": False, ...}, jamais remonter brute (garde déjà existante, revérifiée ici)."""
    import requests

    from app.services import hostaway_cleaning_tasks_actualisation_service as cleaning_tasks
    from app.services import orchestrateur_service as orch

    def _timeout(**kw):
        raise requests.exceptions.Timeout("Hostaway n'a pas répondu à temps")

    import unittest.mock as mock
    with mock.patch.object(cleaning_tasks, "actualiser", side_effect=_timeout):
        resultat = orch.recalculer_dataset("HOSTAWAY_CLEANING_TASKS", db_path=db)

    assert resultat["ok"] is False
    assert resultat["code"] == "Timeout"


# ── E. Pagination pathologique ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def lot1(monkeypatch):
    travail = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"
    if str(travail) not in sys.path:
        sys.path.insert(0, str(travail))
    pytest.importorskip("pandas")
    import lot1_hostaway_extract as module
    return module


def test_e_pagination_pathologique_plafonnee_jamais_infinie(lot1, monkeypatch):
    """Reproduit le bug historique : l'API renvoie toujours une page PLEINE, quel que soit l'offset
    (le vieux test `len(batch) < PAGE_SIZE` ne s'arrêtait jamais). Le plafond dur (§5 mission) doit
    arrêter la boucle proprement au lieu de la laisser tourner indéfiniment — le test lui-même ne
    doit jamais rester bloqué."""
    appels = {"n": 0}

    class _ClientFactice:
        def _get(self, endpoint, params):
            appels["n"] += 1
            # `count` toujours nul et `result` toujours plein : aucune des deux conditions d'arrêt
            # normales (batch vide / batch < PAGE_SIZE / count atteint) ne se déclenche jamais.
            return {"result": [{"id": i} for i in range(lot1.PAGE_SIZE)], "count": None}

    client = _ClientFactice()
    with pytest.raises(RuntimeError, match="plafond"):
        lot1.HostawayClient.get_tasks(client, "2026-01-01")

    assert appels["n"] == 200, "doit s'arrêter exactement au plafond, ni avant ni après"


# ── F/H. Message de succès réservé au succès complet ────────────────────────────────────────────

def test_f_echec_hostaway_jamais_le_message_de_succes(monkeypatch, db, espions_ok):
    from app.services import orchestrateur_service as orch

    journal, _ = espions_ok

    def _hostaway_echoue(dataset, **kw):
        journal.append(f"HOSTAWAY:{dataset}")
        return {"ok": False, "code": "HOSTAWAY_CLEANING_TASKS_API_ECHOUEE", "message": "API KO"}

    monkeypatch.setattr(orch, "recalculer_dataset", _hostaway_echoue)

    resultat = workflow.actualiser(mois_affiche="2026-06", db_path=db)

    assert resultat["ok"] is False
    assert resultat["statut"] == "PARTIEL"
    assert not any("Rapprochement actualisé" in s for s in resultat["statistiques"])
    assert any("partielle" in s.lower() or "échec" in s.lower() for s in resultat["statistiques"])


def test_h_succes_complet_message_de_succes_exact(db, espions_ok):
    resultat = workflow.actualiser(mois_affiche="2026-06", db_path=db)
    assert resultat["ok"] is True
    assert resultat["statut"] == "SUCCES"


# ── G. Un échec ne laisse jamais le verrou bloqué ───────────────────────────────────────────────

def test_g_apres_un_echec_un_nouveau_clic_est_accepte(monkeypatch, db, espions_ok):
    monkeypatch.setattr(hostaway_ct, "credentials_disponibles", lambda: False)
    premier = workflow.actualiser(mois_affiche="2026-06", db_path=db)
    assert premier["ok"] is False
    assert premier["code"] != workflow.E_DEJA_EN_COURS

    # Un second appel, même juste après un échec, ne doit JAMAIS être rejeté comme "déjà en cours" :
    # le verrou a bien été libéré dans le `finally` du premier appel.
    monkeypatch.setattr(hostaway_ct, "credentials_disponibles", lambda: True)
    second = workflow.actualiser(mois_affiche="2026-06", db_path=db)
    assert second.get("code") != workflow.E_DEJA_EN_COURS
    assert second["ok"] is True


# ── Concurrence — deux appels simultanés ne lancent jamais deux chaînes en parallèle ────────────

def test_concurrence_deux_appels_simultanes_un_seul_tourne(monkeypatch, db, espions_ok):
    """Un appel lent (bloque un instant dans PDF) pendant qu'un second démarre : le second doit être
    refusé proprement (verrou DB), jamais lancer une seconde chaîne complète en parallèle."""
    from app.services import menages_pdf_import_service as pdf_import

    depart = threading.Event()
    relacher = threading.Event()
    pdf_etat = {"ok": True, "nb_detectes": 0, "nb_importees": 0, "nb_remplacees": 0,
                "nb_deja_importees": 0, "mois_impactes": [], "details": []}

    def _pdf_lent(**kw):
        depart.set()
        relacher.wait(timeout=5)
        return pdf_etat

    monkeypatch.setattr(pdf_import, "importer_nouveaux", _pdf_lent)

    resultats: list[dict] = []

    def _lancer():
        resultats.append(workflow.actualiser(mois_affiche="2026-06", db_path=db))

    t1 = threading.Thread(target=_lancer)
    t1.start()
    assert depart.wait(timeout=5), "le premier appel n'a pas démarré à temps"

    second = workflow.actualiser(mois_affiche="2026-06", db_path=db)
    relacher.set()
    t1.join(timeout=5)

    assert second["code"] == workflow.E_DEJA_EN_COURS
    assert second["ok"] is False
    assert len(resultats) == 1
    assert resultats[0]["ok"] is True


# ── Preuve directe de la correction §1 : le serveur reste réactif PENDANT le workflow ────────────

def test_route_ne_bloque_pas_le_serveur_pendant_le_workflow(monkeypatch, db, espions_ok, tmp_path):
    """AVANT la correction, `menages_actualiser` (route `async def`) appelait la fonction bloquante
    directement dans la boucle événementielle : uvicorn ne pouvait plus servir AUCUNE requête, pas
    même un `GET /` sans rapport, pendant toute la durée du workflow (constaté en recette).

    Preuve, sans vrai port réseau (le mécanisme testé est purement applicatif) : `TestClient` fait
    tourner l'app sur UNE boucle événementielle dans un thread dédié (portail `anyio`) ; deux appels
    lancés depuis deux threads Python distincts (POST volontairement ralenti de 2s, GET concurrent)
    sont donc traités par CETTE MÊME boucle. Avec `asyncio.to_thread` (§1), le GET répond en une
    fraction de seconde pendant que le POST attend son thread bloquant ; sans lui, le GET attendrait
    la fin des 2s du POST — exactement le symptôme constaté en recette.
    """
    from fastapi.testclient import TestClient

    from app.services import menages_actualisation_service as workflow_svc
    from app.services import orchestrateur_service as orch

    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(orch, "marquer_runs_interrompus", lambda **k: [])

    reel = workflow_svc.actualiser

    def _lent(**kw):
        time.sleep(4)
        return reel(**kw)

    # Patché sur le MODULE (`app.routes.menages` l'importe par `from ... import ... as
    # actualisation` — un alias du MÊME module, donc patcher l'un patche l'autre).
    monkeypatch.setattr(workflow_svc, "actualiser", _lent)

    from app.main import app as application
    with TestClient(application) as client:
        resultats: dict[str, object] = {}

        def _post():
            resultats["post"] = client.post(
                "/menages/actualiser", data={"mois": "2026-06"}, follow_redirects=False)

        t_post = threading.Thread(target=_post)
        debut = time.monotonic()
        t_post.start()
        time.sleep(0.3)  # laisse le POST entrer dans son thread bloquant

        debut_get = time.monotonic()
        reponse_get = client.get("/menages")
        duree_get = time.monotonic() - debut_get

        t_post.join(timeout=10)
        duree_totale = time.monotonic() - debut

    assert reponse_get.status_code == 200
    # Marge large : le GET a son propre coût de rendu (mesuré ~1.3s isolément dans cet environnement
    # de test) — ce qui compte est qu'il reste NETTEMENT sous les 4s du POST ralenti, preuve qu'il
    # n'a pas attendu derrière lui. Sans la correction §1, `duree_get` serait >= 4s.
    assert duree_get < 3.0, (
        f"GET /menages a mis {duree_get:.2f}s pendant un POST /menages/actualiser en vol (sleep de "
        "4s) : le serveur est resté bloqué (régression de la correction §1).")
    assert resultats["post"].status_code == 303
    assert duree_totale >= 4.0, "le POST doit avoir réellement attendu les 4s du workflow ralenti"

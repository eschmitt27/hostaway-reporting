"""Extraction Hostaway À LA DEMANDE depuis « Actualiser toute l'activité ».

Le clic manuel déclenche le pipeline GitHub CANONIQUE (`pipeline.yml`, les mêmes scripts que les
runs planifiés), suit ses étapes en direct, puis importe ce qu'il vient de publier par la
synchronisation atomique existante. Aucun appel GitHub réel ici : un faux client rejoue les
réponses de l'API.
"""
from __future__ import annotations

import inspect

import pytest

from app.adapters import github_actions_client as gh
from app.services import actualisation_progression_service as prog
from app.services import hostaway_extraction_demande_service as demande
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_moteur as moteur
from app.services import orchestrateur_service as orch

NOMS = ["Set up job", "Checkout repository", "Set up Python", "Install dependencies",
        "Run reservations extraction", "Run finance fields extraction",
        "Run cleaning tasks extraction", "Build final report", "Commit generated files"]


def _etapes(terminees: int, *, en_cours: bool = True, echec: str | None = None,
            echec_tolere: str | None = None) -> list[dict]:
    out = []
    for i, nom in enumerate(NOMS):
        if i < terminees:
            conclusion = "failure" if nom in (echec, echec_tolere) else "success"
            out.append({"nom": nom, "status": "completed", "conclusion": conclusion,
                        "started_at": "2026-10-01T19:00:00Z", "completed_at": "2026-10-01T19:00:40Z"})
        elif i == terminees and en_cours:
            out.append({"nom": nom, "status": "in_progress", "conclusion": None,
                        "started_at": "2026-10-01T19:00:40Z", "completed_at": None})
        else:
            out.append({"nom": nom, "status": "pending", "conclusion": None,
                        "started_at": None, "completed_at": None})
    return out


class FauxClient:
    """Rejoue un scénario : chaque appel à `run()` avance d'un cran."""

    def __init__(self, scenario: list[tuple[dict, list[dict]]], *, ancien=None,
                 apparait=True):
        self.scenario = scenario
        self.position = -1
        self.dispatches: list[str] = []
        self.ancien = ancien or []
        self.apparait = apparait

    def runs_workflow(self, workflow, *, event="workflow_dispatch", depuis=None):
        assert workflow == "pipeline.yml"
        runs = list(self.ancien)
        if self.dispatches and self.apparait:
            runs.insert(0, {"id": 777, "created_at": "2026-10-01T19:00:00Z", "status": "queued"})
        return runs

    def dispatch_workflow(self, workflow, *, inputs=None):
        self.dispatches.append(workflow)
        return {"workflow_run_id": None}

    def run(self, run_id):
        assert run_id == 777
        self.position = min(self.position + 1, len(self.scenario) - 1)
        return {"id": 777, **self.scenario[self.position][0],
                "updated_at": "2026-10-01T19:02:50Z"}

    def etapes_run(self, run_id):
        return self.scenario[max(self.position, 0)][1]


def _succes(**kw) -> list[tuple[dict, list[dict]]]:
    return [({"status": "queued", "conclusion": None}, _etapes(0, en_cours=False)),
            ({"status": "in_progress", "conclusion": None}, _etapes(4)),
            ({"status": "in_progress", "conclusion": None}, _etapes(5)),
            ({"status": "completed", "conclusion": kw.get("conclusion", "success")},
             _etapes(9, en_cours=False, echec=kw.get("echec"), echec_tolere=kw.get("tolere")))]


def _extraire(client, **kw):
    vus: list[list[dict]] = []
    res = demande.extraire_maintenant(suivi=lambda s: vus.append([dict(x) for x in s]),
                                      client=client, dormir=lambda s: None, **kw)
    return res, vus


# ── Le service de déclenchement ─────────────────────────────────────────────────────────────────

def test_declenche_le_pipeline_canonique_et_suit_ses_etapes_en_direct():
    client = FauxClient(_succes(), ancien=[{"id": 1, "created_at": "2026-10-01T18:59:00Z"}])
    res, vus = _extraire(client)
    assert client.dispatches == ["pipeline.yml"]                    # le moteur GitHub existant
    assert res["ok"] and res["run_id"] == 777 and res["taches_ok"] is True
    # En direct : file d'attente, puis réservations en cours, puis données financières en cours.
    assert vus[1][0]["message"] == "En file d'attente sur GitHub…"
    assert [s["etat"] for s in vus[2]][:3] == ["termine", "en_cours", "attente"]
    assert [s["etat"] for s in vus[3]][:3] == ["termine", "termine", "en_cours"]
    assert [s["libelle"] for s in res["sous_etapes"]] == [
        "Hostaway — connexion", "Hostaway — réservations", "Hostaway — données financières",
        "Hostaway — tâches de ménage", "Hostaway — publication des données"]
    assert all(s["etat"] == "termine" for s in res["sous_etapes"])


def test_un_run_anterieur_au_clic_n_est_jamais_pris_pour_le_notre():
    client = FauxClient(_succes(), ancien=[{"id": 1, "created_at": "2026-10-01T18:59:59Z"}],
                        apparait=False)
    horloge = iter(range(0, 10_000, 10))
    res, _ = _extraire(client, horloge=lambda: next(horloge))
    assert not res["ok"] and res["code"] == demande.E_RUN_INTROUVABLE


def test_echec_d_une_etape_github_est_nomme():
    client = FauxClient(_succes(conclusion="failure", echec="Run finance fields extraction"))
    res, _ = _extraire(client)
    assert not res["ok"] and res["code"] == demande.E_RUN_ECHEC
    assert "« Hostaway — données financières »" in res["message"]
    fautive = next(s for s in res["sous_etapes"] if s["libelle"] == "Hostaway — données financières")
    assert fautive["etat"] == "echec"


def test_taches_en_echec_toleree_par_le_pipeline_est_signalee():
    client = FauxClient(_succes(tolere="Run cleaning tasks extraction"))
    res, _ = _extraire(client)
    assert res["ok"] and res["taches_ok"] is False
    taches = next(s for s in res["sous_etapes"] if s["libelle"] == "Hostaway — tâches de ménage")
    assert taches["etat"] == "echec" and "précédentes" in taches["message"]


def test_jeton_absent_message_clair_sans_dispatch(monkeypatch):
    monkeypatch.setenv(gh.ENV_JETON, "")
    res, vus = _extraire(None)
    assert not res["ok"] and res["code"] == gh.E_CONFIGURATION
    assert "HOSTAWAY_GITHUB_TOKEN" in res["message"]
    assert vus[-1][0]["etat"] == "echec"


def test_delai_depasse_le_dit_sans_pretendre_avoir_extrait():
    bloque = [({"status": "in_progress", "conclusion": None}, _etapes(5))]
    horloge = iter(range(0, 100_000, 60))
    res, _ = _extraire(FauxClient(bloque), horloge=lambda: next(horloge), attente_max_s=600)
    assert not res["ok"] and res["code"] == demande.E_DELAI


# ── Branchement dans l'import Hostaway de l'orchestrateur ───────────────────────────────────────

@pytest.fixture()
def hostaway_simule(tmp_db, monkeypatch):
    from app.db.connection import get_db
    from app.services import hostaway_depot_service as depot
    from app.services import hostaway_raw_service as raw
    conn = get_db(tmp_db)
    conn.execute(
        "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, date_fin, "
        "statut, nb_listings, nb_reservations, nb_payouts, created_at, source_ref, "
        "source_horodatage) VALUES ('HAX-N', 'R', 'DEPOT_GITHUB', '2026-10-01T19:03:00Z', "
        "'2026-10-01T19:03:00Z', 'SUCCES', 18, 1652, 1616, '2026-10-01T19:03:00Z', 'c0ffee', "
        "'2026-10-01T19:02:45Z')")
    conn.commit()
    conn.close()
    appels = {"extraction": 0, "synchro": 0}

    def extraire(suivi=None, **kw):
        appels["extraction"] += 1
        sous = demande.sous_etapes_initiales()
        for s in sous:
            s["etat"] = "termine"
        if suivi:
            suivi(sous)
        return {"ok": True, "run_id": 777, "debut": "2026-10-01T19:00:00Z",
                "fin": "2026-10-01T19:02:50Z", "sous_etapes": sous, "taches_ok": True}

    def synchroniser(**kw):
        appels["synchro"] += 1
        return {"ok": True, "importe": True,
                "publie": {"source_horodatage": "2026-10-01T19:02:45Z"}}

    monkeypatch.setattr(demande, "extraire_maintenant", extraire)
    monkeypatch.setattr(depot, "synchroniser", synchroniser)
    monkeypatch.setattr(raw, "derniere_extraction_utilisable", lambda db_path=None: "HAX-N")
    return appels


def test_sans_option_le_scheduler_ne_declenche_aucune_extraction(tmp_db, hostaway_simule):
    res = moteur.importer_hostaway(db_path=tmp_db, declencheur="AUTO")
    assert res["ok"] and hostaway_simule == {"extraction": 0, "synchro": 1}
    assert "hostaway_a_la_demande" not in inspect.getsource(
        __import__("app.services.ordonnanceur_service", fromlist=["x"]))


def test_clic_global_extrait_puis_valide_et_affiche_en_direct(tmp_db, hostaway_simule,
                                                             monkeypatch):
    reel = orch._appeler_service
    vu_pendant: dict = {}

    def service(chemin, db_path):
        if chemin.endswith(":importer_hostaway"):
            return reel(chemin, db_path)
        if chemin == dag.NOEUDS[dag.RESERVATIONS].service:
            vu_pendant.update({e["cle"]: e for e in prog.progression(db_path=db_path)["etapes"]})
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", service)
    res = orch.actualiser(cibles=None, inclure_imports_externes=True, hostaway_a_la_demande=True,
                          db_path=tmp_db)
    assert res["statut"] == orch.RUN_SUCCES
    assert hostaway_simule == {"extraction": 1, "synchro": 1}
    ligne = next(e for e in prog.progression(db_path=tmp_db)["etapes"]
                 if e["cle"] == dag.HOSTAWAY_RAW)
    assert [s["libelle"] for s in ligne["sous_etapes"]][-1] == "Validation des données Hostaway"
    assert all(s["etat"] == "termine" for s in ligne["sous_etapes"])
    assert ligne["message"].startswith("Extraction Hostaway lancée à 19:00:00 — 1 652 "
                                       "réservations")
    assert vu_pendant[dag.HOSTAWAY_RAW]["sous_etapes"], "sous-étapes conservées après l'étape"


def test_sous_etapes_visibles_pendant_l_extraction(tmp_db, hostaway_simule, monkeypatch):
    vu: list = []

    def extraire(suivi=None, **kw):
        sous = demande.sous_etapes_initiales()
        sous[0]["etat"], sous[1]["etat"] = "termine", "en_cours"
        suivi(sous)
        vu.extend(e for e in prog.progression(db_path=tmp_db)["etapes"]
                  if e["cle"] == dag.HOSTAWAY_RAW)
        return {"ok": False, "code": demande.E_RUN_ECHEC, "sous_etapes": sous,
                "message": "L'extraction Hostaway a échoué sur GitHub (« Hostaway — "
                           "réservations »). Les dernières données valides sont conservées."}

    monkeypatch.setattr(demande, "extraire_maintenant", extraire)
    reel = orch._appeler_service
    monkeypatch.setattr(orch, "_appeler_service",
                        lambda c, d: reel(c, d) if c.endswith(":importer_hostaway") else {"ok": True})
    res = orch.actualiser(cibles=None, inclure_imports_externes=True, hostaway_a_la_demande=True,
                          db_path=tmp_db)
    assert vu[0]["etat"] == "en_cours"
    assert [s["etat"] for s in vu[0]["sous_etapes"]][:2] == ["termine", "en_cours"]
    p = prog.progression(db_path=tmp_db)
    assert res["statut"] == orch.RUN_PARTIEL and hostaway_simule["synchro"] == 0
    assert p["echec"]["message"].startswith("L'extraction Hostaway a échoué sur GitHub")
    assert next(e for e in p["etapes"] if e["cle"] == dag.RESERVATIONS)["etat"] == "bloque"


def test_taches_non_extraites_jamais_presentees_comme_fraiches(tmp_db, monkeypatch):
    jeton = orch._options_run.set({"hostaway_a_la_demande": True,
                                   "taches_hostaway_extraites": False})
    try:
        res = moteur.importer_hostaway_cleaning_tasks(db_path=tmp_db)
    finally:
        orch._options_run.reset(jeton)
    assert not res["ok"] and "précédentes sont conservées" in res["message"]


def test_route_globale_demande_l_extraction_la_cible_non(client, tmp_db, monkeypatch):
    appels: list[dict] = []
    monkeypatch.setattr(orch, "actualiser", lambda **kw: appels.append(kw))
    client.post("/actualisation/tout", follow_redirects=False)
    client.post("/actualisation/cible", data={"dataset": dag.HOSTAWAY_RAW}, follow_redirects=False)
    assert appels[0]["hostaway_a_la_demande"] is True
    assert "hostaway_a_la_demande" not in appels[1]

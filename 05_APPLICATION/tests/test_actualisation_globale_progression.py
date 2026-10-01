"""« Actualiser toute l'activité » réellement global + progression en direct (mission 2026-10-01).

Contrat prouvé ici :
  - toutes les sources configurées sont interrogées par leur service CANONIQUE (banque Qonto en
    lecture seule, Hostaway réservations/paiements/logements et tâches de ménage depuis le dépôt
    publié, déclarations et factures PDF de ménage) ; une source non configurée est dite telle,
    jamais présentée comme actualisée ;
  - tous les calculs du DAG sont rejoués, dans l'ordre des dépendances, même « à jour » ;
  - la progression est écrite étape par étape dans `moteur_run_etapes` (planifiée EN_ATTENTE,
    EN_COURS au démarrage, statut final) et relue par l'écran — aucun minuteur.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import actualisation_progression_service as prog
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_moteur as moteur
from app.services import orchestrateur_service as orch

SOURCES_GLOBALES = (dag.BANQUE_QONTO, dag.HOSTAWAY_RAW, dag.HOSTAWAY_CLEANING_TASKS,
                    dag.MENAGES_DECLARATIONS, dag.MENAGES_PDF)
CALCULS = tuple(n for n in dag.ordre_topologique()
                if dag.NOEUDS[n].type_noeud == dag.TYPE_CALCUL)


def _espion(monkeypatch, echecs: tuple[str, ...] = (), pendant=None) -> list[str]:
    """Remplace chaque service par un double qui réussit (ou échoue), en notant l'ordre d'appel."""
    appels: list[str] = []

    def faux(chemin, db_path):
        appels.append(chemin)
        if pendant:
            pendant(chemin, db_path)
        return {"ok": not any(e in chemin for e in echecs), "message": "panne simulée"}

    monkeypatch.setattr(orch, "_appeler_service", faux)
    return appels


def _fonction(dataset: str) -> str:
    return dag.NOEUDS[dataset].service.split(":")[1]


def _global(db, **kw):
    return orch.actualiser(cibles=None, inclure_imports_externes=True, db_path=db, **kw)


def _etapes(db, run_id) -> dict[str, dict]:
    return {e["etape"]: e for e in orch.etapes_run(run_id, db_path=db)}


# ── 1. Sauvegarde ───────────────────────────────────────────────────────────────────────────────

def test_01_clic_global_sauvegarde_d_abord(tmp_db, monkeypatch):
    _espion(monkeypatch)
    res = _global(tmp_db)
    etapes = orch.etapes_run(res["run_id"], db_path=tmp_db)
    assert etapes[0]["etape"] == orch.ETAPE_SAUVEGARDE and etapes[0]["statut"] == "SUCCES"
    assert res["sauvegarde_id"] and res["sauvegarde_id"] in (etapes[0]["detail"] or "")
    assert list(cfg.BACKUPS_DIR.glob("*")), "une vraie copie a été écrite (dans le dossier isolé)"


# ── 2-3. Banque ─────────────────────────────────────────────────────────────────────────────────

def test_02_banque_configuree_appelle_le_connecteur_qonto_canonique(tmp_db, monkeypatch):
    from app.adapters import qonto_client
    from app.services import qonto_ecran_service as ecran
    appels = []
    monkeypatch.setattr(qonto_client, "identifiants_presents", lambda env=None: True)
    monkeypatch.setattr(ecran, "actualiser",
                        lambda **kw: appels.append(kw) or {"ok": True, "vues": 13, "comptes": 1,
                                                           "creees": 2})
    res = moteur.importer_banque_qonto(db_path=tmp_db)
    assert appels == [{"db_path": tmp_db}] and res["ok"] and not res.get("non_configure")
    assert prog.volume(dag.BANQUE_QONTO, res) == "13 mouvements lus sur 1 compte, 2 nouveaux"


def test_03_banque_non_configuree_jamais_un_faux_succes(tmp_db, monkeypatch):
    from app.adapters import qonto_client
    from app.services import qonto_ecran_service as ecran
    monkeypatch.setattr(qonto_client, "identifiants_presents", lambda env=None: False)
    monkeypatch.setattr(ecran, "actualiser", lambda **kw: pytest.fail("aucun appel sans identifiants"))
    reel = orch._appeler_service

    def service(chemin, db_path):
        return reel(chemin, db_path) if "banque_qonto" in chemin else {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", service)
    res = _global(tmp_db)
    assert res["statut"] == orch.RUN_SUCCES      # ni échec, ni succès compté pour la banque
    banque = _etapes(tmp_db, res["run_id"])[dag.BANQUE_QONTO]
    assert banque["statut"] == orch.ETAPE_NON_CONFIGUREE and "QONTO_LOGIN" in banque["erreur"]
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.BANQUE_QONTO)
    assert etat["statut"] != orch.ST_A_JOUR and etat["calcule_le"] is None
    ligne = next(e for e in prog.progression(db_path=tmp_db)["etapes"]
                 if e["cle"] == dag.BANQUE_QONTO)
    assert (ligne["etat"], ligne["icone"], ligne["libelle"]) == ("non_configure", "⚠️",
                                                                 "Banque — Qonto")


# ── 4-9. Hostaway ───────────────────────────────────────────────────────────────────────────────

def test_04_a_08_toutes_les_sources_globales_sont_demandees(tmp_db, monkeypatch):
    appels = _espion(monkeypatch)
    _global(tmp_db)
    for source in SOURCES_GLOBALES:
        assert any(_fonction(source) in a for a in appels), f"{source} non interrogée"


def test_04_hostaway_passe_par_le_service_canonique_du_depot(tmp_db, monkeypatch):
    """Réservations, paiements (champs financiers) et logements arrivent par UNE extraction."""
    from app.services import hostaway_depot_service as depot
    from app.services import hostaway_raw_service as raw
    conn = get_db(tmp_db)
    conn.execute(
        "INSERT INTO hostaway_extractions (extraction_id, run_id, mode, date_debut, date_fin, "
        "statut, nb_listings, nb_reservations, nb_payouts, created_at, source_ref, "
        "source_horodatage) VALUES ('HAX-T', 'R', 'DEPOT_GITHUB', '2026-10-01T12:00:00Z', "
        "'2026-10-01T12:00:00Z', 'SUCCES', 18, 1651, 1610, '2026-10-01T12:00:00Z', 'abc', "
        "'2026-10-01T12:01:26Z')")
    conn.commit()
    conn.close()
    appels = []
    monkeypatch.setattr(depot, "synchroniser", lambda **kw: appels.append(kw) or {
        "ok": True, "importe": True, "publie": {"source_horodatage": "2026-10-01T12:01:26Z"}})
    monkeypatch.setattr(raw, "derniere_extraction_utilisable", lambda db_path=None: "HAX-T")
    res = moteur.importer_hostaway(db_path=tmp_db, declencheur="MANUEL")
    assert len(appels) == 1 and appels[0]["attendre"] is True
    texte = prog.volume(dag.HOSTAWAY_RAW, res, db_path=tmp_db)
    assert texte == ("1 651 réservations, 1 610 paiements, 18 logements — publication "
                     "du 01/10 à 12:01 importée")


def test_09_aucun_second_moteur_hostaway():
    services = {n: dag.NOEUDS[n].service for n in dag.NOEUDS if "HOSTAWAY" in n}
    assert services == {
        dag.HOSTAWAY_RAW: "app.services.orchestrateur_moteur:importer_hostaway",
        dag.HOSTAWAY_CLEANING_TASKS:
            "app.services.orchestrateur_moteur:importer_hostaway_cleaning_tasks"}
    import inspect
    source = inspect.getsource(moteur.importer_hostaway)
    assert "depot.synchroniser(" in source and "HostawayClient" not in source


def test_scheduler_inchange_ne_tire_ni_banque_ni_menages(tmp_db, monkeypatch):
    """Le scheduler cible HOSTAWAY_RAW : ses optimisations et son périmètre restent les siens."""
    appels = _espion(monkeypatch)
    orch.actualiser(cibles=[dag.HOSTAWAY_RAW], inclure_imports_externes=True, db_path=tmp_db)
    for source in (dag.BANQUE_QONTO, dag.HOSTAWAY_CLEANING_TASKS, dag.MENAGES_DECLARATIONS,
                   dag.MENAGES_PDF):
        assert not any(_fonction(source) in a for a in appels)


# ── 10-12. Recalcul complet, forcé, ordonné ─────────────────────────────────────────────────────

def test_10_11_12_tous_les_calculs_rejoues_meme_a_jour_et_dans_l_ordre(tmp_db, monkeypatch):
    for n in dag.NOEUDS:
        orch.marquer_dataset(n, orch.ST_A_JOUR, run_id="AVANT", db_path=tmp_db)
    appels = _espion(monkeypatch)
    res = _global(tmp_db)
    assert res["statut"] == orch.RUN_SUCCES
    rang = {}
    for n in dag.NOEUDS:
        if dag.NOEUDS[n].service:
            trouves = [i for i, a in enumerate(appels) if a == dag.NOEUDS[n].service]
            if n != dag.LOT13_EXPORT:
                assert trouves, f"{n} n'a pas été rejoué"
                rang[n] = trouves[0]
    for n, i in rang.items():
        for amont in dag.NOEUDS[n].depend_de:
            if amont in rang:
                assert rang[amont] < i, f"{amont} doit précéder {n}"
    assert not any(e["statut"] == "IGNOREE" and "inchangé" in e.get("motif", "")
                   for e in res["etapes"])


# ── 13-14. Progression enregistrée étape par étape, endpoint ───────────────────────────────────

def test_13_progression_reelle_pendant_le_run(tmp_db, monkeypatch):
    vu: dict = {}

    def pendant(chemin, db_path):
        if chemin == dag.NOEUDS[dag.LOT10].service:
            vu.update({e["cle"]: e["etat"] for e in prog.progression(db_path=db_path)["etapes"]})
            vu["_pourcentage"] = prog.progression(db_path=db_path)["pourcentage"]

    _espion(monkeypatch, pendant=pendant)
    _global(tmp_db)
    assert vu[dag.LOT10] == "en_cours"
    assert vu[dag.FLUX_LOT9] == "termine" and vu[orch.ETAPE_SAUVEGARDE] == "termine"
    assert vu[dag.LOT11] == "attente" and vu[dag.LOT12] == "attente"
    assert 0 < vu["_pourcentage"] < 100
    fin = prog.progression(db_path=tmp_db)
    assert fin["en_cours"] is False and fin["pourcentage"] == 100
    assert fin["run"]["titre"] == "Actualisation terminée" and fin["run"]["duree"]


def test_14_endpoint_progression(client, tmp_db, monkeypatch):
    _espion(monkeypatch)
    res = _global(tmp_db)
    donnees = client.get(f"/actualisation/progression?run_id={res['run_id']}").json()
    assert donnees["run"]["run_id"] == res["run_id"] and donnees["total"] == len(donnees["etapes"])
    libelles = [e["libelle"] for e in donnees["etapes"]]
    assert "Calcul des résultats" in libelles and "Hostaway — tâches de ménage" in libelles
    # Aucun code technique montré à l'utilisateur.
    assert not any(re.search(r"LOT\d|HOSTAWAY_|FLUX_|_RAW|\bH6\b", l) for l in libelles)


# ── 15-19. Écran ────────────────────────────────────────────────────────────────────────────────

def test_15_a_19_ecran_attente_en_cours_termine_echec_et_reprise(client, tmp_db):
    prep = orch.preparer_actualisation_globale(db_path=tmp_db)
    page = client.get("/actualisation").text                     # 19 : run retrouvé au chargement
    assert f'data-run="{prep["run_id"]}"' in page and 'data-en-cours="1"' in page
    assert "En attente" in page and "Actualisation en cours…" in page          # 15
    assert 'data-testid="bouton-actualiser-tout" disabled' in page
    debut = orch._maintenant()
    orch._etape(prep["run_id"], orch.ETAPE_SAUVEGARDE, 0, "SUCCES", debut, "", None, tmp_db)
    orch._etape_debut(prep["run_id"], dag.BANQUE_QONTO, tmp_db)
    page = client.get("/actualisation").text
    assert "En cours" in page and "Terminée" in page                           # 16, 17
    orch._etape(prep["run_id"], dag.BANQUE_QONTO, 2, "ECHEC", debut, "Qonto injoignable.", None,
                tmp_db)
    page = client.get("/actualisation").text
    assert "Échec lors de l&#39;étape « Banque — Qonto »" in page or \
        "Échec lors de l'étape « Banque — Qonto »" in page                    # 18
    assert 'act-etape--echec' in page and "Qonto injoignable." in page


def test_20_double_clic_un_seul_run(client, tmp_db, monkeypatch):
    lances = []
    monkeypatch.setattr(orch, "actualiser", lambda **kw: lances.append(kw))   # verrou gardé
    assert client.post("/actualisation/tout", follow_redirects=False).status_code == 303
    r = client.post("/actualisation/tout", follow_redirects=False)
    assert "message=" in r.headers["location"]
    assert len(lances) == 1 and lances[0]["run_id"]
    conn = get_db(tmp_db)
    n = conn.execute("SELECT COUNT(*) FROM moteur_runs WHERE lot = 'orchestrateur'").fetchone()[0]
    conn.close()
    assert n == 1, "le second clic ne crée aucun run fantôme"


# ── 21-22. Échecs ───────────────────────────────────────────────────────────────────────────────

def test_21_echec_hostaway_etape_rouge_et_aval_non_execute(tmp_db, monkeypatch):
    appels = _espion(monkeypatch, echecs=(":importer_hostaway",))
    res = _global(tmp_db)
    assert res["statut"] == orch.RUN_PARTIEL
    assert not any(dag.NOEUDS[n].service in appels for n in (dag.RESERVATIONS, dag.FLUX_LOT9,
                                                              dag.LOT10, dag.LOT12))
    p = prog.progression(db_path=tmp_db)
    etats = {e["cle"]: e for e in p["etapes"]}
    assert etats[dag.HOSTAWAY_RAW]["etat"] == "echec"
    assert etats[dag.RESERVATIONS]["etat"] == "bloque" and "Hostaway" in etats[dag.RESERVATIONS][
        "message"]
    assert etats[dag.LOT12]["etat"] == "bloque"
    assert p["echec"]["titre"] == ("Échec lors de l'étape « Hostaway — réservations, logements "
                                   "et paiements »")


def test_22_echec_banque_rouge_sans_bloquer_les_calculs(tmp_db, monkeypatch):
    """Comportement documenté : aucun calcul du DAG ne lit Qonto aujourd'hui ; son échec est
    affiché, le run est PARTIEL, la synchronisation précédente reste en place."""
    appels = _espion(monkeypatch, echecs=("importer_banque_qonto",))
    res = _global(tmp_db)
    assert res["statut"] == orch.RUN_PARTIEL
    assert dag.NOEUDS[dag.LOT12].service in appels
    assert prog.progression(db_path=tmp_db)["echec"]["libelle"] == "Banque — Qonto"


def test_echec_source_d_appoint_menages_ne_bloque_pas(tmp_db, monkeypatch):
    appels = _espion(monkeypatch, echecs=("importer_declarations_menages",))
    res = _global(tmp_db)
    assert res["statut"] == orch.RUN_PARTIEL and dag.NOEUDS[dag.MENAGES].service in appels


def test_run_interrompu_ne_laisse_aucune_etape_en_cours(tmp_db):
    prep = orch.preparer_actualisation_globale(db_path=tmp_db)
    orch._etape_debut(prep["run_id"], orch.ETAPE_SAUVEGARDE, tmp_db)
    orch.liberer_verrou(orch.PORTEE_GLOBALE, prep["run_id"], db_path=tmp_db)
    assert prep["run_id"] in orch.marquer_runs_interrompus(db_path=tmp_db)
    p = prog.progression(db_path=tmp_db)
    assert p["en_cours"] is False and p["compteurs"]["attente"] == p["compteurs"]["en_cours"] == 0


# ── 23-24. Pas de minuteur factice, base réelle protégée ────────────────────────────────────────

def test_23_aucune_progression_simulee():
    gabarit = (Path(__file__).resolve().parents[1] / "app" / "templates"
               / "actualisation.html").read_text(encoding="utf-8")
    script = gabarit[gabarit.index("<script>"):gabarit.index("</script>")]
    assert "fetch('/actualisation/progression" in script
    # Le pourcentage affiché vient du serveur, jamais d'un calcul fondé sur le temps.
    assert "p.pourcentage" in script and "Date.now" not in script and "performance.now" not in script


def test_24_base_reelle_jamais_touchee(tmp_db):
    assert "PilotageConciergerie" not in str(cfg.DB_PATH)
    assert str(cfg.DB_PATH) == str(tmp_db)

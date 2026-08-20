"""Orchestrateur — DAG, propagation, fraîcheur, atomicité, concurrence, reprise (§25-38).

Aucun de ces tests ne touche à une source réelle ni ne déclenche d'appel externe : les services de
calcul sont remplacés par des doubles quand il s'agit de vérifier le COMPORTEMENT de
l'orchestrateur, et les vrais services sont appelés quand c'est eux qu'on vérifie.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch


# ── DAG (§27/§28) ───────────────────────────────────────────────────────────

def test_dag_sans_cycle_et_ordonne():
    ordre = dag.ordre_topologique()
    assert len(ordre) == len(dag.NOEUDS)
    position = {nom: i for i, nom in enumerate(ordre)}
    for nom, noeud in dag.NOEUDS.items():
        for amont in noeud.depend_de:
            assert position[amont] < position[nom], f"{amont} doit précéder {nom}"


def test_dag_ne_contient_aucun_noeud_fichier():
    """§27 — le DAG raisonne en datasets : aucun nœud « MASTER_XLSX »."""
    for nom in dag.NOEUDS:
        assert "XLSX" not in nom.upper() and "MASTER" not in nom.upper()


def test_chaine_reelle_respecte_les_dependances_attendues():
    """La chaîne économique attendue est bien celle décrite par le DAG."""
    aval_menages = dag.descendants(dag.MENAGES)
    for attendu in (dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12):
        assert attendu in aval_menages
    assert dag.LOT10 in dag.descendants(dag.FLUX_LOT9)
    assert dag.FLUX_LOT9 in dag.ascendants(dag.LOT12)


# ── Fraîcheur (§33) ─────────────────────────────────────────────────────────

def test_dataset_jamais_calcule_nest_jamais_a_jour(tmp_db):
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert set(etats.values()) == {orch.ST_JAMAIS}


def test_fraicheur_vient_du_run_pas_du_fichier(tmp_db):
    orch.marquer_dataset(dag.FLUX_LOT9, orch.ST_A_JOUR, run_id="R1", nb_lignes=10, db_path=tmp_db)
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.FLUX_LOT9)
    assert etat["statut"] == orch.ST_A_JOUR
    assert etat["source_run_id"] == "R1"
    assert etat["calcule_le"]


# ── Invalidation des descendants (§34) ──────────────────────────────────────

def test_invalidation_descendants(tmp_db):
    for d in (dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12):
        orch.marquer_dataset(d, orch.ST_A_JOUR, run_id="R1", db_path=tmp_db)

    touches = orch.invalider_descendants(dag.FLUX_LOT9, db_path=tmp_db)
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert dag.LOT10 in touches and dag.LOT11 in touches and dag.LOT12 in touches
    assert etats[dag.LOT10] == orch.ST_A_RECALCULER
    assert etats[dag.LOT12] == orch.ST_A_RECALCULER
    # L'amont recalculé reste à jour : seule l'aval est invalidée.
    assert etats[dag.FLUX_LOT9] == orch.ST_A_JOUR


def test_un_lot10_calcule_avant_lot9_nest_pas_affiche_a_jour(tmp_db):
    """§34 — le cas précis que la mission demande d'empêcher."""
    orch.marquer_dataset(dag.LOT10, orch.ST_A_JOUR, run_id="ANCIEN", db_path=tmp_db)
    orch.marquer_dataset(dag.FLUX_LOT9, orch.ST_A_JOUR, run_id="NOUVEAU", db_path=tmp_db)
    orch.invalider_descendants(dag.FLUX_LOT9, db_path=tmp_db)
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert etats[dag.LOT10] == orch.ST_A_RECALCULER


def test_exports_jamais_invalides_automatiquement(tmp_db):
    """Un export est reconstructible : il n'est pas « périmé », il est régénérable."""
    orch.marquer_dataset(dag.LOT13_EXPORT, orch.ST_A_JOUR, run_id="R1", db_path=tmp_db)
    orch.invalider_descendants(dag.LOT10, db_path=tmp_db)
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert etats[dag.LOT13_EXPORT] == orch.ST_A_JOUR


# ── Exécution ciblée (§29) ──────────────────────────────────────────────────

def test_actualisation_ciblee_traite_la_cible_et_ses_descendants(tmp_db, monkeypatch):
    appels: list[str] = []

    def faux_service(chemin, db_path):
        appels.append(chemin)
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", faux_service)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    traites = [e["dataset"] for e in res["etapes"]]
    assert traites == [dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12]
    assert res["statut"] == orch.RUN_SUCCES


def test_actualisation_ciblee_dataset_inconnu_refuse(tmp_db):
    res = orch.actualiser(cibles=["N_EXISTE_PAS"], db_path=tmp_db)
    assert res["ok"] is False and res["code"] == orch.E_DATASET_INCONNU


# ── Atomicité et PARTIEL (§30.5/§35) ────────────────────────────────────────

def test_echec_intermediaire_rend_le_run_partiel_sans_corrompre_lamont(tmp_db, monkeypatch):
    def faux_service(chemin, db_path):
        if "lot12" in chemin:
            return {"ok": False, "code": "PANNE", "message": "panne simulée"}
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", faux_service)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    assert res["statut"] == orch.RUN_PARTIEL
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert etats[dag.FLUX_LOT9] == orch.ST_A_JOUR      # l'amont réussi reste valide
    assert etats[dag.LOT12] == orch.ST_ECHEC


def test_dataset_dont_lamont_a_echoue_nest_pas_calcule(tmp_db, monkeypatch):
    """§35 — jamais de calcul sur une entrée périmée présentée comme fraîche."""
    def faux_service(chemin, db_path):
        if "lot10" in chemin or "orchestrateur_moteur" in chemin:
            return {"ok": False, "code": "PANNE", "message": "Lot10 en panne"}
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", faux_service)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    par_dataset = {e["dataset"]: e for e in res["etapes"]}
    assert par_dataset[dag.LOT10]["statut"] == "ECHEC"
    assert par_dataset[dag.LOT11]["statut"] == "IGNOREE"
    assert "Amont" in par_dataset[dag.LOT11]["motif"]


def test_erreur_journalisee_avec_dataset_code_et_message(tmp_db, monkeypatch):
    """§38 — jamais un simple « erreur de calcul »."""
    monkeypatch.setattr(orch, "_appeler_service",
                        lambda chemin, db_path: {"ok": False, "code": "E_TEST",
                                                 "message": "cause précise"})
    orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.FLUX_LOT9)
    assert etat["erreur_code"] == "E_TEST" and "cause précise" in etat["erreur_message"]
    run = orch.dernier_run(db_path=tmp_db)
    etapes = orch.etapes_run(run["run_id"], db_path=tmp_db)
    assert any(e["erreur"] and "cause précise" in e["erreur"] for e in etapes)


def test_exception_devient_un_etat_lisible(tmp_db, monkeypatch):
    def service_qui_leve(chemin, db_path):
        raise RuntimeError("boum")

    monkeypatch.setattr(orch, "_appeler_service", service_qui_leve)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    assert res["statut"] in (orch.RUN_ECHEC, orch.RUN_PARTIEL)
    etat = next(d for d in orch.etat_datasets(tmp_db) if d["dataset"] == dag.FLUX_LOT9)
    assert etat["erreur_code"] == "RuntimeError" and "boum" in etat["erreur_message"]


# ── Concurrence (§36) ───────────────────────────────────────────────────────

def test_verrou_empeche_deux_runs_du_meme_pipeline(tmp_db):
    pris = orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-A", db_path=tmp_db)
    assert pris["ok"]
    refuse = orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-B", db_path=tmp_db)
    assert refuse["ok"] is False and refuse["code"] == orch.E_VERROU


def test_verrou_libere_autorise_un_nouveau_run(tmp_db):
    orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-A", db_path=tmp_db)
    orch.liberer_verrou(orch.PORTEE_GLOBALE, "RUN-A", db_path=tmp_db)
    assert orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-B", db_path=tmp_db)["ok"]


def test_bail_expire_est_repris(tmp_db):
    """Un processus tué ne bloque pas le pipeline pour toujours."""
    orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-MORT", bail_s=-1, db_path=tmp_db)
    assert orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-VIVANT", db_path=tmp_db)["ok"]


def test_portees_differentes_ne_se_bloquent_pas(tmp_db):
    """Deux actualisations réellement indépendantes ne doivent pas s'attendre."""
    assert orch.prendre_verrou(dag.MENAGES, "RUN-A", db_path=tmp_db)["ok"]
    assert orch.prendre_verrou(dag.BANQUE, "RUN-B", db_path=tmp_db)["ok"]


def test_actualisation_refusee_si_verrou_global_pris(tmp_db):
    orch.prendre_verrou(orch.PORTEE_GLOBALE, "RUN-AUTRE", db_path=tmp_db)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    assert res["ok"] is False and res["code"] == orch.E_VERROU


# ── Reprise après crash (§37) ───────────────────────────────────────────────

def test_run_sans_verrou_actif_devient_interrompu(tmp_db):
    from app.db.connection import get_db

    conn = get_db(tmp_db)
    try:
        conn.execute(
            "INSERT INTO moteur_runs (run_id, lot, started_at, statut, declencheur) "
            "VALUES ('ORCH-CRASH', ?, '2099-01-01T00:00:00Z', 'EN_COURS', 'MANUEL')",
            (orch.LOT_ORCHESTRATEUR,))
        conn.commit()
    finally:
        conn.close()

    orch.marquer_dataset(dag.LOT10, orch.ST_EN_COURS, run_id="ORCH-CRASH", db_path=tmp_db)
    interrompus = orch.marquer_runs_interrompus(db_path=tmp_db)
    assert "ORCH-CRASH" in interrompus

    run = orch.dernier_run(db_path=tmp_db)
    assert run["statut"] == orch.RUN_INTERROMPU
    # Le dataset laissé en cours n'est jamais présenté comme à jour.
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert etats[dag.LOT10] == orch.ST_A_RECALCULER


def test_un_nouveau_run_repart_apres_interruption(tmp_db, monkeypatch):
    monkeypatch.setattr(orch, "_appeler_service", lambda chemin, db_path: {"ok": True})
    orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    orch.marquer_runs_interrompus(db_path=tmp_db)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    assert res["statut"] == orch.RUN_SUCCES


# ── Imports externes non déclenchés par défaut (§44) ────────────────────────

def test_import_externe_non_declenche_par_actualisation_globale(tmp_db, monkeypatch):
    appels: list[str] = []
    monkeypatch.setattr(orch, "_appeler_service",
                        lambda chemin, db_path: appels.append(chemin) or {"ok": True})
    res = orch.actualiser(db_path=tmp_db)
    hostaway = next(e for e in res["etapes"] if e["dataset"] == dag.HOSTAWAY_RAW)
    assert hostaway["statut"] == "IGNOREE"
    assert not any("hostaway" in a.lower() for a in appels)


def test_import_externe_declenche_si_demande_explicitement(tmp_db, monkeypatch):
    appels: list[str] = []
    monkeypatch.setattr(orch, "_appeler_service",
                        lambda chemin, db_path: appels.append(chemin) or {"ok": True})
    orch.actualiser(cibles=[dag.HOSTAWAY_RAW], inclure_imports_externes=True, db_path=tmp_db)
    assert any("hostaway" in a.lower() for a in appels)

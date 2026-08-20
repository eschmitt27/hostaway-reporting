"""Écran Pilotage / Actualisation (§31/§32) et démarrage sur base neuve (§46).

L'utilisateur doit pouvoir lancer une actualisation sans terminal, voir l'état de chaque dataset et
les erreurs — et l'écran doit rester lisible même quand rien n'a jamais été calculé.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch


def test_ecran_repond_sur_base_neuve(client, tmp_db):
    """§46 — aucune donnée, aucun master : l'écran s'affiche, il ne plante pas."""
    r = client.get("/actualisation")
    assert r.status_code == 200
    assert "Actualiser toute l'activité" in r.text


def test_ecran_liste_tous_les_datasets_du_dag(client, tmp_db):
    r = client.get("/actualisation")
    for nom in dag.NOEUDS:
        assert nom in r.text, f"{nom} absent de l'écran"


def test_ecran_affiche_le_vocabulaire_de_statut(client, tmp_db):
    """§32 — les statuts affichés sont ceux du vocabulaire existant."""
    orch.marquer_dataset(dag.FLUX_LOT9, orch.ST_A_JOUR, run_id="R1", db_path=tmp_db)
    orch.marquer_dataset(dag.LOT10, orch.ST_A_RECALCULER, run_id="R1", db_path=tmp_db)
    orch.marquer_dataset(dag.LOT11, orch.ST_ECHEC, run_id="R1", erreur_code="E_X",
                         erreur_message="cause lisible", db_path=tmp_db)
    r = client.get("/actualisation")
    assert "À jour" in r.text and "À recalculer" in r.text and "Échec" in r.text
    # L'erreur est affichée, pas masquée derrière un statut générique (§38).
    assert "cause lisible" in r.text


def test_etat_json_expose_datasets_et_run(client, tmp_db):
    orch.marquer_dataset(dag.FLUX_LOT9, orch.ST_A_JOUR, run_id="R1", db_path=tmp_db)
    donnees = client.get("/actualisation/etat").json()
    assert "datasets" in donnees and "dernier_run" in donnees
    flux = next(d for d in donnees["datasets"] if d["dataset"] == dag.FLUX_LOT9)
    assert flux["statut"] == orch.ST_A_JOUR


def test_bouton_tout_actualiser_lance_sans_bloquer(client, tmp_db, monkeypatch):
    """L'action rend la main immédiatement : un recalcul dure des minutes."""
    appels: list[dict] = []
    monkeypatch.setattr(orch, "actualiser",
                        lambda **kw: appels.append(kw) or {"ok": True, "run_id": "R"})
    r = client.post("/actualisation/tout", follow_redirects=False)
    assert r.status_code == 303
    assert appels and appels[0]["cibles"] is None


def test_action_ciblee_transmet_la_cible(client, tmp_db, monkeypatch):
    appels: list[dict] = []
    monkeypatch.setattr(orch, "actualiser",
                        lambda **kw: appels.append(kw) or {"ok": True, "run_id": "R"})
    r = client.post("/actualisation/cible", data={"dataset": dag.LOT11},
                    follow_redirects=False)
    assert r.status_code == 303
    assert appels[0]["cibles"] == [dag.LOT11]
    # Une chaîne interne ne déclenche jamais un appel externe.
    assert appels[0]["inclure_imports_externes"] is False


def test_action_ciblee_source_externe_autorise_l_import(client, tmp_db, monkeypatch):
    appels: list[dict] = []
    monkeypatch.setattr(orch, "actualiser",
                        lambda **kw: appels.append(kw) or {"ok": True, "run_id": "R"})
    client.post("/actualisation/cible", data={"dataset": dag.HOSTAWAY_RAW},
                follow_redirects=False)
    assert appels[0]["inclure_imports_externes"] is True


def test_dataset_inconnu_refuse_sans_erreur_500(client, tmp_db):
    r = client.post("/actualisation/cible", data={"dataset": "N_EXISTE_PAS"},
                    follow_redirects=False)
    assert r.status_code == 303


def test_ecran_reste_lisible_sur_base_non_migree(tmp_path, monkeypatch):
    """La base réelle est en 0016 : l'écran doit dire « jamais calculé », jamais planter."""
    from fastapi.testclient import TestClient

    import sqlite3

    base = tmp_path / "ancienne.db"
    sqlite3.connect(str(base)).close()   # base vide, aucune table
    monkeypatch.setattr(cfg, "DB_PATH", base)

    from app.main import app
    with TestClient(app) as c:
        r = c.get("/actualisation")
    assert r.status_code == 200
    assert "Jamais calculé" in r.text

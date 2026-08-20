"""§46 — démarrage sur environnement neuf : aucun master calculé préexistant n'est nécessaire.

Le pipeline doit pouvoir partir d'une base SQLite vide et de sources fournies par l'utilisateur.
C'est la preuve que le système ne dépend plus d'un historique de classeurs pour s'amorcer : un
MASTER_* préexistant n'est plus une condition de démarrage, seulement un artefact historique.

Aucun classeur n'est ouvert ici : l'interception `openpyxl.load_workbook` échoue immédiatement si
un chemin de code tentait d'en lire un pour s'amorcer.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch


@pytest.fixture
def base_neuve(tmp_path):
    """Une base migrée, totalement vide : ni réservations, ni flux, ni résultats, ni référentiel."""
    db = tmp_path / "neuve.db"
    apply_migrations(db)
    return db


@pytest.fixture(autouse=True)
def _aucun_classeur(monkeypatch):
    import openpyxl

    def garde(chemin, *a, **kw):
        raise AssertionError(
            f"Amorçage sur base neuve : aucun classeur ne doit être ouvert ({chemin}).")

    monkeypatch.setattr(openpyxl, "load_workbook", garde)


def test_migrations_suffisent_a_creer_le_socle(base_neuve):
    conn = get_db(base_neuve)
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    # Les tables des chaînes migrées existent sans qu'aucun classeur n'ait été lu.
    for attendue in ("flux_unifies", "lot10_runs", "controles_lot11_constats",
                     "lot12_prefactures_entete", "orchestrateur_datasets"):
        assert attendue in tables


def test_etat_initial_est_jamais_calcule(base_neuve):
    etats = {d["dataset"]: d["statut"] for d in orch.etat_datasets(base_neuve)}
    assert set(etats.values()) == {orch.ST_JAMAIS}


def test_chaines_sqlite_sexecutent_sur_base_vide(base_neuve):
    """Lot9, Lot11 et Lot12 doivent aboutir sur une base vide : 0 ligne, jamais une exception.

    C'est la différence entre « pas de données » et « pipeline cassé » : un système qui ne démarre
    qu'avec un historique n'est pas amorçable.
    """
    from app.services import controles_lot11_service as lot11
    from app.services import flux_unifie_service as lot9
    from app.services import lot12_prefactures_service as lot12

    assert lot9.construire(db_path=base_neuve)["ok"]
    assert lot11.construire(db_path=base_neuve)["ok"]
    assert lot12.construire(db_path=base_neuve)["ok"]


def test_actualisation_globale_sur_base_neuve_ne_plante_pas(base_neuve, monkeypatch):
    """L'orchestrateur traverse tout le DAG sans master préexistant.

    Lot10 (moteur pandas) est remplacé ici : ce test vérifie l'AMORÇAGE, pas l'exécution d'un
    sous-processus — celle-ci est couverte par la parité réelle de Lot10.
    """
    vrai_appel = orch._appeler_service

    def appel(chemin, db_path):
        if "orchestrateur_moteur" in chemin:
            return {"ok": True, "simule": True}
        return vrai_appel(chemin, db_path)

    monkeypatch.setattr(orch, "_appeler_service", appel)
    res = orch.actualiser(db_path=base_neuve)
    assert res["statut"] in (orch.RUN_SUCCES, orch.RUN_PARTIEL), res
    etats = {d["dataset"]: d["statut"] for d in res["datasets"]}
    for calcule in (dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12):
        assert etats[calcule] == orch.ST_A_JOUR, (calcule, etats[calcule])


def test_aucun_master_calcule_nest_requis_pour_amorcer(base_neuve, monkeypatch):
    """Reformulation directe de §46 : le bootstrap ne lit aucun MASTER_* calculé.

    Garanti par le fixture `_aucun_classeur` : si un chemin d'amorçage ouvrait un classeur, le test
    échouerait avant d'arriver ici.
    """
    from app.services import flux_unifie_service as lot9

    resultat = lot9.construire(db_path=base_neuve)
    assert resultat["ok"]
    assert lot9.lire(db_path=base_neuve) == []

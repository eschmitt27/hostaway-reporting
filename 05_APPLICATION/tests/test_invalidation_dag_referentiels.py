"""Mission 6 ter §24/§25 — une modification d'un référentiel temporel invalide réellement les
datasets aval (DAG existant, `orchestrateur_dag`/`orchestrateur_service`, aucune deuxième carte),
mais ne déclenche JAMAIS elle-même un recalcul : la route d'administration marque les descendants
`A_RECALCULER`, l'utilisateur relance ensuite explicitement (`orchestrateur_service.actualiser` /
« Actualiser maintenant »), jamais l'inverse.

`referentiel_admin_service.invalider_dag_referentiel()` appelle `orchestrateur_service.
invalider_descendants(orchestrateur_dag.REF_SETUP)` — le nœud DAG unique qui porte déjà tous les
référentiels (`ref_logements`, `ref_taux_commission`, `ref_couts_standards_menage`,
`ref_canape_parametres`, `ref_regles_versions`), pas une liste indépendante.
"""
import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import canape_gestion_service as canape
from app.services import couts_menage_gestion_service as cm
from app.services import logements_gestion_service as logs
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch
from app.services import regle_version_gestion_service as regv


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _marquer_tout_a_jour(db_path):
    """Simule un état post-actualisation normal : les datasets aval du référentiel sont A_JOUR
    avant la modification testée."""
    for nom in dag.descendants(dag.REF_SETUP):
        if dag.NOEUDS[nom].type_noeud == dag.TYPE_EXPORT:
            continue
        orch.marquer_dataset(nom, orch.ST_A_JOUR, db_path=db_path)


def _statuts(db_path):
    return {e["dataset"]: e["statut"] for e in orch.etat_datasets(db_path=db_path)}


def test_changement_taux_commission_invalide_les_datasets_aval(ref):
    _marquer_tout_a_jour(ref)
    res = logs.changer_taux_commission("LOG_A1", 0.15, "2026-07-01", db_path=ref)
    assert res["ok"], res

    statuts = _statuts(ref)
    for nom in dag.descendants(dag.REF_SETUP):
        if dag.NOEUDS[nom].type_noeud == dag.TYPE_EXPORT:
            continue
        assert statuts[nom] == orch.ST_A_RECALCULER, f"{nom} pas invalidé : {statuts[nom]}"


def test_export_optionnel_nest_jamais_invalide(ref):
    """LOT13_EXPORT est reconstructible à la demande : il ne doit jamais être marqué obsolète par
    la propagation automatique d'une modification référentielle."""
    _marquer_tout_a_jour(ref)
    orch.marquer_dataset(dag.LOT13_EXPORT, orch.ST_A_JOUR, db_path=ref)
    logs.changer_taux_commission("LOG_A1", 0.15, "2026-07-01", db_path=ref)
    statuts = _statuts(ref)
    assert statuts[dag.LOT13_EXPORT] == orch.ST_A_JOUR


def test_invalidation_ne_recalcule_rien(ref, monkeypatch):
    """Aucun appel à l'orchestrateur d'exécution (`actualiser`) ou à un sous-processus lot10/11/12
    ne doit se produire lors d'une modification référentielle — seule la marque `A_RECALCULER` est
    écrite. On le prouve en s'assurant qu'aucun `moteur_runs`/`lot10_runs` n'apparaît après coup."""
    _marquer_tout_a_jour(ref)
    from app.db.connection import get_db
    conn = get_db(ref)
    try:
        avant_lot10 = conn.execute("SELECT COUNT(*) FROM lot10_runs").fetchone()[0]
    finally:
        conn.close()

    logs.changer_taux_commission("LOG_A1", 0.15, "2026-07-01", db_path=ref)

    conn = get_db(ref)
    try:
        apres_lot10 = conn.execute("SELECT COUNT(*) FROM lot10_runs").fetchone()[0]
    finally:
        conn.close()
    assert apres_lot10 == avant_lot10   # aucun recalcul déclenché


def test_changement_cout_menage_invalide_aussi(ref):
    _marquer_tout_a_jour(ref)
    res = cm.changer_cout("TYPE_001", 55, "2026-07-01", db_path=ref)
    assert res["ok"], res
    statuts = _statuts(ref)
    assert statuts[dag.LOT10] == orch.ST_A_RECALCULER


def test_changement_canape_invalide_aussi(ref):
    _marquer_tout_a_jour(ref)
    res = canape.changer_parametres("LOG_A1", 5, 30, "2026-07-01", db_path=ref)
    assert res["ok"], res
    statuts = _statuts(ref)
    assert statuts[dag.LOT10] == orch.ST_A_RECALCULER


def test_changement_version_regle_invalide_aussi(ref):
    _marquer_tout_a_jour(ref)
    res = regv.changer_version("ASSIETTE_COMMISSION", "V2_TEST", "2027-01-01", db_path=ref)
    assert res["ok"], res
    statuts = _statuts(ref)
    assert statuts[dag.LOT10] == orch.ST_A_RECALCULER

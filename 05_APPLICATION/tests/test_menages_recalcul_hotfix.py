"""HOTFIX-1 — le 500 de /menages/recalculer/confirmer ne doit plus se produire.

Cause historique : `_construire_workspace` copiait les scripts moteur (lot6d/lot6e) sans vérifier
leur présence -> FileNotFoundError non capturée -> 500. Deux garde-fous : préflight des scripts +
garde d'exception générale. Le parcours rend TOUJOURS une réponse lisible (jamais un traceback).

Precondition désormais DATASET SQLite, pas fichier Excel (`menages_recalcul_service` SQLite-first,
`DATASETS_REQUIS`) — les fixtures seedent `menages_taches_enrichies`/`menages_declarations_internes`
en base plutôt que d'écrire des classeurs.
"""
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import menages_recalcul_service as rc


def _seed_datasets(db_path, mois="2026-05"):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage) VALUES (?,?,?,?,?,?)",
            ("HA-HOTFIX-001", mois, "LOG_SEED", "completed", "réalisé", "OUI"))
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle) VALUES (?,?,?,?,?)",
            (mois, "LOG_SEED", "INT_SEED", 1, "VALIDE"))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def projet_sans_scripts(tmp_path, monkeypatch):
    """Dataset SQLite présent mais scripts moteur ABSENTS (cas d'un PROJECT_ROOT de démonstration)."""
    root = tmp_path / "projet"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cfg, "PROJECT_ROOT", root)
    monkeypatch.setattr(cfg, "MENAGES_RECALC_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    return root


def test_confirmer_scripts_absents_echec_lisible(projet_sans_scripts, tmp_db):
    """Scripts moteur absents : ECHEC tracé lisible, AUCUNE exception (plus de 500)."""
    _seed_datasets(tmp_db)
    res = rc.confirmer(rc.MODE_COPIES, mois="2026-05", db_path=tmp_db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == "E_SCRIPT_MOTEUR_ABSENT"
    assert res["run_id"] is not None
    assert "rien n'a été touché" in res["message"].lower()


def test_route_confirmer_pas_de_500(projet_sans_scripts, client, tmp_db, monkeypatch):
    """La route POST /menages/recalculer/confirmer ne renvoie jamais 500 (303 ou 200)."""
    # le client (fixture) migre sa propre base ; confirmer lit cfg.DB_PATH à chaud
    _seed_datasets(tmp_db)
    r = client.post("/menages/recalculer/confirmer",
                    data={"mode": "COPIES", "mois": "2026-05"}, follow_redirects=False)
    assert r.status_code in (303, 200)
    assert r.status_code != 500


def test_confirmer_garde_exception_inattendue(projet_sans_scripts, tmp_db, monkeypatch):
    """Toute erreur inattendue dans le corps -> run ECHEC E_INATTENDU, jamais une exception propagée."""
    _seed_datasets(tmp_db)
    # les scripts sont présents mais on force une panne au moment du snapshot
    for rel in rc.SCRIPTS_A_COPIER:
        f = Path(cfg.PROJECT_ROOT) / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"SCRIPT")
    from app.services import snapshot_service

    def boom(*a, **k):
        raise RuntimeError("panne simulée snapshot")
    monkeypatch.setattr(snapshot_service, "create_snapshot", boom)
    res = rc.confirmer(rc.MODE_COPIES, mois="2026-05", db_path=tmp_db)
    assert res["statut"] == rc.STATUT_ECHEC
    assert res["erreur_code"] == "E_INATTENDU"
    assert "RuntimeError" in (res.get("message") or "") or res["run_id"] is not None

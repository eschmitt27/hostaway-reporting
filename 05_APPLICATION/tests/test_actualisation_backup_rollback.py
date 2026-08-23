"""Orchestrateur — sauvegarde obligatoire, rollback automatique, dry-run (mission industrialisation
orchestrateur global).

Aucune écriture réelle : `tmp_db` isole `cfg.DB_PATH`/`cfg.BACKUPS_DIR` (conftest.py).
"""
from __future__ import annotations

from app.services import backup_service
from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch
from app.services import run_history_service as history


def _appeler_service_ok(chemin, db_path):
    return {"ok": True}


def _appeler_service_echec(chemin, db_path):
    return {"ok": False, "code": "E_TEST", "message": "échec simulé"}


# ── Phase 4 — sauvegarde obligatoire sur actualisation globale ───────────────

def test_actualisation_globale_prend_une_sauvegarde(tmp_db, monkeypatch):
    monkeypatch.setattr(orch, "_appeler_service", _appeler_service_ok)
    res = orch.actualiser(db_path=tmp_db)  # cibles=None → global
    assert res["sauvegarde_id"] is not None
    sauvegardes = backup_service.lister(db_path=tmp_db)
    assert any(s["sauvegarde_id_opaque"] == res["sauvegarde_id"] for s in sauvegardes)
    assert res["history_run_id"] is not None


def test_actualisation_ciblee_ne_prend_pas_de_sauvegarde(tmp_db, monkeypatch):
    """Coût disproportionné sur une cible unique — la mission ne le demande que pour le global."""
    monkeypatch.setattr(orch, "_appeler_service", _appeler_service_ok)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    assert res["sauvegarde_id"] is None
    assert res["history_run_id"] is None


def test_actualisation_globale_journalise_dans_run_history(tmp_db, monkeypatch):
    monkeypatch.setattr(orch, "_appeler_service", _appeler_service_ok)
    res = orch.actualiser(db_path=tmp_db)
    entree = next(r for r in history.derniers(db_path=tmp_db)
                  if r["run_id_opaque"] == res["history_run_id"])
    assert entree["statut"] == "SUCCESS"
    assert entree["sauvegarde_id_opaque"] == res["sauvegarde_id"]


# ── Phase 4 — rollback automatique si l'intégrité échoue après le run ────────

def test_integrite_echouee_apres_run_declenche_rollback_automatique(tmp_db, monkeypatch):
    monkeypatch.setattr(orch, "_appeler_service", _appeler_service_ok)
    # Simule une base corrompue APRÈS le run (la seule panne qu'aucun état de dataset ne peut
    # représenter honnêtement) — sans corrompre réellement le fichier, pour ne pas casser la
    # restauration elle-même dans ce test.
    monkeypatch.setattr(orch, "_integrity_ok", lambda db_path: False)
    restaurer_appels = []
    vrai_restaurer = backup_service.restaurer

    def restaurer_espion(sauvegarde_id, **kwargs):
        restaurer_appels.append(sauvegarde_id)
        return vrai_restaurer(sauvegarde_id, **kwargs)

    monkeypatch.setattr(backup_service, "restaurer", restaurer_espion)

    res = orch.actualiser(db_path=tmp_db)
    assert restaurer_appels == [res["sauvegarde_id"]]
    assert res["rollback"] is not None and res["rollback"]["ok"]
    entree = next(r for r in history.derniers(db_path=tmp_db)
                  if r["run_id_opaque"] == res["history_run_id"])
    assert entree["statut"] == "ROLLED_BACK"


def test_echec_partiel_ne_declenche_pas_de_rollback(tmp_db, monkeypatch):
    """Un dataset en échec (PARTIEL) n'est pas une panne critique : pas de restauration, les
    données précédentes valides restent en place par construction."""
    def service_mixte(chemin, db_path):
        if "flux_unifie" in chemin:
            return {"ok": False, "code": "E_TEST", "message": "échec simulé"}
        return {"ok": True}

    monkeypatch.setattr(orch, "_appeler_service", service_mixte)
    res = orch.actualiser(db_path=tmp_db)
    assert res["statut"] == orch.RUN_PARTIEL
    assert res["rollback"] is None
    entree = next(r for r in history.derniers(db_path=tmp_db)
                  if r["run_id_opaque"] == res["history_run_id"])
    assert entree["statut"] == "SUCCESS"  # PARTIEL reste une issue normale, pas un rollback


# ── Phase 8 — dry-run ─────────────────────────────────────────────────────────

def test_dry_run_ne_modifie_aucun_dataset(tmp_db, monkeypatch):
    appels = []
    monkeypatch.setattr(orch, "_appeler_service",
                        lambda chemin, db_path: appels.append(chemin) or {"ok": True})
    avant = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    res = orch.actualiser(db_path=tmp_db, dry_run=True)
    apres = {d["dataset"]: d["statut"] for d in orch.etat_datasets(tmp_db)}
    assert avant == apres, "dry-run ne doit activer aucun dataset"
    assert appels == [], "dry-run ne doit appeler aucun service réel"
    assert res["statut"] == "DRY_RUN"
    assert res["sauvegarde_id"] is None, "dry-run ne prend aucune sauvegarde"
    assert all(e["statut"] == "IGNOREE" for e in res["etapes"])


def test_dry_run_montre_le_plan_reel(tmp_db, monkeypatch):
    """Le dry-run doit distinguer ce qui SERAIT exécuté de ce qui SERAIT ignoré."""
    monkeypatch.setattr(orch, "_appeler_service", _appeler_service_ok)
    res = orch.actualiser(db_path=tmp_db, dry_run=True)
    flux = next(e for e in res["etapes"] if e["dataset"] == dag.FLUX_LOT9)
    assert "serait exécuté" in flux["motif"]
    hostaway = next(e for e in res["etapes"] if e["dataset"] == dag.HOSTAWAY_RAW)
    assert "import externe" in hostaway["motif"]

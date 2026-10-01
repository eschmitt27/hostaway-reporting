"""« Actualiser toute l'activité » bloqué par un ancien échec Hostaway — reproduction et correctif.

Situation constatée sur la base réelle le 2026-10-01 : HOSTAWAY_RAW resté en ÉCHEC depuis le
2026-09-10 (alors que le dépôt publié avait été synchronisé avec succès sept fois depuis, par
« Actualiser les ménages » et l'écran Hostaway), RESERVATIONS « à recalculer », et un clic sur
« Actualiser toute l'activité » qui :
  - ne relançait pas Hostaway (import externe non déclenché) ;
  - bloquait donc toute la chaîne, jusqu'aux préfactures ;
  - marquait en plus les tâches de ménage (un IMPORT) « à recalculer », ce qui bloquait Ménages
    sans qu'aucun parcours ne puisse plus jamais lever cet état.
"""
from __future__ import annotations

import pytest

from app.services import orchestrateur_dag as dag
from app.services import orchestrateur_service as orch

CHAINE = (dag.RESERVATIONS, dag.MENAGES, dag.FLUX_LOT9, dag.LOT10, dag.LOT11, dag.LOT12)


def _espion(monkeypatch, echecs: tuple[str, ...] = ()) -> list[str]:
    appels: list[str] = []

    def faux_service(chemin, db_path):
        appels.append(chemin)
        return {"ok": not any(e in chemin for e in echecs), "message": "panne simulée"}

    monkeypatch.setattr(orch, "_appeler_service", faux_service)
    return appels


@pytest.fixture()
def base_bloquee(tmp_db):
    """L'état exact de la base réelle avant correctif."""
    orch.marquer_dataset(dag.HOSTAWAY_RAW, orch.ST_ECHEC, run_id="ORCH-0910",
                         erreur_code="MOTEUR_CODE_RETOUR",
                         erreur_message="lot1_hostaway_extract rc=1", db_path=tmp_db)
    orch.marquer_dataset(dag.HOSTAWAY_CLEANING_TASKS, orch.ST_A_RECALCULER, run_id="ORCH-1001",
                         db_path=tmp_db)
    for d in CHAINE:
        orch.marquer_dataset(d, orch.ST_A_RECALCULER, run_id="ORCH-1001", db_path=tmp_db)
    return tmp_db


def _etats(db) -> dict[str, str]:
    return {d["dataset"]: d["statut"] for d in orch.etat_datasets(db)}


def test_actualisation_globale_ecran_synchronise_hostaway_et_debloque_tout(base_bloquee,
                                                                         monkeypatch):
    appels = _espion(monkeypatch)
    res = orch.actualiser(cibles=None, inclure_imports_externes=True, db_path=base_bloquee)
    assert res["statut"] == orch.RUN_SUCCES, res
    assert any("importer_hostaway" in a and "cleaning" not in a for a in appels)
    etats = _etats(base_bloquee)
    assert etats[dag.HOSTAWAY_RAW] == orch.ST_A_JOUR
    for d in CHAINE:
        assert etats[d] == orch.ST_A_JOUR, (d, etats[d])


def test_actualisation_globale_ne_lance_jamais_les_taches_de_menage(base_bloquee, monkeypatch):
    """Les tâches H6 gardent leur parcours propre (« Actualiser les ménages ») : leur échec
    bloquerait sinon toute la chaîne à chaque clic global."""
    appels = _espion(monkeypatch)
    res = orch.actualiser(cibles=None, inclure_imports_externes=True, db_path=base_bloquee)
    assert not any("cleaning_tasks" in a for a in appels)
    h6 = next(e for e in res["etapes"] if e["dataset"] == dag.HOSTAWAY_CLEANING_TASKS)
    assert h6["statut"] == "IGNOREE" and "non déclenché" in h6["motif"]


def test_un_import_n_est_jamais_invalide_en_cascade(tmp_db, monkeypatch):
    orch.marquer_dataset(dag.HOSTAWAY_RAW, orch.ST_ECHEC, run_id="R0", db_path=tmp_db)
    orch.marquer_dataset(dag.HOSTAWAY_CLEANING_TASKS, orch.ST_A_JOUR, run_id="R0", db_path=tmp_db)
    _espion(monkeypatch)
    res = orch.actualiser(cibles=None, db_path=tmp_db)   # sans import : l'échec reste bloquant
    assert _etats(tmp_db)[dag.HOSTAWAY_CLEANING_TASKS] == orch.ST_A_JOUR
    assert _etats(tmp_db)[dag.RESERVATIONS] == orch.ST_A_RECALCULER
    h6 = next(e for e in res["etapes"] if e["dataset"] == dag.HOSTAWAY_CLEANING_TASKS)
    assert not h6.get("bloque"), "un import non demandé n'est pas « bloqué », il n'est pas voulu"


def test_un_import_reste_a_recalculer_ne_bloque_plus_menages(tmp_db, monkeypatch):
    """L'état hérité de l'ancien marquage en cascade ne gèle plus Ménages."""
    orch.marquer_dataset(dag.HOSTAWAY_CLEANING_TASKS, orch.ST_A_RECALCULER, run_id="R0",
                         db_path=tmp_db)
    orch.marquer_dataset(dag.RESERVATIONS, orch.ST_A_JOUR, run_id="R0", db_path=tmp_db)
    appels = _espion(monkeypatch)
    res = orch.actualiser(cibles=[dag.MENAGES], db_path=tmp_db)
    assert res["statut"] == orch.RUN_SUCCES, res
    assert any("executer_menages" in a for a in appels)


def test_un_calcul_a_recalculer_reste_bloquant(tmp_db, monkeypatch):
    """Le garde-fou de la mission 14b est intact pour les CALCULS."""
    orch.marquer_dataset(dag.RESERVATIONS, orch.ST_A_RECALCULER, run_id="R0", db_path=tmp_db)
    orch.marquer_dataset(dag.MENAGES, orch.ST_A_JOUR, run_id="R0", db_path=tmp_db)
    _espion(monkeypatch)
    res = orch.actualiser(cibles=[dag.FLUX_LOT9], db_path=tmp_db)
    flux = next(e for e in res["etapes"] if e["dataset"] == dag.FLUX_LOT9)
    assert flux["statut"] == "IGNOREE" and flux.get("bloque")


def test_echec_hostaway_reste_bloquant_et_le_resume_dit_la_vraie_cause(base_bloquee, monkeypatch):
    _espion(monkeypatch, echecs=("importer_hostaway",))
    res = orch.actualiser(cibles=None, inclure_imports_externes=True, db_path=base_bloquee)
    # Ménages, indépendant des réservations, est calculé ; tout ce qui lit les réservations attend.
    assert res["statut"] == orch.RUN_PARTIEL
    etats = _etats(base_bloquee)
    assert etats[dag.HOSTAWAY_RAW] == orch.ST_ECHEC and etats[dag.MENAGES] == orch.ST_A_JOUR
    assert all(etats[d] == orch.ST_A_RECALCULER for d in (dag.RESERVATIONS, dag.FLUX_LOT9,
                                                          dag.LOT10, dag.LOT12))
    run = orch.dernier_run(db_path=base_bloquee)
    assert run["erreur_resume"].startswith("HOSTAWAY_RAW: panne simulée")
    # Les étapes normalement ignorées (relevé bancaire, référentiel) ne noient plus la cause.
    assert "BANQUE" not in run["erreur_resume"] and "REF_SETUP" not in run["erreur_resume"]


def test_bouton_global_et_dry_run_demandent_la_synchronisation_hostaway(client, tmp_db,
                                                                       monkeypatch):
    appels: list[dict] = []

    def espion(**kw):
        appels.append(kw)
        return {"ok": True, "run_id": "R", "statut": "DRY_RUN", "etapes": []}

    monkeypatch.setattr(orch, "actualiser", espion)
    assert client.post("/actualisation/tout", follow_redirects=False).status_code == 303
    assert client.post("/actualisation/tout/dry-run", follow_redirects=False).status_code == 200
    assert [a["inclure_imports_externes"] for a in appels] == [True, True]
    assert all(a["cibles"] is None for a in appels)


def test_actualisation_accessible_depuis_observabilite(client, tmp_db):
    page = client.get("/observabilite/runs").text
    assert 'href="/actualisation"' in page and 'data-testid="lien-actualisation"' in page
    ecran = client.get("/actualisation").text
    assert 'href="/observabilite/runs"' in ecran
    # Le menu « Observabilité & outils » reste sélectionné sur l'écran Actualisation.
    assert 'href="/observabilite/runs" class="nav-item active"' in ecran


# ── Chaîne « Actualiser les ménages » : les réservations importées atteignent l'aval ──────────

@pytest.fixture()
def chaine_menages(monkeypatch, tmp_db):
    from app.services import hostaway_cleaning_tasks_actualisation_service as hostaway_ct
    from app.services import hostaway_depot_service as depot_ha
    from app.services import menages_actualisation_service as workflow
    from app.services import menages_declarations_service as decl
    from app.services import menages_pdf_import_service as pdf_import
    from app.services import menages_service as svc
    from app.services import orchestrateur_moteur as moteur

    etat = {"depot_ok": True, "cascades": []}
    monkeypatch.setattr(depot_ha, "synchroniser",
                        lambda **k: {"ok": etat["depot_ok"], "importe": etat["depot_ok"],
                                     "message": "", "publie": {}})
    monkeypatch.setattr(pdf_import, "importer_nouveaux",
                        lambda **k: {"ok": True, "nb_importees": 0, "mois_impactes": []})
    monkeypatch.setattr(workflow, "_google_sheet_config_ok", lambda **k: False)
    monkeypatch.setattr(hostaway_ct, "source_disponible", lambda: True)
    monkeypatch.setattr(orch, "recalculer_dataset", lambda dataset, **k: {"ok": True})
    monkeypatch.setattr(moteur, "executer_menages_cible",
                        lambda *, mois, **k: {"ok": True, "mois_traite": mois})
    monkeypatch.setattr(orch, "marquer_dataset", lambda *a, **k: None)
    monkeypatch.setattr(orch, "actualiser",
                        lambda **k: etat["cascades"].append(k) or {"ok": True})
    monkeypatch.setattr(decl, "mois_cloture", lambda mois, db_path=None: False)
    monkeypatch.setattr(svc, "invalidate_menages_cache", lambda: None)
    return workflow, etat


def test_menages_propage_les_reservations_synchronisees(chaine_menages, tmp_db):
    workflow, etat = chaine_menages
    workflow.actualiser(mois_affiche="2026-09", db_path=tmp_db)
    assert etat["cascades"][-1]["cibles"] == [dag.HOSTAWAY_RAW, dag.FLUX_LOT9]
    assert etat["cascades"][-1]["inclure_imports_externes"] is True


def test_menages_depot_illisible_ne_cible_que_le_flux(chaine_menages, tmp_db):
    """Dépôt injoignable : on ne transforme pas une panne réseau en blocage de toute la chaîne."""
    workflow, etat = chaine_menages
    etat["depot_ok"] = False
    workflow.actualiser(mois_affiche="2026-09", db_path=tmp_db)
    assert etat["cascades"][-1]["cibles"] == [dag.FLUX_LOT9]

"""Tâches de ménage Hostaway par le dépôt publié — recette n°3 §17 bis (lot6a).

Chaîne prouvée, sans réseau ni identifiant : un dépôt Git jetable publie `cleaning_tasks_hostaway.tsv`
comme le fait le pipeline GitHub → le lecteur de dépôt → le service H6 (MÊME normalisation, MÊME
couche RAW versionnée) → SQLite ; puis le nœud H6 de l'orchestrateur enchaîne lot6a.
"""
from __future__ import annotations

import json
import subprocess

import pytest

import app.config as cfg
from app.services import hostaway_cleaning_tasks_actualisation_service as svc
from app.services import hostaway_cleaning_tasks_raw_service as raw
from test_hostaway_depot_github import _Log, depot, lib  # noqa: F401 — fixtures réutilisées

ENTETE = ("id", "reservationId", "listingMapId", "title", "status", "taskType", "type",
          "canStartFrom", "shouldEndBy", "assigneeUserId")


def _tsv(taches) -> str:
    lignes = ["\t".join(ENTETE)]
    for t in taches:
        lignes.append("\t".join(str(t.get(c, "")) for c in ENTETE))
    return "\n".join(lignes) + "\n"


TACHES = [
    {"id": 501, "reservationId": 9001, "listingMapId": 481998, "title": "Ménage Studio",
     "status": "completed", "taskType": "cleaning", "canStartFrom": "2026-03-04 10:00:00",
     "assigneeUserId": 77},
    {"id": 502, "reservationId": 9002, "listingMapId": 485104, "title": "Ménage T2",
     "status": "pending", "taskType": "cleaning", "canStartFrom": "2026-03-12 10:00:00",
     "assigneeUserId": 78},
]


def _publier(depot, taches, *, avec_fichier=True, message="Automated data refresh"):
    chemin = depot["source"] / "cleaning_tasks_hostaway.tsv"
    if avec_fichier:
        chemin.write_text(_tsv(taches), encoding="utf-8-sig")
        depot["git"]("add", "-A")
    elif chemin.exists():
        depot["git"]("rm", "-q", "cleaning_tasks_hostaway.tsv")
    depot["git"]("commit", "--allow-empty", "-m", message)


@pytest.fixture()
def poste(depot, lib, monkeypatch):
    """L'installation pointe le clone : `origin` est le dépôt de données jetable."""
    monkeypatch.setattr(cfg, "PROJECT_ROOT", depot["poste"])
    return depot["poste"]


@pytest.fixture()
def sans_env_local(monkeypatch):
    """Toute lecture d'identifiants locaux fait échouer le test."""
    import dotenv

    for nom in ("HOSTAWAY_CLIENT_ID", "HOSTAWAY_CLIENT_SECRET", "HOSTAWAY_ACCOUNT_ID"):
        monkeypatch.delenv(nom, raising=False)

    def interdit(*_a, **_k):
        raise AssertionError("le chemin dépôt ne doit jamais lire d'identifiants locaux")

    monkeypatch.setattr(svc, "_credentials", interdit)
    monkeypatch.setattr(dotenv, "load_dotenv", interdit)


def _extractions(db):
    from app.db.connection import get_db

    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT extraction_id, mode, statut, nb_taches, message "
            "FROM hostaway_cleaning_tasks_extractions ORDER BY id")]
    finally:
        conn.close()


# ── Lecteur de dépôt ────────────────────────────────────────────────────────────────────────────

def test_sans_fichier_publie_les_taches_sont_refusees_et_les_reservations_restent_lisibles(
        depot, lib):
    etat = lib.etat(depot["poste"])
    assert etat["disponible"] is True and etat["cleaning_tasks_publiees"] is False
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    with pytest.raises(lib.DonneeNonFournieParCetteSource):
        source.get_tasks("2026-01-01")
    assert source.count_reservations("2026-01-01") == 3


def test_taches_publiees_rendues_comme_l_api_sans_doublon(depot, lib):
    _publier(depot, TACHES + [TACHES[0]])
    source = lib.SourceDepotGitHub(depot["poste"], _Log())
    taches = source.get_tasks("2026-01-01")
    assert [t["id"] for t in taches] == [501, 502]
    assert taches[0]["reservationId"] == 9001 and taches[0]["listingMapId"] == 481998
    assert taches[0]["canStartFrom"] == "2026-03-04 10:00:00"
    assert taches[0]["assigneeUserId"] == 77


# ── Service H6 : dépôt → SQLite ─────────────────────────────────────────────────────────────────

def test_import_depuis_le_depot_sans_identifiant_local(tmp_db, depot, poste, sans_env_local):
    _publier(depot, TACHES)
    res = svc.actualiser(db_path=tmp_db)

    assert res["ok"] is True and res["importe"] is True and res["nb_taches"] == 2
    [extraction] = _extractions(tmp_db)
    assert extraction["mode"] == raw.MODE_DEPOT_GITHUB and extraction["statut"] == raw.ST_SUCCES
    assert res["commit_court"] in extraction["message"] and "produites le" in extraction["message"]
    assert [t["task_id"] for t in raw.taches(db_path=tmp_db)] == ["501", "502"]
    assert str(poste) not in json.dumps(res, default=str)


def test_idempotent_meme_etat_puis_meme_contenu_republie(tmp_db, depot, poste, sans_env_local):
    _publier(depot, TACHES)
    assert svc.actualiser(db_path=tmp_db)["importe"] is True

    deuxieme = svc.actualiser(db_path=tmp_db)
    assert deuxieme["ok"] is True and deuxieme["importe"] is False
    assert deuxieme["code"] == svc.E_DEJA_SYNCHRONISEES

    _publier(depot, TACHES, message="Automated data refresh (contenu identique)")
    assert svc.actualiser(db_path=tmp_db)["importe"] is False
    assert len(_extractions(tmp_db)) == 1
    assert len(raw.taches(db_path=tmp_db)) == 2


def test_contenu_modifie_nouvelle_extraction_qui_devient_la_courante(tmp_db, depot, poste,
                                                                     sans_env_local):
    _publier(depot, TACHES)
    premiere = svc.actualiser(db_path=tmp_db)["extraction_id"]
    modifiees = [dict(TACHES[0]), dict(TACHES[1], status="completed")]
    _publier(depot, modifiees)

    res = svc.actualiser(db_path=tmp_db)
    assert res["importe"] is True and res["extraction_id"] != premiere
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == res["extraction_id"]
    assert [t["status"] for t in raw.taches(db_path=tmp_db)] == ["completed", "completed"]


def test_taches_retirees_du_depot_le_dernier_jeu_valide_est_conserve(tmp_db, depot, poste,
                                                                     sans_env_local):
    _publier(depot, TACHES)
    valide = svc.actualiser(db_path=tmp_db)["extraction_id"]
    _publier(depot, TACHES, avec_fichier=False)

    res = svc.actualiser(db_path=tmp_db)
    assert res["ok"] is False and res["code"] == svc.E_TACHES_NON_PUBLIEES
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == valide
    assert len(_extractions(tmp_db)) == 1


def test_depot_illisible_le_dernier_jeu_valide_est_conserve(tmp_db, depot, poste, sans_env_local,
                                                           tmp_path, monkeypatch):
    _publier(depot, TACHES)
    valide = svc.actualiser(db_path=tmp_db)["extraction_id"]
    pas_un_depot = tmp_path / "pas_un_depot"
    pas_un_depot.mkdir()
    monkeypatch.setattr(cfg, "PROJECT_ROOT", pas_un_depot)

    res = svc.actualiser(db_path=tmp_db)
    assert res["ok"] is False and res["code"] == svc.E_DEPOT_INDISPONIBLE
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == valide
    assert str(pas_un_depot) not in json.dumps(res, default=str)


def test_jeu_publie_vide_jamais_importe(tmp_db, depot, poste, sans_env_local):
    _publier(depot, TACHES)
    valide = svc.actualiser(db_path=tmp_db)["extraction_id"]
    _publier(depot, [])

    res = svc.actualiser(db_path=tmp_db)
    assert res["ok"] is False
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == valide


def test_preflight_sans_reseau_suit_la_publication(depot, poste):
    subprocess.run(["git", "fetch", "origin", "main"], cwd=str(poste), check=True,
                   capture_output=True)
    assert svc.source_disponible() is False
    _publier(depot, TACHES)
    subprocess.run(["git", "fetch", "origin", "main"], cwd=str(poste), check=True,
                   capture_output=True)
    assert svc.source_disponible() is True


# ── Nœud H6 de l'orchestrateur : import puis lot6a ──────────────────────────────────────────────

def test_import_nouveau_enchaine_lot6a_en_sqlite(tmp_db, monkeypatch):
    from app.services import orchestrateur_moteur as moteur

    monkeypatch.setattr(svc, "actualiser", lambda **k: {"ok": True, "importe": True, "nb_taches": 2})
    appels = []
    monkeypatch.setattr(moteur, "executer",
                        lambda script, **k: appels.append((script, k.get("arguments"))) or {"ok": True})

    res = moteur.importer_hostaway_cleaning_tasks(db_path=tmp_db)
    assert res["ok"] is True
    assert appels == [("lot6a_cleaning_tasks_comptage.py", ("--source", "SQLITE", "--sans-excel"))]


def test_rien_de_nouveau_et_comptage_present_pas_de_relance(tmp_db, monkeypatch):
    from app.services import orchestrateur_moteur as moteur

    monkeypatch.setattr(svc, "actualiser", lambda **k: {"ok": True, "importe": False})
    monkeypatch.setattr(moteur, "_taches_enrichies_presentes", lambda db_path: True)
    appels = []
    monkeypatch.setattr(moteur, "executer", lambda script, **k: appels.append(script) or {"ok": True})

    res = moteur.importer_hostaway_cleaning_tasks(db_path=tmp_db)
    assert res["ok"] is True and res["donnees_modifiees"] is False and appels == []


def test_echec_de_lot6a_remonte_en_echec(tmp_db, monkeypatch):
    from app.services import orchestrateur_moteur as moteur

    monkeypatch.setattr(svc, "actualiser", lambda **k: {"ok": True, "importe": True})
    monkeypatch.setattr(moteur, "executer",
                        lambda script, **k: {"ok": False, "code": "MOTEUR_CODE_RETOUR",
                                             "message": "rc=1"})
    res = moteur.importer_hostaway_cleaning_tasks(db_path=tmp_db)
    assert res["ok"] is False and "lot6a" in res["message"]

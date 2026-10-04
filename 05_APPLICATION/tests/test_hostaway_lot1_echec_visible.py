"""Lot1 (Hostaway) — un échec interne n'est JAMAIS rendu comme un succès.

LE DÉFAUT CORRIGÉ
`hostaway_actualisation_service` juge le run sur son seul code retour. Or `lot1.main()` interceptait
toute erreur fatale (`statut_run = "FAILED"`) et rendait 0 ; une écriture RAW SQLite en échec
n'enregistrait même aucune étape, et le journal — dont le statut est DÉDUIT des étapes — concluait
SUCCES. L'orchestrateur aurait déclaré Hostaway « à jour » sur l'extraction PRÉCÉDENTE, l'aval
« inchangé », et la version publiée ne serait jamais entrée.

CE QUI EST PROUVÉ ICI, sur le vrai `lot1.main()` lancé contre un dépôt git jetable (même topologie
que le poste réel : un clone dont `origin` porte les données publiées), sans réseau ni base réelle :
  - succès : code 0, extraction SUCCES devenue l'extraction servie ;
  - persistance SQLite en échec : code 1, candidat clos ECHEC, extraction précédente toujours
    servie, journal non SUCCES, message sanitisé (aucun chemin) ;
  - erreur fatale après des étapes réussies : code 1, journal non SUCCES ;
  - le service canonique (`synchroniser`) transforme ce code 1 en échec : run history FAILED,
    aucune propagation possible.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db

from test_hostaway_depot_github import depot  # noqa: F401  (fixture : dépôt publié jetable)

MOTEUR = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


@pytest.fixture()
def lot1(depot, tmp_path, monkeypatch):  # noqa: F811
    """Le module lot1 réel, dont TOUS les chemins d'écriture pointent hors de l'arbre réel."""
    if str(MOTEUR) not in sys.path:
        sys.path.insert(0, str(MOTEUR))
    import lot1_hostaway_extract as module

    monkeypatch.setattr(module, "BASE_DIR", depot["poste"])
    monkeypatch.setattr(module, "OUT_DIR", tmp_path / "out")
    monkeypatch.setattr(module, "LOG_DIR", tmp_path / "logs")
    (tmp_path / "logs").mkdir()
    return module


def _lancer(module, db, monkeypatch) -> int:
    """`main()` tel que le service le lance (ARGUMENTS_DEPOT + --db), sans `git fetch`."""
    from app.services import hostaway_actualisation_service as ha

    monkeypatch.setattr(sys, "argv", ["lot1_hostaway_extract.py", "--db", str(db),
                                      *ha.ARGUMENTS_DEPOT, "--sans-fetch"])
    try:
        module.main()
    except SystemExit as fin:
        return int(fin.code or 0)
    return 0


def _extractions(db):
    conn = get_db(db)
    try:
        return [dict(r) for r in conn.execute(
            "SELECT extraction_id, statut, message, nb_reservations FROM hostaway_extractions "
            "ORDER BY id")]
    finally:
        conn.close()


def _dernier_run(db):
    conn = get_db(db)
    try:
        return dict(conn.execute(
            "SELECT statut, nb_etapes_ko FROM moteur_runs WHERE lot = 'lot1_hostaway_extract' "
            "ORDER BY id DESC LIMIT 1").fetchone())
    finally:
        conn.close()


def _bloquer_ecriture_reservations(db):
    """Une vraie panne de persistance : la base refuse l'insertion (contrainte)."""
    conn = sqlite3.connect(db)
    conn.execute("CREATE TRIGGER panne BEFORE INSERT ON hostaway_reservations "
                 "BEGIN SELECT RAISE(ABORT, 'contrainte simulee'); END")
    conn.commit()
    conn.close()


def test_succes_code_zero_et_extraction_servie(lot1, tmp_db, monkeypatch):
    from app.services import hostaway_raw_service as raw

    assert _lancer(lot1, tmp_db, monkeypatch) == 0
    ext = _extractions(tmp_db)
    assert [e["statut"] for e in ext] == ["SUCCES"]
    assert ext[0]["nb_reservations"] > 0
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == ext[0]["extraction_id"]
    assert _dernier_run(tmp_db)["statut"] == "SUCCES"


def test_persistance_en_echec_code_un_candidat_clos_et_precedent_conserve(lot1, tmp_db,
                                                                           monkeypatch):
    from app.services import hostaway_raw_service as raw

    assert _lancer(lot1, tmp_db, monkeypatch) == 0
    valide = raw.derniere_extraction_utilisable(db_path=tmp_db)

    # `run_id` est horodaté à la seconde : deux runs dans la même seconde partageraient la ligne
    # `moteur_runs` du premier. Artefact de test, pas de production (un run réel dure > 1 s).
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE moteur_runs SET run_id = run_id || '-premier'")
    conn.commit()
    conn.close()
    _bloquer_ecriture_reservations(tmp_db)
    assert _lancer(lot1, tmp_db, monkeypatch) == 1, "un import raté ne rend jamais 0"

    ext = _extractions(tmp_db)
    assert [e["statut"] for e in ext] == ["SUCCES", "ECHEC"], "candidat clos, jamais EN_COURS"
    assert ext[1]["nb_reservations"] == 0, "aucune ligne partielle du candidat"
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == valide
    run = _dernier_run(tmp_db)
    assert run["statut"] != "SUCCES" and run["nb_etapes_ko"] >= 1
    assert "contrainte simulee" in ext[1]["message"]
    assert str(tmp_db) not in ext[1]["message"] and str(Path(tmp_db).parent) not in ext[1]["message"]


def test_erreur_fatale_apres_des_etapes_reussies_code_un(lot1, tmp_db, monkeypatch):
    def panne(*a, **k):
        raise RuntimeError(f"panne au milieu du run ({tmp_db})")

    monkeypatch.setattr(lot1, "ecrire_raw_sqlite", panne)
    assert _lancer(lot1, tmp_db, monkeypatch) == 1
    run = _dernier_run(tmp_db)
    assert run["statut"] != "SUCCES", "le journal ne conclut plus SUCCES après une erreur fatale"
    conn = get_db(tmp_db)
    try:
        erreur = conn.execute("SELECT erreur FROM moteur_run_etapes WHERE etape = 'RUN' "
                              "ORDER BY id DESC LIMIT 1").fetchone()[0]
    finally:
        conn.close()
    assert "RuntimeError" in erreur and str(tmp_db) not in erreur, "sanitisé, sans chemin"


def test_le_service_canonique_traduit_le_code_un_en_echec_visible(tmp_db, monkeypatch):
    """Maillon suivant, déjà en place : rc≠0 → `synchroniser` en échec, run history FAILED, et
    l'orchestrateur n'a aucune extraction nouvelle à propager."""
    from app.services import hostaway_actualisation_service as ha
    from app.services import hostaway_depot_service as depot_svc
    from app.services import orchestrateur_moteur as om
    from app.services import run_history_service as history

    monkeypatch.setattr(depot_svc, "etat_publie",
                        lambda **k: {"disponible": True, "commit": "nouveau",
                                     "commit_court": "nouveau",
                                     "source_horodatage": "2026-10-03T20:20:48Z"})
    monkeypatch.setattr(ha, "actualiser", lambda **k: {"ok": True, "code_retour": 1})

    resultat = om.importer_hostaway(db_path=tmp_db)
    assert resultat["ok"] is False and resultat["code"] == om.E_CODE_RETOUR
    assert history.dernier("HOSTAWAY", db_path=tmp_db)["statut"] == "FAILED"

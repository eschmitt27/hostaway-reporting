"""Couche RAW SQLite pour CleaningTasks (Lot6a) — préparation du chemin API H6 → SQLite.

N'exige pas de relancer H6 en réel (l'API a déjà essuyé des 429 sur cet endpoint) : ces tests
couvrent le service RAW sur données synthétiques et la reprise depuis le master legacy existant.
"""
from __future__ import annotations

import openpyxl
import pytest

from app.services import hostaway_cleaning_tasks_raw_service as raw
from app.services import hostaway_cleaning_tasks_adaptateur as adapt


def _tache(task_id, *, reservation_id="70001", listing_map_id="480001", status="completed"):
    return {"id": task_id, "reservationId": reservation_id, "listingMapId": listing_map_id,
            "title": "Ménage standard", "status": status, "canStartFrom": "2026-07-05",
            "assigneeUserId": "1001"}


def test_extraction_absente_rend_liste_vide(tmp_db):
    assert raw.taches(db_path=tmp_db) == []
    assert raw.fraicheur(db_path=tmp_db) is None


def test_ecriture_et_lecture_dans_l_ordre_d_insertion(tmp_db):
    eid = raw.ouvrir(mode=raw.MODE_FIXTURE, db_path=tmp_db)
    assert eid.startswith("HCT-")
    raw.enregistrer(eid, taches=[_tache("T3"), _tache("T1"), _tache("T2")], db_path=tmp_db)
    resultat = raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)
    assert resultat["nb_taches"] == 3

    lues = raw.taches(db_path=tmp_db)
    assert [t["task_id"] for t in lues] == ["T3", "T1", "T2"]


def test_extraction_echouee_exclue_de_la_derniere_utilisable(tmp_db):
    eid_ko = raw.ouvrir(mode=raw.MODE_FIXTURE, db_path=tmp_db)
    raw.cloturer(eid_ko, statut=raw.ST_ECHEC, message="429", db_path=tmp_db)

    eid_ok = raw.ouvrir(mode=raw.MODE_FIXTURE, db_path=tmp_db)
    raw.enregistrer(eid_ok, taches=[_tache("T1")], db_path=tmp_db)
    raw.cloturer(eid_ok, statut=raw.ST_SUCCES, db_path=tmp_db)

    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == eid_ok


def test_extraction_partielle_utilisable_et_signale_les_segments_plafonnes(tmp_db):
    eid = raw.ouvrir(mode=raw.MODE_API, db_path=tmp_db)
    raw.enregistrer(eid, taches=[_tache("T1")], db_path=tmp_db)
    resultat = raw.cloturer(eid, statut=raw.ST_PARTIEL, message="listing 480003 plafonne",
                            segments_plafonnes=["480003"], db_path=tmp_db)
    assert resultat["statut"] == raw.ST_PARTIEL
    assert raw.derniere_extraction_utilisable(db_path=tmp_db) == eid


def test_reprise_master_absent_refuse_proprement(tmp_path, tmp_db):
    resultat = adapt.reprendre(racine=tmp_path, db_path=tmp_db)
    assert resultat["ok"] is False
    assert resultat["code"] == adapt.E_MASTER_ABSENT
    assert raw.taches(db_path=tmp_db) == []


def test_reprise_depuis_master_legacy_alimente_la_base(tmp_path, tmp_db):
    dossier = tmp_path / "02_TRAVAIL" / "Lot1_Hostaway"
    dossier.mkdir(parents=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(["task_id", "reservation_id", "listingMapId", "title", "status", "canStartFrom",
               "assigneeUserId", "cost", "h6_note", "extrait_le", "ROW_HASH"])
    ws.append(["T1", "70001", "480001", "Ménage standard", "completed", "2026-07-05", "1001",
               None, "cost=NULL_H6_comptage_uniquement", "2026-08-18T00:00:00Z", "H-T1"])
    wb.save(dossier / adapt.MASTER)
    wb.close()

    resultat = adapt.reprendre(racine=tmp_path, db_path=tmp_db)
    assert resultat["ok"] is True
    assert resultat["statut"] == raw.ST_SUCCES
    assert resultat["nb_taches"] == 1

    lues = raw.taches(db_path=tmp_db)
    assert len(lues) == 1
    assert lues[0]["task_id"] == "T1"
    assert lues[0]["reservation_id"] == "70001"


def test_rejeu_meme_extraction_pas_de_doublon_sur_relecture(tmp_db):
    eid = raw.ouvrir(mode=raw.MODE_FIXTURE, db_path=tmp_db)
    raw.enregistrer(eid, taches=[_tache("T1")], db_path=tmp_db)
    raw.cloturer(eid, statut=raw.ST_SUCCES, db_path=tmp_db)
    premiere = raw.taches(db_path=tmp_db)
    seconde = raw.taches(db_path=tmp_db)
    assert premiere == seconde

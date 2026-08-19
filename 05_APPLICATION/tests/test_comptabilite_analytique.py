"""Moteur analytique (Phase 2) — lecture des sorties Lot10, jamais un recalcul."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
import fixtures_lot10 as fx
from app.db.connection import apply_migrations, get_db
from app.readers import proprietaires_reglements_reader as reader
from app.services import comptabilite_analytique_service as ana


def _wb(path, sheets):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, (cols, rows) in sheets.items():
        ws = wb.create_sheet(name)
        ws.append(cols)
        for r in rows:
            ws.append([r.get(c) for c in cols])
    wb.save(str(path))
    wb.close()


LOG_COLS = ["mois", "logement_id", "proprietaire_id", "total_produits", "total_charges",
           "resultat", "nb_flux", "vision", "commentaire"]
PROP_COLS = ["mois", "proprietaire_id", "total_produits", "total_charges", "resultat", "nb_flux", "vision"]
GLOBAL_COLS = ["vision", "total_produits", "total_charges", "resultat", "commentaire_hc"]


@pytest.fixture
def resultats_files(tmp_path, monkeypatch):
    """Lot10 est SQLite (migration 0044) : seul le GRAIN FIN (`lot10_resultats`) est semé.

    PAR_MOIS_PROPRIETAIRE et GLOBAL ne sont plus des sources distinctes — le reader les dérive de
    ce grain avec la règle du moteur. Les totaux attendus (REEL 1100, COMPTABLE 650, HORS_COMPTA
    450, donc identité REEL = COMPTABLE + HC vérifiée à 0,00 €) découlent des lignes semées : ils
    ne peuvent plus contredire le détail, contrairement à trois onglets posés côte à côte.
    """
    par_logement = [
        {"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 1000.0, "total_charges": 300.0, "resultat": 700.0, "nb_flux": 5,
         "vision": "REEL", "commentaire": ""},
        {"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 900.0, "total_charges": 250.0, "resultat": 650.0, "nb_flux": 4,
         "vision": "COMPTABLE", "commentaire": ""},
        {"mois": "2026-06", "logement_id": "LOG_B1", "proprietaire_id": "PROP_B",
         "total_produits": 500.0, "total_charges": 100.0, "resultat": 400.0, "nb_flux": 3,
         "vision": "REEL", "commentaire": ""},
        # HORS_COMPTA : nécessaire pour que le total dérivé (450) existe et que l'identité
        # REEL = COMPTABLE + HC tombe à 0,00 €. L'ancien onglet GLOBAL l'affirmait sans qu'aucune
        # ligne de détail ne le porte.
        {"mois": "2026-06", "logement_id": "LOG_B1", "proprietaire_id": "PROP_B",
         "total_produits": 600.0, "total_charges": 150.0, "resultat": 450.0, "nb_flux": 2,
         "vision": "HORS_COMPTA", "commentaire": ""},
    ]
    db = tmp_path / "app.db"
    apply_migrations(db)
    fx.seeder(db, resultats=par_logement)
    monkeypatch.setattr(cfg, "DB_PATH", db)
    reader.vider_cache()
    yield tmp_path
    reader.vider_cache()


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.db"
    apply_migrations(p)
    return p


def test_source_indisponible(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", tmp_path / "absent.xlsx")
    reader.vider_cache()
    assert ana.source_disponible() is False
    assert ana.mesures_globales()["statut"] == ana.NON_DISPONIBLE
    reader.vider_cache()


def test_mesures_globales_par_vision(resultats_files):
    m = ana.mesures_globales()
    assert m["statut"] == "OK"
    assert m["visions"]["REEL"]["resultat"] == 1100.0
    assert m["visions"]["COMPTABLE"]["resultat"] == 650.0
    assert m["visions"]["HORS_COMPTA"]["resultat"] == 450.0
    assert "verifie" in m["visions"]["REEL"]["commentaire"]


def test_mesures_par_logement_filtre_vision(resultats_files):
    m = ana.mesures_par_logement(mois="2026-06", vision="REEL")
    assert m["statut"] == "OK"
    logements = {l["logement_id"] for l in m["lignes"]}
    assert logements == {"LOG_A1", "LOG_B1"}
    assert all(l["vision"] == "REEL" for l in m["lignes"])


def test_fiche_logement_toutes_visions(resultats_files):
    fiche = ana.fiche_logement("LOG_A1", mois="2026-06")
    assert fiche["statut"] == "OK"
    assert set(fiche["visions"].keys()) == {"REEL", "COMPTABLE"}
    assert fiche["visions"]["REEL"]["resultat"] == 700.0
    assert fiche["visions"]["COMPTABLE"]["resultat"] == 650.0


def test_fiche_logement_inconnu_non_disponible(resultats_files):
    fiche = ana.fiche_logement("LOG_INCONNU", mois="2026-06")
    assert fiche["statut"] == ana.NON_DISPONIBLE


def test_fiche_proprietaire(resultats_files):
    fiche = ana.fiche_proprietaire("PROP_B", mois="2026-06")
    assert fiche["statut"] == "OK"
    assert fiche["visions"]["REEL"]["resultat"] == 400.0


def test_drill_down_logement_jusqua_lecriture(db):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, piece, "
        "libelle, origine_type, origine_id_opaque, statut, total_debit, total_credit) "
        "VALUES ('ECR-DD1','ACHATS','2026-06-05','2026-06','P','L','FACTURE','F1','VALIDEE',50,50)")
    conn.execute(
        "INSERT INTO ecriture_lignes (ecriture_id_opaque, ligne_num, compte, debit, credit, logement_id) "
        "VALUES ('ECR-DD1', 1, '606000', 50, 0, 'LOG_A1')")
    conn.commit()
    conn.close()
    lignes = ana.drill_down_logement("LOG_A1", mois="2026-06", db_path=db)
    assert len(lignes) == 1
    assert lignes[0]["ecriture_id_opaque"] == "ECR-DD1"


def test_drill_down_logement_sans_mouvement(db):
    assert ana.drill_down_logement("LOG_VIDE", db_path=db) == []


def test_mois_disponibles(resultats_files):
    assert ana.mois_disponibles() == ["2026-06"]


def test_mois_disponibles_source_absente(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", tmp_path / "absent.xlsx")
    reader.vider_cache()
    assert ana.mois_disponibles() == []
    reader.vider_cache()


def test_mesures_cumulees(resultats_files):
    m = ana.mesures_cumulees(vision="REEL")
    assert m["statut"] == "OK"
    assert m["resultat"] == 1100.0    # 700 (LOG_A1) + 400 (LOG_B1)
    assert m["nb_mois_couverts"] == 1
    assert m["mois_couverts"] == ["2026-06"]


def test_mesures_cumulees_source_absente(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", tmp_path / "absent.xlsx")
    reader.vider_cache()
    m = ana.mesures_cumulees()
    assert m["statut"] == ana.NON_DISPONIBLE
    reader.vider_cache()


def test_mois_precedent():
    assert ana.mois_precedent("2026-06") == "2026-05"
    assert ana.mois_precedent("2026-01") == "2025-12"
    assert ana.mois_precedent("") == ""

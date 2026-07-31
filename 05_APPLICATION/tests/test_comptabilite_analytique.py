"""Moteur analytique (Phase 2) — lecture des sorties Lot10, jamais un recalcul."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
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
    res = tmp_path / "RES.xlsx"
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
    ]
    par_prop = [
        {"mois": "2026-06", "proprietaire_id": "PROP_A", "total_produits": 1000.0,
         "total_charges": 300.0, "resultat": 700.0, "nb_flux": 5, "vision": "REEL"},
        {"mois": "2026-06", "proprietaire_id": "PROP_B", "total_produits": 500.0,
         "total_charges": 100.0, "resultat": 400.0, "nb_flux": 3, "vision": "REEL"},
    ]
    glob = [
        {"vision": "REEL", "total_produits": 1500.0, "total_charges": 400.0, "resultat": 1100.0,
         "commentaire_hc": "REEL=COMPTABLE+HC verifie (ecart=0.00 EUR)"},
        {"vision": "COMPTABLE", "total_produits": 900.0, "total_charges": 250.0, "resultat": 650.0,
         "commentaire_hc": "Vision comptable (IC)"},
        {"vision": "HORS_COMPTA", "total_produits": 600.0, "total_charges": 150.0, "resultat": 450.0,
         "commentaire_hc": "Flux HC presents"},
    ]
    _wb(res, {"PAR_MOIS_LOGEMENT": (LOG_COLS, par_logement),
             "PAR_MOIS_PROPRIETAIRE": (PROP_COLS, par_prop),
             "GLOBAL": (GLOBAL_COLS, glob)})
    monkeypatch.setattr(cfg, "MASTER_RESULTATS", res)
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

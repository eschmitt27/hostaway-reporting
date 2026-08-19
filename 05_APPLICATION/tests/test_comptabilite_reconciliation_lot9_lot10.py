"""Réconciliation Lot9 ↔ Lot10 (Bloc 2, mission Analytique/Résultats — fermeture des 8 réconciliations).

Ne recalcule jamais Lot10 : applique la même règle de filtrage (inclure_resultat_<vision>) que
`lot10_calculer_resultats.build_resultats` documente elle-même, sur le grain déjà produit par Lot9.
"""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.db.connection import get_db as _get_db
from app.readers import proprietaires_reglements_reader as reader
from app.services import comptabilite_reconciliations_service as recon

FLUX_COLS = ["flux_id", "ROW_HASH", "source_module", "source_table", "source_pk", "date_flux",
            "mois", "logement_id", "proprietaire_id", "associe_id", "type_flux_id", "sens",
            "montant", "code_impact", "inclure_resultat_reel", "inclure_resultat_comptable",
            "inclure_resultat_hors_compta", "statut_controle", "niveau_anomalie", "code_anomalie",
            "commentaire", "date_integration"]
_FLUX_DB_COLS = [c if c != "ROW_HASH" else "row_hash" for c in FLUX_COLS]

RES_LOG_COLS = ["mois", "logement_id", "proprietaire_id", "total_produits", "total_charges",
               "resultat", "nb_flux", "vision", "commentaire"]
RES_PROP_COLS = ["mois", "proprietaire_id", "total_produits", "total_charges", "resultat", "nb_flux", "vision"]
RES_GLOBAL_COLS = ["vision", "total_produits", "total_charges", "resultat", "commentaire_hc"]


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


def _flux_row(flux_id, mois, logement_id, sens, montant, *, vision_ok=True, row_hash=None):
    return {"flux_id": flux_id, "ROW_HASH": row_hash or flux_id, "mois": mois,
           "logement_id": logement_id, "sens": sens, "montant": montant,
           "source_module": "TEST", "source_table": "test_fixture",
           "inclure_resultat_reel": "OUI" if vision_ok else "NON",
           "inclure_resultat_comptable": "OUI" if vision_ok else "NON",
           "inclure_resultat_hors_compta": "NON"}


@pytest.fixture
def sources(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    res = tmp_path / "RES.xlsx"

    def build(flux_rows, res_log_rows, res_global_rows=None):
        # Lot9 ET Lot10 sont désormais SQLite : `res_log_rows` alimente `lot10_resultats` (grain
        # mois × logement × propriétaire × vision) sous un run ACTIF, plus l'onglet
        # PAR_MOIS_LOGEMENT d'un classeur. `res_global_rows` n'a plus de table : GLOBAL est dérivé
        # du grain fin par le reader, avec la règle du moteur — le paramètre reste accepté pour ne
        # pas réécrire les appels, mais n'est plus une source distincte qui pourrait diverger.
        conn = _get_db(db)
        try:
            conn.execute("DELETE FROM flux_unifies")
            conn.executemany(
                f"INSERT INTO flux_unifies ({', '.join(_FLUX_DB_COLS)}) "
                f"VALUES ({', '.join(['?'] * len(_FLUX_DB_COLS))})",
                [tuple(r.get(c) for c in FLUX_COLS) for r in flux_rows])
            conn.execute("DELETE FROM lot10_resultats")
            conn.execute("DELETE FROM lot10_runs")
            conn.execute("INSERT INTO lot10_runs (run_id, statut, actif) VALUES (?,?,1)",
                         ("L10-TEST", "SUCCES"))
            conn.executemany(
                "INSERT INTO lot10_resultats (run_id, mois, logement_id, proprietaire_id, vision, "
                "total_produits, total_charges, resultat, nb_flux, commentaire) "
                "VALUES ('L10-TEST',?,?,?,?,?,?,?,?,?)",
                [(r.get("mois"), r.get("logement_id"), r.get("proprietaire_id"),
                  r.get("vision"), r.get("total_produits"), r.get("total_charges"),
                  r.get("resultat"), r.get("nb_flux"), r.get("commentaire"))
                 for r in res_log_rows])
            conn.commit()
        finally:
            conn.close()
        monkeypatch.setattr(cfg, "DB_PATH", db)
        reader.vider_cache()

    yield build
    reader.vider_cache()


def test_source_absente(tmp_path, monkeypatch):
    db = tmp_path / "app.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "DB_PATH", db)
    res = recon.lot9_vs_lot10()
    assert res["statut"] == recon.ST_NON_DISPONIBLE
    assert "raison" in res


def test_egalite_exacte(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 1000.0),
         _flux_row("F2", "2026-06", "LOG_A1", "CHARGE", 300.0)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 1000.0, "total_charges": 300.0, "resultat": 700.0, "nb_flux": 2,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_OK
    assert res["montant_gauche"] == res["montant_droit"] == 700.0


def test_ecart_tolere(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 700.01)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_ECART_TOLERE
    assert res["ecart"] == 0.01


def test_ecart_superieur_a_controler(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 705.0)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_A_CONTROLER
    assert res["ecart"] == 5.0


def test_flux_lot9_sans_lot10(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 700.0),
         _flux_row("F2", "2026-06", "LOG_B1", "PRODUIT", 200.0)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_A_CONTROLER
    assert any("cle_sans_lot10" in d for d in res["detail"])


def test_resultat_lot10_sans_lot9(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 700.0)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""},
         {"mois": "2026-06", "logement_id": "LOG_C1", "proprietaire_id": "PROP_C",
          "total_produits": 500.0, "total_charges": 0.0, "resultat": 500.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_A_CONTROLER
    assert any("cle_sans_lot9" in d for d in res["detail"])


def test_doublon_detecte(sources):
    # `flux_id` est désormais clé primaire (migration 0043) : deux lignes avec le même flux_id ne
    # peuvent plus coexister en base, la duplication réelle est prévenue au stockage. Ce test garde
    # sa raison d'être pour un ROW_HASH dupliqué sous deux flux_id distincts (deux modules qui
    # dériveraient par erreur le même hash) — le cas que la détection applicative couvre encore.
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 700.0, row_hash="HASH1"),
         _flux_row("F2", "2026-06", "LOG_A1", "PRODUIT", 700.0, row_hash="HASH1")],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    # Le doublon est exclu de la somme (montants identiques par ailleurs) mais reste signalé :
    # un ROW_HASH dupliqué est une anomalie en soi, jamais masquée même si les totaux coïncident.
    assert res["statut"] == recon.ST_A_CONTROLER
    assert res["montant_gauche"] == res["montant_droit"] == 700.0
    assert any("doublon_lot9" in d for d in res["detail"])


def test_mauvais_sens_neutralisation_exclu(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 700.0),
         _flux_row("F2", "2026-06", "LOG_A1", "NEUTRALISATION", 999.0)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_OK   # le flux NEUTRALISATION ne doit pas fausser le total


def test_mauvaise_vision_filtre_correctement(sources):
    sources(
        [_flux_row("F1", "2026-06", "LOG_A1", "PRODUIT", 700.0, vision_ok=False)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    # Le flux n'a pas inclure_resultat_reel=OUI : gauche=0, écart avec Lot10=700 -> A_CONTROLER
    assert res["statut"] == recon.ST_A_CONTROLER
    assert res["montant_gauche"] == 0.0


def test_mauvaise_periode_filtre_correctement(sources):
    sources(
        [_flux_row("F1", "2026-05", "LOG_A1", "PRODUIT", 700.0)],
        [{"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
          "total_produits": 700.0, "total_charges": 0.0, "resultat": 700.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["montant_gauche"] == 0.0   # le flux de mai est exclu du filtre juin


def test_logement_sans_affectation_utilise_sentinelle_lot10(sources):
    sources(
        [_flux_row("F1", "2026-06", "", "CHARGE", 50.0)],
        [{"mois": "2026-06", "logement_id": "GLOBAL_NON_AFFECTE", "proprietaire_id": "",
          "total_produits": 0.0, "total_charges": 50.0, "resultat": -50.0, "nb_flux": 1,
          "vision": "REEL", "commentaire": ""}],
    )
    res = recon.lot9_vs_lot10(mois="2026-06", vision="REEL")
    assert res["statut"] == recon.ST_OK
    assert res["montant_gauche"] == res["montant_droit"] == -50.0

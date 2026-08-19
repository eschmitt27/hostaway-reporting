"""Écrans Résultats (Phase 3) — couche HTTP, fixtures Excel isolées pour Lot10."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
import fixtures_lot10 as fx
from app.readers import proprietaires_reglements_reader as reader
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


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
def resultats_files(tmp_db, monkeypatch):
    """Lot10 est SQLite (0044) : seul le grain fin est semé, dans la base déjà isolée par `_env`.

    PAR_MOIS_PROPRIETAIRE et GLOBAL sont dérivés par le reader (règle du moteur), plus des onglets
    posés à côté du détail — ils ne peuvent donc plus le contredire.
    """
    par_logement = [
        {"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 1000.0, "total_charges": 300.0, "resultat": 700.0, "nb_flux": 5,
         "vision": "REEL", "commentaire": ""},
        {"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 900.0, "total_charges": 250.0, "resultat": 650.0, "nb_flux": 4,
         "vision": "COMPTABLE", "commentaire": ""},
    ]
    fx.seeder(tmp_db, resultats=par_logement)
    reader.vider_cache()
    yield tmp_db
    reader.vider_cache()


@pytest.fixture(autouse=True)
def _env(tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    # Isolation : le worktree réel porte un vrai MASTER_CALC_Flux.xlsx (Lot9) — sans ce
    # monkeypatch, la réconciliation Lot9↔Lot10 lirait de vraies données au lieu d'être testée
    # de façon déterministe.
    monkeypatch.setattr(cfg, "MASTER_CALC_FLUX", tmp_path / "absent_flux.xlsx")
    return tmp_db


def test_dashboard_sans_source(client):
    assert client.get("/resultats").status_code == 200


def test_dashboard_avec_source(client, resultats_files):
    r = client.get("/resultats?mois=2026-06&vision=REEL")
    assert r.status_code == 200
    assert "700.00" in r.text


def test_mensuel(client, resultats_files):
    r = client.get("/resultats/mensuel?mois=2026-06&vision=REEL")
    assert r.status_code == 200
    assert "LOG_A1" in r.text and "PROP_A" in r.text


def test_cumule(client, resultats_files):
    r = client.get("/resultats/cumule?vision=REEL")
    assert r.status_code == 200
    assert "700.00" in r.text


def test_cumule_sans_source(client):
    assert client.get("/resultats/cumule").status_code == 200


def test_logements_liste(client, resultats_files):
    r = client.get("/resultats/logements?mois=2026-06&vision=REEL")
    assert r.status_code == 200 and "LOG_A1" in r.text


def test_logement_detail(client, resultats_files):
    r = client.get("/resultats/logements/LOG_A1?mois=2026-06")
    assert r.status_code == 200 and "700.00" in r.text


def test_logement_detail_inconnu(client, resultats_files):
    r = client.get("/resultats/logements/LOG_INCONNU?mois=2026-06")
    assert r.status_code == 200
    assert "NON_DISPONIBLE" in r.text


def test_proprietaires_liste(client, resultats_files):
    r = client.get("/resultats/proprietaires?mois=2026-06&vision=REEL")
    assert r.status_code == 200 and "PROP_A" in r.text


def test_proprietaire_detail(client, resultats_files):
    r = client.get("/resultats/proprietaires/PROP_A?mois=2026-06")
    assert r.status_code == 200
    assert "net propriétaire" in r.text.lower()


def test_plateformes_non_disponible(client):
    r = client.get("/resultats/plateformes")
    assert r.status_code == 200 and "NON_DISPONIBLE" in r.text


def test_fournisseurs(client, tmp_db):
    frs = frs_svc.creer("Fournisseur Resultats", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-RES-1",
                   "date_facture": "2026-06-10", "montant_ttc": 40.0}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    res_achat = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)
    compta.valider(res_achat["ecriture_id_opaque"], db_path=tmp_db)

    html = client.get("/resultats/fournisseurs").text
    assert frs in html


def test_charges(client):
    assert client.get("/resultats/charges").status_code == 200


def test_menages(client):
    assert client.get("/resultats/menages").status_code == 200


def test_comptabilite(client, tmp_db):
    frs = frs_svc.creer("Fournisseur Resultats Compta", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-RES-2",
                   "date_facture": "2026-06-10", "montant_ttc": 20.0}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)
    html = client.get("/resultats/comptabilite?mois=2026-06").text
    assert "ACHATS" in html


def test_reconciliation(client, resultats_files):
    r = client.get("/resultats/reconciliation?mois=2026-06")
    assert r.status_code == 200
    assert "NON_DISPONIBLE" in r.text   # au moins Lot9<->Lot10


def test_reconciliation_b_reste_ok_quel_que_soit_le_mois_filtre(client, tmp_db, monkeypatch):
    """Bloc 7 (recette navigateur) : détecté en réel — B compare Lot10 GLOBAL (tout le jeu de
    données) à l'Analytique. GLOBAL n'a pas de grain mensuel : filtrer par mois côté route cassait
    la comparaison (grains incompatibles) dès qu'il y avait plus d'un mois de données. B doit
    toujours comparer GLOBAL à la somme Analytique COMPLÈTE, jamais un sous-ensemble filtré.

    Deux mois semés (400 + 700) : le total GLOBAL de 1100 est désormais DÉRIVÉ de ces lignes
    (Lot10 SQLite, 0044) — le test ne peut plus poser un total qui ignorerait le détail, ce qui est
    précisément le piège que ce cas surveille.
    """
    par_logement = [
        {"mois": "2026-05", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 500.0, "total_charges": 100.0, "resultat": 400.0, "nb_flux": 2,
         "vision": "REEL", "commentaire": ""},
        {"mois": "2026-06", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
         "total_produits": 1000.0, "total_charges": 300.0, "resultat": 700.0, "nb_flux": 5,
         "vision": "REEL", "commentaire": ""},
    ]
    fx.seeder(tmp_db, resultats=par_logement)
    reader.vider_cache()

    r = client.get("/resultats/reconciliation?mois=2026-06")
    assert "B — Lot10 ↔ Analytique" in r.text
    idx = r.text.index("B — Lot10 ↔ Analytique")
    bloc_b = r.text[idx:idx + 600]
    assert "1100.00" in bloc_b and "1100.00" in bloc_b  # gauche = droit = cumul complet
    assert ">OK<" in bloc_b or "status-valide" in bloc_b
    reader.vider_cache()


def test_ligne_redirige_vers_ecriture(client, tmp_db):
    frs = frs_svc.creer("Fournisseur Resultats Ligne", "MAINTENANCE", db_path=tmp_db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-RES-3",
                   "date_facture": "2026-06-10", "montant_ttc": 15.0}, db_path=tmp_db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=tmp_db)
    res_achat = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=tmp_db)
    resp = client.get(f"/resultats/lignes/{res_achat['ecriture_id_opaque']}", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/comptabilite/ecritures/{res_achat['ecriture_id_opaque']}"


def test_export_csv(client, resultats_files):
    r = client.get("/resultats/export.csv?mois=2026-06&vision=REEL")
    assert r.status_code == 200
    assert "text/csv" in r.headers["content-type"]
    assert "LOG_A1" in r.text

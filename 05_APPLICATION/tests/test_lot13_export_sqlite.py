"""Lot13 — exports Power BI : terminus SQLite, reconstructibles, sans dépendance applicative.

Trois propriétés verrouillées ici :
  1. l'export lit SQLite et JAMAIS un classeur (interception globale `openpyxl.load_workbook`) ;
  2. supprimer les exports ne casse rien, et les régénérer redonne exactement les mêmes fichiers
     (§21 : un export est un artefact terminal, pas un étage de stockage) ;
  3. l'application ne lit rien dans `03_EXPORTS/` (§22).
Le filet anti-sensible et la table de renommages sont verrouillés eux aussi : ce sont eux qui
empêchent une donnée personnelle de sortir, et un renommage est précisément ce qui permettrait de
contourner le filet.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from app.db.connection import get_db
from app.services import lot13_export_service as svc

FICHIERS_ATTENDUS = {f"{nom}.csv" for nom, _, _ in svc.EXPORTS} | {"PBI_Dictionnaire_Colonnes.csv"}


@pytest.fixture(autouse=True)
def _interdire_tout_excel(monkeypatch):
    """Lot13 ne doit ouvrir aucun classeur : ses sources sont toutes en base."""
    import openpyxl

    def garde(chemin, *a, **kw):
        raise AssertionError(f"Lot13 ne doit ouvrir aucun classeur Excel : {chemin}")

    monkeypatch.setattr(openpyxl, "load_workbook", garde)


def _seed(db):
    """Jeu minimal mais réaliste : un run Lot10 et un run Lot12 actifs, plus du référentiel."""
    conn = get_db(db)
    try:
        conn.execute("INSERT INTO lot10_runs (run_id, statut, actif) VALUES ('L10-T','SUCCES',1)")
        conn.execute(
            "INSERT INTO lot10_resultats (run_id, mois, logement_id, proprietaire_id, vision, "
            "total_produits, total_charges, resultat, nb_flux) "
            "VALUES ('L10-T','2026-06','LOG_A','PROP_A','REEL', 1000, 300, 700, 5)")
        conn.execute(
            "INSERT INTO lot10_commissions (run_id, reservation_calc_id, logement_id, "
            "proprietaire_id, mois, payout_calcule, commission_conciergerie, "
            "preparation_canape_voyageurs, nuits) "
            "VALUES ('L10-T','RES-1','LOG_A','PROP_A','2026-06', 500, 75, 30, 3)")
        conn.execute(
            "INSERT INTO lot10_net_vue_mois (run_id, mois, proprietaire_id, total_payout_mois, "
            "nb_reservations) VALUES ('L10-T','2026-06','PROP_A', 500, 1)")
        conn.execute(
            "INSERT INTO reservations_datasets (dataset_id, etape, actif, statut) "
            "VALUES ('RDS-T','RESOLUES',1,'SUCCES')")
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, mois, "
            "logement_id, proprietaire_id, montant_retenu, nuits, statut_controle) "
            "VALUES ('RDS-T','RES-1','2026-06','LOG_A','PROP_A', 500, 3, 'VALIDE')")
        conn.execute("INSERT INTO lot12_runs (run_id, statut, actif) VALUES ('L12-T','SUCCES',1)")
        conn.execute(
            "INSERT INTO lot12_prefactures_lignes (run_id, facture_id, ligne_num, type_ligne, "
            "libelle, montant, bloc) "
            "VALUES ('L12-T','PREF-2026-06-PROP_A-LOG_A',1,'TOTAL_PAYOUT','Total payout',500,"
            "'EXPLOITATION')")
        conn.execute(
            "INSERT INTO controles_lot11_constats (ctrl_pk, code_controle, severity, message, "
            "statut_resolution) VALUES ('CTRL-1','VRBO_MONTANT_NON_RENSEIGNE','A_CONTROLER',"
            "'msg','OUVERT')")
        conn.execute(
            "INSERT INTO controles_lot11_constats (ctrl_pk, code_controle, severity, message, "
            "statut_resolution) VALUES ('CTRL-2','AUTRE','INFO','msg','OUVERT')")
        conn.execute(
            "INSERT INTO controles_lot11_constats_champs (ctrl_pk, mois, logement_id) "
            "VALUES ('CTRL-1','2026-06','LOG_A')")
        conn.execute(
            "INSERT INTO ref_setup_imports (import_id, horodatage, chemin_source, "
            "empreinte_source, statut) VALUES ('IMP','2026-01-01T00:00:00Z','t','t','IMPORTE')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, actif, statut_parc, "
            "import_id) VALUES ('LOG_A','Logement A','OUI','GERE','IMP')")
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, mode_facturation, "
            "actif, import_id) VALUES ('PROP_A','Dupont','MENSUEL','OUI','IMP')")
        conn.commit()
    finally:
        conn.close()


def _lire(p: Path):
    with open(p, encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f, delimiter=";"))


def test_export_sans_excel_base_vide(tmp_db, tmp_path):
    """Base vide : l'export réussit, sans jamais ouvrir de classeur."""
    res = svc.exporter(db_path=tmp_db, destination=tmp_path / "out")
    assert res["ok"], res


def test_export_produit_les_13_exports_et_le_dictionnaire(tmp_db, tmp_path):
    _seed(tmp_db)
    sortie = tmp_path / "out"
    res = svc.exporter(db_path=tmp_db, destination=sortie)
    assert res["ok"], res
    produits = {p.name for p in sortie.glob("*.csv")}
    assert FICHIERS_ATTENDUS <= produits, FICHIERS_ATTENDUS - produits
    assert (sortie / "PBI_Dictionnaire_Colonnes.csv").exists()


def test_controles_ouverts_filtre_a_controler_ouvert(tmp_db, tmp_path):
    """Même filtre que le legacy : `severity = A_CONTROLER` ET `statut_resolution = OUVERT`."""
    _seed(tmp_db)
    sortie = tmp_path / "out"
    svc.exporter(db_path=tmp_db, destination=sortie)
    lignes = _lire(sortie / "PBI_Controles_Ouverts.csv")
    assert lignes[0] == svc.WL_CONTROLES
    corps = lignes[1:]
    assert len(corps) == 1                       # CTRL-2 est INFO : exclu
    assert corps[0][0] == "CTRL-1"
    assert corps[0][3] == "2026-06"              # mois vient de l'extension 1-1 (0042)


def test_canape_renomme_a_la_frontiere(tmp_db, tmp_path):
    """§20 — la préparation canapé sort sous `montant_preparation_canape`, sans régression."""
    _seed(tmp_db)
    sortie = tmp_path / "out"
    svc.exporter(db_path=tmp_db, destination=sortie)
    entetes = _lire(sortie / "PBI_Commissions.csv")[0]
    assert "montant_preparation_canape" in entetes
    assert "preparation_canape_voyageurs" not in entetes
    valeurs = _lire(sortie / "PBI_Commissions.csv")[1]
    assert valeurs[entetes.index("montant_preparation_canape")] == "30"


def test_nombre_entier_sans_decimale(tmp_db, tmp_path):
    """Un REAL entier doit sortir comme le legacy l'écrivait : `500`, jamais `500.0`."""
    _seed(tmp_db)
    sortie = tmp_path / "out"
    svc.exporter(db_path=tmp_db, destination=sortie)
    entetes, *corps = _lire(sortie / "PBI_Resultats_Mensuels.csv")
    assert corps[0][entetes.index("total_produits")] == "1000"
    assert corps[0][entetes.index("resultat")] == "700"


def test_suppression_puis_regeneration_identique(tmp_db, tmp_path):
    """§21 — supprimer un export ne casse rien et il se régénère à l'identique depuis SQLite."""
    _seed(tmp_db)
    sortie = tmp_path / "out"
    assert svc.exporter(db_path=tmp_db, destination=sortie)["ok"]
    avant = {p.name: p.read_bytes() for p in sorted(sortie.glob("*.csv"))}
    assert avant

    for p in sortie.glob("*.csv"):
        p.unlink()
    assert list(sortie.glob("*.csv")) == []

    # L'application reste fonctionnelle sans aucun export sur le disque : on LIT les données
    # métier (jamais un recalcul, qui changerait le dataset que l'on compare juste après).
    from app.readers import proprietaires_reglements_reader as reader
    reader.vider_cache()
    assert reader.factures_entetes(db_path=tmp_db) is not None
    reader.vider_cache()

    assert svc.exporter(db_path=tmp_db, destination=sortie)["ok"]
    apres = {p.name: p.read_bytes() for p in sorted(sortie.glob("*.csv"))}
    assert apres == avant


def test_application_ne_lit_aucun_export(tmp_db, tmp_path, client):
    """§22 — écrans principaux fonctionnels alors que le répertoire d'exports n'existe pas."""
    import app.config as cfg

    absent = tmp_path / "exports_absents"
    orig = cfg.EXPORTS_POWERBI
    cfg.EXPORTS_POWERBI = absent
    try:
        assert not absent.exists()
        for route in ("/", "/logements", "/resultats"):
            assert client.get(route).status_code == 200, route
    finally:
        cfg.EXPORTS_POWERBI = orig


def test_filet_anti_sensible_actif():
    """Le filet doit refuser les noms de colonnes personnels — jamais affaibli."""
    for nom in ("email", "telephone", "iban", "adresse_proprietaire", "libelle_brut",
                "nom_voyageur", "token"):
        assert svc.SENSIBLE.search(nom), nom


def test_table_de_renommages_fermee():
    """Un renommage contourne le filet par construction : la table reste minuscule et explicite."""
    assert svc.RENOMMAGES_EXPORT == {
        "PBI_Commissions": {"preparation_canape_voyageurs": "montant_preparation_canape"}}


def test_abort_si_colonne_sensible(tmp_db, tmp_path, monkeypatch):
    """Si une colonne sensible apparaît, AUCUN fichier n'est écrit (ABORT, comme le legacy)."""
    _seed(tmp_db)
    monkeypatch.setitem(svc.RENOMMAGES_EXPORT, "PBI_Commissions",
                        {"payout_calcule": "email_proprietaire"})
    sortie = tmp_path / "out_abort"
    res = svc.exporter(db_path=tmp_db, destination=sortie)
    assert res["ok"] is False and res["code"] == svc.E_COLONNE_SENSIBLE
    assert not sortie.exists() or list(sortie.glob("*.csv")) == []

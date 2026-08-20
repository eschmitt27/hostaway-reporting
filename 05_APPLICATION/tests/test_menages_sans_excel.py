"""Ménages sans Excel — test bloquant (mission §7-8).

Preuve, pas déclaration : `openpyxl.load_workbook` est intercepté pour lever explicitement dès
qu'un appelant tente d'ouvrir l'un des deux MASTERS permanents historiquement requis
(`MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`, `M04_MENAGES_PowerQuery.xlsx`). Si l'un des
parcours ci-dessous les rouvrait encore, ce test échouerait avec l'appel fautif dans la trace —
pas seulement « les fichiers existent ailleurs », mais qu'ils ne sont RÉELLEMENT plus lus.

Tous les datasets SQLite nécessaires sont seedés explicitement : ce test isole la dépendance
Excel, pas la disponibilité des données.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db

INTERDITS = ("MASTER_FACT_HA_CleaningTasks_Discovery.xlsx", "M04_MENAGES_PowerQuery.xlsx")


@pytest.fixture(autouse=True)
def _interdire_masters_permanents(monkeypatch):
    """Toute tentative d'ouverture d'un des deux MASTERS interdits lève immédiatement."""
    import openpyxl

    original = openpyxl.load_workbook

    def garde(chemin, *a, **kw):
        nom = Path(chemin).name
        if nom in INTERDITS:
            raise AssertionError(f"MASTER permanent interdit rouvert : {nom} ({chemin})")
        return original(chemin, *a, **kw)

    monkeypatch.setattr(openpyxl, "load_workbook", garde)
    # `menages_reader`/`app.readers.excel_reader` importent `openpyxl` directement à leur module :
    # patcher aussi la référence locale pour couvrir ces imports déjà résolus.
    import app.readers.excel_reader as excel_reader
    if hasattr(excel_reader, "openpyxl"):
        monkeypatch.setattr(excel_reader.openpyxl, "load_workbook", garde)


def _seed_complet(db_path, mois="2026-05"):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO menages_taches_enrichies (task_id, mois, logement_id, status, "
            "statut_menage, compte_comme_menage) VALUES (?,?,?,?,?,?)",
            ("HA-NOXL-001", mois, "LOG_0001", "completed", "réalisé", "OUI"))
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nom_intervenant, type_intervenant, nb_menages, statut_controle) "
            "VALUES (?,?,?,?,?,?,?)",
            (mois, "LOG_0001", "INT_0002", "Kheira", "INTERNE", 1, "VALIDE"))
        conn.execute(
            "INSERT INTO menages_rapprochement (mois, logement_id, intervenant_id, "
            "nb_menages_tasks_hostaway_completed, nb_menages_declares_interne_m04, "
            "total_menages_declares, ecart, statut_controle) VALUES (?,?,?,?,?,?,?,?)",
            (mois, "LOG_0001", "INT_0002", 1, 1, 1, 0, "VALIDE"))
        conn.execute(
            "INSERT INTO menages_gainperte (mois, logement_id, intervenant_id, statut_controle) "
            "VALUES (?,?,?,?)", (mois, "LOG_0001", "INT_0002", "VALIDE"))
        conn.execute(
            "INSERT INTO menages_cout_complet (mois, logement_id, intervenant_id, statut_controle) "
            "VALUES (?,?,?,?)", (mois, "LOG_0001", "INT_0002", "VALIDE"))
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc, devise, statut, source) VALUES (?,?,?,?,?,?,?,?)",
            ("FAC-NOXL-001", "FRS-NOXL-001", "REF-NOXL-001", f"{mois}-15", 55.0, "EUR",
             "A_CONTROLER", "TEST"))
        conn.execute(
            "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
            "logement_id, montant_ttc, source) VALUES (?,?,?,?,?,?)",
            ("FLM-NOXL-001", "FAC-NOXL-001", "MENAGE_EXTERNE", "LOG_0001", 55.0, "PDF_EXTRACTION"))
        conn.commit()
    finally:
        conn.close()


# ── menages_reader : les 10 sources réelles ──────────────────────────────────

def test_menages_reader_sans_excel(tmp_db):
    from app.readers import menages_reader as reader
    reader.vider_cache()
    _seed_complet(tmp_db)
    import app.config as cfg2
    monkeypatch_db = tmp_db
    cfg2.DB_PATH = monkeypatch_db
    try:
        assert reader.hostaway_taches().etat.etat == reader.ETAT_OK
        assert reader.hostaway_comptage().etat.etat == reader.ETAT_OK
        assert reader.internes().etat.etat == reader.ETAT_OK
        assert reader.externes().etat.etat == reader.ETAT_OK
        assert reader.rapprochement().etat.etat == reader.ETAT_OK
        assert reader.gainperte().etat.etat == reader.ETAT_OK
        assert reader.cout_complet().etat.etat == reader.ETAT_OK
        # diagnostic_pdf/controles_rapprochement/controles_lot11 : vides ici (non seedés), mais
        # l'état doit rester un état SQLite propre (VIDE/FICHIER_ABSENT), jamais un plantage Excel.
        reader.diagnostic_pdf()
        reader.controles_rapprochement()
        reader.controles_lot11()
    finally:
        reader.vider_cache()


def test_menages_service_parcours_metier_sans_excel(tmp_db, monkeypatch):
    """CleaningTask → rapprochement/déclaration → contrôle, bout en bout, sans Excel."""
    from app.readers import menages_reader as reader
    from app.services import menages_service as svc
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    reader.vider_cache()
    _seed_complet(tmp_db)

    rows = svc.load_reconciliation_rows(mois="2026-05")
    assert rows["status"] == "OK"
    assert any(r["logement_id"] == "LOG_0001" for r in rows["rows"])

    detail = svc.load_reconciliation_detail("2026-05", "LOG_0001", "INT_0002")
    assert detail["status"] == "OK"
    assert detail["hostaway"]["taches"], "tâche Hostaway attendue dans le détail"
    assert detail["interne"]["lignes"], "déclaration interne attendue dans le détail"

    # `load_summary` n'est pas planté par l'absence d'Excel (c'est tout ce qui compte ici : ce test
    # isole la dépendance Excel, pas la disponibilité complète des 8 sources du reader).
    summary = svc.load_summary("2026-05")
    assert summary["etat_global"] in ("CONFORME", "A_CONTROLER", "SOURCE_INCOMPLETE")
    reader.vider_cache()


# ── menages_recalcul_service : précondition dataset, pas fichier ────────────

def test_menages_recalcul_sans_excel(tmp_db, monkeypatch, tmp_path):
    from app.services import menages_recalcul_service as rc
    _seed_complet(tmp_db)
    monkeypatch.setattr(cfg, "MENAGES_RECALC_WORKSPACE", tmp_path / "ws")
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    prep = rc.preparer(rc.MODE_COPIES, mois="2026-05")
    assert all(d["present"] for d in prep["datasets"]), prep["datasets"]


# ── controles_runner_service : verdict Lot11 relu depuis SQLite ─────────────

def test_controles_runner_ne_lit_pas_les_masters_menages(tmp_db):
    """Le verdict Lot11 est RECALCULÉ en SQLite, sans ouvrir ni CleaningTasks ni M04 ni le
    classeur de contrôles : il n'existe plus qu'une seule implémentation des règles Lot11."""
    from app.services import controles_lot11_service as l11
    resultat = l11.construire(db_path=tmp_db)
    assert resultat["ok"], resultat


# ── menages_chaine_service : préflight + workspace sans master permanent ────

def test_menages_chaine_preflight_sans_master_permanent(tmp_db):
    from app.services import menages_chaine_service as chaine
    _seed_complet(tmp_db)
    plan = chaine.preparer_chaine(chaine.MODE_COPIES, db_path=tmp_db)
    assert plan["coeur_absents"] == [], plan["coeur_absents"]
    # Les deux fichiers interdits ne figurent plus dans les préconditions fichier.
    noms_coeur = [s["nom"] for s in plan["sources_coeur"]]
    assert INTERDITS[0] not in noms_coeur
    assert INTERDITS[1] not in noms_coeur

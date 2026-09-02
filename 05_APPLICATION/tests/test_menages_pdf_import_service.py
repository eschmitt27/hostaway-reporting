"""Import par lot des factures PDF ménage externe (mission « Actualisation Ménages »).

Utilise les 2 PDF réels de juillet (copies uniquement, jamais les originaux) — squelette de test
identique à `test_facture_menage_pdf_service.py` : écriture activée par un flag de test, jamais
`MENAGES_REAL_RECALC_ENABLED` (non concerné ici).
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pytest.importorskip("fitz")
pytest.importorskip("lib_menages_externes_pdf")

from app.services import menages_pdf_import_service as svc  # noqa: E402

REAL_PDF_DIR = cfg.MENAGES_PDF_DIR
AISSATA = REAL_PDF_DIR / "Facture juillet Aissata.pdf"
MOUNIR = REAL_PDF_DIR / "Facture juillet Mounir.pdf"
pdf_reels = pytest.mark.skipif(not (AISSATA.exists() and MOUNIR.exists()),
                               reason="PDF réels absents d'un checkout propre")


@pytest.fixture
def dossier(tmp_path):
    d = tmp_path / "Factures_PDF"
    d.mkdir()
    shutil.copy2(AISSATA, d / AISSATA.name)
    shutil.copy2(MOUNIR, d / MOUNIR.name)
    return d


def test_apercu_dossier_absent(tmp_path, tmp_db):
    res = svc.apercu(dossier=tmp_path / "absent", db_path=tmp_db)
    assert res["nb_detectes"] == 0
    assert res["dossier_present"] is False


@pdf_reels
def test_apercu_pdf_presents_tous_nouveaux(dossier, tmp_db):
    res = svc.apercu(dossier=dossier, db_path=tmp_db)
    assert res["nb_detectes"] == 2
    assert res["nb_nouveaux"] == 2
    assert res["nb_deja_importes"] == 0


@pdf_reels
def test_importer_sans_flags_comptables_cree_des_factures_a_controler_sans_comptabilite(
        dossier, tmp_db, monkeypatch):
    """Décision produit : importer un PDF est une écriture OPÉRATIONNELLE (niveau A). Elle doit
    aboutir en production normale, SANS les flags comptables — mais sans rien comptabiliser.
    L'intention de sécurité de l'ancien test est conservée, seul le mécanisme change : ce qui doit
    rester à zéro, ce n'est pas le nombre de factures, c'est la comptabilité."""
    import sqlite3

    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", False)
    res = svc.importer_nouveaux(dossier=dossier, db_path=tmp_db, acteur="test")
    assert res["nb_detectes"] == 2
    assert res["nb_importees"] == 2
    assert res["nb_ecriture_desactivee"] == 0

    conn = sqlite3.connect(str(tmp_db))
    try:
        statuts = {r[0] for r in conn.execute("SELECT statut FROM factures")}
        assert statuts == {"A_CONTROLER"}, statuts
        # Le cœur du garde-fou : un import ne comptabilise rien, ne paie rien, ne bouge aucun compte.
        for table in ("ecritures", "charges", "banque_mouvements", "reglements_fournisseurs"):
            existe = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone()[0]
            if existe:
                assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0, table
    finally:
        conn.close()


@pdf_reels
def test_importer_refuse_si_niveau_operationnel_off(dossier, tmp_db, monkeypatch):
    """Le niveau A reste un interrupteur : coupé, les PDF sont détectés mais aucune facture créée."""
    monkeypatch.setattr(cfg, "ECRITURE_OPERATIONNELLE_ENABLED", False)
    res = svc.importer_nouveaux(dossier=dossier, db_path=tmp_db)
    assert res["nb_detectes"] == 2
    assert res["nb_importees"] == 0
    assert res["nb_ecriture_desactivee"] == 2
    apercu = svc.apercu(dossier=dossier, db_path=tmp_db)
    assert apercu["nb_deja_importes"] == 0  # aucune facture créée : rien à considérer « traité »


@pdf_reels
def test_importer_deux_factures_reelles_creees(dossier, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    res = svc.importer_nouveaux(dossier=dossier, db_path=tmp_db, acteur="test")
    assert res["ok"] is True
    assert res["nb_detectes"] == 2
    assert res["nb_importees"] == 2
    assert res["nb_deja_importees"] == 0


@pdf_reels
def test_importer_idempotent_deuxieme_passage_pas_de_doublon(dossier, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    premier = svc.importer_nouveaux(dossier=dossier, db_path=tmp_db, acteur="test")
    assert premier["nb_importees"] == 2

    second = svc.importer_nouveaux(dossier=dossier, db_path=tmp_db, acteur="test")
    assert second["nb_importees"] == 0
    assert second["nb_deja_importees"] == 2

    from app.db.connection import get_db
    conn = get_db(tmp_db)
    try:
        nb = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()
    assert nb == 2  # jamais 4 : le deuxième passage n'a rien dupliqué


@pdf_reels
def test_apercu_reflete_les_factures_deja_importees(dossier, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    svc.importer_nouveaux(dossier=dossier, db_path=tmp_db, acteur="test")
    res = svc.apercu(dossier=dossier, db_path=tmp_db)
    assert res["nb_nouveaux"] == 0
    assert res["nb_deja_importes"] == 2

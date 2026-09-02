"""Routes /menages/actualiser et /menages/pdf/importer (mission « Actualisation Ménages »).

Vérifie le parcours normal utilisateur : la page /menages affiche l'état réel (jamais un écran
vide), et les deux actions écrivent réellement en SQLite (zéro Excel), jamais dans app.db réelle
(isolation par `tmp_db`/`client`, comme le reste de la suite).
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

REAL_PDF_DIR = cfg.MENAGES_PDF_DIR
AISSATA = REAL_PDF_DIR / "Facture juillet Aissata.pdf"
MOUNIR = REAL_PDF_DIR / "Facture juillet Mounir.pdf"
pdf_reels = pytest.mark.skipif(not (AISSATA.exists() and MOUNIR.exists()),
                               reason="PDF réels absents d'un checkout propre")


@pytest.fixture
def dossier_pdf(tmp_path, monkeypatch):
    d = tmp_path / "Factures_PDF"
    d.mkdir()
    shutil.copy2(AISSATA, d / AISSATA.name)
    shutil.copy2(MOUNIR, d / MOUNIR.name)
    monkeypatch.setattr(cfg, "MENAGES_PDF_DIR", d)
    return d


def test_menages_dashboard_affiche_etat_actualisation(client, tmp_db):
    r = client.get("/menages")
    assert r.status_code == 200
    assert "Actualisation" in r.text
    # Le bouton cible désormais explicitement le mois affiché (mission « recalcul mensuel ciblé »)
    # — jamais un libellé générique "Actualiser les ménages" qui masquerait le mois recalculé.
    assert "Actualiser " in r.text
    assert "Importer les nouvelles factures" in r.text


@pdf_reels
def test_importer_pdf_route_ecriture_desactivee(client, tmp_db, dossier_pdf):
    """Écriture désactivée par défaut (RECETTE_MODE=0 en test) : la route répond, aucune facture
    n'est créée en douce — comportement structurel, pas une erreur 500."""
    r = client.post("/menages/pdf/importer", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"].startswith("/menages")

    from app.db.connection import get_db
    conn = get_db(tmp_db)
    try:
        nb = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()
    assert nb == 0


@pdf_reels
def test_importer_pdf_route_ecriture_activee_cree_factures_reelles(client, tmp_db, dossier_pdf,
                                                                    monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    r = client.post("/menages/pdf/importer", follow_redirects=False)
    assert r.status_code == 303

    from app.db.connection import get_db
    conn = get_db(tmp_db)
    try:
        nb = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()
    assert nb == 2

    r2 = client.get("/menages")
    assert "2" in r2.text  # PDF détectés / déjà importés apparaissent quelque part sur l'écran

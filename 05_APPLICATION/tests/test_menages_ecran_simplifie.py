"""Écran Ménages allégé et compteurs honnêtes — recette utilisateur n°4, §17 à §20.

Ce que ces tests protègent :
  · §20 — le dossier et la base ne sont plus opposés par deux nombres incomparables : l'écran
    publie un rapprochement, et distingue un PDF présent d'un PDF retiré du dossier ;
  · le compteur des ménages attendus porte sur la PÉRIODE affichée, et non sur tout l'historique ;
  · le détail technique a bien été déplacé dans Observabilité, et pas seulement supprimé.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import app.config as cfg
from app.db.connection import get_db
from app.main import app
from app.services import menages_service as svc


@pytest.fixture()
def dossier_pdf(tmp_path, monkeypatch):
    dossier = tmp_path / "MenagesExternes"
    dossier.mkdir()
    for nom in ("05-26-Aissata.pdf", "05-26-Mounir.pdf", "07-26-Imrane.pdf"):
        (dossier / nom).write_bytes(b"%PDF-1.4 fixture")
    monkeypatch.setattr(cfg, "MENAGES_PDF_DIR", dossier, raising=False)
    return dossier


@pytest.fixture()
def base(tmp_db, dossier_pdf):
    """Trois fichiers présents : un rattaché, un en doublon, un jamais importé.
    Plus une facture dont le PDF a été retiré du dossier — le cas qui a motivé la mission."""
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref,"
                     " date_facture, montant_ttc, statut, source) "
                     "VALUES ('FAC-OK','FRS-1','2026-37','2026-05-31',1439.0,'A_CONTROLER',"
                     "'PDF_EXTRACTION')")
        conn.execute("INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref,"
                     " date_facture, montant_ttc, statut, source) "
                     "VALUES ('FAC-ORPHELINE','FRS-1','2026-40','2026-07-31',1056.0,'A_CONTROLER',"
                     "'PDF_EXTRACTION')")
        for nom, statut, doublon, facture in (
                ("05-26-Aissata.pdf", "OK", None, "FAC-OK"),
                ("05-26-Mounir.pdf", "OK", "FAC-OK", None),
                ("Facture juillet Aissata.pdf", "OK", None, "FAC-ORPHELINE")):
            conn.execute(
                "INSERT INTO facture_pdf_diagnostics (nom_fichier, format_detecte, "
                "statut_extraction, doublon_de, facture_id_opaque) VALUES (?,?,?,?,?)",
                (nom, "AISSATA", statut, doublon, facture))
        conn.commit()
    finally:
        conn.close()
    return tmp_db


@pytest.fixture()
def client(base, tmp_path, monkeypatch):
    from app.services import orchestrateur_service as orch
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setattr(orch, "marquer_runs_interrompus", lambda **k: [])
    with TestClient(app) as c:
        yield c


# ── §20 : le dossier et la base, rapprochés ─────────────────────────────────────────────────────

def test_le_rapprochement_distingue_presents_rattaches_et_non_exploites(base):
    etat = svc.charger_etat_actualisation(db_path=base)
    rap = etat["pdf"]["rapprochement"]

    assert rap["presents"] == 3
    assert rap["rattaches"] == 1
    motifs = {f["fichier"]: f["motif"] for f in rap["non_exploites"]}
    assert set(motifs) == {"05-26-Mounir.pdf", "07-26-Imrane.pdf"}
    assert "doublon" in motifs["05-26-Mounir.pdf"]
    assert "pas encore importé" in motifs["07-26-Imrane.pdf"]


def test_une_facture_dont_le_pdf_a_disparu_est_signalee(base):
    """Le cas qui a motivé la mission : la facture reste, le fichier n'est plus là."""
    rap = svc.charger_etat_actualisation(db_path=base)["pdf"]["rapprochement"]
    assert rap["factures_sans_fichier"] == ["Facture juillet Aissata.pdf"]


def test_l_ecran_publie_le_rapprochement_et_non_deux_nombres_opposes(client):
    page = client.get("/menages")
    assert page.status_code == 200
    assert "<strong>3</strong> fichiers dans le dossier" in page.text
    assert "<strong>1</strong> rattaché à une facture" in page.text
    assert "<strong>2</strong> non exploité" in page.text
    assert "dont le PDF n'est plus dans le dossier" in page.text
    # Les deux nombres incomparables d'avant ne doivent plus être mis face à face.
    assert "fichier(s) dans le dossier" not in page.text


# ── La période affichée est celle des compteurs ─────────────────────────────────────────────────

def test_les_menages_attendus_portent_sur_la_periode_affichee(client, monkeypatch):
    """Sans ?mois dans l'URL, la route passait une chaîne vide au comptage des origines : l'écran
    titrait « septembre 2026 » et affichait les attendus de TOUT l'historique."""
    from app.routes import menages as route_menages

    recus = []
    monkeypatch.setattr(route_menages.origines_svc, "origines",
                        lambda mois, logement_id=None: recus.append(mois) or {})
    monkeypatch.setattr(svc, "periode_par_defaut", lambda: "2026-05")

    client.get("/menages")
    assert recus == ["2026-05"], "le comptage doit recevoir la période réellement affichée"


# ── §17 : le technique est déplacé, pas seulement supprimé ──────────────────────────────────────

def test_observabilite_porte_le_detail_retire_de_l_ecran_menages(client):
    page = client.get("/observabilite/runs")
    assert page.status_code == 200
    assert "diagnostic fichier par fichier" in page.text
    assert "aucun identifiant Hostaway n'est stocké" in page.text
    # Un PDF retiré du dossier se distingue d'un PDF présent — impossible à voir auparavant.
    assert "Retiré du dossier" in page.text
    assert "Présent" in page.text

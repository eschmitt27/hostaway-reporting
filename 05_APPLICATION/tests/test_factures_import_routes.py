"""Couche HTTP de l'import PDF de facture."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs

fitz = pytest.importorskip("fitz", reason="PyMuPDF requis")


def _pdf(chemin, texte="Facture test") -> bytes:
    doc = fitz.open()
    doc.new_page().insert_text((72, 100), texte, fontsize=11)
    doc.save(str(chemin))
    doc.close()
    return chemin.read_bytes()


@pytest.fixture
def env(tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "DRYRUNS_DIR", tmp_path / "dryruns")
    f = frs.creer("Fournisseur HTTP PDF", "MENAGE", acteur="t", db_path=tmp_db)
    return {"db": tmp_db, "frs": f["fournisseur_id_opaque"], "tmp": tmp_path}


def test_page_import_affiche_le_formulaire(client, env):
    html = client.get("/factures/importer").text
    assert 'action="/factures/importer/previsualiser"' in html
    assert 'enctype="multipart/form-data"' in html
    assert "jamais d'OCR" in html


def test_liste_expose_le_lien_import(client, env):
    assert "/factures/importer" in client.get("/factures").text


def test_fichier_manquant_redirige(client, env):
    r = client.post("/factures/importer/previsualiser", data={}, follow_redirects=False)
    assert r.status_code == 303
    assert "Aucun fichier" in client.get(r.headers["location"]).text


def test_non_pdf_refuse(client, env):
    files = {"fichier": ("x.csv", b"a;b", "text/csv")}
    r = client.post("/factures/importer/previsualiser", files=files, follow_redirects=False)
    assert "Format de fichier non reconnu" in client.get(r.headers["location"]).text


def test_parcours_complet_previsualiser_puis_confirmer(client, env):
    contenu = _pdf(env["tmp"] / "f.pdf")
    files = {"fichier": ("f.pdf", contenu, "application/pdf")}
    r = client.post("/factures/importer/previsualiser", files=files,
                    data={"fournisseur_id_opaque": env["frs"]}, follow_redirects=False)
    assert r.status_code == 303
    loc = r.headers["location"]
    assert "/factures/importer/previsualisation/" in loc

    html = client.get(loc).text
    assert 'data-testid="bloc-confirmation"' in html
    assert 'data-testid="incertitudes"' in html      # format non reconnu -> incertitudes affichées
    assert "La confirmation crée la <strong>facture</strong> uniquement" in html

    token = loc.rsplit("/", 1)[-1]
    r2 = client.post(f"/factures/importer/confirmer/{token}", data={
        "fournisseur_id_opaque": env["frs"], "facture_ref": "FA-HTTP-PDF",
        "date_facture": "2026-06-10", "montant_ttc": "300.00", "acteur": "recette",
    }, follow_redirects=False)
    assert r2.status_code == 303
    html2 = client.get(r2.headers["location"]).text
    assert "Facture importée depuis le PDF" in html2
    assert "FA-HTTP-PDF" in html2


def test_confirmation_invalide_revient_sur_la_previsualisation(client, env):
    contenu = _pdf(env["tmp"] / "g.pdf")
    files = {"fichier": ("g.pdf", contenu, "application/pdf")}
    r = client.post("/factures/importer/previsualiser", files=files,
                    data={"fournisseur_id_opaque": env["frs"]}, follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]
    r2 = client.post(f"/factures/importer/confirmer/{token}",
                     data={"fournisseur_id_opaque": env["frs"], "facture_ref": "",
                           "montant_ttc": "0"}, follow_redirects=False)
    assert "/factures/importer/previsualisation/" in r2.headers["location"]
    assert "erreur=" in r2.headers["location"]


def test_previsualisation_token_inconnu_404(client, env):
    assert client.get("/factures/importer/previsualisation/inexistant").status_code == 404


def test_import_refuse_si_flags_off(client, env, monkeypatch):
    contenu = _pdf(env["tmp"] / "h.pdf")
    files = {"fichier": ("h.pdf", contenu, "application/pdf")}
    r = client.post("/factures/importer/previsualiser", files=files,
                    data={"fournisseur_id_opaque": env["frs"]}, follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]

    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    r2 = client.post(f"/factures/importer/confirmer/{token}",
                     data={"fournisseur_id_opaque": env["frs"], "facture_ref": "FA-OFF",
                           "montant_ttc": "10"}, follow_redirects=False)
    assert "Écriture désactivée" in client.get(r2.headers["location"]).text
    assert fact.lister() == []

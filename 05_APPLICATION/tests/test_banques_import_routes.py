"""Couche HTTP du module Banque — import (upload → prévisualisation → confirmation) et
rapprochement depuis la fiche mouvement. Complète `test_banques_import.py` (service) et
`test_banques_rapprochement.py` (service) par des tests de bout en bout HTTP."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.services import banques_controle_service as ctrl_svc

CSV_VALIDE = (
    "Date operation;Libelle;Debit;Credit\n"
    "05/06/2026;VIR HOSTAWAY PAYOUT;;850,00\n"
).encode("utf-8")


@pytest.fixture
def ref(tmp_path, monkeypatch):
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    ctrl_svc.vider_cache()
    yield p
    ctrl_svc.vider_cache()


def _lignes(p):
    if not p.exists():
        return []
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
    ws = wb["NORM_Banque"]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if len(rows) <= 1:
        return []
    hdr = rows[0]
    return [dict(zip(hdr, r)) for r in rows[1:]]


# ── Page d'import ─────────────────────────────────────────────────────────────

def test_page_import_affiche_le_formulaire(client, ref):
    html = client.get("/banques-caisse/importer").text
    assert 'action="/banques-caisse/importer/previsualiser"' in html
    assert 'enctype="multipart/form-data"' in html


def test_import_bouton_desactive_si_flags_off(client, ref, monkeypatch):
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", False)
    html = client.get("/banques-caisse/importer").text
    assert "Écriture désactivée" in html


# ── Prévisualisation ──────────────────────────────────────────────────────────

def test_previsualiser_puis_confirmer_via_http(client, ref):
    files = {"fichier": ("releve.csv", CSV_VALIDE, "text/csv")}
    data = {"compte_id": "CM_TEST"}
    r = client.post("/banques-caisse/importer/previsualiser", files=files, data=data,
                    follow_redirects=False)
    assert r.status_code == 303
    assert "/banques-caisse/importer/previsualisation/" in r.headers["location"]

    r2 = client.get(r.headers["location"])
    assert r2.status_code == 200
    assert "Mouvements valides" in r2.text
    assert "1" in r2.text                        # 1 ligne valide

    token = r.headers["location"].rsplit("/", 1)[-1]
    r3 = client.post(f"/banques-caisse/importer/confirmer/{token}",
                     data={"acteur": "recette"}, follow_redirects=False)
    assert r3.status_code == 200
    assert "Import réussi" in r3.text
    assert len(_lignes(ref)) == 1


def test_previsualisation_token_inconnu_404(client, ref):
    r = client.get("/banques-caisse/importer/previsualisation/token-inexistant")
    assert r.status_code == 404


def test_import_fichier_manquant_redirige_avec_erreur(client, ref):
    r = client.post("/banques-caisse/importer/previsualiser", data={"compte_id": "CM_TEST"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Aucun fichier" in html


def test_import_format_inconnu_redirige_avec_erreur(client, ref):
    files = {"fichier": ("releve.pdf", b"peu importe", "application/pdf")}
    r = client.post("/banques-caisse/importer/previsualiser", files=files,
                    data={"compte_id": "CM_TEST"}, follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Format de fichier non reconnu" in html


def test_confirmer_refuse_si_flags_off(client, ref, monkeypatch):
    files = {"fichier": ("releve.csv", CSV_VALIDE, "text/csv")}
    r = client.post("/banques-caisse/importer/previsualiser", files=files,
                    data={"compte_id": "CM_TEST"}, follow_redirects=False)
    token = r.headers["location"].rsplit("/", 1)[-1]

    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", False)
    r2 = client.post(f"/banques-caisse/importer/confirmer/{token}",
                     data={"acteur": "recette"}, follow_redirects=False)
    assert r2.status_code == 303
    html = client.get(r2.headers["location"]).text
    assert "Écriture désactivée" in html
    assert not ref.exists()

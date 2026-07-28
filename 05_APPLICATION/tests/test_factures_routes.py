"""Couche HTTP Factures / Règlements : liste, création, fiche, statut, charge, règlement."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.services import factures_service as svc
from app.services import fournisseurs_referentiel_service as frs


@pytest.fixture
def env(tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    f = frs.creer("Fournisseur Ménage Test", "MENAGE", acteur="recette", db_path=tmp_db)
    return {"db": tmp_db, "fournisseur": f["fournisseur_id_opaque"]}


def _creer_facture(env, ref="FA-HTTP-001", ttc=120.0):
    r = svc.creer({"fournisseur_id_opaque": env["fournisseur"], "facture_ref": ref,
                   "date_facture": "2026-06-01", "date_echeance": "2026-07-01",
                   "montant_ttc": ttc}, acteur="recette", db_path=env["db"])
    assert r["ok"], r
    return r["facture_id_opaque"]


# ── Navigation et listes ─────────────────────────────────────────────────────

def test_nav_expose_factures(client, env):
    html = client.get("/factures").text
    assert 'href="/factures"' in html
    assert "Factures fournisseurs" in html


def test_liste_factures_affiche_les_factures(client, env):
    _creer_facture(env)
    html = client.get("/factures").text
    assert "FA-HTTP-001" in html
    assert "120.00" in html


def test_page_a_payer(client, env):
    _creer_facture(env)
    html = client.get("/factures/a-payer").text
    assert "Factures à payer" in html
    assert "FA-HTTP-001" in html


def test_page_reglements(client, env):
    html = client.get("/reglements").text
    assert "Règlements fournisseurs" in html


# ── Création ──────────────────────────────────────────────────────────────────

def test_page_nouvelle_facture(client, env):
    html = client.get("/factures/nouvelle").text
    assert 'action="/factures"' in html
    assert "Fournisseur Ménage Test" in html


def test_creer_facture_via_http(client, env):
    r = client.post("/factures", data={
        "fournisseur_id_opaque": env["fournisseur"], "facture_ref": "FA-WEB-1",
        "date_facture": "2026-06-01", "date_echeance": "2026-07-01", "montant_ttc": "250.00",
    }, follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Facture enregistrée" in html
    assert "FA-WEB-1" in html


def test_creer_facture_invalide_renvoie_erreur(client, env):
    r = client.post("/factures", data={
        "fournisseur_id_opaque": env["fournisseur"], "facture_ref": "", "montant_ttc": "0",
    }, follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"].startswith("/factures/nouvelle?erreur=")


def test_creer_refuse_si_flags_off(client, env, monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    r = client.post("/factures", data={
        "fournisseur_id_opaque": env["fournisseur"], "facture_ref": "FA-OFF", "montant_ttc": "10",
    }, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Écriture désactivée" in html


# ── Fiche ─────────────────────────────────────────────────────────────────────

def test_fiche_facture(client, env):
    fid = _creer_facture(env)
    html = client.get(f"/factures/{fid}").text
    assert "FA-HTTP-001" in html
    assert "Solde restant" in html
    assert "Enregistrer un règlement" in html


def test_fiche_inconnue_404(client, env):
    assert client.get("/factures/FAC-INEXISTANT").status_code == 404


def test_changer_statut_via_http(client, env):
    fid = _creer_facture(env)
    r = client.post(f"/factures/{fid}/statut", data={"statut": svc.ST_VALIDEE},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Statut mis à jour" in html
    assert svc.ST_VALIDEE in html


def test_lier_charge_via_http(client, env):
    fid = _creer_facture(env)
    r = client.post(f"/factures/{fid}/lier-charge", data={"charge_id": "CHG_WEB_1"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Charge rattachée" in html
    assert "CHG_WEB_1" in html


def test_ajouter_ligne_via_http(client, env):
    fid = _creer_facture(env, ttc=120.0)
    r = client.post(f"/factures/{fid}/lignes",
                    data={"charge_id": "CHG_WEB_L1", "logement_id": "LOG_A1", "montant_ttc": "60.0"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Ligne ajoutée" in html
    assert "CHG_WEB_L1" in html and "LOG_A1" in html


# ── Règlement depuis la fiche ────────────────────────────────────────────────

def test_reglement_partiel_puis_solde_via_http(client, env):
    fid = _creer_facture(env, "FA-REG", 100.0)
    client.post(f"/factures/{fid}/statut", data={"statut": svc.ST_VALIDEE})

    r = client.post(f"/factures/{fid}/reglement", data={
        "date_reglement": "2026-07-01", "montant": "40.00", "moyen": "BANQUE",
    }, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Règlement enregistré" in html
    assert svc.ST_PARTIELLEMENT_REGLEE in html

    r2 = client.post(f"/factures/{fid}/reglement", data={
        "date_reglement": "2026-07-15", "montant": "60.00", "moyen": "CAISSE",
    }, follow_redirects=False)
    html2 = client.get(r2.headers["location"]).text
    assert svc.ST_REGLEE in html2


def test_reglement_depassement_refuse_via_http(client, env):
    fid = _creer_facture(env, "FA-DEP", 100.0)
    client.post(f"/factures/{fid}/statut", data={"statut": svc.ST_VALIDEE})
    r = client.post(f"/factures/{fid}/reglement", data={
        "date_reglement": "2026-07-01", "montant": "500.00", "moyen": "BANQUE",
    }, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "dépasserait" in html


def test_annuler_reglement_via_http(client, env):
    fid = _creer_facture(env, "FA-ANN", 100.0)
    client.post(f"/factures/{fid}/statut", data={"statut": svc.ST_VALIDEE})
    client.post(f"/factures/{fid}/reglement", data={
        "date_reglement": "2026-07-01", "montant": "100.00", "moyen": "BANQUE"})

    from app.services import reglements_fournisseurs_service as reg
    regs = reg.reglements_de_facture(fid)
    assert len(regs) == 1
    r = client.post(f"/reglements/{regs[0]['reglement_id_opaque']}/annuler",
                    data={"retour": f"/factures/{fid}"}, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Règlement annulé" in html
    assert "ANNULE" in html

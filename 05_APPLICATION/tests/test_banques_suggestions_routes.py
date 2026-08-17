"""Couche HTTP des suggestions et de la page de contrôles Banque."""
from __future__ import annotations

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.services import banques_candidats_service as candidats
from app.services import banques_controle_service as ctrl_svc
from app.services import banques_rapprochement_service as rappro


MID = "MVT-CM_TEST-20260605-DEBIT-12000-ABCDEF"


@pytest.fixture
def ref(tmp_db, tmp_path, monkeypatch):
    """Un paiement fournisseur, en base. Aucun classeur."""
    monkeypatch.setattr(cfg, "MASTER_BANQUE", tmp_path / "CLASSEUR_ABSENT.xlsx")
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    fx.construire(tmp_db, mouvements=[
        fx.mouvement(MID, "2026-06-12", "VIR FOURNISSEUR CHG_TEST01", 120.0, "DEBIT",
                     compte="CM_TEST", tiers="FOURNISSEUR", categorie="FACTURE_PRESTATAIRE",
                     type_flux="TYPE_FLUX_014", niveau_risque="FAIBLE"),
    ])
    # Candidats réels simulés au niveau du service de collecte (pas de faux objet dans le modèle).
    monkeypatch.setattr(candidats, "candidats_pour", lambda m: [
        {"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_TEST01", "montant": 120.0,
         "date": "2026-06-12", "reference": "CHG_TEST01", "libelle": "Charge test"},
    ])
    reader.vider_cache()
    ctrl_svc.vider_cache()
    yield tmp_db
    reader.vider_cache()
    ctrl_svc.vider_cache()


def _opaque() -> str:
    return ctrl_svc.id_opaque(MID)


# ── Suggestions sur la fiche ─────────────────────────────────────────────────

def test_fiche_affiche_les_suggestions(client, ref):
    html = client.get(f"/banques-caisse/mouvements/{_opaque()}").text
    assert 'data-testid="suggestions"' in html
    assert "CHG_TEST01" in html
    assert "EXACT" in html
    assert "Accepter" in html and "Refuser" in html


def test_accepter_suggestion_cree_un_rapprochement_propose(client, ref):
    opaque = _opaque()
    r = client.post(f"/banques-caisse/mouvements/{opaque}/suggestions/accepter",
                    data={"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_TEST01",
                          "montant": "120.00"}, follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Suggestion acceptée" in html
    assert "à confirmer" in html

    liens = rappro.lister(opaque)
    assert len(liens) == 1
    assert liens[0]["statut"] == rappro.ST_PROPOSE     # jamais CONFIRME automatiquement
    assert liens[0]["source"] == "AUTO"


def test_accepter_avec_montant_modifie(client, ref):
    opaque = _opaque()
    client.post(f"/banques-caisse/mouvements/{opaque}/suggestions/accepter",
               data={"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_TEST01",
                     "montant": "50.00"})
    etat = rappro.etat_rapprochement(opaque, 120.0)
    assert etat["statut"] == "PARTIEL" and etat["montant_restant"] == 70.0


def test_refuser_suggestion_la_fait_disparaitre(client, ref):
    opaque = _opaque()
    r = client.post(f"/banques-caisse/mouvements/{opaque}/suggestions/refuser",
                    data={"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_TEST01",
                          "definitif": "1"}, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "Suggestion refusée" in html
    html2 = client.get(f"/banques-caisse/mouvements/{opaque}").text
    assert 'data-testid="suggestions"' not in html2      # ne réapparaît pas
    assert "Historique des suggestions" in html2         # mais reste tracée


def test_ignorer_temporairement_via_http(client, ref):
    opaque = _opaque()
    r = client.post(f"/banques-caisse/mouvements/{opaque}/suggestions/refuser",
                    data={"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_TEST01",
                          "definitif": "0"}, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "ignorée temporairement" in html


def test_suggestion_inconnue_refusee_proprement(client, ref):
    opaque = _opaque()
    r = client.post(f"/banques-caisse/mouvements/{opaque}/suggestions/accepter",
                    data={"type_objet": "RESERVATION", "objet_id": "INEXISTANT",
                          "montant": "10"}, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "introuvable ou expirée" in html


# ── Vue à rapprocher : résumé des suggestions ────────────────────────────────

def test_vue_a_rapprocher_affiche_le_resume(client, ref):
    html = client.get("/banques-caisse/a-rapprocher?mois=2026-06").text
    assert "Suggestions" in html
    assert "Meilleure suggestion" in html


# ── Page de contrôles ─────────────────────────────────────────────────────────

def test_page_controles_accessible(client, ref):
    html = client.get("/banques-caisse/controles").text
    assert "Contrôles bancaires" in html
    assert "Bloquants" in html


def test_page_controles_filtre_par_severite(client, ref):
    r = client.get("/banques-caisse/controles?severite=BLOQUANT")
    assert r.status_code == 200
    assert "BLOQUANT" in r.text


def test_dashboard_expose_le_lien_controles(client, ref):
    html = client.get("/banques-caisse").text
    assert '/banques-caisse/controles' in html

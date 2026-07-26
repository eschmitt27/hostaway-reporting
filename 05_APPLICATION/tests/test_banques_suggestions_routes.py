"""Couche HTTP des suggestions et de la page de contrôles Banque."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.services import banques_candidats_service as candidats
from app.services import banques_controle_service as ctrl_svc
from app.services import banques_rapprochement_service as rappro

NORM_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source", "date_operation", "date_valeur",
    "libelle", "libelle_brut", "montant", "sens", "devise", "compte_id", "tiers_detecte",
    "categorie", "type_flux_id", "code_impact", "source_classification", "source_economique",
    "statut_controle", "niveau_risque", "codes_anomalie", "date_integration", "commentaire",
]

MID = "MVT-CM_TEST-20260605-DEBIT-12000-ABCDEF"


@pytest.fixture
def ref(tmp_path, monkeypatch):
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet("NORM_Banque")
    ws.append(NORM_HDR)
    ws.append([MID, "HASH1", "IMP1", 2, "2026-06-12", "2026-06-12",
               "VIR FOURNISSEUR CHG_TEST01", "VIR FOURNISSEUR CHG_TEST01", 120.0, "DEBIT",
               "EUR", "CM_TEST", "FOURNISSEUR", "FACTURE_PRESTATAIRE", "TYPE_FLUX_014", "",
               "REGLE_DETERMINISTE", "", "VALIDE", "FAIBLE", "", "2026-06-30", ""])
    wb.save(p); wb.close()
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    # Candidats réels simulés au niveau du service de collecte (pas de faux objet dans le modèle).
    monkeypatch.setattr(candidats, "candidats_pour", lambda m: [
        {"type_objet": "CHARGE_FOURNISSEUR", "objet_id": "CHG_TEST01", "montant": 120.0,
         "date": "2026-06-12", "reference": "CHG_TEST01", "libelle": "Charge test"},
    ])
    ctrl_svc.vider_cache()
    yield p
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

"""Couche HTTP : rapprochement depuis un règlement, fiche fournisseur avec solde, page de contrôles,
et vue inverse depuis la fiche mouvement bancaire."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.services import banques_controle_service as ctrl
from app.services import factures_banque_service as pont
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs
from app.services import reglements_fournisseurs_service as regl

NORM_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source", "date_operation", "date_valeur",
    "libelle", "libelle_brut", "montant", "sens", "devise", "compte_id", "tiers_detecte",
    "categorie", "type_flux_id", "code_impact", "source_classification", "source_economique",
    "statut_controle", "niveau_risque", "codes_anomalie", "date_integration", "commentaire",
]
MID = "MVT-FAC-01"


@pytest.fixture
def env(tmp_db, tmp_path, monkeypatch):
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet("NORM_Banque")
    ws.append(NORM_HDR)
    d = {h: None for h in NORM_HDR}
    d.update({"mouvement_id": MID, "ROW_HASH": "H1", "import_id": "I", "ligne_source": 2,
              "date_operation": "2026-07-05", "date_valeur": "2026-07-05",
              "libelle": "VIR FOURNISSEUR", "libelle_brut": "VIR FOURNISSEUR",
              "montant": 120.0, "sens": "DEBIT", "devise": "EUR", "compte_id": "CM_T",
              "categorie": "FACTURE_PRESTATAIRE", "statut_controle": "VALIDE",
              "niveau_risque": "FAIBLE", "date_integration": "2026-07-31"})
    ws.append([d[h] for h in NORM_HDR])
    wb.save(p); wb.close()

    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    ctrl.vider_cache()

    f = frs.creer("Fournisseur Pont", "MENAGE", acteur="t", db_path=tmp_db)
    fid = fact.creer({"fournisseur_id_opaque": f["fournisseur_id_opaque"],
                      "facture_ref": "FA-PONT-1", "date_facture": "2026-06-01",
                      "date_echeance": "2026-07-01", "montant_ttc": 120.0,
                      "justificatif": "pj.pdf"}, acteur="t", db_path=tmp_db)["facture_id_opaque"]
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=tmp_db)
    fact.lier_charge(fid, "CHG_PONT", acteur="t", db_path=tmp_db)
    g = regl.enregistrer(f["fournisseur_id_opaque"], [{"facture_id_opaque": fid, "montant": 120.0}],
                         date_reglement="2026-07-05", moyen="BANQUE", acteur="t", db_path=tmp_db)
    yield {"frs": f["fournisseur_id_opaque"], "facture": fid, "reglement": g["reglement_id_opaque"]}
    ctrl.vider_cache()


def _op() -> str:
    return ctrl.id_opaque(MID)


# ── Écran de rapprochement d'un règlement ────────────────────────────────────

def test_page_rapprocher_affiche_les_candidats(client, env):
    html = client.get(f"/reglements/{env['reglement']}/rapprocher").text
    assert 'data-testid="candidats"' in html
    assert "VIR FOURNISSEUR" in html
    assert "NON_RAPPROCHE" in html


def test_rapprocher_puis_confirmer_via_http(client, env):
    r = client.post(f"/reglements/{env['reglement']}/rapprocher",
                    data={"mouvement_id_opaque": _op(), "montant": "120.00"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Rapprochement proposé" in html
    assert "PROPOSE" in html

    import re
    m = re.search(r"/rapprochements/([A-Za-z0-9_-]+)/confirmer", html)
    assert m, html
    r2 = client.post(
        f"/reglements/{env['reglement']}/rapprochements/{m.group(1)}/confirmer",
        follow_redirects=False)
    html2 = client.get(r2.headers["location"]).text
    assert "Rapprochement confirmé" in html2
    assert "CONFIRME" in html2


def test_rapprochement_partiel_via_http(client, env):
    client.post(f"/reglements/{env['reglement']}/rapprocher",
               data={"mouvement_id_opaque": _op(), "montant": "50.00"})
    html = client.get(f"/reglements/{env['reglement']}/rapprocher").text
    assert "PARTIEL" in html
    assert "70.00" in html                     # restant à rapprocher


def test_depassement_refuse_via_http(client, env):
    r = client.post(f"/reglements/{env['reglement']}/rapprocher",
                    data={"mouvement_id_opaque": _op(), "montant": "500.00"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "dépasserait" in html


def test_annuler_rapprochement_via_http(client, env):
    r = client.post(f"/reglements/{env['reglement']}/rapprocher",
                    data={"mouvement_id_opaque": _op(), "montant": "120.00"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    import re
    m = re.search(r"/rapprochements/([A-Za-z0-9_-]+)/annuler", html)
    assert m, html
    r2 = client.post(f"/reglements/{env['reglement']}/rapprochements/{m.group(1)}/annuler",
                     follow_redirects=False)
    html2 = client.get(r2.headers["location"]).text
    assert "Rapprochement annulé" in html2
    assert "NON_RAPPROCHE" in html2


def test_reglement_inconnu_404(client, env):
    assert client.get("/reglements/REG-INEXISTANT/rapprocher").status_code == 404


# ── Vue inverse depuis la fiche mouvement bancaire ──────────────────────────

def test_fiche_mouvement_affiche_facture_et_soldes(client, env):
    client.post(f"/reglements/{env['reglement']}/rapprocher",
               data={"mouvement_id_opaque": _op(), "montant": "120.00"})
    html = client.get(f"/banques-caisse/mouvements/{_op()}").text
    assert 'data-testid="contexte-metier"' in html
    assert "FA-PONT-1" in html
    assert "Ouvrir la facture" in html
    assert "Solde avant" in html


# ── Fiche fournisseur avec solde ────────────────────────────────────────────

def test_fiche_fournisseur_solde(client, env):
    html = client.get(f"/fournisseurs-soldes/{env['frs']}").text
    assert "Total facturé" in html
    assert "Solde à payer" in html
    assert "FA-PONT-1" in html
    assert "Fournisseur Pont" in html


def test_fiche_fournisseur_inconnue_404(client, env):
    assert client.get("/fournisseurs-soldes/FRS-INEXISTANT").status_code == 404


def test_fournisseur_archive_ne_propose_plus_de_facture(client, env):
    f = frs.charger_par_opaque(env["frs"])
    frs.desactiver(f, acteur="t")
    html = client.get(f"/fournisseurs-soldes/{env['frs']}").text
    assert "Fournisseur archivé" in html
    assert "+ Nouvelle facture" not in html


# ── Page de contrôles ────────────────────────────────────────────────────────

def test_page_controles_factures(client, env):
    html = client.get("/factures/controles").text
    assert "Contrôles factures et règlements" in html
    assert "Bloquants" in html


def test_page_controles_filtre(client, env):
    r = client.get("/factures/controles?severite=AVERTISSEMENT")
    assert r.status_code == 200


def test_liste_factures_expose_le_lien_controles(client, env):
    assert "/factures/controles" in client.get("/factures").text

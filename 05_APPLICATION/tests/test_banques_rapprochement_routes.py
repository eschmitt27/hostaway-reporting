"""Rapprochement depuis la fiche mouvement (HTTP) : proposer, confirmer, refuser, annuler."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.services import banques_controle_service as ctrl_svc

NORM_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source", "date_operation", "date_valeur",
    "libelle", "libelle_brut", "montant", "sens", "devise", "compte_id", "tiers_detecte",
    "categorie", "type_flux_id", "code_impact", "source_classification", "source_economique",
    "statut_controle", "niveau_risque", "codes_anomalie", "date_integration", "commentaire",
]


@pytest.fixture
def ref(tmp_path, monkeypatch):
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet("NORM_Banque")
    ws.append(NORM_HDR)
    ws.append(["MVT-CM_TEST-20260605-CREDIT-85000-ABCDEF", "HASH1", "IMP1", 2,
              "2026-06-05", "2026-06-05", "VIR HOSTAWAY PAYOUT", "VIR HOSTAWAY PAYOUT",
              850.0, "CREDIT", "EUR", "CM_TEST", "HOSTAWAY", "PAYOUT_PLATEFORME",
              "TYPE_FLUX_017", "", "REGLE_DETERMINISTE", "", "VALIDE", "FAIBLE", "", "2026-06-30", ""])
    wb.save(p); wb.close()
    monkeypatch.setattr(cfg, "MASTER_BANQUE", p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "RECETTE_ROOT", tmp_path.resolve())
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    ctrl_svc.vider_cache()
    yield p
    ctrl_svc.vider_cache()


def _opaque(mouvement_id: str) -> str:
    return ctrl_svc.id_opaque(mouvement_id)


def test_fiche_affiche_le_bloc_rapprochement(client, ref):
    opaque = _opaque("MVT-CM_TEST-20260605-CREDIT-85000-ABCDEF")
    html = client.get(f"/banques-caisse/mouvements/{opaque}").text
    assert "Rapprochement" in html
    assert "NON_RAPPROCHE" in html
    assert 'action="/banques-caisse/mouvements/' in html and "/rapprocher" in html


def test_proposer_rapprochement_puis_confirmer(client, ref):
    opaque = _opaque("MVT-CM_TEST-20260605-CREDIT-85000-ABCDEF")
    r = client.post(f"/banques-caisse/mouvements/{opaque}/rapprocher",
                    data={"type_objet": "PAYOUT_PLATEFORME", "objet_id": "PAYOUT_2026_06_05",
                          "montant_rapproche": "850.00", "commentaire": "test"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Rapprochement proposé" in html
    assert "PROPOSE" in html
    assert "PARTIEL" not in html  # 850 == montant total -> RAPPROCHE, jamais partiel affiché comme tel

    import re
    m = re.search(r"/rapprochements/([A-Za-z0-9_-]+)/confirmer", html)
    assert m, html
    rap_opaque = m.group(1)
    r2 = client.post(f"/banques-caisse/rapprochements/{rap_opaque}/confirmer?mvt={opaque}",
                     data={"commentaire": "ok"}, follow_redirects=False)
    assert r2.status_code == 303
    html2 = client.get(r2.headers["location"]).text
    assert "Rapprochement confirmé" in html2
    assert "CONFIRME" in html2
    assert "RAPPROCHE" in html2                  # état global du mouvement


def test_rapprochement_partiel_visible_sur_la_fiche(client, ref):
    opaque = _opaque("MVT-CM_TEST-20260605-CREDIT-85000-ABCDEF")
    client.post(f"/banques-caisse/mouvements/{opaque}/rapprocher",
               data={"type_objet": "PAYOUT_PLATEFORME", "objet_id": "P1",
                     "montant_rapproche": "300.00"})
    html = client.get(f"/banques-caisse/mouvements/{opaque}").text
    assert "PARTIEL" in html
    assert "550.00" in html                      # montant restant


def test_depassement_refuse_via_http(client, ref):
    opaque = _opaque("MVT-CM_TEST-20260605-CREDIT-85000-ABCDEF")
    client.post(f"/banques-caisse/mouvements/{opaque}/rapprocher",
               data={"type_objet": "PAYOUT_PLATEFORME", "objet_id": "P1",
                     "montant_rapproche": "600.00"})
    r = client.post(f"/banques-caisse/mouvements/{opaque}/rapprocher",
                    data={"type_objet": "PAYOUT_PLATEFORME", "objet_id": "P2",
                          "montant_rapproche": "300.00"}, follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "dépasserait" in html


def test_rapprocher_refuse_si_flags_off(client, ref, monkeypatch):
    monkeypatch.setattr(cfg, "BANQUE_REAL_WRITE_ENABLED", False)
    opaque = _opaque("MVT-CM_TEST-20260605-CREDIT-85000-ABCDEF")
    html = client.get(f"/banques-caisse/mouvements/{opaque}").text
    assert "Écriture désactivée" in html

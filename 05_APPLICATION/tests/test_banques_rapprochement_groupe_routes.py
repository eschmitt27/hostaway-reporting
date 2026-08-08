"""Rapprochement groupé depuis la fiche mouvement (HTTP) : proposition, confirmation."""
from __future__ import annotations

import openpyxl
import pytest

import app.config as cfg
from app.readers import proprietaires_reader
from app.services import banques_controle_service as ctrl_svc
from app.services import banques_rapprochement_service as rappro
from app.services import proprietaires_tresorerie_service as tresorerie

NORM_HDR = [
    "mouvement_id", "ROW_HASH", "import_id", "ligne_source", "date_operation", "date_valeur",
    "libelle", "libelle_brut", "montant", "sens", "devise", "compte_id", "tiers_detecte",
    "categorie", "type_flux_id", "code_impact", "source_classification", "source_economique",
    "statut_controle", "niveau_risque", "codes_anomalie", "date_integration", "commentaire",
]


@pytest.fixture(autouse=True)
def _proprietaire_connu(monkeypatch):
    monkeypatch.setattr(proprietaires_reader, "find_proprietaire",
                        lambda pid: {"proprietaire_id": pid} if pid == "PROP_0001" else None)


@pytest.fixture
def ref(tmp_path, monkeypatch):
    p = tmp_path / "BANQUE_LOT8_IMPORT.xlsx"
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    ws = wb.create_sheet("NORM_Banque")
    ws.append(NORM_HDR)
    ws.append(["MVT-CM_TEST-20260615-CREDIT-110000-GRP001", "HASHG1", "IMP1", 2,
              "2026-06-15", "2026-06-15", "VIR PROPRIETAIRE", "VIR PROPRIETAIRE",
              1100.0, "CREDIT", "EUR", "CM_TEST", "PROP_0001", "REVERSEMENT_PROPRIETAIRE",
              "TYPE_FLUX_020", "", "REGLE_DETERMINISTE", "", "VALIDE", "FAIBLE", "", "2026-06-30", ""])
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


def _deux_mouvements_valides(tmp_db):
    m1 = tresorerie.creer("PROP_0001", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                          500.0, "2026-06-10", db_path=tmp_db)
    m2 = tresorerie.creer("PROP_0001", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                          600.0, "2026-06-12", db_path=tmp_db)
    tresorerie.valider(m1["mouvement_opaque"], db_path=tmp_db)
    tresorerie.valider(m2["mouvement_opaque"], db_path=tmp_db)
    return m1["mouvement_opaque"], m2["mouvement_opaque"]


def test_groupe_propose_affiche_sur_la_fiche(client, ref, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    _deux_mouvements_valides(tmp_db)
    opaque = _opaque("MVT-CM_TEST-20260615-CREDIT-110000-GRP001")
    html = client.get(f"/banques-caisse/mouvements/{opaque}").text
    assert "Rapprochements groupés proposés" in html
    assert "Groupe de 2 objets" in html
    assert "1100.00" in html or "1100,00" in html


def test_groupe_confirmer_cree_les_liens(client, ref, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    m1, m2 = _deux_mouvements_valides(tmp_db)
    opaque = _opaque("MVT-CM_TEST-20260615-CREDIT-110000-GRP001")

    import re
    html = client.get(f"/banques-caisse/mouvements/{opaque}").text
    m = re.search(r'name="affectations" value="([^"]*)"', html)
    assert m, html
    affectations_html = m.group(1)

    import html as _html
    affectations_raw = _html.unescape(affectations_html)

    r = client.post(f"/banques-caisse/mouvements/{opaque}/groupes/confirmer",
                    data={"affectations": affectations_raw}, follow_redirects=False)
    assert r.status_code == 303
    html2 = client.get(r.headers["location"]).text
    assert "Groupe proposé" in html2

    liens = rappro.lister(opaque, db_path=tmp_db)
    assert len(liens) == 2
    objets = {l["objet_id"] for l in liens}
    assert objets == {m1, m2}
    assert all(l["statut"] == rappro.ST_PROPOSE for l in liens)   # jamais confirmé automatiquement


def test_groupe_confirmer_mouvement_introuvable(client, ref, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    r = client.post("/banques-caisse/mouvements/MVT-INEXISTANT/groupes/confirmer",
                    data={"affectations": "[]"}, follow_redirects=False)
    assert r.status_code == 303 and "erreur" in r.headers["location"]


def test_groupe_confirmer_affectations_invalides(client, ref, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    opaque = _opaque("MVT-CM_TEST-20260615-CREDIT-110000-GRP001")
    r = client.post(f"/banques-caisse/mouvements/{opaque}/groupes/confirmer",
                    data={"affectations": "pas du json"}, follow_redirects=False)
    assert r.status_code == 303 and "erreur" in r.headers["location"]


def test_aucun_groupe_si_moins_de_deux_candidats(client, ref, tmp_db, monkeypatch):
    monkeypatch.setattr(cfg, "DB_PATH", tmp_db)
    m1 = tresorerie.creer("PROP_0001", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                         1100.0, "2026-06-10", db_path=tmp_db)
    tresorerie.valider(m1["mouvement_opaque"], db_path=tmp_db)
    opaque = _opaque("MVT-CM_TEST-20260615-CREDIT-110000-GRP001")
    html = client.get(f"/banques-caisse/mouvements/{opaque}").text
    # un seul objet à 1100 -> couvert par les suggestions simples (exact), jamais un "groupe"
    assert "Rapprochements groupés proposés" not in html

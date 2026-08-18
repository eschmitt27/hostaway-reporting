"""PDF facture ménage externe → SQLite (mission Ménages §6-14/§26/§34-35).

Couvre les CAS 1-6 de la mission (§35) sur les deux PDF réels (copies uniquement, comme
`test_menages_pdf_extraction.py`) plus des fixtures fabriquées pour les cas de ventilation.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pytest.importorskip("fitz")
pdfex = pytest.importorskip("lib_menages_externes_pdf")

from app.services import facture_lignes_menage_service as flm  # noqa: E402
from app.services import facture_menage_pdf_service as svc  # noqa: E402
from app.services import factures_service as fact  # noqa: E402

REAL_PDF_DIR = cfg.MENAGES_PDF_DIR
AISSATA = REAL_PDF_DIR / "Facture mai Aissata.pdf"
MOUNIR = REAL_PDF_DIR / "Facture mai Mounir.pdf"
pdf_reels = pytest.mark.skipif(not (AISSATA.exists() and MOUNIR.exists()),
                               reason="PDF réels absents d'un checkout propre")


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)


@pytest.fixture
def aissata(tmp_path):
    dst = tmp_path / "Facture mai Aissata.pdf"
    shutil.copy2(AISSATA, dst)
    return dst


@pytest.fixture
def mounir(tmp_path):
    dst = tmp_path / "Facture mai Mounir.pdf"
    shutil.copy2(MOUNIR, dst)
    return dst


# ── CAS 1 : facture parfaitement affectée → total OK ────────────────────────────────────────────

@pdf_reels
def test_cas1_facture_totalement_affectee_total_ok(aissata, tmp_db):
    fac = pdfex.extraire_pdf(aissata)
    assert all(l.logement_id for l in fac.lignes), "cette facture doit être 100% affectée"

    r = svc.importer(aissata, db_path=tmp_db)
    assert r["ok"] is True
    assert r["controle_total"]["coherent"] is True
    assert r["controle_total"]["ecart"] == 0.0
    assert r["ventilations"] == []


# ── CAS 2 : ligne non affectée + A/B → ventilation 55/90 → 3.79/6.21, facture A_CONTROLER ───────

def test_cas2_ventilation_55_90_10_exemple_mission(tmp_db):
    facture_id = fact.creer(
        {"fournisseur_id_opaque": "FRS-EXT-TEST", "facture_ref": "CAS2-001",
         "date_facture": "2026-05-31", "montant_ttc": 155.0},
        db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(facture_id, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A",
                      montant_ttc=55.0, db_path=tmp_db)
    flm.ajouter_ligne(facture_id, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_B",
                      montant_ttc=90.0, db_path=tmp_db)
    flm.ajouter_ligne(facture_id, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=10.0,
                      description="Frais supplémentaire", db_path=tmp_db)

    base = flm.cout_menages_par_logement(facture_id, db_path=tmp_db)
    assert base == {"LOG_A": 55.0, "LOG_B": 90.0}

    import app.services.facture_ventilation_menage_service as vent
    resultat = vent.ventiler_et_enregistrer(facture_id, 10.0, base, db_path=tmp_db)
    parts = {p["logement_id"]: p["part_montant"] for p in resultat["parts"]}
    assert parts == {"LOG_A": 3.79, "LOG_B": 6.21}
    assert round(sum(parts.values()), 2) == 10.0

    controle = flm.controler_total(facture_id, db_path=tmp_db)
    assert controle["coherent"] is True  # 55+90+10 = 155 = montant_ttc de la facture
    # La ventilation, même appliquée, ne valide jamais seule la facture.
    f = fact.charger(facture_id, db_path=tmp_db)
    assert f["statut"] == fact.ST_A_CONTROLER


# ── CAS 3 : ligne non affectée sans base → pas de ventilation → A_CONTROLER ─────────────────────

def test_cas3_ligne_non_affectee_sans_base_pas_de_ventilation(tmp_db):
    facture_id = fact.creer(
        {"fournisseur_id_opaque": "FRS-EXT-TEST", "facture_ref": "CAS3-001",
         "date_facture": "2026-05-31", "montant_ttc": 10.0}, db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(facture_id, type_ligne=flm.TYPE_FRAIS_NON_AFFECTE, montant_ttc=10.0,
                      db_path=tmp_db)

    import app.services.facture_ventilation_menage_service as vent
    base = flm.cout_menages_par_logement(facture_id, db_path=tmp_db)
    assert base == {}
    resultat = vent.ventiler_et_enregistrer(facture_id, 10.0, base, db_path=tmp_db)
    assert resultat["statut"] == vent.ST_NON_EFFECTUEE
    assert resultat["parts"] == []
    assert "logement" in resultat["message"].lower()


# ── CAS 4 : somme lignes ≠ facture → A_CONTROLER (écart détecté) ────────────────────────────────

def test_cas4_somme_lignes_differente_du_total_facture(tmp_db):
    facture_id = fact.creer(
        {"fournisseur_id_opaque": "FRS-EXT-TEST", "facture_ref": "CAS4-001",
         "date_facture": "2026-05-31", "montant_ttc": 100.0}, db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(facture_id, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A",
                      montant_ttc=55.0, db_path=tmp_db)

    controle = flm.controler_total(facture_id, db_path=tmp_db)
    assert controle["coherent"] is False
    assert controle["ecart"] == 45.0


# ── CAS 5/6 : task sans ligne facture / ligne facture sans task ─────────────────────────────────
# Le rapprochement Task Hostaway ↔ déclaration/facture est un mécanisme DÉJÀ existant du module
# Ménages (menages_reader/menages_service, non retouché ici) : ces deux cas relèvent de ce
# rapprochement, pas de la ventilation. Vérifié seulement que les lignes ménage externes portent de
# quoi être rapprochées (logement_id, montant), sans dupliquer ce contrôle ici.

def test_cas5_6_ligne_menage_porte_les_champs_de_rapprochement(tmp_db):
    facture_id = fact.creer(
        {"fournisseur_id_opaque": "FRS-EXT-TEST", "facture_ref": "CAS56-001",
         "date_facture": "2026-05-31", "montant_ttc": 55.0}, db_path=tmp_db)["facture_id_opaque"]
    flm.ajouter_ligne(facture_id, type_ligne=flm.TYPE_MENAGE_EXTERNE, logement_id="LOG_A",
                      montant_ttc=55.0, menage_id_opaque="MEN-0001", db_path=tmp_db)
    lignes = flm.lignes(facture_id, db_path=tmp_db)
    assert lignes[0]["logement_id"] == "LOG_A"
    assert lignes[0]["menage_id_opaque"] == "MEN-0001"


# ── Import réel bout-en-bout : les deux PDF fournis par le métier ───────────────────────────────

@pdf_reels
def test_import_mounir_produit_facture_et_lignes(mounir, tmp_db):
    r = svc.importer(mounir, db_path=tmp_db)
    assert r["ok"] is True
    lignes = flm.lignes(r["facture_id_opaque"], db_path=tmp_db)
    assert len(lignes) == r["nb_lignes"] == 4
    f = fact.charger(r["facture_id_opaque"], db_path=tmp_db)
    assert f["montant_ttc"] == 942.0
    assert f["statut"] == fact.ST_A_CONTROLER


# ── Idempotence (§34) : réutilise le mécanisme existant de factures_service ─────────────────────

@pdf_reels
def test_meme_pdf_importe_deux_fois_pas_deux_factures(aissata, tmp_path, tmp_db):
    r1 = svc.importer(aissata, db_path=tmp_db)
    assert r1["ok"] is True

    copie = tmp_path / "copie_renommee.pdf"
    shutil.copy2(aissata, copie)
    r2 = svc.importer(copie, db_path=tmp_db)
    assert r2["ok"] is False
    assert r2["code"] == fact.E_DOUBLON_CERTAIN

    conn = get_db(tmp_db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()
    assert n == 1


# ── Extraction échouée : ne crée rien ────────────────────────────────────────────────────────────

def test_pdf_non_supporte_ne_cree_aucune_facture(tmp_path, tmp_db):
    import fitz
    p = tmp_path / "inconnu.pdf"
    doc = fitz.open()
    doc.new_page().insert_text((72, 72), "Facture ACME\nTotal 100 EUR")
    doc.save(p)
    doc.close()

    r = svc.importer(p, db_path=tmp_db)
    assert r["ok"] is False
    assert r["code"] == svc.E_EXTRACTION_ECHOUEE

    conn = get_db(tmp_db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0]
    finally:
        conn.close()
    assert n == 0

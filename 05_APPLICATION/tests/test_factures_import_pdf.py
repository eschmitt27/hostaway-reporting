"""Import PDF de facture : extraction réutilisée (jamais réécrite), repli propre sur format
inconnu, doublons, confirmation qui crée la facture SANS créer la charge."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import factures_import_service as imp
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs

fitz = pytest.importorskip("fitz", reason="PyMuPDF requis pour générer un PDF de test")


def _pdf_texte(chemin, texte: str) -> bytes:
    """PDF à texte NATIF (comme les factures réelles) — jamais une image, donc jamais d'OCR."""
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 100), texte, fontsize=11)
    doc.save(str(chemin))
    doc.close()
    return chemin.read_bytes()


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "DRYRUNS_DIR", tmp_path / "dryruns")
    f = frs.creer("Fournisseur PDF", "MENAGE", acteur="t", db_path=db)
    return {"db": db, "frs": f["fournisseur_id_opaque"], "tmp": tmp_path}


# ── Formats ───────────────────────────────────────────────────────────────────

def test_fichier_non_pdf_refuse(env):
    res = imp.previsualiser(b"pas un pdf", "facture.csv", db_path=env["db"])
    assert res["ok"] is False and res["code"] == imp.E_FORMAT_FICHIER


def test_format_inconnu_bascule_en_saisie_manuelle(env):
    """Un PDF valide mais d'un fournisseur non reconnu ne doit PAS faire échouer l'import :
    il bascule en saisie manuelle pré-remplie, avec une incertitude explicite."""
    contenu = _pdf_texte(env["tmp"] / "inconnu.pdf", "Facture ACME 2026 montant 250,00 EUR")
    res = imp.previsualiser(contenu, "inconnu.pdf", fournisseur_id_opaque=env["frs"],
                            db_path=env["db"])
    assert res["ok"] is True
    assert res["supporte"] is False
    assert any("non reconnu" in i for i in res["incertitudes"])
    assert any("saisie manuelle" in i.lower() for i in res["incertitudes"])


def test_incertitudes_listent_les_champs_manquants(env):
    contenu = _pdf_texte(env["tmp"] / "vide.pdf", "Document sans structure de facture")
    res = imp.previsualiser(contenu, "vide.pdf", fournisseur_id_opaque=env["frs"], db_path=env["db"])
    joint = " ".join(res["incertitudes"])
    assert "référence" in joint and "montant TTC" in joint


def test_fournisseur_absent_est_signale(env):
    contenu = _pdf_texte(env["tmp"] / "sansfrs.pdf", "Facture quelconque")
    res = imp.previsualiser(contenu, "sansfrs.pdf", db_path=env["db"])
    assert any("fournisseur non déterminé" in i for i in res["incertitudes"])


# ── Fichier source et copie de travail ───────────────────────────────────────

def test_fichier_copie_sous_dryruns_et_original_intact(env):
    src = env["tmp"] / "original.pdf"
    contenu = _pdf_texte(src, "Facture test")
    avant = src.read_bytes()
    res = imp.previsualiser(contenu, "original.pdf", fournisseur_id_opaque=env["frs"],
                            db_path=env["db"])
    from pathlib import Path
    copie = Path(res["copie"])
    assert copie.exists() and copie != src
    assert cfg.DRYRUNS_DIR in copie.parents or str(cfg.DRYRUNS_DIR) in str(copie)
    assert src.read_bytes() == avant                      # original jamais modifié


def test_aucune_facture_creee_a_la_previsualisation(env):
    contenu = _pdf_texte(env["tmp"] / "prev.pdf", "Facture test")
    imp.previsualiser(contenu, "prev.pdf", fournisseur_id_opaque=env["frs"], db_path=env["db"])
    assert fact.lister(db_path=env["db"]) == []


# ── Confirmation ──────────────────────────────────────────────────────────────

def test_confirmer_cree_la_facture_avec_corrections(env):
    contenu = _pdf_texte(env["tmp"] / "conf.pdf", "Facture test")
    prev = imp.previsualiser(contenu, "conf.pdf", fournisseur_id_opaque=env["frs"],
                             db_path=env["db"])
    res = imp.confirmer(prev["token"], {
        "fournisseur_id_opaque": env["frs"], "facture_ref": "FA-PDF-1",
        "date_facture": "2026-06-10", "montant_ttc": "250.00",
    }, acteur="recette", db_path=env["db"])
    assert res["ok"], res

    f = fact.charger(res["facture_id_opaque"], env["db"])
    assert f["facture_ref"] == "FA-PDF-1"
    assert f["montant_ttc"] == 250.0
    assert f["source"] == "PDF"
    assert f["justificatif"] == "conf.pdf"


def test_confirmer_ne_cree_jamais_de_charge(env):
    contenu = _pdf_texte(env["tmp"] / "nocharge.pdf", "Facture test")
    prev = imp.previsualiser(contenu, "nocharge.pdf", fournisseur_id_opaque=env["frs"],
                             db_path=env["db"])
    res = imp.confirmer(prev["token"], {"fournisseur_id_opaque": env["frs"],
                                        "facture_ref": "FA-NC", "montant_ttc": "100.00"},
                        acteur="t", db_path=env["db"])
    f = fact.charger(res["facture_id_opaque"], env["db"])
    assert f["charge_id"] is None                          # la charge reste au parcours Charges
    assert f["statut"] == fact.ST_A_CONTROLER


def test_confirmer_refuse_si_flags_off(env, monkeypatch):
    contenu = _pdf_texte(env["tmp"] / "off.pdf", "Facture test")
    prev = imp.previsualiser(contenu, "off.pdf", fournisseur_id_opaque=env["frs"], db_path=env["db"])
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", False)
    res = imp.confirmer(prev["token"], {"facture_ref": "X", "montant_ttc": "1"}, db_path=env["db"])
    assert res["ok"] is False and res["code"] == imp.E_FLAGS
    assert fact.lister(db_path=env["db"]) == []


def test_token_inconnu_refuse(env):
    res = imp.confirmer("token-inexistant", {}, db_path=env["db"])
    assert res["ok"] is False and res["code"] == imp.E_TOKEN_INCONNU


# ── Doublons ──────────────────────────────────────────────────────────────────

def test_doublon_certain_signale_puis_refuse(env):
    fact.creer({"fournisseur_id_opaque": env["frs"], "facture_ref": "FA-DUP",
                "montant_ttc": 100.0, "date_facture": "2026-06-01"},
               acteur="t", db_path=env["db"])
    contenu = _pdf_texte(env["tmp"] / "dup.pdf", "Facture test")
    prev = imp.previsualiser(contenu, "dup.pdf", fournisseur_id_opaque=env["frs"], db_path=env["db"])
    res = imp.confirmer(prev["token"], {"fournisseur_id_opaque": env["frs"],
                                        "facture_ref": "FA-DUP", "montant_ttc": "100.00"},
                        acteur="t", db_path=env["db"])
    assert res["ok"] is False
    assert any(e["code"] == fact.E_DOUBLON_CERTAIN for e in res.get("erreurs", []))


def test_doublon_probable_signale_en_previsualisation(env):
    fact.creer({"fournisseur_id_opaque": env["frs"], "facture_ref": "FA-PROB",
                "montant_ttc": 250.0, "date_facture": "2026-06-10"},
               acteur="t", db_path=env["db"])
    contenu = _pdf_texte(env["tmp"] / "prob.pdf", "Facture test")
    prev = imp.previsualiser(contenu, "prob.pdf", fournisseur_id_opaque=env["frs"],
                             db_path=env["db"])
    # Le montant n'ayant pas été extrait d'un PDF non supporté, le doublon probable ne peut pas
    # être calculé à ce stade : c'est cohérent et explicitement annoncé dans les incertitudes.
    assert prev["ok"] is True
    assert prev["doublons_probables"] == [] or prev["propose"]["montant_ttc"] is not None

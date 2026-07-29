"""Cœur Comptabilité — périodes comptables, clôture, réouverture."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import comptabilite_ecritures_service as compta
from app.services import comptabilite_periodes_service as per
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def test_periode_implicite_ouverte(db):
    p = per.obtenir_ou_creer("2026-06", db_path=db)
    assert p["statut"] == per.ST_OUVERTE


def test_transitions_valides(db):
    assert per.passer_en_controle("2026-06", db_path=db)["ok"]
    assert per.valider("2026-06", db_path=db)["ok"]
    assert per.cloturer("2026-06", acteur="recette", db_path=db)["ok"]
    p = per.charger("2026-06", db)
    assert p["statut"] == per.ST_CLOTUREE


def test_transition_interdite_refusee(db):
    res = per.valider("2026-06", db_path=db)   # OUVERTE -> VALIDEE directe interdite
    assert res["ok"] is False and res["code"] == per.E_STATUT


def test_format_periode_invalide_refuse(db):
    res = per.passer_en_controle("juin-2026", db_path=db)
    assert res["ok"] is False and res["code"] == per.E_PERIODE_FORMAT


def test_reouverture_exige_justification(db):
    per.passer_en_controle("2026-06", db_path=db)
    per.valider("2026-06", db_path=db)
    per.cloturer("2026-06", db_path=db)
    res = per.rouvrir("2026-06", justification="", db_path=db)
    assert res["ok"] is False and res["code"] == per.E_REOUVERTURE_SANS_JUSTIFICATION


def test_reouverture_avec_justification(db):
    per.passer_en_controle("2026-06", db_path=db)
    per.valider("2026-06", db_path=db)
    per.cloturer("2026-06", db_path=db)
    res = per.rouvrir("2026-06", justification="Erreur détectée après clôture", db_path=db)
    assert res["ok"], res
    assert per.charger("2026-06", db)["statut"] == per.ST_ROUVERTE


def test_periode_clouturee_refuse_ecriture_directe(db):
    frs = frs_svc.creer("Fournisseur Periode Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-PERIODE-1",
                   "date_facture": "2026-06-10", "montant_ttc": 60.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)

    per.passer_en_controle("2026-06", db_path=db)
    per.valider("2026-06", db_path=db)
    per.cloturer("2026-06", db_path=db)

    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert res["ok"] is False and res["code"] == compta.E_PERIODE_CLOTUREE


def test_cloture_refusee_si_ecriture_desequilibree_forcee(db, monkeypatch):
    """Défense en profondeur : si une écriture déséquilibrée existait malgré tout (contournement
    direct de la base), la clôture doit la détecter et refuser."""
    from app.db.connection import get_db
    conn = get_db(db)
    conn.execute(
        "INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, piece, "
        "libelle, origine_type, origine_id_opaque, statut, total_debit, total_credit) "
        "VALUES ('ECR-FORCE','ACHATS','2026-06-01','2026-06','P','L','MANUEL',NULL,'VALIDEE',10,20)")
    conn.commit()
    conn.close()
    per.passer_en_controle("2026-06", db_path=db)
    per.valider("2026-06", db_path=db)
    res = per.cloturer("2026-06", db_path=db)
    assert res["ok"] is False and res["code"] == per.E_CLOTURE_BLOQUEE


def test_historique_periode_trace_les_transitions(db):
    per.passer_en_controle("2026-06", db_path=db)
    per.valider("2026-06", db_path=db)
    h = per.historique("2026-06", db)
    assert len(h) == 2
    assert h[0]["nouveau_statut"] == per.ST_VALIDEE

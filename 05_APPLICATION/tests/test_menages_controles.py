"""Catalogue de contrôles Ménages."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import fournisseurs_referentiel_service as frs_svc
from app.services import menages_controles_service as ctrl
from app.services import menages_cycle_service as svc

LOGEMENT = "LOG_A1"
PROPRIETAIRE = "PROP_A"
FORM = {"logement_id": LOGEMENT, "mois": "2026-06", "type_menage": "EXTERNE",
       "date_prevue": "2026-06-10", "cout_prevu": 45.0}


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "MENAGES_CYCLE_REAL_WRITE_CONFIRMATION_ENABLED", True)

    def _fake_load_detail(logement_id):
        if logement_id == LOGEMENT:
            return {"logement_id": LOGEMENT, "proprietaire_id": PROPRIETAIRE, "status": "OK"}
        return None

    from app.services import logements_service
    monkeypatch.setattr(logements_service, "load_detail", _fake_load_detail)
    return p


def test_aucune_anomalie_sur_jeu_vide(db):
    r = ctrl.controler(db_path=db)
    assert r["statut"] == "OK" and r["nb_bloquants"] == 0


def test_menage_annule_avec_facture_signale(db):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.lier_facture(m, "FAC-X", db_path=db)
    svc.changer_statut(m, svc.ST_ANNULE, db_path=db)
    r = ctrl.controler(db_path=db)
    codes = [a["code"] for a in r["anomalies"]]
    assert ctrl.M_ANNULE_FACTURE in codes


def test_duree_negative_bloquante(db):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    conn = get_db(db)
    conn.execute("UPDATE menages SET duree_reelle_h=-1 WHERE menage_id_opaque=?", (m,))
    conn.commit(); conn.close()
    r = ctrl.controler(db_path=db)
    codes = [a["code"] for a in r["anomalies"]]
    assert ctrl.M_DUREE_NEGATIVE in codes
    assert not r["fiable"]


def test_prestataire_archive_signale(db):
    frs = frs_svc.creer("Presta Archive Test", "MENAGE", db_path=db)["fournisseur_id_opaque"]
    conn = get_db(db)
    conn.execute("INSERT INTO fournisseur_menage_qualification (fournisseur_id_opaque, "
                "type_menage, date_debut_validite) VALUES (?,?,?)", (frs, "EXTERNE", "2026-01-01"))
    conn.commit(); conn.close()
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    svc.affecter(m, frs, db_path=db)
    frs_svc.desactiver(frs_svc.charger_par_opaque(frs, db), db_path=db)
    r = ctrl.controler(db_path=db)
    codes = [a["code"] for a in r["anomalies"]]
    assert ctrl.M_PRESTATAIRE_ARCHIVE in codes


def test_charge_dupliquee_impossible_par_construction(db):
    """Défense en profondeur au niveau du SCHÉMA (index unique 0019, même règle que factures 0017) :
    une charge ne peut être rattachée qu'à un seul ménage, même en écriture SQL directe."""
    import sqlite3
    m1 = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    m2 = svc.creer(dict(FORM, date_prevue="2026-06-11"), db_path=db)["menage_id_opaque"]
    conn = get_db(db)
    try:
        conn.execute("UPDATE menages SET charge_id='CHG_X' WHERE menage_id_opaque=?", (m1,))
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE menages SET charge_id='CHG_X' WHERE menage_id_opaque=?", (m2,))
            conn.commit()
    finally:
        conn.close()
    # Le contrôle applicatif reste présent en défense en profondeur (cf. M_CHARGE_DUPLIQUEE),
    # pour le cas où une donnée incohérente arriverait malgré tout par un autre chemin.
    assert ctrl.M_CHARGE_DUPLIQUEE in ctrl.MESSAGES


def test_validation_sans_cout_reel_bloquante(db):
    m = svc.creer(dict(FORM), db_path=db)["menage_id_opaque"]
    conn = get_db(db)
    conn.execute("UPDATE menages SET statut=? WHERE menage_id_opaque=?", (svc.ST_VALIDE, m))
    conn.commit(); conn.close()
    r = ctrl.controler(db_path=db)
    codes = [a["code"] for a in r["anomalies"]]
    assert ctrl.M_VALIDE_SANS_COUT in codes


def test_doublon_probable_dates_proches(db):
    svc.creer(dict(FORM), db_path=db)
    svc.creer(dict(FORM, date_prevue="2026-06-11"), db_path=db)
    r = ctrl.controler(db_path=db)
    codes = [a["code"] for a in r["anomalies"]]
    assert ctrl.M_DOUBLON in codes


def test_controler_ne_leve_jamais(db, monkeypatch):
    def _casse(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(svc, "lister", _casse)
    r = ctrl.controler(db_path=db)
    assert r["statut"] == "INDISPONIBLE"

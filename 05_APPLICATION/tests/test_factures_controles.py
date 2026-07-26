"""Catalogue de contrôles Factures / Règlements."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_controles_service as cat
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs
from app.services import reglements_fournisseurs_service as regl


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    apply_migrations(db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    f = frs.creer("Fournisseur Controles", "MENAGE", acteur="t", db_path=db)
    return {"db": db, "frs": f["fournisseur_id_opaque"]}


def _facture(env, **kw):
    base = {"fournisseur_id_opaque": env["frs"], "facture_ref": "FA-C1",
            "date_facture": "2026-06-01", "date_echeance": "2026-07-01", "montant_ttc": 120.0}
    base.update(kw)
    r = fact.creer(base, acteur="t", db_path=env["db"])
    assert r["ok"], r
    return r["facture_id_opaque"]


def _codes(env):
    return {a["code"] for a in cat.controler(db_path=env["db"])["anomalies"]}


# ── Factures ──────────────────────────────────────────────────────────────────

def test_facture_saine_aucune_anomalie_bloquante(env):
    fid = _facture(env, justificatif="pj.pdf")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    fact.lier_charge(fid, "CHG_1", acteur="t", db_path=env["db"])
    res = cat.controler(db_path=env["db"])
    assert res["nb_bloquants"] == 0 and res["fiable"] is True


def test_validee_sans_charge_detectee(env):
    fid = _facture(env, justificatif="pj.pdf")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    assert cat.F_VALIDEE_SANS_CHARGE in _codes(env)


def test_justificatif_absent_detecte(env):
    fid = _facture(env)
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    assert cat.F_JUSTIFICATIF_ABSENT in _codes(env)


def test_charge_liee_a_plusieurs_factures_impossible_au_niveau_du_schema(env):
    """Le contrôle F_CHARGE_MULTI_FACTURES existe en défense en profondeur, mais l'état est en
    réalité IMPOSSIBLE à créer : l'index unique du schéma 0017 le refuse, même en écriture SQL
    directe. C'est la garantie la plus forte — on la vérifie explicitement."""
    import sqlite3
    f1 = _facture(env, facture_ref="FA-M1")
    f2 = _facture(env, facture_ref="FA-M2")
    fact.lier_charge(f1, "CHG_X", acteur="t", db_path=env["db"])

    assert fact.lier_charge(f2, "CHG_X", acteur="t", db_path=env["db"])["ok"] is False
    conn = get_db(env["db"])
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE factures SET charge_id=? WHERE facture_id_opaque=?",
                         ("CHG_X", f2))
    finally:
        conn.close()


def test_facture_en_retard_detectee(env):
    fid = _facture(env, facture_ref="FA-RET", date_facture="2020-01-01",
                   date_echeance="2020-02-01", justificatif="pj.pdf")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    assert cat.F_EN_RETARD in _codes(env)


def test_doublon_probable_detecte(env):
    _facture(env, facture_ref="FA-P1", montant_ttc=120.0, date_facture="2026-06-01")
    _facture(env, facture_ref="FA-P2", montant_ttc=120.0, date_facture="2026-06-03")
    assert cat.F_DOUBLON_PROBABLE in _codes(env)


def test_doublon_certain_impossible_au_niveau_du_schema(env):
    """Même logique : le doublon certain (même fournisseur + même référence) est refusé par le
    service ET par l'index unique du schéma. L'état ne peut pas exister en base."""
    import sqlite3
    _facture(env, facture_ref="FA-DC")
    f2 = _facture(env, facture_ref="FA-AUTRE")

    assert fact.creer({"fournisseur_id_opaque": env["frs"], "facture_ref": "FA-DC",
                       "montant_ttc": 10.0}, acteur="t", db_path=env["db"])["ok"] is False
    conn = get_db(env["db"])
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE factures SET facture_ref=? WHERE facture_id_opaque=?",
                         ("FA-DC", f2))
    finally:
        conn.close()


def test_tva_incoherente_detectee_si_forcee(env):
    fid = _facture(env, facture_ref="FA-TVA")
    conn = get_db(env["db"])
    conn.execute("UPDATE factures SET montant_ht=?, montant_tva=? WHERE facture_id_opaque=?",
                 (10.0, 5.0, fid))
    conn.commit(); conn.close()
    assert cat.F_TVA_INCOHERENTE in _codes(env)


def test_devise_inconnue_detectee(env):
    _facture(env, facture_ref="FA-USD", devise="USD")
    assert cat.F_DEVISE_INCONNUE in _codes(env)


def test_reglee_avec_solde_non_nul_est_bloquante(env):
    fid = _facture(env, facture_ref="FA-RS")
    conn = get_db(env["db"])
    conn.execute("UPDATE factures SET statut=? WHERE facture_id_opaque=?", (fact.ST_REGLEE, fid))
    conn.commit(); conn.close()
    assert cat.F_REGLEE_SOLDE_NON_NUL in _codes(env)


def test_annulee_avec_reglement_actif_est_bloquante(env):
    fid = _facture(env, facture_ref="FA-AN")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    regl.enregistrer(env["frs"], [{"facture_id_opaque": fid, "montant": 120.0}],
                     date_reglement="2026-07-01", moyen="CAISSE", db_path=env["db"])
    conn = get_db(env["db"])
    conn.execute("UPDATE factures SET statut=? WHERE facture_id_opaque=?", (fact.ST_ANNULEE, fid))
    conn.commit(); conn.close()
    assert cat.F_ANNULEE_AVEC_REGLEMENT in _codes(env)


def test_fournisseur_archive_signale(env):
    fid = _facture(env, facture_ref="FA-ARCH")
    f = frs.charger_par_opaque(env["frs"], db_path=env["db"])
    frs.desactiver(f, acteur="t", db_path=env["db"])
    assert cat.F_FOURNISSEUR_ARCHIVE in _codes(env)


# ── Règlements ────────────────────────────────────────────────────────────────

def test_reglement_banque_sans_rapprochement_signale(env):
    fid = _facture(env, facture_ref="FA-BQ")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    regl.enregistrer(env["frs"], [{"facture_id_opaque": fid, "montant": 120.0}],
                     date_reglement="2026-07-01", moyen="BANQUE", db_path=env["db"])
    assert cat.R_BANQUE_SANS_MOUVEMENT in _codes(env)


def test_reglement_caisse_ne_declenche_pas_le_controle_banque(env):
    fid = _facture(env, facture_ref="FA-CA")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    regl.enregistrer(env["frs"], [{"facture_id_opaque": fid, "montant": 120.0}],
                     date_reglement="2026-07-01", moyen="CAISSE", db_path=env["db"])
    assert cat.R_BANQUE_SANS_MOUVEMENT not in _codes(env)


def test_repartition_incoherente_detectee(env):
    fid = _facture(env, facture_ref="FA-REP")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    r = regl.enregistrer(env["frs"], [{"facture_id_opaque": fid, "montant": 100.0}],
                         date_reglement="2026-07-01", moyen="CAISSE", db_path=env["db"])
    conn = get_db(env["db"])
    conn.execute("UPDATE reglements_fournisseurs SET montant=? WHERE reglement_id_opaque=?",
                 (999.0, r["reglement_id_opaque"]))
    conn.commit(); conn.close()
    assert cat.R_REPARTITION_INCOHERENTE in _codes(env)


def test_compteurs_et_tri_par_severite(env):
    fid = _facture(env, facture_ref="FA-CT")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    res = cat.controler(db_path=env["db"])
    assert set(res["compteurs"]) == set(cat.NIVEAUX)
    sev = [a["severite"] for a in res["anomalies"]]
    assert sev == sorted(sev, key=lambda s: cat.NIVEAUX.index(s))


def test_chaque_anomalie_porte_les_champs_attendus(env):
    fid = _facture(env, facture_ref="FA-CH")
    fact.changer_statut(fid, fact.ST_VALIDEE, acteur="t", db_path=env["db"])
    a = cat.controler(db_path=env["db"])["anomalies"][0]
    for champ in ("code", "severite", "objet", "identifiant", "message", "montant", "action",
                  "statut", "justification"):
        assert champ in a

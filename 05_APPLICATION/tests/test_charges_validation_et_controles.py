"""Parcours de validation des charges + contrôles d'intégrité (charge refacturable orpheline).

Table `charges` (migration 0052) et référentiels `ref_*` (migration 0029) — plus aucun classeur.
Chaque test travaille sur une base temporaire isolée (`tmp_db`).
"""
from __future__ import annotations

import pytest

from app.db.connection import get_db
from app.services import charges_controles_integrite_service as ctrl
from app.services import charges_saisie_service as saisie
from app.services import charges_validation_service as val

CHARGE = {"charge_id": "CHG-1", "date_charge": "2026-06-15", "montant": 100,
          "logement_id": "LOG_A1", "proprietaire_id": "PROP_A", "refacturable": "OUI",
          "code_impact": "IC", "categorie_charge_id": "CHG_017",
          "statut_controle": "A_CONTROLER"}


def _creer_charge(db_path, **overrides) -> str:
    donnees = dict(CHARGE, **overrides)
    res = saisie.creer(donnees, acteur="fixture", db_path=db_path)
    assert res["ok"], res
    return res["charge_id"]


def _forcer_statut_controle(db_path, charge_id: str, statut_controle: str) -> None:
    conn = get_db(db_path)
    try:
        conn.execute("UPDATE charges SET statut_controle = ? WHERE charge_id = ?",
                     (statut_controle, charge_id))
        conn.commit()
    finally:
        conn.close()


def _semer_gestion(db_path, *, proprios=("PROP_A",),
                   gestion=(("LOG_A1", "PROP_A", "2026-01-01", ""))) -> None:
    conn = get_db(db_path)
    try:
        for p in proprios:
            conn.execute(
                "INSERT OR IGNORE INTO ref_proprietaires (proprietaire_id, actif, import_id) "
                "VALUES (?,?,?)", (p, "OUI", "IMP-TEST"))
        conn.commit()
    finally:
        conn.close()


def _semer_gestion_logements(db_path, entrees) -> None:
    """`entrees` : liste de (logement_id, proprietaire_id, date_debut, date_fin)."""
    conn = get_db(db_path)
    try:
        for i, (lg, pr, d, f) in enumerate(entrees, 1):
            conn.execute(
                "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, "
                "proprietaire_id, date_debut, date_fin, statut_gestion, import_id) "
                "VALUES (?,?,?,?,?,?,?)",
                (f"G{i}-{lg}", lg, pr, d, f, "ACTIF", "IMP-TEST"))
        conn.commit()
    finally:
        conn.close()


# ── Parcours de validation ───────────────────────────────────────────────────

def test_liste_expose_les_charges_a_controler(tmp_db):
    _creer_charge(tmp_db)
    res = val.lister(statut="A_CONTROLER", db_path=tmp_db)
    assert res["status"] == "OK"
    assert len(res["rows"]) == 1
    assert res["compteurs"]["A_CONTROLER"] == 1
    assert "aucun impact financier" in res["avertissement"]


def test_validation_passe_le_statut_a_valide_et_journalise(tmp_db):
    _creer_charge(tmp_db)
    r = val.valider("CHG-1", acteur="testeur", commentaire="ok", db_path=tmp_db)
    assert r["ok"] and r["nouveau_statut"] == "VALIDE" and r["recalcul_requis"] is True

    conn = get_db(tmp_db)
    try:
        row = conn.execute("SELECT statut_controle FROM charges WHERE charge_id = ?",
                           ("CHG-1",)).fetchone()
    finally:
        conn.close()
    assert row["statut_controle"] == "VALIDE"

    hist = val.historique("CHG-1", db_path=tmp_db)
    assert hist and hist[0]["nouveau_statut"] == "VALIDE" and hist[0]["acteur"] == "testeur"


def test_rejet_exige_un_commentaire(tmp_db):
    _creer_charge(tmp_db)
    assert val.rejeter("CHG-1", commentaire="", db_path=tmp_db)["code"] == val.E_COMMENTAIRE


def test_rejet_conserve_la_ligne(tmp_db):
    _creer_charge(tmp_db)
    assert val.rejeter("CHG-1", commentaire="montant erroné", db_path=tmp_db)["ok"]

    conn = get_db(tmp_db)
    try:
        rows = conn.execute("SELECT * FROM charges WHERE charge_id = ?", ("CHG-1",)).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1                      # jamais supprimée
    assert rows[0]["statut_controle"] == "REJETE"
    assert rows[0]["statut"] == "ACTIVE"        # rejeter ≠ annuler


def test_double_validation_refusee(tmp_db):
    _creer_charge(tmp_db)
    assert val.valider("CHG-1", db_path=tmp_db)["ok"]
    assert val.valider("CHG-1", db_path=tmp_db)["code"] == val.E_DEJA


def test_charge_inexistante_refusee(tmp_db):
    assert val.valider("CHG-INCONNUE", db_path=tmp_db)["code"] == val.E_INTROUVABLE


def test_charge_annulee_refusee(tmp_db):
    _creer_charge(tmp_db)
    saisie.annuler("CHG-1", acteur="test", db_path=tmp_db)
    assert val.valider("CHG-1", db_path=tmp_db)["code"] == val.E_INTROUVABLE


# ── Contrôles d'intégrité (esprit Lot11) ─────────────────────────────────────

def test_controle_bloque_charge_refacturable_orpheline(tmp_db):
    _semer_gestion(tmp_db)
    _semer_gestion_logements(tmp_db, [("LOG_A1", "PROP_A", "2026-01-01", "")])
    _creer_charge(tmp_db, proprietaire_id="", statut_controle="A_CONTROLER")
    _forcer_statut_controle(tmp_db, "CHG-1", "VALIDE")

    res = ctrl.controler(db_path=tmp_db)
    assert res["statut"] == ctrl.BLOQUANT and res["nb_bloquants"] == 1
    a = res["anomalies"][0]
    assert a["code"] == ctrl.C_REFAC_ORPHELINE
    assert "préfacture" in a["message"]
    assert a["charge_id"] == "CHG-1" and a["montant"] == 100.0


def test_controle_ok_quand_proprietaire_materialise(tmp_db):
    _semer_gestion(tmp_db)
    _semer_gestion_logements(tmp_db, [("LOG_A1", "PROP_A", "2026-01-01", "")])
    _creer_charge(tmp_db, statut_controle="A_CONTROLER")
    _forcer_statut_controle(tmp_db, "CHG-1", "VALIDE")

    res = ctrl.controler(db_path=tmp_db)
    assert res["statut"] == "OK" and res["recalcul_fiable"] is True


def test_controle_bloque_proprietaire_inconnu(tmp_db):
    _semer_gestion(tmp_db)
    _semer_gestion_logements(tmp_db, [("LOG_A1", "PROP_A", "2026-01-01", "")])
    _creer_charge(tmp_db, proprietaire_id="PROP_ZZ", statut_controle="A_CONTROLER")
    _forcer_statut_controle(tmp_db, "CHG-1", "VALIDE")

    res = ctrl.controler(db_path=tmp_db)
    codes = [a["code"] for a in res["anomalies"]]
    assert ctrl.C_PROP_INCONNU in codes and res["nb_bloquants"] >= 1


def test_controle_bloque_proprietaire_incoherent_avec_logement(tmp_db):
    _semer_gestion(tmp_db, proprios=("PROP_A", "PROP_B"))
    _semer_gestion_logements(tmp_db, [("LOG_A1", "PROP_A", "2026-01-01", "")])
    _creer_charge(tmp_db, proprietaire_id="PROP_B", statut_controle="A_CONTROLER")
    _forcer_statut_controle(tmp_db, "CHG-1", "VALIDE")

    res = ctrl.controler(db_path=tmp_db)
    codes = [a["code"] for a in res["anomalies"]]
    assert ctrl.C_PROP_INCOHERENT in codes


def test_controle_ignore_charge_non_refacturable_sans_proprietaire(tmp_db):
    _semer_gestion(tmp_db)
    _semer_gestion_logements(tmp_db, [("LOG_A1", "PROP_A", "2026-01-01", "")])
    _creer_charge(tmp_db, proprietaire_id="", logement_id="", refacturable="NON",
                 statut_controle="A_CONTROLER")
    _forcer_statut_controle(tmp_db, "CHG-1", "VALIDE")

    res = ctrl.controler(db_path=tmp_db)
    assert res["statut"] == "OK"


def test_controle_ignore_charge_non_validee(tmp_db):
    """Une charge refacturable sans propriétaire mais NON validée n'est pas bloquante."""
    _semer_gestion(tmp_db)
    _semer_gestion_logements(tmp_db, [("LOG_A1", "PROP_A", "2026-01-01", "")])
    _creer_charge(tmp_db, proprietaire_id="", statut_controle="A_CONTROLER")

    res = ctrl.controler(db_path=tmp_db)
    assert res["nb_bloquants"] == 0

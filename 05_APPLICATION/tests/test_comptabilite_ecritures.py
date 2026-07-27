"""Premier socle Comptabilité — génération d'écritures ACHATS/BANQUE, équilibre, idempotence,
contrepassation."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_ecritures_service as compta
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


@pytest.fixture
def facture_validee(db):
    frs = frs_svc.creer("Fournisseur Compta Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-COMPTA-1",
                   "date_facture": "2026-06-10", "montant_ttc": 120.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    return r["facture_id_opaque"]


# ── Plan comptable ───────────────────────────────────────────────────────────

def test_plan_comptable_seed(db):
    conn = get_db(db)
    comptes = {r["compte"] for r in conn.execute("SELECT compte FROM plan_comptable")}
    conn.close()
    assert {"401000", "512000", "606000"} <= comptes


# ── Génération ACHATS ─────────────────────────────────────────────────────────

def test_generer_ecriture_achat_equilibree(db, facture_validee):
    res = compta.generer_ecriture_achat(facture_validee, acteur="recette", db_path=db)
    assert res["ok"], res
    e = compta.charger(res["ecriture_id_opaque"], db)
    assert e["journal"] == "ACHATS"
    assert e["total_debit"] == e["total_credit"] == 120.0
    lignes = compta.lignes(res["ecriture_id_opaque"], db)
    assert len(lignes) == 2
    assert {l["compte"] for l in lignes} == {"606000", "401000"}


def test_facture_non_validee_refusee(db):
    frs = frs_svc.creer("Fournisseur Test 2", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-BROUILLON",
                   "montant_ttc": 50.0}, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    assert not res["ok"] and res["code"] == compta.E_ORIGINE_INVALIDE


def test_idempotence_generation_achat(db, facture_validee):
    """Regénérer pour la même facture est un no-op explicite, jamais une deuxième écriture."""
    r1 = compta.generer_ecriture_achat(facture_validee, db_path=db)
    r2 = compta.generer_ecriture_achat(facture_validee, db_path=db)
    assert r1["ecriture_id_opaque"] == r2["ecriture_id_opaque"]
    assert r2["deja_generee"] is True
    conn = get_db(db)
    n = conn.execute("SELECT COUNT(*) c FROM ecritures WHERE origine_id_opaque=?",
                     (facture_validee,)).fetchone()["c"]
    conn.close()
    assert n == 1


def test_idempotence_impossible_a_contourner_en_sql_direct(db, facture_validee):
    """Défense en profondeur : l'index unique refuse une seconde écriture pour la même origine."""
    import sqlite3
    compta.generer_ecriture_achat(facture_validee, db_path=db)
    conn = get_db(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO ecritures (ecriture_id_opaque, journal, date_ecriture, periode, "
                "libelle, origine_type, origine_id_opaque, total_debit, total_credit) "
                "VALUES ('ECR-DOUBLON','ACHATS','2026-06-10','2026-06','x','FACTURE',?,1,1)",
                (facture_validee,))
            conn.commit()
    finally:
        conn.close()


# ── Équilibre et comptes ──────────────────────────────────────────────────────

def test_ecriture_desequilibree_refusee(db):
    res = compta._inserer_ecriture(
        "ODIVERSES", "2026-06-01", "2026-06", "P1", "Test", "MANUEL", "X1",
        [{"compte": "606000", "debit": 100, "credit": 0},
         {"compte": "401000", "debit": 0, "credit": 90}],
        db_path=db)
    assert not res["ok"] and res["code"] == compta.E_DESEQUILIBRE


def test_compte_inconnu_refuse(db):
    res = compta._inserer_ecriture(
        "ODIVERSES", "2026-06-01", "2026-06", "P1", "Test", "MANUEL", "X2",
        [{"compte": "999999", "debit": 100, "credit": 0},
         {"compte": "401000", "debit": 0, "credit": 100}],
        db_path=db)
    assert not res["ok"] and res["code"] == compta.E_COMPTE_INCONNU


# ── Validation et solde ───────────────────────────────────────────────────────

def test_solde_compte_ignore_les_ecritures_non_validees(db, facture_validee):
    r = compta.generer_ecriture_achat(facture_validee, db_path=db)
    avant = compta.solde_compte("401000", db_path=db)
    assert avant["credit"] == 0.0          # PROPOSEE : pas encore compté
    compta.valider(r["ecriture_id_opaque"], db_path=db)
    apres = compta.solde_compte("401000", db_path=db)
    assert apres["credit"] == 120.0


def test_solde_auxiliaire_fournisseur(db, facture_validee):
    r = compta.generer_ecriture_achat(facture_validee, db_path=db)
    compta.valider(r["ecriture_id_opaque"], db_path=db)
    f = fact.charger(facture_validee, db)
    s = compta.solde_auxiliaire(f["fournisseur_id_opaque"], db_path=db)
    assert s["credit"] == 120.0


# ── Contrepassation ───────────────────────────────────────────────────────────

def test_contrepasser_cree_une_ecriture_miroir(db, facture_validee):
    r = compta.generer_ecriture_achat(facture_validee, db_path=db)
    compta.valider(r["ecriture_id_opaque"], db_path=db)
    res = compta.contrepasser(r["ecriture_id_opaque"], commentaire="Annulation test", db_path=db)
    assert res["ok"]
    origine = compta.charger(r["ecriture_id_opaque"], db)
    assert origine["statut"] == compta.ST_CONTREPASSEE
    miroir = compta.charger(res["miroir_id_opaque"], db)
    assert miroir["contrepasse_de"] == r["ecriture_id_opaque"]
    # Le solde net redevient nul : l'écriture d'origine reste, jamais supprimée.
    apres = compta.solde_compte("401000", db_path=db)
    assert apres["solde"] == 0.0


def test_ecriture_jamais_supprimee_apres_contrepassation(db, facture_validee):
    r = compta.generer_ecriture_achat(facture_validee, db_path=db)
    compta.contrepasser(r["ecriture_id_opaque"], db_path=db)
    assert compta.charger(r["ecriture_id_opaque"], db) is not None


def test_regenerer_apres_contrepassation_est_possible(db, facture_validee):
    """Contrepasser libère l'origine (index unique filtre `statut <> CONTREPASSEE`) : une nouvelle
    écriture peut être régénérée si la facture redevient d'actualité."""
    r1 = compta.generer_ecriture_achat(facture_validee, db_path=db)
    compta.contrepasser(r1["ecriture_id_opaque"], db_path=db)
    r2 = compta.generer_ecriture_achat(facture_validee, db_path=db)
    assert r2["ok"] and r2["ecriture_id_opaque"] != r1["ecriture_id_opaque"]


# ── Génération BANQUE ─────────────────────────────────────────────────────────

def test_generer_ecriture_banque_depuis_rapprochement_confirme(db):
    """`generer_ecriture_banque` ne dépend que de `banque_rapprochements` et
    `reglements_fournisseurs` : testé directement, sans le lecteur Excel des mouvements."""
    frs = frs_svc.creer("Fournisseur Banque Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    conn = get_db(db)
    conn.execute(
        "INSERT INTO reglements_fournisseurs (reglement_id_opaque, fournisseur_id_opaque, "
        "date_reglement, montant, moyen, statut) VALUES (?,?,?,?,?,?)",
        ("REG-COMPTA-1", frs, "2026-06-15", 80.0, "BANQUE", "RAPPROCHE"))
    conn.execute(
        "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
        "type_objet, objet_id, montant_rapproche, statut) VALUES (?,?,?,?,?,?)",
        ("BRP-COMPTA-1", "MVT-COMPTA-1", "REGLEMENT_CHARGE", "REG-COMPTA-1", 80.0, "CONFIRME"))
    conn.commit(); conn.close()

    res = compta.generer_ecriture_banque("BRP-COMPTA-1", acteur="recette", db_path=db)
    assert res["ok"], res
    e = compta.charger(res["ecriture_id_opaque"], db)
    assert e["journal"] == "BANQUE"
    assert e["total_debit"] == e["total_credit"] == 80.0
    lignes = {l["compte"]: l for l in compta.lignes(res["ecriture_id_opaque"], db)}
    assert lignes["401000"]["debit"] == 80.0 and lignes["401000"]["auxiliaire"] == frs
    assert lignes["512000"]["credit"] == 80.0


def test_rapprochement_non_confirme_refuse(db):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO reglements_fournisseurs (reglement_id_opaque, fournisseur_id_opaque, "
        "date_reglement, montant, moyen, statut) VALUES (?,?,?,?,?,?)",
        ("REG-COMPTA-2", "FRS-X", "2026-06-15", 50.0, "BANQUE", "ENREGISTRE"))
    conn.execute(
        "INSERT INTO banque_rapprochements (rapprochement_id_opaque, mouvement_id_opaque, "
        "type_objet, objet_id, montant_rapproche, statut) VALUES (?,?,?,?,?,?)",
        ("BRP-COMPTA-2", "MVT-COMPTA-2", "REGLEMENT_CHARGE", "REG-COMPTA-2", 50.0, "PROPOSE"))
    conn.commit(); conn.close()
    res = compta.generer_ecriture_banque("BRP-COMPTA-2", db_path=db)
    assert not res["ok"] and res["code"] == compta.E_ORIGINE_INVALIDE


# ── Double verrou ─────────────────────────────────────────────────────────────

def test_generer_refuse_sans_flags(db, facture_validee, monkeypatch):
    monkeypatch.setattr(cfg, "COMPTABILITE_REAL_WRITE_ENABLED", False)
    res = compta.generer_ecriture_achat(facture_validee, db_path=db)
    assert not res["ok"] and res["code"] == compta.E_FLAGS

"""Axes analytiques (Bloc 3) : fournisseur, catégorie, prestataire — plateforme/activité assumés
NON_DISPONIBLE."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import comptabilite_axes_service as axes
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


# ── Fournisseur ────────────────────────────────────────────────────────────

def test_fournisseurs_vide(db):
    assert axes.fournisseurs(db_path=db)["statut"] == axes.NON_DISPONIBLE


def test_fournisseurs_avec_dette(db):
    frs = frs_svc.creer("Fournisseur Axe Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-AXE-1",
                   "date_facture": "2026-06-10", "montant_ttc": 50.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    res = compta.generer_ecriture_achat(r["facture_id_opaque"], db_path=db)
    compta.valider(res["ecriture_id_opaque"], db_path=db)

    liste = axes.fournisseurs(db_path=db)
    assert liste["statut"] == "OK"
    assert any(l["auxiliaire"] == frs for l in liste["lignes"])

    detail = axes.fournisseur_detail(frs, db_path=db)
    assert detail["statut"] == "OK"
    assert detail["fiche"]["solde"]["solde"] == -50.0


def test_fournisseur_detail_inconnu(db):
    detail = axes.fournisseur_detail("FRS-INCONNU", db_path=db)
    assert detail["statut"] == axes.NON_DISPONIBLE


# ── Catégorie ──────────────────────────────────────────────────────────────

def test_categories_sans_source(db, monkeypatch):
    import app.readers.charges_reader as cr
    monkeypatch.setattr(cr, "read_charges", lambda: [])
    assert axes.categories()["statut"] == axes.NON_DISPONIBLE


def test_categories_agregation(db, monkeypatch):
    import app.readers.charges_reader as cr
    monkeypatch.setattr(cr, "read_charges", lambda: [
        {"charge_id": "CHG_1", "categorie_charge_id": "CAT_A", "montant": 100.0},
        {"charge_id": "CHG_2", "categorie_charge_id": "CAT_A", "montant": 50.0},
        {"charge_id": "CHG_3", "categorie_charge_id": "CAT_B", "montant": 30.0},
    ])
    res = axes.categories()
    assert res["statut"] == "OK"
    par_cat = {l["categorie"]: l for l in res["lignes"]}
    assert par_cat["CAT_A"]["montant"] == 150.0 and par_cat["CAT_A"]["nb"] == 2
    assert par_cat["CAT_B"]["montant"] == 30.0

    detail = axes.categorie_detail("CAT_A")
    assert detail["statut"] == "OK"
    assert detail["montant_total"] == 150.0
    assert len(detail["lignes"]) == 2


def test_categorie_detail_inconnue(db, monkeypatch):
    import app.readers.charges_reader as cr
    monkeypatch.setattr(cr, "read_charges", lambda: [
        {"charge_id": "CHG_1", "categorie_charge_id": "CAT_A", "montant": 100.0}])
    detail = axes.categorie_detail("CAT_INCONNUE")
    assert detail["statut"] == axes.NON_DISPONIBLE


# ── Prestataire ────────────────────────────────────────────────────────────

def _menage(db, menage_id, fournisseur_id, mois, logement_id, cout_prevu, cout_reel, statut="REGLE"):
    conn = get_db(db)
    conn.execute(
        "INSERT INTO menages (menage_id_opaque, logement_id, proprietaire_id, type_menage, mois, "
        "statut, fournisseur_id_opaque, cout_prevu, cout_reel) VALUES (?,?,?,?,?,?,?,?,?)",
        (menage_id, logement_id, "PROP_X", "EXTERNE", mois, statut, fournisseur_id,
         cout_prevu, cout_reel))
    conn.commit()
    conn.close()


def test_prestataires_vide(db):
    assert axes.prestataires(db_path=db)["statut"] == axes.NON_DISPONIBLE


def test_prestataires_agregation(db):
    _menage(db, "MEN-1", "FRS-PREST-1", "2026-06", "LOG_A1", 30.0, 32.0)
    _menage(db, "MEN-2", "FRS-PREST-1", "2026-06", "LOG_B1", 30.0, 28.0)
    _menage(db, "MEN-3", "FRS-PREST-2", "2026-06", "LOG_A1", 40.0, 40.0)

    res = axes.prestataires(mois="2026-06", db_path=db)
    assert res["statut"] == "OK"
    par_prest = {l["prestataire_id"]: l for l in res["lignes"]}
    assert par_prest["FRS-PREST-1"]["nb_menages"] == 2
    assert par_prest["FRS-PREST-1"]["cout_prevu"] == 60.0
    assert par_prest["FRS-PREST-1"]["cout_reel"] == 60.0
    assert par_prest["FRS-PREST-1"]["nb_logements"] == 2
    assert par_prest["FRS-PREST-2"]["nb_menages"] == 1


def test_prestataire_detail(db):
    _menage(db, "MEN-4", "FRS-PREST-3", "2026-06", "LOG_C1", 25.0, 27.0)
    detail = axes.prestataire_detail("FRS-PREST-3", mois="2026-06", db_path=db)
    assert detail["statut"] == "OK"
    assert len(detail["lignes"]) == 1
    assert detail["cout_prevu_total"] == 25.0
    assert detail["cout_reel_total"] == 27.0


def test_prestataire_detail_inconnu(db):
    detail = axes.prestataire_detail("FRS-INCONNU", db_path=db)
    assert detail["statut"] == axes.NON_DISPONIBLE


def test_menage_annule_exclu(db):
    _menage(db, "MEN-5", "FRS-PREST-4", "2026-06", "LOG_D1", 20.0, 20.0, statut="ANNULE")
    res = axes.prestataires(mois="2026-06", db_path=db)
    assert res["statut"] == axes.NON_DISPONIBLE


# ── Plateforme / Activité : NON_DISPONIBLE assumé ─────────────────────────

def test_plateformes_non_disponible():
    res = axes.plateformes()
    assert res["statut"] == axes.NON_DISPONIBLE
    assert "raison" in res


def test_activites_non_disponible():
    res = axes.activites()
    assert res["statut"] == axes.NON_DISPONIBLE
    assert "raison" in res

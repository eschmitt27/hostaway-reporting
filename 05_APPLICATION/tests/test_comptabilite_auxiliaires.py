"""Cœur Comptabilité — auxiliaires fournisseurs/propriétaires/associés."""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations
from app.services import comptabilite_auxiliaires_service as aux
from app.services import comptabilite_ecritures_service as compta
from app.services import factures_service as fact
from app.services import fournisseurs_referentiel_service as frs_svc
from app.services import operations_caisse_service as caisse


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


def test_fournisseur_dette_et_element_ouvert(db):
    frs = frs_svc.creer("Fournisseur Aux Test", "MAINTENANCE", db_path=db)["fournisseur_id_opaque"]
    r = fact.creer({"fournisseur_id_opaque": frs, "facture_ref": "FA-AUX-1",
                   "date_facture": "2026-06-10", "montant_ttc": 90.0}, db_path=db)
    fact.changer_statut(r["facture_id_opaque"], fact.ST_VALIDEE, db_path=db)
    compta.generer_ecriture_achat(r["facture_id_opaque"], acteur="recette", db_path=db)
    res = compta.charger(compta.lister(journal="ACHATS", db_path=db)[0]["ecriture_id_opaque"], db)
    compta.valider(res["ecriture_id_opaque"], db_path=db)

    assert frs in aux.lister_auxiliaires(aux.FAMILLE_FOURNISSEUR, db)
    fiche = aux.fiche_auxiliaire(aux.FAMILLE_FOURNISSEUR, frs, db_path=db)
    assert fiche["solde"]["solde"] == -90.0    # dette (compte PASSIF)
    assert len(fiche["elements_ouverts"]) == 1
    assert fiche["elements_ouverts"][0]["identifiant"] == "FA-AUX-1"


def test_proprietaire_ecriture_proposee_est_element_ouvert(db):
    compta.generer_ecriture_vente("PROP_AUX", "2026-06", 200.0, db_path=db)
    fiche = aux.fiche_auxiliaire(aux.FAMILLE_PROPRIETAIRE, "PROP_AUX", db_path=db)
    # PROPOSEE ne compte pas encore dans le solde (validation explicite requise) — mais visible
    # comme élément ouvert.
    assert fiche["solde"]["solde"] == 0.0
    assert len(fiche["elements_ouverts"]) == 1


def test_associe_via_operation_caisse(db):
    op = caisse.creer("REMBOURSEMENT_ASSOCIE", 45.0, tiers_id="PERS_X", db_path=db)
    res = compta.generer_ecriture_caisse_operation(op["operation_id_opaque"], db_path=db)
    compta.valider(res["ecriture_id_opaque"], db_path=db)
    assert "PERS_X" in aux.lister_auxiliaires(aux.FAMILLE_ASSOCIE, db)
    fiche = aux.fiche_auxiliaire(aux.FAMILLE_ASSOCIE, "PERS_X", db_path=db)
    assert fiche["solde"]["debit"] == 45.0


def test_synthese_regroupe_par_famille(db):
    compta.generer_ecriture_vente("PROP_Y", "2026-06", 100.0, db_path=db)
    s = aux.synthese(db_path=db)
    assert set(s.keys()) == set(aux.FAMILLES)
    assert any(l["auxiliaire"] == "PROP_Y" for l in s[aux.FAMILLE_PROPRIETAIRE])


# ── §78 — un compte auxiliaire s'appelle par le nom du tiers ────────────────────────────────────

def test_l_auxiliaire_porte_le_nom_du_tiers(tmp_path):
    """« Didier UZON », pas « PROP_0001 » : personne ne tient une balance en lisant des codes."""
    from app.db.connection import apply_migrations, get_db
    from app.services import comptabilite_auxiliaires_service as aux

    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES (?,?,?,?,?)",
            ("PROP_0001", "UZON", "Didier", "OUI", "TEST"))
        conn.commit()
    finally:
        conn.close()
    libelle = aux.libelle_auxiliaire(aux.FAMILLE_PROPRIETAIRE, "PROP_0001", db_path=db)
    assert "UZON" in libelle and libelle != "PROP_0001"


def test_un_tiers_inconnu_garde_son_code(tmp_path):
    """Mieux vaut un code qu'un nom inventé — et le code dit quoi aller corriger."""
    from app.db.connection import apply_migrations
    from app.services import comptabilite_auxiliaires_service as aux

    db = tmp_path / "test.db"
    apply_migrations(db)
    assert aux.libelle_auxiliaire(aux.FAMILLE_PROPRIETAIRE, "PROP_9999", db_path=db) == "PROP_9999"


def test_les_familles_ont_un_libelle_comptable():
    from app.services import comptabilite_auxiliaires_service as aux

    assert aux.LIBELLES_FAMILLE[aux.FAMILLE_PROPRIETAIRE] == "Propriétaires (411)"
    assert set(aux.LIBELLES_FAMILLE) == set(aux.FAMILLES)


def test_le_libelle_d_ecriture_ne_porte_plus_de_code_interne(tmp_path):
    """« Facture 2026-08-001 — PROP_0002 / LOG_0002 » se lisait dans le journal et le grand livre."""
    from app.db.connection import apply_migrations, get_db
    from app.services import comptabilite_ecritures_service as compta

    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO ref_proprietaires (proprietaire_id, nom_proprietaire, "
            "prenom_proprietaire, actif, import_id) VALUES (?,?,?,?,?)",
            ("PROP_0002", "Delrieu", "Cédrine", "OUI", "TEST"))
        conn.execute(
            "INSERT INTO ref_logements (logement_id, nom_logement_officiel, nom_court, adresse, "
            "ville, actif, statut_parc, import_id) VALUES (?,?,?,?,?,?,?,?)",
            ("LOG_0002", "T4 - 90 Blagnac", "T4 - 90 Blagnac", "", "BLAGNAC", "OUI", "GERE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    assert compta._nom_tiers("PROP_0002", db) == "Cédrine Delrieu"
    assert compta._nom_logement("LOG_0002", db) == "T4 - 90 Blagnac"
    # L'identifiant reste disponible là où il sert : la colonne `auxiliaire`.
    assert compta._nom_tiers("PROP_INCONNU", db) == "PROP_INCONNU"

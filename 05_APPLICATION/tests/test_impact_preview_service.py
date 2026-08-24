"""Mission 6 quater — aperçu structurel des impacts d'une modification de règle temporelle.

`impact_preview_service.py` ne recalcule AUCUN montant économique : il compte des objets
potentiellement concernés (réservations, factures) à partir de `date_debut`, et liste les datasets
aval du DAG existant. Ces tests prouvent l'absence de calcul financier inventé et la cohérence des
comptages avec des données seedées directement.
"""
import app.config as cfg
import fixtures_referentiel as fx
import pytest
from app.db.connection import get_db
from app.services import impact_preview_service as preview


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _seed_reservation(db_path, logement_id, date_arrivee, guest_count=4):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO reservations_resolues (dataset_id, reservation_calc_id, logement_id, "
            "date_arrivee, guest_count) VALUES (?,?,?,?,?)",
            ("DS_TEST", f"RES-{logement_id}-{date_arrivee}", logement_id, date_arrivee, guest_count))
        conn.commit()
    finally:
        conn.close()


def _seed_facture_proprietaire(db_path, logement_id, date_facture):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO factures_proprietaires (facture_id_opaque, proprietaire_id, logement_id, "
            "mois, date_facture) VALUES (?,?,?,?,?)",
            (f"FPR-{logement_id}-{date_facture}", "PROP_A", logement_id, date_facture[:7], date_facture))
        conn.commit()
    finally:
        conn.close()


def _seed_facture_fournisseur(db_path, facture_id, date_facture):
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "date_facture, montant_ttc) VALUES (?,?,?,?,?)",
            (facture_id, "FRS-1", facture_id, date_facture, 100.0))
        conn.commit()
    finally:
        conn.close()


def test_taux_commission_compte_reservations_a_partir_de_la_date(ref):
    _seed_reservation(ref, "LOG_A1", "2026-05-01")   # avant la date d'effet -> pas compté
    _seed_reservation(ref, "LOG_A1", "2026-07-15")   # après -> compté
    _seed_reservation(ref, "LOG_B1", "2026-07-15")   # autre logement -> pas compté
    res = preview.previsualiser_taux_commission("LOG_A1", "2026-07-01", db_path=ref)
    assert res["reservations_potentielles"] == 1
    assert res["type_regle"] == "ASSIETTE_COMMISSION/TAUX_COMMISSION"
    assert "datasets_aval" in res and len(res["datasets_aval"]) > 0


def test_taux_commission_compte_les_factures_proprietaires(ref):
    _seed_facture_proprietaire(ref, "LOG_A1", "2026-05-01")
    _seed_facture_proprietaire(ref, "LOG_A1", "2026-08-01")
    res = preview.previsualiser_taux_commission("LOG_A1", "2026-07-01", db_path=ref)
    assert res["factures_proprietaires_potentielles"] == 1


def test_canape_compte_uniquement_les_reservations_avec_guest_count(ref):
    _seed_reservation(ref, "LOG_A1", "2026-07-15", guest_count=6)
    res = preview.previsualiser_canape("LOG_A1", "2026-07-01", db_path=ref)
    assert res["reservations_potentielles"] == 1


def test_cout_menage_compte_logements_du_type_et_leurs_reservations(ref):
    _seed_reservation(ref, "LOG_A1", "2026-07-15")
    res = preview.previsualiser_cout_menage("TYPE_001", "2026-07-01", db_path=ref)
    assert res["nb_logements_concernes"] >= 1
    assert res["reservations_potentielles"] == 1


def test_regle_repartition_compte_les_factures_fournisseur(ref):
    _seed_facture_fournisseur(ref, "FAC-1", "2026-05-01")
    _seed_facture_fournisseur(ref, "FAC-2", "2026-07-15")
    res = preview.previsualiser_regle_repartition("2026-07-01", db_path=ref)
    assert res["factures_potentielles"] == 1


def test_aucun_montant_financier_dans_le_resultat(ref):
    """§12 de la mission : le preview est structurel — jamais un montant € inventé."""
    for res in (
        preview.previsualiser_taux_commission("LOG_A1", "2026-07-01", db_path=ref),
        preview.previsualiser_canape("LOG_A1", "2026-07-01", db_path=ref),
        preview.previsualiser_cout_menage("TYPE_001", "2026-07-01", db_path=ref),
        preview.previsualiser_regle_repartition("2026-07-01", db_path=ref),
    ):
        for cle in res:
            assert "montant" not in cle and "€" not in cle and "financier" not in cle


def test_point_entree_generique_par_rule_code(ref):
    _seed_reservation(ref, "LOG_A1", "2026-07-15")
    res = preview.previsualiser_impacts_regle(
        "ASSIETTE_COMMISSION", logement_id="LOG_A1", date_debut="2026-07-01", db_path=ref)
    assert res["reservations_potentielles"] == 1

    res_rep = preview.previsualiser_impacts_regle(
        "REGLE_REPARTITION_CHARGE_COMMUNE", date_debut="2026-07-01", db_path=ref)
    assert "factures_potentielles" in res_rep

    res_inconnu = preview.previsualiser_impacts_regle(
        "REGLE_INCONNUE", date_debut="2026-07-01", db_path=ref)
    assert "message" in res_inconnu


def test_base_absente_ne_leve_jamais_exception(tmp_path):
    """Base non initialisée / tables absentes : compte 0, jamais une exception."""
    db = tmp_path / "vide.db"
    from app.db.connection import apply_migrations
    apply_migrations(db)
    res = preview.previsualiser_taux_commission("LOG_X", "2026-07-01", db_path=db)
    assert res["reservations_potentielles"] == 0

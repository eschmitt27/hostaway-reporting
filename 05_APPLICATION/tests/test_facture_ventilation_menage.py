"""Ventilation d'un montant non affecté (facture prestataire ménage externe) — mission §9-14/§35.

Cas 2/3 du plan de tests métier de la mission : ligne non affectée + base exploitable → ventilation
proportionnelle centimes exacts ; ligne non affectée sans base → A_CONTROLER, pas de ventilation.
"""
from __future__ import annotations

from app.services import facture_ventilation_menage_service as vent
from app.services import intervenant_menage_compte_service as compte_interne


def test_frais_internes_non_geres_par_le_module_de_ventilation_externe():
    """§15/§37 : un frais/heures supplémentaire interne n'est PAS ventilé automatiquement par ce
    module — il passe par le module Charges existant. Vérifié structurellement : le service du
    compte intervenant interne n'importe jamais le service de ventilation externe."""
    import inspect
    source = inspect.getsource(compte_interne)
    assert "facture_ventilation_menage_service" not in source
    assert "ventiler" not in source


def test_exemple_metier_55_90_10():
    """Logement A : 1 ménage x 55€ = 55€. Logement B : 3 ménages x 30€ = 90€. Frais non affecté : 10€.
    Attendu : A += 3,79€ ; B += 6,21€ ; somme des parts = 10,00€ exactement."""
    resultat = vent.ventiler(10.0, {"LOG_A": 55.0, "LOG_B": 90.0})
    assert resultat["statut"] == vent.ST_APPLIQUEE
    parts = {p["logement_id"]: p["part_montant"] for p in resultat["parts"]}
    assert parts == {"LOG_A": 3.79, "LOG_B": 6.21}
    assert resultat["somme"] == 10.0


def test_somme_toujours_exacte_au_centime_meme_avec_arrondis_difficiles():
    # 3 logements de poids proches : cas classique de dérive d'arrondi sans résidu déterministe.
    resultat = vent.ventiler(10.0, {"A": 33.33, "B": 33.33, "C": 33.34})
    assert resultat["somme"] == 10.0
    assert sum(p["part_montant"] for p in resultat["parts"]) == 10.0


def test_ligne_sans_base_exploitable_refuse_toute_ventilation():
    resultat = vent.ventiler(10.0, {})
    assert resultat["statut"] == vent.ST_NON_EFFECTUEE
    assert resultat["parts"] == []
    assert resultat["message"] == vent.MOTIF_AUCUNE_BASE


def test_logements_a_cout_nul_exclus_de_la_base():
    resultat = vent.ventiler(10.0, {"A": 55.0, "B": 0.0, "C": -5.0})
    assert resultat["statut"] == vent.ST_APPLIQUEE
    assert {p["logement_id"] for p in resultat["parts"]} == {"A"}
    assert resultat["somme"] == 10.0


def test_montant_nul_rien_a_ventiler():
    resultat = vent.ventiler(0.0, {"A": 55.0})
    assert resultat["statut"] == vent.ST_NON_EFFECTUEE
    assert resultat["parts"] == []


def test_enregistrement_trace_la_regle_utilisee(tmp_db):
    resultat = vent.ventiler_et_enregistrer("FAC-TEST-1", 10.0, {"LOG_A": 55.0, "LOG_B": 90.0},
                                            db_path=tmp_db)
    assert resultat["ventilation_id_opaque"].startswith("VENT-")
    assert resultat["statut"] == vent.ST_APPLIQUEE

    tracees = vent.ventilations("FAC-TEST-1", db_path=tmp_db)
    assert len(tracees) == 1
    assert tracees[0]["base_ponderation"] == vent.BASE_COUT_MENAGES_FACTURE
    assert tracees[0]["montant_non_affecte"] == 10.0
    parts = {p["logement_id"]: p["part_montant"] for p in tracees[0]["parts"]}
    assert parts == {"LOG_A": 3.79, "LOG_B": 6.21}


def test_enregistrement_sans_base_trace_le_motif_non_effectuee(tmp_db):
    resultat = vent.ventiler_et_enregistrer("FAC-TEST-2", 10.0, {}, db_path=tmp_db)
    assert resultat["statut"] == vent.ST_NON_EFFECTUEE

    tracees = vent.ventilations("FAC-TEST-2", db_path=tmp_db)
    assert tracees[0]["statut"] == vent.ST_NON_EFFECTUEE
    assert tracees[0]["motif"] == vent.MOTIF_AUCUNE_BASE
    assert tracees[0]["parts"] == []


def test_rejeu_meme_entrees_meme_resultat(tmp_db):
    a = vent.ventiler_et_enregistrer("FAC-TEST-3", 10.0, {"LOG_A": 55.0, "LOG_B": 90.0},
                                     db_path=tmp_db)
    b = vent.ventiler_et_enregistrer("FAC-TEST-3", 10.0, {"LOG_A": 55.0, "LOG_B": 90.0},
                                     db_path=tmp_db)
    # Deux ventilations tracées (append-only, comme les événements de facture) mais avec le même
    # résultat chiffré : le calcul est déterministe, rejouer ne change rien au montant.
    assert a["somme"] == b["somme"] == 10.0
    assert len(vent.ventilations("FAC-TEST-3", db_path=tmp_db)) == 2

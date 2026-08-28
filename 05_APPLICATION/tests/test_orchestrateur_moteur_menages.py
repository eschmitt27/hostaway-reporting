"""MENAGES branché au DAG (mission 14e) — `executer_menages()` exécute réellement lot6d puis
lot6e puis lot6f en sous-processus, tout SQLite, sur une base isolée.

Test réel (pas de mock de subprocess) : preuve que le chemin qu'emprunte l'orchestrateur en
production produit un résultat non nul, pas seulement que le DAG le déclare (« un test avec
Lot9=0 n'est pas une preuve de fonctionnement »).
"""
from __future__ import annotations

import datetime
import sqlite3

import pytest

from app.db.connection import apply_migrations, get_db
from app.services import orchestrateur_moteur as om

# lot6d/6e/6f traitent UN mois par appel : sans --mois explicite (le cas réel de
# `executer_menages`), ils retombent sur MAX(mois) de `menages_taches_enrichies`, ou sur le mois
# calendaire courant si cette table est vide. La fixture doit donc utiliser le mois courant réel
# pour rester valide quel que soit le jour d'exécution du test.
MOIS_COURANT = datetime.date.today().strftime("%Y-%m")


@pytest.fixture
def db_avec_menage_interne(tmp_path):
    """Base réelle (migrations applicatives) + une déclaration de ménage interne, résolvable de
    bout en bout jusqu'à `menages_cout_complet`."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)
    conn = get_db(db_path)
    try:
        conn.execute(
            "INSERT INTO ref_intervenants (intervenant_id, nom_intervenant, type_intervenant, "
            "actif, nom_normalise, import_id) "
            "VALUES ('INT1','Femme de menage 1','INTERNE','OUI','FEMMEDEMENAGE1','IMP-1')")
        conn.execute(
            "INSERT INTO ref_logements (logement_id, hostaway_listing_id, statut_parc, actif, "
            "import_id) VALUES ('LOG_A1','480136','GERE','OUI','IMP-1')")
        conn.execute(
            "INSERT INTO ref_gestion_logements_hist (gestion_id, logement_id, proprietaire_id, "
            "date_debut, date_fin, statut_gestion, import_id) "
            "VALUES ('GST-1','LOG_A1','PROP_A','2025-01-01','','ACTIF','IMP-1')")
        conn.execute(
            "INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
            "VALUES (?, 'CLOTURE', 'IMP-1')", (MOIS_COURANT,))
        conn.execute(
            "INSERT INTO menages_declarations_internes (mois, logement_id, intervenant_id, "
            "nb_menages, statut_controle, run_id) "
            "VALUES (?, 'LOG_A1', 'INT1', 1, 'VALIDE', 'SEED-1')", (MOIS_COURANT,))
        conn.execute(
            "INSERT INTO ref_couts_standards_menage (cout_standard_id, type_logement_id, "
            "cout_standard_menage, date_debut_validite, actif, import_id) "
            "VALUES ('CSM-1','STD',45.0,'2025-01-01','OUI','IMP-1')")
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_executer_menages_produit_cout_complet_non_nul(db_avec_menage_interne):
    resultat = om.executer_menages(db_path=db_avec_menage_interne)

    assert resultat["ok"] is True, resultat

    conn = sqlite3.connect(str(db_avec_menage_interne))
    rappro = conn.execute("SELECT COUNT(*) FROM menages_rapprochement").fetchone()[0]
    gainperte = conn.execute("SELECT COUNT(*) FROM menages_gainperte").fetchone()[0]
    cout_complet = conn.execute("SELECT COUNT(*) FROM menages_cout_complet").fetchone()[0]
    conn.close()

    assert rappro == 1
    assert gainperte == 1
    assert cout_complet == 1


def test_executer_menages_echoue_proprement_sans_referentiel_intervenants(tmp_path):
    """Base sans `ref_intervenants` importé : fail-closed, pas de recalcul silencieux à zéro sens."""
    db_path = tmp_path / "app.db"
    apply_migrations(db_path)

    resultat = om.executer_menages(db_path=db_path)
    assert resultat["ok"] is False
    assert resultat["code"] == "MENAGES_REFERENTIEL_ABSENT"

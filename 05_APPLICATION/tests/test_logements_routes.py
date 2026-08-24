"""Couche HTTP du module Logements : fiche = écran central du cycle de vie.

Vérifie que création, modification, changement de propriétaire, changement de taux, archivage et
réactivation sont pilotables depuis le navigateur (routes + templates), sans jamais manipuler les
fichiers Excel directement. Complète les tests de service (`test_logements_creation.py`,
`test_logements_gestion.py`) par des tests de bout en bout HTTP.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from test_logements import construire_referentiel


@pytest.fixture
def ref(tmp_path, monkeypatch):
    """Référentiel SQLite pour les lectures ET les écritures.

    Les actions d'administration (créer, changer de propriétaire, changer de taux) écrivaient dans
    `REF_Setup.xlsm` ; depuis la migration 0051 elles écrivent dans les tables `ref_*`. La fixture
    n'a donc plus qu'une seule source à décrire.
    """
    db = construire_referentiel(
        tmp_path,
        logements=[{"logement_id": "LOG_A1", "nom_logement_officiel": "Fictif A1",
                    "nom_court": "A1", "adresse": "1 rue", "ville": "RECETTE",
                    "type_logement_id": "TYPE_001", "sur_hostaway": "OUI", "actif": "OUI",
                    "statut_parc": "GERE",
                    "forfait_logiciel_consommables_mensuel": "0"}],
        gestion=[{"gestion_id": "GST_1", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
                  "date_debut": "2026-01-01", "date_fin": "", "statut_gestion": "ACTIF",
                  "source": "FICTIF"}],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
        taux=[{"taux_commission_id": "TX_A1", "proprietaire_id": "PROP_A",
               "logement_id": "LOG_A1", "taux_commission": "0.19", "date_debut": "2026-01-01",
               "date_fin": "", "actif": "OUI"}],
        proprietaires=[{"proprietaire_id": "PROP_A", "nom_proprietaire": "Nom PROP_A",
                        "actif": "OUI"},
                       {"proprietaire_id": "PROP_B", "nom_proprietaire": "Nom PROP_B",
                        "actif": "OUI"}])
    monkeypatch.setattr(cfg, "DB_PATH", db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return db


# ── Fiche logement = écran central ───────────────────────────────────────────

def test_fiche_affiche_etat_actuel_historique_et_actions(client, ref):
    html = client.get("/logements/LOG_A1").text
    assert "État actuel (temps réel)" in html
    assert "Modifier les informations" in html
    assert "Changer de propriétaire" in html
    assert "Changer le taux de commission" in html
    assert "Archiver ce logement" in html            # actif -> proposé
    assert "Réactiver ce logement" not in html
    assert "Historique — rattachements de gestion" in html
    assert "Historique — taux de commission" in html
    assert 'action="/logements/LOG_A1/modifier"' in html
    assert 'action="/logements/LOG_A1/changer-proprietaire"' in html
    assert 'action="/logements/LOG_A1/changer-taux-commission"' in html
    assert 'action="/logements/LOG_A1/archiver"' in html


def test_fiche_logement_inconnu_404(client, ref):
    r = client.get("/logements/LOG_ZZZ")
    assert r.status_code == 404


def test_fiche_affiche_boutons_desactives_si_flags_off(client, ref, monkeypatch):
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", False)
    html = client.get("/logements/LOG_A1").text
    assert "ces actions sont indisponibles" in html
    assert "disabled" in html


# ── Création ─────────────────────────────────────────────────────────────────

def test_page_creation_affiche_le_formulaire(client, ref):
    html = client.get("/logements/nouveau").text
    assert 'action="/logements"' in html
    assert "PROP_A" in html and "PROP_B" in html


def test_creer_puis_consulter_la_fiche(client, ref):
    r = client.post("/logements", data={
        "logement_id": "LOG_NEW", "nom_logement_officiel": "Nouveau", "nom_court": "NEW",
        "proprietaire_id": "PROP_B", "date_debut": "2026-06-01",
    }, follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"].startswith("/logements/LOG_NEW")

    r2 = client.get(r.headers["location"])
    assert r2.status_code == 200
    assert "Logement créé" in r2.text
    # Le logement est immédiatement COMPLET : il est écrit dans le référentiel SQLite, que la fiche
    # lit directement. Il n'y a plus de fiche « minimale en attente de l'export PBI » — c'était la
    # conséquence d'une création qui n'atterrissait que dans le classeur.
    assert "PROP_B" in r2.text
    assert "LOG_NEW" in r2.text


def test_creer_refuse_une_saisie_invalide_et_reste_sur_le_formulaire(client, ref):
    """Une saisie invalide ramène au formulaire avec l'erreur, sans rien écrire.

    Remplace l'ancien test des flags `CHARGES_REAL_WRITE_*`, qui protégeaient l'écriture du
    CLASSEUR. L'écriture allant désormais en base, ce qui doit être vérifié est le refus d'une
    saisie invalide — ici un propriétaire inconnu du référentiel.
    """
    r = client.post("/logements", data={
        "logement_id": "LOG_NEW", "nom_court": "NEW",
        "proprietaire_id": "PROP_INCONNU", "date_debut": "2026-06-01",
    }, follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"].startswith("/logements/nouveau?erreur=")


# ── Modification ─────────────────────────────────────────────────────────────

def test_modifier_depuis_la_fiche(client, ref):
    r = client.post("/logements/LOG_A1/modifier",
                    data={"nom_court": "A1-bis"}, follow_redirects=False)
    assert r.status_code == 303
    assert "message=" in r.headers["location"]

    html = client.get(r.headers["location"]).text
    assert "Logement modifié" in html


# ── Changement de propriétaire (historisation) ───────────────────────────────

def test_changer_proprietaire_historise_et_message_recalcul(client, ref):
    r = client.post("/logements/LOG_A1/changer-proprietaire",
                    data={"proprietaire_id": "PROP_B", "date_debut": "2026-07-01", "justification": "Test"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Propriétaire changé" in html
    assert "relancer les calculs" in html

    fiche = client.get("/logements/LOG_A1").text
    assert "Historique — rattachements de gestion" in fiche
    assert "PROP_B" in fiche and "PROP_A" in fiche       # ancienne + nouvelle ligne visibles


def test_changer_proprietaire_inconnu_affiche_erreur(client, ref):
    r = client.post("/logements/LOG_A1/changer-proprietaire",
                    data={"proprietaire_id": "PROP_ZZ", "date_debut": "2026-07-01", "justification": "Test"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "existe pas dans le référentiel" in html


# ── Changement de taux (historisation) ───────────────────────────────────────

def test_changer_taux_historise_et_message_recalcul(client, ref):
    r = client.post("/logements/LOG_A1/changer-taux-commission",
                    data={"taux_commission": "0.12", "date_debut": "2026-07-01", "justification": "Test"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "Taux de commission changé" in html
    assert "relancer Lot10" in html

    fiche = client.get("/logements/LOG_A1").text
    assert "12.0 %" in fiche or "12 %" in fiche
    assert "19.0 %" in fiche or "19 %" in fiche           # ancien taux toujours visible, jamais effacé


def test_changer_taux_invalide_affiche_erreur(client, ref):
    r = client.post("/logements/LOG_A1/changer-taux-commission",
                    data={"taux_commission": "3", "date_debut": "2026-07-01", "justification": "Test"},
                    follow_redirects=False)
    html = client.get(r.headers["location"]).text
    assert "entre 0 et 1" in html


# ── Archivage / réactivation ─────────────────────────────────────────────────

def test_archiver_puis_reactiver(client, ref):
    r = client.post("/logements/LOG_A1/archiver", data={"date_fin": "2026-06-30", "justification": "Test"},
                    follow_redirects=False)
    assert r.status_code == 303
    html = client.get(r.headers["location"]).text
    assert "archivé" in html

    fiche = client.get("/logements/LOG_A1").text
    assert "Réactiver ce logement" in fiche
    assert "Archiver ce logement" not in fiche

    r2 = client.post("/logements/LOG_A1/reactiver",
                     data={"date_debut": "2026-07-01", "proprietaire_id": "PROP_A", "justification": "Test"},
                     follow_redirects=False)
    assert r2.status_code == 303
    html2 = client.get(r2.headers["location"]).text
    assert "réactivé" in html2

    fiche2 = client.get("/logements/LOG_A1").text
    assert "Archiver ce logement" in fiche2
    assert "Réactiver ce logement" not in fiche2


def test_toutes_les_ecritures_refusent_si_flags_off(client, ref, monkeypatch):
    monkeypatch.setattr(cfg, "CHARGES_REAL_WRITE_ENABLED", False)
    routes = [
        ("/logements/LOG_A1/modifier", {"nom_court": "X"}),
        ("/logements/LOG_A1/archiver", {"date_fin": "2026-06-30"}),
        ("/logements/LOG_A1/changer-proprietaire", {"proprietaire_id": "PROP_B", "date_debut": "2026-07-01"}),
        ("/logements/LOG_A1/changer-taux-commission", {"taux_commission": "0.10", "date_debut": "2026-07-01"}),
    ]
    for url, data in routes:
        r = client.post(url, data=data, follow_redirects=False)
        html = client.get(r.headers["location"]).text
        assert "Écriture désactivée" in html

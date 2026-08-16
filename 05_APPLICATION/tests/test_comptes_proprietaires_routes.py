"""Écrans du compte global propriétaire.

Vérifient surtout ce que l'interface ne doit PAS proposer : aucun choix manuel de facture.
"""
import pytest

from app.services import compte_proprietaire_service as cpt
from test_compte_proprietaire_fifo import _facture, _mouvement  # noqa: F401

PROP = "PROP_9001"


@pytest.fixture
def db_app(monkeypatch, tmp_path):
    """Redirige la base de l'application vers une base isolée, alimentée par les fixtures."""
    import app.config as cfg
    from app.db.connection import apply_migrations
    p = tmp_path / "routes.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "DB_PATH", p)
    return p


def test_liste_repond_200(client, db_app):
    r = client.get("/comptes-proprietaires")
    assert r.status_code == 200
    assert "Comptes propriétaires" in r.text


def test_liste_vide_est_explicite(client, db_app):
    r = client.get("/comptes-proprietaires")
    assert "Aucun propriétaire" in r.text


def test_liste_affiche_la_position(client, db_app):
    _facture(db_app, PROP, 200, "2026-01-31")
    _mouvement(db_app, PROP, 250, "2026-02-05")
    r = client.get("/comptes-proprietaires")
    assert PROP in r.text
    assert "50.00" in r.text, "le crédit de 50 € doit apparaître"


def test_detail_montre_les_composants_separement(client, db_app):
    """§28 — aucun chiffre unique ambigu : les composants doivent tous être lisibles."""
    _facture(db_app, PROP, 200, "2026-01-31")
    _mouvement(db_app, PROP, 1000, "2026-02-05", sens="SOCIETE_VERS_PROPRIETAIRE",
               nature="REMBOURSEMENT_PROPRIETAIRE")
    r = client.get(f"/comptes-proprietaires/{PROP}")
    assert r.status_code == 200
    for libelle in ("Factures à recevoir", "Paiements reçus", "Créances restantes",
                    "Acompte / crédit disponible", "Sommes à reverser",
                    "Compensations appliquées", "Virement net", "Position nette"):
        assert libelle in r.text, libelle
    assert "800.00" in r.text, "le virement net après compensation doit être affiché"


def test_detail_expose_les_allocations(client, db_app):
    """§25 — le solde doit être explicable ligne à ligne."""
    _facture(db_app, PROP, 200, "2026-01-31")
    _mouvement(db_app, PROP, 120, "2026-02-05")
    r = client.get(f"/comptes-proprietaires/{PROP}")
    assert "Allocations" in r.text
    assert "120.00" in r.text


def test_aucune_imputation_manuelle_proposee(client, db_app):
    """L'affectation d'un paiement à une facture n'est pas une décision d'écran."""
    _facture(db_app, PROP, 200, "2026-01-31")
    _mouvement(db_app, PROP, 120, "2026-02-05")
    r = client.get(f"/comptes-proprietaires/{PROP}")
    texte = r.text.lower()
    assert "imputer sur" not in texte
    assert "choisir la facture" not in texte


def test_recalcul_manuel(client, db_app):
    _facture(db_app, PROP, 200, "2026-01-31")
    _mouvement(db_app, PROP, 120, "2026-02-05")
    r = client.post(f"/comptes-proprietaires/{PROP}/recalculer", follow_redirects=False)
    assert r.status_code == 303
    assert cpt.historique_recalculs(PROP, db_path=db_app)[0]["declencheur"] == "MANUEL"


def test_filtre_logement_n_altere_pas_les_totaux(client, db_app):
    """Le filtre sert à analyser ; il ne cloisonne pas le compte global."""
    _facture(db_app, PROP, 100, "2026-01-31", logement="LOG_9001")
    _facture(db_app, PROP, 100, "2026-02-28", logement="LOG_9002")
    r = client.get(f"/comptes-proprietaires/{PROP}?logement=LOG_9001")
    assert r.status_code == 200
    assert "les totaux ci-dessus restent ceux du" in r.text
    # Le total du compte reste 200 alors qu'un seul logement est affiché.
    assert "200.00" in r.text

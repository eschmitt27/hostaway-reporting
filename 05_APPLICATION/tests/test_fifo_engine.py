"""Moteur FIFO pur (`app.moteurs.fifo_engine`) — Mission 5, extraction du moteur pilote.

Tests unitaires PURS : aucune DB, aucun `tmp_db`/`tmp_path`, aucun FastAPI, aucun monkeypatch de
chemin ou d'environnement — seulement le module et des dicts Python (rule §16 de la mission).

Reprend les scénarios déjà couverts côté `tests/test_compte_proprietaire_fifo.py` (qui continue de
passer par le ré-export `compte_proprietaire_service.calculer_fifo`) : même fonction, même objet en
mémoire — la parité entre les deux points d'import est garantie par construction, pas mesurée.
"""
from app.moteurs import fifo_engine as fifo


def test_fifo_solde_de_la_plus_ancienne_a_la_plus_recente():
    """§22 — F1=200, F2=300, F3=400, paiement 750 → 200 / 300 / 250, reste 150 sur F3."""
    factures = [
        {"facture_id_opaque": "F1", "montant_total": 200.0},
        {"facture_id_opaque": "F2", "montant_total": 300.0},
        {"facture_id_opaque": "F3", "montant_total": 400.0},
    ]
    sources = [{"source_type": "PAIEMENT", "source_ref": "P1",
                "source_date": "2026-03-01", "montant": 750.0}]
    alloc = fifo.calculer_fifo(factures, sources)
    assert [(a["facture_id_opaque"], a["montant_alloue"]) for a in alloc] == [
        ("F1", 200.0), ("F2", 300.0), ("F3", 250.0)]
    assert [a["rang_fifo"] for a in alloc] == [1, 2, 3]


def test_fifo_n_alloue_jamais_plus_que_le_du():
    factures = [{"facture_id_opaque": "F1", "montant_total": 100.0}]
    sources = [{"source_type": "PAIEMENT", "source_ref": "P1",
                "source_date": "2026-03-01", "montant": 500.0}]
    alloc = fifo.calculer_fifo(factures, sources)
    assert sum(a["montant_alloue"] for a in alloc) == 100.0


def test_fifo_sans_facture_n_alloue_rien():
    alloc = fifo.calculer_fifo([], [{"source_type": "PAIEMENT", "source_ref": "P1",
                                     "source_date": "2026-03-01", "montant": 100.0}])
    assert alloc == []


def test_fifo_sans_source_n_alloue_rien():
    """Cas limite non testé côté compte propriétaire : aucune source disponible."""
    factures = [{"facture_id_opaque": "F1", "montant_total": 100.0}]
    assert fifo.calculer_fifo(factures, []) == []


def test_fifo_deterministe():
    factures = [{"facture_id_opaque": f"F{i}", "montant_total": 100.0} for i in range(1, 6)]
    sources = [{"source_type": "PAIEMENT", "source_ref": f"P{i}",
                "source_date": f"2026-0{i}-01", "montant": 130.0} for i in range(1, 4)]
    assert fifo.calculer_fifo(factures, sources) == fifo.calculer_fifo(factures, sources)


def test_fifo_montant_exactement_egal_au_du():
    """Cas limite : source == somme des créances, aucun reliquat, aucune erreur d'arrondi créée."""
    factures = [{"facture_id_opaque": "F1", "montant_total": 250.0}]
    sources = [{"source_type": "PAIEMENT", "source_ref": "P1",
                "source_date": "2026-01-01", "montant": 250.0}]
    alloc = fifo.calculer_fifo(factures, sources)
    assert alloc == [{"source_type": "PAIEMENT", "source_ref": "P1", "source_date": "2026-01-01",
                      "facture_id_opaque": "F1", "montant_alloue": 250.0, "rang_fifo": 1}]


def test_fifo_source_sous_tolerance_n_alloue_rien():
    """Une source de montant infra-tolérance (arrondi) ne doit produire aucune allocation."""
    factures = [{"facture_id_opaque": "F1", "montant_total": 100.0}]
    sources = [{"source_type": "PAIEMENT", "source_ref": "P1",
                "source_date": "2026-01-01", "montant": 0.001}]
    assert fifo.calculer_fifo(factures, sources) == []


def test_fifo_plusieurs_sources_consomment_dans_lordre_donne():
    """Le moteur ne trie rien lui-même : il consomme les sources dans l'ordre reçu."""
    factures = [{"facture_id_opaque": "F1", "montant_total": 100.0},
                {"facture_id_opaque": "F2", "montant_total": 100.0}]
    sources = [{"source_type": "PAIEMENT", "source_ref": "P_TARDIF",
                "source_date": "2026-02-01", "montant": 50.0},
               {"source_type": "PAIEMENT", "source_ref": "P_PRECOCE",
                "source_date": "2026-01-01", "montant": 150.0}]
    alloc = fifo.calculer_fifo(factures, sources)
    assert [a["source_ref"] for a in alloc] == ["P_TARDIF", "P_PRECOCE", "P_PRECOCE"]


def test_module_ne_depend_de_rien_d_applicatif():
    """Garantie structurelle (§16) : le module n'importe ni sqlite3, ni FastAPI, ni app.config."""
    import sys
    mod = sys.modules["app.moteurs.fifo_engine"]
    source = open(mod.__file__, encoding="utf-8").read()
    for interdit in ("sqlite3", "fastapi", "app.config", "app.db", "Path(", "os.environ"):
        assert interdit not in source, f"dépendance applicative inattendue : {interdit}"

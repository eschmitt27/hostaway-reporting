"""Non-régression mission 14b — le premier run réel (2026-08-27, ORCH-20260827212519-fdafa2) a
révélé qu'`actualiser()` plantait avec `AttributeError: 'CompletedProcess' object has no attribute
'pid'` sur le chemin `attendre=True` (celui de l'orchestrateur ET du scheduler) — APRÈS le succès
réel de l'extraction Hostaway (1577 réservations écrites en SQLite, `hostaway_extractions.statut=
'SUCCES'`), en construisant seulement la valeur de retour. Conséquence réelle observée : le dataset
`HOSTAWAY_RAW` était marqué ECHEC dans `orchestrateur_datasets` malgré un succès effectif — un faux
négatif qui aurait bloqué structurellement le scheduler à chaque cycle.

Ces tests utilisent un VRAI sous-processus Python (pas un double simplifié) pour que la stub ne
puisse plus jamais masquer ce genre de contrat de retour incorrect.
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from app.db.connection import apply_migrations
from app.services import hostaway_actualisation_service as svc
from app.services import orchestrateur_moteur


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "app.db"
    apply_migrations(p)
    return p


def _installer_faux_script(tmp_path, monkeypatch, code_retour: int):
    """Un vrai script Python, lancé par un vrai sous-processus — exactement le chemin qui a
    planté en réel (`subprocess.run` rend un `CompletedProcess`, jamais un `Popen`)."""
    script = tmp_path / svc.SCRIPT
    script.write_text(textwrap.dedent(f"""
        import sys
        print("faux lot1 : simulation")
        sys.exit({code_retour})
    """), encoding="utf-8")
    monkeypatch.setattr(svc, "_racine_moteur", lambda: tmp_path)
    monkeypatch.setattr(svc, "_interpreteur", lambda: sys.executable)
    monkeypatch.setattr(svc, "actualisation_en_cours", lambda **k: None)


def test_attendre_true_succes_ne_leve_aucune_exception(db, tmp_path, monkeypatch):
    """Le scénario exact du premier run réel : succès du sous-processus, `attendre=True`."""
    _installer_faux_script(tmp_path, monkeypatch, code_retour=0)

    resultat = svc.actualiser(db_path=db, attendre=True)

    assert resultat["ok"] is True
    assert resultat["code_retour"] == 0
    assert resultat["pid"] is None, (
        "subprocess.run() ne renseigne aucun pid réel — jamais en inventer un")
    assert resultat["attendu"] is True


def test_attendre_true_echec_ne_leve_aucune_exception(db, tmp_path, monkeypatch):
    _installer_faux_script(tmp_path, monkeypatch, code_retour=1)

    resultat = svc.actualiser(db_path=db, attendre=True)

    assert resultat["ok"] is True, "le LANCEMENT a réussi — c'est code_retour qui porte l'échec"
    assert resultat["code_retour"] == 1
    assert resultat["pid"] is None


def test_attendre_false_rend_un_pid_reel(db, tmp_path, monkeypatch):
    """Chemin fire-and-forget (bouton manuel) : `Popen` expose un vrai pid immédiatement."""
    _installer_faux_script(tmp_path, monkeypatch, code_retour=0)

    resultat = svc.actualiser(db_path=db, attendre=False)

    assert resultat["ok"] is True
    assert resultat["pid"] is not None
    assert isinstance(resultat["pid"], int)
    assert resultat["code_retour"] is None, "fire-and-forget : l'issue n'est pas encore connue"


def test_orchestrateur_importer_hostaway_apres_extraction_reelle_reussie(db, tmp_path, monkeypatch):
    """Chemin exact emprunté par `orch.actualiser()` (orchestrateur ET scheduler, `attendre=True`
    interne à `importer_hostaway`) : une extraction réussie ne doit jamais remonter en échec."""
    _installer_faux_script(tmp_path, monkeypatch, code_retour=0)

    resultat = orchestrateur_moteur.importer_hostaway(db_path=db)

    assert resultat["ok"] is True, resultat
    assert resultat.get("code_retour") == 0


def test_orchestrateur_importer_hostaway_apres_extraction_reelle_echouee(db, tmp_path, monkeypatch):
    _installer_faux_script(tmp_path, monkeypatch, code_retour=1)

    resultat = orchestrateur_moteur.importer_hostaway(db_path=db)

    assert resultat["ok"] is False
    assert resultat["code"] == orchestrateur_moteur.E_CODE_RETOUR

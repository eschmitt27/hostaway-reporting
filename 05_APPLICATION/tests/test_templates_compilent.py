"""Tous les gabarits Jinja se COMPILENT (syntaxe, héritage, imports de macros).

Filet minimal : une faute de frappe dans un gabarit que plus aucun écran testé n'atteint (page rare,
branche conditionnelle) ne se verrait qu'en production. Compiler ne rend rien et n'exige aucune donnée.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.main  # noqa: F401 — les routeurs enregistrent leurs propres filtres sur l'environnement partagé
from app.template_env import get_templates

RACINE = Path(__file__).resolve().parents[1] / "app" / "templates"
GABARITS = sorted(p.relative_to(RACINE).as_posix() for p in RACINE.rglob("*.html"))


def test_il_y_a_des_gabarits_a_verifier():
    assert len(GABARITS) > 50
    assert "_cloture_tableau.html" in GABARITS and "cloture_module_confirmer.html" in GABARITS


@pytest.mark.parametrize("nom", GABARITS)
def test_le_gabarit_se_compile(nom):
    get_templates().env.get_template(nom)

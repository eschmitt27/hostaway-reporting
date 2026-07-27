"""Cohérence interne du filet anti-sensible de `lot13_export_powerbi.py`.

Contexte : le pilotage des calculs a permis d'exécuter la chaîne aval de bout en bout pour la
première fois. lot4quater → lot12 passent ; **lot13 échoue systématiquement** parce que sa
whitelist d'export et son filet anti-sensible se contredisent : la colonne
`preparation_canape_voyageurs`, explicitement whitelistée pour `PBI_Commissions`, est captée par le
motif `voyageur` du filet, ce qui déclenche un ABORT inconditionnel.

Le défaut est **statique** : il ne dépend d'aucune donnée. Il concerne donc aussi le mode réel,
et il est postérieur au commit qui a introduit la colonne (`8763676`).

Ce test lit le moteur **sans le modifier** (règle du chantier : ne jamais toucher aux sources
réelles, ne jamais masquer une anomalie moteur). Il échouera tant que la contradiction subsiste :
c'est voulu — il tient la place du défaut jusqu'à ce qu'une décision soit prise sur le moteur.

Correction attendue côté moteur (au choix, décision métier) :
- retirer `preparation_canape_voyageurs` de la whitelist `PBI_Commissions` ; ou
- renommer la colonne (elle ne porte aucune donnée voyageur, seulement un montant) ; ou
- restreindre le motif `voyageur` du filet aux colonnes réellement nominatives.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import app.config as cfg

MOTEUR = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL" / "lot13_export_powerbi.py"


def _source() -> str:
    if not MOTEUR.exists():
        pytest.skip(f"Moteur absent de cette racine : {MOTEUR}")
    return MOTEUR.read_text(encoding="utf-8")


def _regex_sensible(src: str) -> re.Pattern:
    """Reconstruit le motif SENSIBLE depuis le source, sans importer le moteur (il exécute au
    chargement du module et exigerait pandas)."""
    m = re.search(r"SENSIBLE\s*=\s*re\.compile\((.*?),\s*re\.I\)", src, re.S)
    assert m, "Motif SENSIBLE introuvable dans lot13 — la structure du moteur a changé."
    # Le motif est écrit en littéraux implicitement concaténés : ast les évalue fidèlement.
    return re.compile(ast.literal_eval(f"({m.group(1)})"), re.I)


def _colonnes_whitelistees(src: str) -> dict[str, list[str]]:
    """Extrait {nom_export: [colonnes]} des tuples EXPORTS, par analyse syntaxique du source."""
    arbre = ast.parse(src)
    out: dict[str, list[str]] = {}
    for noeud in ast.walk(arbre):
        if not isinstance(noeud, ast.Tuple) or len(noeud.elts) != 4:
            continue
        nom, _, _, cols = noeud.elts
        if not (isinstance(nom, ast.Constant) and isinstance(nom.value, str)
                and nom.value.startswith("PBI_")):
            continue
        if not isinstance(cols, ast.List):
            continue
        out[nom.value] = [c.value for c in cols.elts
                          if isinstance(c, ast.Constant) and isinstance(c.value, str)]
    return out


def test_le_filet_anti_sensible_est_reconstructible():
    rx = _regex_sensible(_source())
    # Ancrage : le filet doit continuer de capter les vrais motifs sensibles.
    assert rx.search("email_proprietaire")
    assert rx.search("iban")
    assert not rx.search("logement_id")


def test_des_exports_sont_bien_declares():
    exports = _colonnes_whitelistees(_source())
    assert "PBI_Commissions" in exports, "Whitelist PBI_Commissions introuvable."


@pytest.mark.xfail(
    reason="Défaut moteur ouvert : preparation_canape_voyageurs est whitelistée ET captée par le "
           "motif 'voyageur' du filet anti-sensible — lot13 abort systématiquement (code 1). "
           "Décision moteur attendue, hors périmètre applicatif.",
    strict=True)
def test_aucune_colonne_whitelistee_n_est_captee_par_le_filet():
    src = _source()
    rx = _regex_sensible(src)
    fautives = {
        export: [c for c in cols if rx.search(c)]
        for export, cols in _colonnes_whitelistees(src).items()
    }
    fautives = {k: v for k, v in fautives.items() if v}
    assert not fautives, (
        "Colonnes à la fois whitelistées et interdites — lot13 ne peut pas aboutir : "
        f"{fautives}")

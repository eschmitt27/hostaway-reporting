"""Lot11 ne lit plus le classeur M04 (mission « supprimer la derniere dependance M04 de lot11 »).

Lot11 n'en tirait qu'un signal booleen de VACUITE (`menage_calc_id` presente ?) servant a emettre
un INFO « controles M04 non representatifs ». Aucun montant, aucune donnee economique. La question
est desormais posee a `menages_declarations_internes`, sortie canonique de lot6b.
"""
from __future__ import annotations

import re
from pathlib import Path

import app.config as cfg

LOT11 = Path(cfg.APP_ROOT).parent / "02_TRAVAIL" / "lot11_controles_coherence.py"


def _source() -> str:
    return LOT11.read_text(encoding="utf-8")


def test_lot11_ne_declare_plus_de_chemin_vers_le_classeur_m04():
    """Le classeur ne doit plus etre une source runtime : ni constante, ni lecture."""
    source = _source()
    assert "M04_FILE" not in source
    assert "M04_MENAGES_PowerQuery.xlsx" not in source


def test_lot11_ne_lit_plus_la_feuille_master_de_m04():
    """`df_m04 = _read_sheet(M04_FILE, ...)` a disparu — hors commentaire explicatif."""
    code = "\n".join(
        ligne for ligne in _source().splitlines()
        if not ligne.lstrip().startswith("#")
    )
    assert not re.search(r"df_m04\s*=", code)


def test_la_vacuite_m04_est_lue_en_sqlite():
    """Le signal vient de `menages_declarations_internes`, pas d'un DataFrame Excel."""
    source = _source()
    assert "_declarations_internes_vides" in source
    assert "menages_declarations_internes" in source
    assert '"M04":     _declarations_internes_vides()' in source


def test_source_illisible_est_declaree_vide_jamais_alimentee():
    """Fail-closed cote signal : une base absente/table manquante rend VIDE, donc le controle INFO
    est emis. Un incident de lecture ne doit jamais passer pour une source alimentee."""
    source = _source()
    debut = source.index("def _declarations_internes_vides")
    corps = source[debut:source.index("def _is_empty", debut)]
    # Chaque sortie d'erreur du helper rend True (= vide), aucune ne rend False.
    assert corps.count("return True") >= 3
    assert "except Exception" in corps

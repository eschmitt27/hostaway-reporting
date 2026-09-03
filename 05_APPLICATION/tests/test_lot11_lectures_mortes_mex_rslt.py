"""Lot11 — suppression de deux lectures Excel mortes (mission « zéro Excel opérationnel
Lot9/10/11 »).

`df_rslt_m` (feuille PAR_MOIS_LOGEMENT de MASTER_CALC_Resultats.xlsx) et `df_mex` (feuille MASTER de
MASTER_FACT_MEN_MenagesExternes.xlsx) étaient chargés puis jamais référencés ailleurs dans tout le
fichier — vérifié par recherche d'occurrence exhaustive sur le nom de variable, même méthode que la
suppression de M04 dans une mission précédente. `MEX_FILE` reste utilisé pour une AUTRE feuille
(`VUE_ECART_HOSTAWAY`, groupe 6f rapprochement) : seule la lecture MASTER (`df_mex`) disparaît.
"""
from __future__ import annotations

import re
from pathlib import Path

import app.config as cfg

LOT11 = Path(cfg.APP_ROOT).parent / "02_TRAVAIL" / "lot11_controles_coherence.py"


def _source() -> str:
    return LOT11.read_text(encoding="utf-8")


def test_df_rslt_m_nest_plus_charge():
    source = _source()
    assert not re.search(r"\bdf_rslt_m\s*=", source)


def test_df_mex_master_nest_plus_charge():
    source = _source()
    assert not re.search(r"\bdf_mex\s*=", source)


def test_mex_file_reste_utilise_pour_vue_ecart_hostaway():
    """La constante MEX_FILE n'est pas retirée : le groupe 6f (rapprochement ménages externes vs
    Hostaway) lit toujours sa feuille VUE_ECART_HOSTAWAY — seule la lecture MASTER, morte, a
    disparu."""
    source = _source()
    assert "MEX_FILE" in source
    assert 'sheet="VUE_ECART_HOSTAWAY"' in source

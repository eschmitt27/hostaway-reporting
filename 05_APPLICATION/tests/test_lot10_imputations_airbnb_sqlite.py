"""Lot10 — imputations Airbnb lues en SQLite, pas depuis SAISIE_ImputationsAirbnb.xlsx
(mission « zéro Excel opérationnel Lot9/10/11 »).

`df_airbnb_imp` était lu inconditionnellement depuis le classeur, y compris en `--source SQLITE`.
La table `imputations_airbnb` existe déjà (mêmes noms de colonnes que ceux qu'exige
`lib_settlements.validated_airbnb_imputation`) : aucune raison de garder ce classeur au runtime.

Acomptes propriétaires (Lot5) reste la SEULE lecture Excel inconditionnelle restante dans
`load_sources` — aucune table SQLite canonique n'existe pour cette donnée à ce jour. Ce test
documente activement cette exception plutôt que de la laisser silencieuse.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_TRAVAIL = Path(__file__).resolve().parents[2] / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
pytest.importorskip("pandas")

import lot10_calculer_resultats as l10  # noqa: E402


def _source_load_sources() -> str:
    import inspect
    return inspect.getsource(l10.load_sources)


def test_charger_imputations_airbnb_sqlite_existe_et_est_fail_closed(tmp_db):
    """Table migrée mais vide : vide accepté (aucune imputation n'est un état réel), jamais un
    crash, jamais un repli sur le classeur."""
    df = l10.charger_imputations_airbnb_sqlite(tmp_db)
    assert len(df) == 0


def test_airbnb_bascule_sur_sqlite_sous_source_sqlite():
    """`df_airbnb_imp` doit dépendre de `charger_imputations_airbnb_sqlite` quand
    `source == "SQLITE"` — pas seulement défini par une lecture Excel inconditionnelle."""
    source = _source_load_sources()
    m = re.search(r"df_airbnb_imp\s*=\s*\((.*?)\)\n", source, re.S)
    assert m, "affectation de df_airbnb_imp introuvable dans load_sources"
    assert "charger_imputations_airbnb_sqlite" in m.group(1)
    assert '"SQLITE"' in m.group(1) or "'SQLITE'" in m.group(1)


def test_acomptes_reste_la_seule_exception_documentee():
    """Contrat explicite : `df_acc` (Acomptes, Lot5) est la SEULE affectation de `load_sources` qui
    lit encore un classeur SANS jamais brancher sur `source == "SQLITE"` — parce qu'aucune table
    SQLite canonique n'existe pour cette donnée à ce jour. Si ce test échoue parce qu'une autre
    variable lit aussi un classeur inconditionnellement, c'est une régression réelle. S'il échoue
    parce que `df_acc` bascule enfin sur SQLite, la mission a trouvé un équivalent : mettre à jour
    ce test, pas le contourner."""
    source = _source_load_sources()
    # Bloc "Sources HH + Acomptes" : seule zone du fichier où une variable dépend ENCORE d'un
    # classeur sans jamais brancher sur `source == "SQLITE"`. Isoler ce bloc plutôt que d'analyser
    # tout le fichier (le bloc `if source == "SQLITE": ... else: df_flux = _read_sheet(...)` du
    # haut de la fonction contiendrait aussi des `_read_sheet(...)` sans le mot-clé sur la même
    # ligne, faux positif si on cherche dans tout le fichier).
    debut = source.index('# Sources HH + Acomptes')
    fin = source.index("if len(df_hh) > 0", debut)
    bloc = source[debut:fin]
    for cible in ("df_hh", "df_charges", "df_airbnb_imp"):
        assert f'{cible} = (charger_' in bloc or f'{cible} = charger_' in bloc, (
            f"{cible} ne bascule plus sur un lecteur SQLite dans ce bloc")
    assert 'df_acc = _read_sheet(ACC_FILE' in bloc
    assert 'df_acc = (charger_' not in bloc, (
        "df_acc bascule maintenant sur SQLite : mettre à jour ce test (l'exception a disparu), "
        "ne pas le supprimer silencieusement")

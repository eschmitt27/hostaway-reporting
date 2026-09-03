"""Lot10 — imputations Airbnb ET acomptes propriétaires lus en SQLite, plus aucun classeur
(mission « supprimer la dernière lecture Excel opérationnelle : Acomptes propriétaires Lot10 »).

`df_airbnb_imp` était lu inconditionnellement depuis le classeur, y compris en `--source SQLITE`.
La table `imputations_airbnb` existe déjà (mêmes noms de colonnes que ceux qu'exige
`lib_settlements.validated_airbnb_imputation`) : aucune raison de garder ce classeur au runtime.

`df_acc` (Acomptes, Lot5) lisait `MASTER_FACT_MAN_AcomptesProprietaires.xlsx` sans jamais
brancher sur `source == "SQLITE"` — la dernière exception documentée par ce fichier. Ce n'est PAS
un nouvel objet : `mouvements_tresorerie_proprietaires` (migration 0025) modélise déjà exactement
cette donnée (`nature='ACOMPTE_PROPRIETAIRE'`, `sens='PROPRIETAIRE_VERS_SOCIETE'`), un objet
antérieur à cette mission et déjà documenté par la migration 0044 comme la source canonique
attendue ici. `charger_acomptes_proprietaires_sqlite` bascule dessus.
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


def test_acomptes_bascule_sur_sqlite_sous_source_sqlite():
    """`df_acc` doit désormais dépendre de `charger_acomptes_proprietaires_sqlite` quand
    `source == "SQLITE"` — l'ancienne exception documentée (lecture Excel inconditionnelle) a
    disparu. Si ce test échoue parce que `df_acc` est redevenu une lecture Excel inconditionnelle,
    c'est une régression réelle."""
    source = _source_load_sources()
    m = re.search(r"df_acc\s*=\s*\((.*?)\)\n", source, re.S)
    assert m, "affectation de df_acc introuvable dans load_sources"
    assert "charger_acomptes_proprietaires_sqlite" in m.group(1)
    assert '"SQLITE"' in m.group(1) or "'SQLITE'" in m.group(1)


def test_plus_aucune_variable_de_load_sources_ne_lit_un_classeur_sans_branche_sqlite():
    """Contrat transverse : dans le bloc "Sources HH + Acomptes", chaque variable doit dépendre
    d'un lecteur SQLite sous `source == "SQLITE"` — plus aucune lecture Excel inconditionnelle."""
    source = _source_load_sources()
    debut = source.index('# Sources HH + Acomptes')
    fin = source.index("if len(df_hh) > 0", debut)
    bloc = source[debut:fin]
    for cible in ("df_hh", "df_acc", "df_charges", "df_airbnb_imp"):
        assert f'{cible} = (charger_' in bloc or f'{cible} = charger_' in bloc, (
            f"{cible} ne bascule plus sur un lecteur SQLite dans ce bloc")


def test_charger_acomptes_proprietaires_sqlite_existe_et_est_fail_closed(tmp_db):
    """Table migrée mais vide : vide accepté (aucun acompte n'est un état réel), jamais un crash,
    jamais un repli sur le classeur."""
    df = l10.charger_acomptes_proprietaires_sqlite(tmp_db)
    assert len(df) == 0
    assert list(df.columns) == [
        "acompte_id", "proprietaire_id", "logement_id", "montant_acompte", "mois",
        "statut_controle"]


def test_acomptes_filtre_nature_sens_statut(tmp_db):
    """Seul un mouvement `ACOMPTE_PROPRIETAIRE` + `PROPRIETAIRE_VERS_SOCIETE` + `VALIDE` compte
    comme acompte. Une autre nature, un sens inverse, ou un statut non validé ne doivent JAMAIS
    être comptés — même vocabulaire, direction opposée ou étape non franchie ne sont pas la même
    donnée."""
    from app.services import proprietaires_tresorerie_service as pts
    import fixtures_referentiel
    fixtures_referentiel.semer(tmp_db, proprietaires=[{"proprietaire_id": "PROP_TEST"}])

    cas = [
        ("ACOMPTE_PROPRIETAIRE", "PROPRIETAIRE_VERS_SOCIETE", True),   # le seul cas positif
        ("REMBOURSEMENT_PROPRIETAIRE", "PROPRIETAIRE_VERS_SOCIETE", False),  # mauvaise nature
        ("ACOMPTE_PROPRIETAIRE", "SOCIETE_VERS_PROPRIETAIRE", False),  # sens inverse
    ]
    opaques_a_valider = []
    for nature, sens, _ in cas:
        r = pts.creer("PROP_TEST", sens, nature, 50.0, "2026-07-10", db_path=tmp_db)
        assert r["ok"], r
        opaques_a_valider.append(r["mouvement_opaque"])
    # Un 4e mouvement, nature/sens corrects mais jamais validé (reste BROUILLON) — ne doit pas
    # compter non plus : seul VALIDE engage l'économie.
    r_brouillon = pts.creer("PROP_TEST", "PROPRIETAIRE_VERS_SOCIETE", "ACOMPTE_PROPRIETAIRE",
                            50.0, "2026-07-11", db_path=tmp_db)
    assert r_brouillon["ok"]

    for opaque in opaques_a_valider:
        assert pts.valider(opaque, db_path=tmp_db)["ok"]

    df = l10.charger_acomptes_proprietaires_sqlite(tmp_db)
    df_test = df[df["proprietaire_id"] == "PROP_TEST"]
    assert len(df_test) == 1, df_test
    assert float(df_test.iloc[0]["montant_acompte"]) == 50.0
    assert df_test.iloc[0]["mois"] == "2026-07"


def test_charger_acomptes_proprietaires_sqlite_ouvre_zero_excel(tmp_db, monkeypatch):
    """`charger_acomptes_proprietaires_sqlite` ne doit ouvrir aucun classeur — ni openpyxl, ni
    pandas.read_excel — que la table soit vide ou peuplée."""
    import openpyxl
    import pandas as pd

    def _piege_ox(*a, **k):
        raise AssertionError("openpyxl.load_workbook appelé en mode SQLITE")

    def _piege_pd(*a, **k):
        raise AssertionError("pandas.read_excel appelé en mode SQLITE")

    monkeypatch.setattr(openpyxl, "load_workbook", _piege_ox)
    monkeypatch.setattr(pd, "read_excel", _piege_pd)

    df = l10.charger_acomptes_proprietaires_sqlite(tmp_db)
    assert df is not None

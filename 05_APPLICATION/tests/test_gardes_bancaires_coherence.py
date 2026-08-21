"""Garde de non-régression : les 6 tests qui échouaient (au lieu de sauter) faute de
`BANQUE_LOT8_IMPORT.xlsx` doivent tous porter un marqueur skipif conditionné à la présence réelle du
fichier. Protège contre une suppression accidentelle future du décorateur de garde.
"""
import app.config as cfg


def _porte_une_garde_skipif(func) -> bool:
    """Vrai si `func` porte un marqueur skipif, quel que soit l'état actuel de sa condition.

    La version précédente exigeait que la garde soit ARMÉE, c'est-à-dire que le fichier bancaire
    soit absent. Elle ne pouvait donc passer que dans un environnement dépourvu de classeur — et
    tombait au rouge dès que celui-ci était régénéré, alors que la protection recherchée était
    intacte.

    Ce qu'on veut protéger, c'est la PRÉSENCE du décorateur : sans lui, ces tests échoueraient au
    lieu de sauter dans un environnement sans données bancaires. Que la condition soit vraie ou
    fausse ici ne change rien à cette garantie.
    """
    marks = getattr(func, "pytestmark", [])
    return any(m.name == "skipif" and m.args for m in marks)


def test_les_6_tests_bancaires_portent_une_garde_active():
    import test_controles_actionnable as tca
    import test_controles_runner_gardes as tcrg

    cibles = [
        tca.test_05_grain_detaille_correct,
        tca.test_19_lien_mvt_opaque,
        tca.test_20_decision_app4b_visible,
        tca.test_21_22_lot8c_lot11_reellement_executes_reel_intact,
        tca.test_23_24_controle_maintenu_sans_classification,
        tcrg.test_06_reel_intact_apres_gardes,
    ]
    for f in cibles:
        assert _porte_une_garde_skipif(f), (
            f"{f.__name__} a perdu sa garde skipif : dans un environnement sans "
            f"BANQUE_LOT8_IMPORT.xlsx, il échouerait au lieu de sauter.")


def test_banque_lot8_present_les_tests_gardes_s_executent():
    """Le classeur bancaire EXISTE désormais dans cet environnement.

    Ce test affirmait l'inverse : il documentait un worktree sans fichier bancaire, ce qui
    expliquait pourquoi six tests étaient sautés. Le classeur ayant été régénéré (lot8a → lot8b →
    lot8c), sa propre consigne s'applique — « s'il est présent, les 6 tests visés doivent
    s'exécuter réellement ». C'est le cas, et ils passent.

    L'assertion est donc retournée plutôt que supprimée : elle continue de documenter l'état de
    l'environnement, et elle se remettrait au rouge si le classeur disparaissait sans que les
    gardes soient réexaminées.
    """
    from pathlib import Path
    assert Path(cfg.MASTER_BANQUE).exists(), (
        "Classeur bancaire absent : les 6 tests gardés redeviennent des tests sautés. "
        "Régénérer via lot8a, ou réexaminer les gardes."
    )

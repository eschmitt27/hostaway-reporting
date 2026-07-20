"""Garde de non-régression : les 6 tests qui échouaient (au lieu de sauter) faute de
`BANQUE_LOT8_IMPORT.xlsx` doivent tous porter un marqueur skipif conditionné à la présence réelle du
fichier. Protège contre une suppression accidentelle future du décorateur de garde.
"""
import app.config as cfg


def _a_un_skipif_arme_si_fichier_absent(func) -> bool:
    """Vrai si `func` porte au moins un marqueur skipif dont la condition est actuellement True
    (donc : le fichier requis est bien absent dans cet environnement, et le test va sauter, pas
    échouer)."""
    marks = getattr(func, "pytestmark", [])
    return any(m.name == "skipif" and m.args and bool(m.args[0]) for m in marks)


def test_les_6_tests_bancaires_portent_une_garde_active():
    import test_controles_actionnable as tca
    import test_controles_runner_gardes as tcrg

    cibles = [
        tca.test_05_grain_detaille_correct,
        tca.test_19_lien_mvt_opaque,
        tca.test_20_decision_app4b_visible,
        tca.test_21_22_lot8c_lot11_reellement_executes_reel_intact,
        tca.test_23_24_controle_maintenu_sans_classification,
        tcrg.test_11_reel_intact_apres_gardes,
    ]
    for f in cibles:
        assert _a_un_skipif_arme_si_fichier_absent(f), (
            f"{f.__name__} n'a pas de garde skipif active alors que BANQUE_LOT8_IMPORT.xlsx est "
            f"absent dans cet environnement — il échouerait au lieu de sauter.")


def test_banque_lot8_absent_confirme_dans_cet_environnement():
    """Documente explicitement pourquoi les tests ci-dessus sautent ici : absence réelle du fichier,
    pas un défaut du code applicatif."""
    from pathlib import Path
    assert not Path(cfg.MASTER_BANQUE).exists(), (
        "Ce test documente un environnement sans fichier bancaire réel ; s'il est présent, les 6 "
        "tests visés doivent s'exécuter réellement (voir test_banque_synthetique.py)."
    )

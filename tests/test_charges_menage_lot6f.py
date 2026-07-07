"""Lot6f — lecture/ventilation de la table MENAGE (impact ménage analytique). Preuve sur fixtures + réel vide."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

import lib_charges_menage as lcm

# Fixtures ménages du mois (nb) + coûts standards unitaires
NB = {"INT_A": 5, "INT_B": 3, "LOG_1": 4, "LOG_2": 2, "LOG_VIDE": 0}
CS = {"INT_A": 30.0, "INT_B": 40.0, "LOG_1": 55.0, "LOG_2": 39.0, "LOG_VIDE": 29.0}


def _men(charge_id, mode, **kw):
    r = {"charge_id": charge_id, "mode": mode, "intervenant_id": None, "logement_id": None}
    r.update(kw)
    return r


class Lot6fMenageTests(unittest.TestCase):
    def test_ventilation_intervenants_somme_egale_montant(self):
        rows = [_men("C1", "INTERVENANT", intervenant_id="INT_A"),
                _men("C1", "INTERVENANT", intervenant_id="INT_B")]
        res = lcm.ventiler_charge_menage(100.0, rows, NB, CS)
        assert res["statut"] == "VALIDE"
        assert res["somme"] == 100.0  # somme exacte, jamais dupliqué
        assert {q["cle"] for q in res["quote_parts"]} == {"INT_A", "INT_B"}

    def test_ventilation_logements(self):
        rows = [_men("C2", "LOGEMENT", logement_id="LOG_1"),
                _men("C2", "LOGEMENT", logement_id="LOG_2")]
        res = lcm.ventiler_charge_menage(90.0, rows, NB, CS)
        assert res["statut"] == "VALIDE"
        assert res["somme"] == 90.0

    def test_tous_logements(self):
        rows = [_men("C3", "LOGEMENT", logement_id=l) for l in ("LOG_1", "LOG_2")]
        res = lcm.ventiler_charge_menage(50.0, rows, NB, CS)
        assert res["somme"] == 50.0

    def test_aucun_menage_eligible_a_controler(self):
        rows = [_men("C4", "LOGEMENT", logement_id="LOG_VIDE")]  # nb=0 → poids 0
        res = lcm.ventiler_charge_menage(100.0, rows, NB, CS)
        assert res["statut"] == "A_CONTROLER"
        assert res["quote_parts"] == []  # jamais de répartition arbitraire

    def test_intervenant_et_logement_simultane_refuse(self):
        rows = [_men("C5", "INTERVENANT", intervenant_id="INT_A"),
                _men("C5", "LOGEMENT", logement_id="LOG_1")]
        res = lcm.ventiler_charge_menage(100.0, rows, NB, CS)
        assert res["statut"] == "REFUSE"

    def test_ponderation_cout_standard(self):
        # INT_A poids=5×30=150 ; INT_B poids=3×40=120 ; total 270. 270€ → 150 / 120.
        rows = [_men("C6", "INTERVENANT", intervenant_id="INT_A"),
                _men("C6", "INTERVENANT", intervenant_id="INT_B")]
        res = lcm.ventiler_charge_menage(270.0, rows, NB, CS)
        parts = {q["cle"]: q["quote_part"] for q in res["quote_parts"]}
        assert parts["INT_A"] == 150.0
        assert parts["INT_B"] == 120.0
        assert res["somme"] == 270.0

    def test_impacts_d_une_charge_filtre_charge_id(self):
        impacts = [_men("C7", "INTERVENANT", intervenant_id="INT_A"),
                   _men("C8", "INTERVENANT", intervenant_id="INT_B")]
        got = lcm.impacts_d_une_charge(impacts, "C7")
        assert len(got) == 1 and got[0]["intervenant_id"] == "INT_A"

    def test_lecture_table_reelle_vide_no_op(self):
        # La table MENAGE réelle est vide → 0 ligne → aucune contribution (pipeline réel inchangé).
        src = ROOT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Impacts.xlsx"
        rows = lcm.charger_impacts_menage(src)
        assert rows == []


if __name__ == "__main__":
    unittest.main()

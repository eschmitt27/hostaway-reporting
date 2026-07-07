"""Lot9/Lot10 — affectations analytiques : une charge unique, N impacts totalisant le montant."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

import lib_charges_affectation as laf


def _aff(charge_id, logement, prop, quote):
    return {"charge_id": charge_id, "logement_id": logement, "proprietaire_id": prop, "quote_part": quote}


class Lot910AffectationTests(unittest.TestCase):
    def test_100e_deux_logements_une_charge_deux_impacts(self):
        rows = [_aff("C1", "LOG_1", "PROP_1", 50.0), _aff("C1", "LOG_2", "PROP_1", 50.0)]
        ctrl = laf.charge_economique_unique(rows, 100.0)
        assert ctrl["montant_economique"] == 100.0
        assert ctrl["total_analytique"] == 100.0       # pas de total global multiplié
        assert ctrl["coherent"] is True
        assert ctrl["nb_impacts_logement"] == 2
        # jamais 200 : la charge économique reste 100
        assert ctrl["total_analytique"] != 200.0

    def test_repartition_proprietaire(self):
        rows = [_aff("C2", "LOG_1", "PROP_1", 40.0), _aff("C2", "LOG_2", "PROP_2", 60.0)]
        assert laf.impacts_par_proprietaire(rows) == {"PROP_1": 40.0, "PROP_2": 60.0}

    def test_global_sans_affectation(self):
        # Charge globale : aucune ligne AFFECTATIONS → aucun impact logement.
        ctrl = laf.charge_economique_unique([], 100.0)
        assert ctrl["nb_impacts_logement"] == 0
        assert ctrl["total_analytique"] == 0.0

    def test_centimes_deterministes(self):
        rows = [_aff("C3", "L1", "P", 33.34), _aff("C3", "L2", "P", 33.33), _aff("C3", "L3", "P", 33.33)]
        assert laf.verifier_somme(rows, 100.0) is True

    def test_filtre_charge_id(self):
        rows = [_aff("C4", "L1", "P", 50.0), _aff("C5", "L2", "P", 50.0)]
        got = laf.affectations_d_une_charge(rows, "C4")
        assert len(got) == 1 and got[0]["logement_id"] == "L1"

    def test_lecture_table_reelle_vide_no_op(self):
        src = ROOT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Impacts.xlsx"
        assert laf.charger_affectations(src) == []


def test_directs_plus_proprietaires_dedup_somme():
    # Affectations issues d'un périmètre dédupliqué (2 logements), 100 € → somme exacte.
    rows = [_aff("CX", "LOG_A", "PROP_1", 33.34), _aff("CX", "LOG_B", "PROP_1", 33.33),
            _aff("CX", "LOG_C", "PROP_2", 33.33)]
    assert laf.verifier_somme(rows, 100.0) is True
    ctrl = laf.charge_economique_unique(rows, 100.0)
    assert ctrl["nb_impacts_logement"] == 3
    assert ctrl["impacts_proprietaire"] == {"PROP_1": 66.67, "PROP_2": 33.33}


if __name__ == "__main__":
    unittest.main()

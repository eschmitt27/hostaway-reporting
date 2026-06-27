import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_menage_costs import resolve_internal_cleaning_cost


class InternalCleaningCostTests(unittest.TestCase):
    def setUp(self):
        self.hourly = [
            {
                "taux_horaire_id": "TH_INT1_2026",
                "intervenant_id": "INT_1",
                "taux_horaire": "18",
                "date_debut": "2026-01-01",
                "date_fin": "2026-05-31",
                "actif": "OUI",
            }
        ]
        self.fixed = [
            {
                "cout_interne_id": "CI_TYPE_T2",
                "intervenant_id": "",
                "logement_id": "",
                "type_logement_id": "T2",
                "cout_fixe_par_menage": "35",
                "date_debut": "2026-06-01",
                "date_fin": "",
                "actif": "OUI",
                "priorite": "4",
            }
        ]

    def test_may_uses_hours_times_worker_rate(self):
        res = resolve_internal_cleaning_cost(
            ref_date="2026-05-01",
            intervenant_id="INT_1",
            logement_id="LOG_1",
            type_logement_id="T2",
            nb_menages=2,
            nb_heures=3.5,
            hourly_rows=self.hourly,
            fixed_rows=self.fixed,
        )
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.method, "INTERNE_HEURES_TAUX_INTERVENANT")
        self.assertEqual(res.total, 63.0)
        self.assertEqual(res.ref_id, "TH_INT1_2026")

    def test_june_uses_fixed_cost_not_hours(self):
        res = resolve_internal_cleaning_cost(
            ref_date="2026-06-01",
            intervenant_id="INT_1",
            logement_id="LOG_1",
            type_logement_id="T2",
            nb_menages=2,
            nb_heures=99,
            hourly_rows=self.hourly,
            fixed_rows=self.fixed,
        )
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.method, "INTERNE_COUT_FIXE_PARAMETRE")
        self.assertEqual(res.total, 70.0)

    def test_june_without_fixed_cost_is_missing(self):
        res = resolve_internal_cleaning_cost(
            ref_date="2026-06-01",
            intervenant_id="INT_1",
            logement_id="LOG_1",
            type_logement_id="T3",
            nb_menages=1,
            nb_heures=2,
            hourly_rows=self.hourly,
            fixed_rows=self.fixed,
        )
        self.assertEqual(res.status, "MISSING")

    def test_two_fixed_costs_same_priority_are_ambiguous(self):
        rows = self.fixed + [
            {
                "cout_interne_id": "CI_TYPE_T2_DUP",
                "intervenant_id": "",
                "logement_id": "",
                "type_logement_id": "T2",
                "cout_fixe_par_menage": "36",
                "date_debut": "2026-06-01",
                "date_fin": "",
                "actif": "OUI",
                "priorite": "4",
            }
        ]
        res = resolve_internal_cleaning_cost(
            ref_date="2026-06-01",
            intervenant_id="INT_1",
            logement_id="LOG_1",
            type_logement_id="T2",
            nb_menages=1,
            nb_heures=2,
            hourly_rows=self.hourly,
            fixed_rows=rows,
        )
        self.assertEqual(res.status, "AMBIGUOUS")

    def test_cave_pool_is_allocated_once_to_internal_rows(self):
        cave = 30.0
        lines = [
            {"type_intervenant": "INTERNE", "poids": 2.0},
            {"type_intervenant": "INTERNE", "poids": 1.0},
            {"type_intervenant": "EXTERNE", "poids": 10.0},
        ]
        total_internal_weight = sum(l["poids"] for l in lines if l["type_intervenant"] == "INTERNE")
        allocated = [
            round(cave * l["poids"] / total_internal_weight, 2)
            if l["type_intervenant"] == "INTERNE"
            else 0.0
            for l in lines
        ]
        self.assertEqual(sum(allocated), cave)
        self.assertEqual(allocated[-1], 0.0)


if __name__ == "__main__":
    unittest.main()

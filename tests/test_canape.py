import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_canape import calculate_canape_amount


class CanapeTests(unittest.TestCase):
    def test_log_0006_threshold(self):
        ref = {"seuil_voyageurs_preparation_canape": 3, "montant_preparation_canape": 10}
        self.assertEqual(calculate_canape_amount("LOG_0006", 2, ref).amount, 0.0)
        self.assertEqual(calculate_canape_amount("LOG_0006", 3, ref).amount, 10.0)

    def test_other_thresholds(self):
        for lid in ("LOG_0008", "LOG_0011", "LOG_0013"):
            with self.subTest(lid=lid):
                ref = {"seuil_voyageurs_preparation_canape": 5, "montant_preparation_canape": 10}
                self.assertEqual(calculate_canape_amount(lid, 4, ref).amount, 0.0)
                self.assertEqual(calculate_canape_amount(lid, 5, ref).amount, 10.0)

    def test_missing_guest_count_is_controlled(self):
        ref = {"seuil_voyageurs_preparation_canape": 5, "montant_preparation_canape": 10}
        res = calculate_canape_amount("LOG_0008", None, ref)
        self.assertEqual(res.status, "A_CONTROLER")
        self.assertEqual(res.amount, 0.0)

    def test_no_rule_is_not_applicable(self):
        res = calculate_canape_amount("LOG_9999", 10)
        self.assertEqual(res.status, "NON_APPLICABLE")
        self.assertEqual(res.amount, 0.0)

    def test_invoice_line_is_absent_when_no_eligible_reservation(self):
        lot12 = (ROOT / "02_TRAVAIL" / "lot12_generer_factures.py").read_text(encoding="utf-8")

        self.assertIn('if rec["total_preparation_canape"] > 0:', lot12)
        self.assertIn('"PREPARATION_CANAPE"', lot12)


if __name__ == "__main__":
    unittest.main()

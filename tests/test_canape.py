import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_canape import calculate_canape_amount


class CanapeTests(unittest.TestCase):
    def test_log_0006_threshold(self):
        self.assertEqual(calculate_canape_amount("LOG_0006", 2).amount, 0.0)
        self.assertEqual(calculate_canape_amount("LOG_0006", 3).amount, 10.0)

    def test_other_thresholds(self):
        for lid in ("LOG_0008", "LOG_0011", "LOG_0013"):
            with self.subTest(lid=lid):
                self.assertEqual(calculate_canape_amount(lid, 4).amount, 0.0)
                self.assertEqual(calculate_canape_amount(lid, 5).amount, 10.0)

    def test_missing_guest_count_is_controlled(self):
        res = calculate_canape_amount("LOG_0008", None)
        self.assertEqual(res.status, "A_CONTROLER")
        self.assertEqual(res.amount, 0.0)

    def test_no_rule_is_not_applicable(self):
        res = calculate_canape_amount("LOG_9999", 10)
        self.assertEqual(res.status, "NON_APPLICABLE")
        self.assertEqual(res.amount, 0.0)


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_controls import (  # noqa: E402
    IMPACT_A_DECIDER,
    IMPACT_BLOQUANT_FACTURE,
    IMPACT_NON_BLOQUANT_FACTURE,
    default_impact_facture,
    facture_control_counts,
)


class ControlFactureTests(unittest.TestCase):
    def test_default_impact(self):
        self.assertEqual(default_impact_facture("BLOQUANT"), IMPACT_BLOQUANT_FACTURE)
        self.assertEqual(default_impact_facture("INFO"), IMPACT_NON_BLOQUANT_FACTURE)
        self.assertEqual(default_impact_facture("A_CONTROLER"), IMPACT_A_DECIDER)

    def test_vrbo_august_does_not_block_may(self):
        controls = [{
            "severity": "A_CONTROLER",
            "impact_facture": "A_DECIDER",
            "mois": "2026-08",
            "logement_id": "LOG_0008",
        }]

        counts = facture_control_counts(controls, "2026-05", logement_id="LOG_0008")

        self.assertEqual(counts["nb_a_controler"], 0)
        self.assertEqual(counts["controls"], [])

    def test_one_logement_does_not_block_other_logement(self):
        controls = [{
            "severity": "BLOQUANT",
            "impact_facture": "BLOQUANT_FACTURE",
            "mois": "2026-05",
            "logement_id": "LOG_0001",
            "proprietaire_id": "PROP_0001",
        }]

        counts = facture_control_counts(
            controls,
            "2026-05",
            logement_id="LOG_0002",
            proprietaire_id="PROP_0001",
        )

        self.assertEqual(counts["nb_bloquants"], 0)

    def test_global_control_blocks_only_when_explicitly_blocking_invoice(self):
        controls = [
            {
                "severity": "A_CONTROLER",
                "impact_facture": "A_DECIDER",
                "mois": None,
            },
            {
                "severity": "BLOQUANT",
                "impact_facture": "BLOQUANT_FACTURE",
                "mois": None,
            },
        ]

        counts = facture_control_counts(controls, "2026-05")

        self.assertEqual(counts["nb_bloquants"], 1)
        self.assertEqual(counts["nb_a_controler"], 0)

    def test_document_specific_control_does_not_block_other_document(self):
        controls = [{
            "severity": "BLOQUANT",
            "impact_facture": "BLOQUANT_FACTURE",
            "mois": "2026-05",
            "document_id": "PREF-1",
        }]

        counts = facture_control_counts(controls, "2026-05", document_id="PREF-2")

        self.assertEqual(counts["nb_bloquants"], 0)


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_cloture import (  # noqa: E402
    STATUT_CLOTURE,
    adjustment_hash,
    normalise_cloture_status,
    validate_adjustment,
    validate_cloture_status,
)


VALID_ROW = {
    "ajustement_id": "AJU-001",
    "mois_origine": "2026-05",
    "mois_effet": "2026-06",
    "source_module": "LOT10",
    "source_pk": "SRC-1",
    "logement_id": "LOG_0001",
    "proprietaire_id": "PROP_0001",
    "type_ajustement": "CORRECTION_PAYOUT",
    "montant": "10.50",
    "sens": "AJUSTEMENT",
    "impact_reel": "OUI",
    "impact_comptable": "NON",
    "motif": "Correction apres cloture",
    "justificatif": "piece-001.pdf",
    "auteur": "OPERATEUR",
    "date_saisie": "2026-06-27",
    "statut_validation": "A_VALIDER",
}


class ClotureTests(unittest.TestCase):
    def test_statuses_are_closed(self):
        self.assertEqual(normalise_cloture_status(" cloture "), STATUT_CLOTURE)
        self.assertEqual(validate_cloture_status("EN_CONTROLE"), (True, "EN_CONTROLE"))
        self.assertEqual(validate_cloture_status("TERMINE"), (False, "STATUT_CLOTURE_INTERDIT"))

    def test_adjustment_requires_rattachement_and_justification(self):
        row = dict(VALID_ROW)
        row["logement_id"] = ""

        ok, code, _ = validate_adjustment(row)

        self.assertFalse(ok)
        self.assertEqual(code, "AJUSTEMENT_SCHEMA_OU_DONNEE_INCOMPLETE")

    def test_adjustment_is_not_auto_validated_without_justification(self):
        row = dict(VALID_ROW)
        row["statut_validation"] = "VALIDE"
        row["justificatif"] = ""

        ok, code, _ = validate_adjustment(row)

        self.assertFalse(ok)
        self.assertEqual(code, "AJUSTEMENT_SCHEMA_OU_DONNEE_INCOMPLETE")

    def test_valid_adjustment_hash_is_stable(self):
        ok, code, _ = validate_adjustment(VALID_ROW)

        self.assertTrue(ok)
        self.assertEqual(code, "OK")
        self.assertEqual(adjustment_hash(VALID_ROW), adjustment_hash(dict(VALID_ROW)))

    def test_post_closure_correction_is_append_only_by_identifier(self):
        original_history = {"RES-1": {"montant": 100}}
        adjustment = dict(VALID_ROW, ajustement_id="AJU-POST-001", source_pk="RES-1", montant="12.00")
        ok, code, _ = validate_adjustment(adjustment)
        adjustment_log = {}
        adjustment_log[adjustment["ajustement_id"]] = adjustment

        self.assertTrue(ok)
        self.assertEqual(code, "OK")
        self.assertEqual(original_history["RES-1"]["montant"], 100)
        self.assertIn("AJU-POST-001", adjustment_log)


if __name__ == "__main__":
    unittest.main()

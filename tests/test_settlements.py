import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_settlements import aircover_auto_impact, settle_invoice, validated_airbnb_imputation


class SettlementTests(unittest.TestCase):
    def test_invoice_100_advance_20_remainder_80(self):
        res = settle_invoice(100, owner_advances=20, airbnb_imputed=0)
        self.assertEqual(res.reste_a_payer, 80.0)
        self.assertEqual(res.credit_a_traiter, 0.0)

    def test_invoice_100_airbnb_40_remainder_60(self):
        res = settle_invoice(100, owner_advances=0, airbnb_imputed=40)
        self.assertEqual(res.reste_a_payer, 60.0)
        self.assertEqual(res.credit_a_traiter, 0.0)

    def test_invoice_100_received_120_credit_20(self):
        res = settle_invoice(100, owner_advances=80, airbnb_imputed=40)
        self.assertEqual(res.reste_a_payer, 0.0)
        self.assertEqual(res.credit_a_traiter, 20.0)
        self.assertEqual(res.statut, "TROP_PERÇU / CRÉDIT À TRAITER")

    def test_airbnb_without_logement_or_month_has_no_validated_impact(self):
        ok, code = validated_airbnb_imputation({
            "transaction_banque_id": "BNQ_1",
            "proprietaire_id": "PROP_1",
            "logement_id": "",
            "mois": "",
            "document_id": "PREF_1",
            "montant_impute": 40,
            "justificatif": "export Airbnb",
            "statut": "VALIDEE",
        })
        self.assertFalse(ok)
        self.assertEqual(code, "AIRBNB_IMPUTATION_INCOMPLETE")

    def test_aircover_owner_has_no_automatic_impact(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_1",
            "montant": 100,
            "beneficiaire_reel": "PROPRIETAIRE",
            "traitement": "",
            "justificatif": "",
        })
        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_A_CONTROLER")


if __name__ == "__main__":
    unittest.main()

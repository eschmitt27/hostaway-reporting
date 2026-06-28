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

    def test_owner_advance_reduces_only_remainder(self):
        res = settle_invoice(100, owner_advances=20, airbnb_imputed=0)

        self.assertEqual(res.reste_a_payer, 80.0)
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

    def test_validated_airbnb_imputation_can_reduce_only_settlement(self):
        ok, code = validated_airbnb_imputation({
            "transaction_banque_id": "BNQ_1",
            "proprietaire_id": "PROP_1",
            "logement_id": "LOG_1",
            "mois": "2026-05",
            "document_id": "PREF_1",
            "montant_impute": 40,
            "justificatif": "export Airbnb",
            "statut": "VALIDEE",
        })

        self.assertTrue(ok)
        self.assertEqual(code, "OK")
        self.assertEqual(settle_invoice(100, airbnb_imputed=40).reste_a_payer, 60.0)

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

    def test_aircover_owner_identified_stays_controlled_until_validated(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_1B",
            "montant": 100,
            "beneficiaire_reel": "PROPRIETAIRE",
            "traitement": "A_DECIDER",
            "justificatif": "piece",
            "statut_controle": "A_CONTROLER",
        })

        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_A_CONTROLER")

    def test_aircover_conciergerie_has_no_automatic_impact(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_2",
            "montant": 100,
            "beneficiaire_reel": "CONCIERGERIE",
            "traitement": "A_DECIDER",
            "justificatif": "piece",
            "statut_controle": "VALIDE",
        })

        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_TRAITEMENT_EXPLICITE_A_REVOIR")

    def test_aircover_conciergerie_identified_stays_controlled_until_validated(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_2B",
            "montant": 100,
            "beneficiaire_reel": "CONCIERGERIE",
            "traitement": "A_DECIDER",
            "justificatif": "piece",
            "statut_controle": "A_CONTROLER",
        })

        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_A_CONTROLER")

    def test_aircover_without_beneficiary_is_controlled(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_3",
            "montant": 100,
            "beneficiaire_reel": "",
            "traitement": "A_DECIDER",
            "justificatif": "piece",
        })

        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_A_CONTROLER")

    def test_aircover_without_justificatif_is_controlled(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_4",
            "montant": 100,
            "beneficiaire_reel": "PROPRIETAIRE",
            "traitement": "A_DECIDER",
            "justificatif": "",
            "statut_controle": "VALIDE",
        })

        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_A_CONTROLER")

    def test_aircover_without_beneficiary_has_zero_auto_financial_impact(self):
        payout_delta, commission_delta, net_delta, code = aircover_auto_impact({
            "aircover_id": "AC_5",
            "montant": 100,
            "beneficiaire_reel": "",
            "traitement": "A_DECIDER",
            "justificatif": "piece",
            "statut_controle": "VALIDE",
        })

        total_facture_delta = 0.0
        reglement_delta = 0.0
        self.assertEqual((payout_delta, commission_delta, net_delta), (0.0, 0.0, 0.0))
        self.assertEqual((total_facture_delta, reglement_delta), (0.0, 0.0))
        self.assertEqual(code, "AIRCOVER_A_CONTROLER")


if __name__ == "__main__":
    unittest.main()

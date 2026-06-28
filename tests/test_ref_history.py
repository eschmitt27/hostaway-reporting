import datetime as dt
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_ref_history import resolve_commission_rate, resolve_management_period


class CommissionHistoryTests(unittest.TestCase):
    def test_logement_rate_overrides_owner_rate_and_periods(self):
        rows = [
            {
                "taux_commission_id": "TX_PROP_OLD",
                "proprietaire_id": "PROP_1",
                "logement_id": "",
                "taux_commission": "0.20",
                "date_debut": "2026-01-01",
                "date_fin": "2026-05-31",
                "actif": "OUI",
            },
            {
                "taux_commission_id": "TX_PROP_NEW",
                "proprietaire_id": "PROP_1",
                "logement_id": "",
                "taux_commission": "0.22",
                "date_debut": "2026-06-01",
                "date_fin": "",
                "actif": "OUI",
            },
            {
                "taux_commission_id": "TX_LOG_NEW",
                "proprietaire_id": "",
                "logement_id": "LOG_1",
                "taux_commission": "0.25",
                "date_debut": "2026-06-01",
                "date_fin": "",
                "actif": "OUI",
            },
        ]

        may = resolve_commission_rate(rows, proprietaire_id="PROP_1", logement_id="LOG_1", ref_date="2026-05-15")
        june = resolve_commission_rate(rows, proprietaire_id="PROP_1", logement_id="LOG_1", ref_date="2026-06-15")

        self.assertEqual(may.status, "OK")
        self.assertEqual(may.value, 0.20)
        self.assertEqual(june.status, "OK")
        self.assertEqual(june.value, 0.25)
        self.assertEqual(june.row["taux_commission_id"], "TX_LOG_NEW")

    def test_missing_rate_is_explicit(self):
        res = resolve_commission_rate([], proprietaire_id="PROP_1", logement_id="LOG_1", ref_date="2026-05-15")
        self.assertEqual(res.status, "MISSING")
        self.assertIsNone(res.value)

    def test_simultaneous_same_priority_rates_are_ambiguous(self):
        rows = [
            {
                "taux_commission_id": "TX_A",
                "proprietaire_id": "PROP_1",
                "logement_id": "",
                "taux_commission": "0.20",
                "date_debut": "2026-01-01",
                "date_fin": "",
                "actif": "OUI",
            },
            {
                "taux_commission_id": "TX_B",
                "proprietaire_id": "PROP_1",
                "logement_id": "",
                "taux_commission": "0.21",
                "date_debut": "2026-05-01",
                "date_fin": "",
                "actif": "OUI",
            },
        ]
        res = resolve_commission_rate(rows, proprietaire_id="PROP_1", logement_id="LOG_1", ref_date="2026-06-01")
        self.assertEqual(res.status, "AMBIGUOUS")


class ManagementHistoryTests(unittest.TestCase):
    def test_owner_change_uses_arrival_period(self):
        rows = [
            {
                "gestion_id": "GST_OLD",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_OLD",
                "date_debut": "2026-01-01",
                "date_fin": "2026-05-31",
                "statut_gestion": "ACTIF",
            },
            {
                "gestion_id": "GST_NEW",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_NEW",
                "date_debut": "2026-06-01",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
        ]
        may = resolve_management_period(rows, logement_id="LOG_1", date_arrivee=dt.date(2026, 5, 10))
        june = resolve_management_period(rows, logement_id="LOG_1", date_arrivee="2026-06-10")
        self.assertEqual(may.value, "PROP_OLD")
        self.assertEqual(june.value, "PROP_NEW")

    def test_stay_after_exit_is_missing(self):
        rows = [
            {
                "gestion_id": "GST_OLD",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_OLD",
                "date_debut": "2026-01-01",
                "date_fin": "2026-05-31",
                "statut_gestion": "ACTIF",
            },
        ]
        res = resolve_management_period(rows, logement_id="LOG_1", date_arrivee="2026-06-01")
        self.assertEqual(res.status, "MISSING")

    def test_period_without_owner_is_controlled(self):
        rows = [
            {
                "gestion_id": "GST_NO_OWNER",
                "logement_id": "LOG_1",
                "proprietaire_id": "",
                "date_debut": "2026-01-01",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
        ]

        res = resolve_management_period(rows, logement_id="LOG_1", date_arrivee="2026-06-01")

        self.assertEqual(res.status, "MISSING_OWNER")

    def test_empty_management_history_is_explicit(self):
        res = resolve_management_period([], logement_id="LOG_1", date_arrivee="2026-06-01")

        self.assertEqual(res.status, "MISSING")
        self.assertIsNone(res.value)

    def test_stay_crossing_exit_is_controlled(self):
        rows = [
            {
                "gestion_id": "GST_OLD",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_OLD",
                "date_debut": "2026-01-01",
                "date_fin": "2026-05-31",
                "statut_gestion": "ACTIF",
            },
        ]
        res = resolve_management_period(
            rows,
            logement_id="LOG_1",
            date_arrivee="2026-05-30",
            date_depart="2026-06-02",
        )
        self.assertEqual(res.status, "OUT_OF_PERIOD")


if __name__ == "__main__":
    unittest.main()

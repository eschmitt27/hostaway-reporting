import sys
import unittest
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_canape import calculate_canape_amount
from lot1_hostaway_extract import guest_count_columns_from_api
from lib_guestcount import (
    CODE_CONFLICT_API,
    CODE_INVALID_API,
    SOURCE_ABSENT,
    SOURCE_API_DETAIL,
    SOURCE_API_LIST,
    SOURCE_LEGACY_SNAPSHOT,
    extract_number_of_guests_from_snapshot,
    guest_count_from_reservation,
    guest_count_from_reservation_or_snapshot,
    resolve_api_guest_count,
    resolve_guest_count_from_sources,
)


class GuestCountResolutionTests(unittest.TestCase):
    def test_extracts_number_of_guests_from_reservation(self):
        self.assertEqual(guest_count_from_reservation({"numberOfGuests": 4}), 4)

    def test_extracts_number_of_guests_from_snapshot(self):
        snapshot = '{"id": 123, "numberOfGuests": 5, "adults": 5}'
        self.assertEqual(extract_number_of_guests_from_snapshot(snapshot), 5)

    def test_extracts_number_of_guests_from_truncated_snapshot_text(self):
        snapshot = '{"id": 123, "numberOfGuests": 5, "adults": 5'
        self.assertEqual(extract_number_of_guests_from_snapshot(snapshot), 5)

    def test_api_list_is_normal_source(self):
        res = resolve_api_guest_count(4, 4)
        self.assertEqual(res.value, 4)
        self.assertEqual(res.source, SOURCE_API_LIST)

    def test_api_detail_is_second_source(self):
        res = resolve_api_guest_count(None, 4)
        self.assertEqual(res.value, 4)
        self.assertEqual(res.source, SOURCE_API_DETAIL)

    def test_conflict_between_list_and_detail_is_controlled(self):
        res = resolve_api_guest_count(4, 5)
        self.assertIsNone(res.value)
        self.assertEqual(res.source, "CONFLIT_API")
        self.assertEqual(res.code, CODE_CONFLICT_API)

    def test_invalid_guest_count_is_controlled(self):
        for value in (math.nan, "", -1, "abc", 2.5, "3.2"):
            with self.subTest(value=value):
                res = resolve_api_guest_count(value)
                self.assertIsNone(res.value)
                self.assertEqual(res.code, CODE_INVALID_API)

    def test_snapshot_backfills_only_as_legacy_source(self):
        res = resolve_guest_count_from_sources(None, None, 3)
        self.assertEqual(res.value, 3)
        self.assertEqual(res.source, SOURCE_LEGACY_SNAPSHOT)

    def test_absent_source_stays_empty(self):
        res = resolve_guest_count_from_sources(None, None, None)
        self.assertIsNone(res.value)
        self.assertEqual(res.source, SOURCE_ABSENT)

    def test_missing_guest_count_does_not_default_to_zero(self):
        self.assertIsNone(guest_count_from_reservation_or_snapshot({"id": 123}, None))

    def test_explicit_zero_is_preserved(self):
        self.assertEqual(guest_count_from_reservation({"numberOfGuests": 0}), 0)
        res = resolve_api_guest_count(0)
        self.assertEqual(res.value, 0)
        self.assertEqual(res.source, SOURCE_API_LIST)

    def test_lot1_columns_from_api_list(self):
        cols = guest_count_columns_from_api({"numberOfGuests": 3})
        self.assertEqual(cols["numberOfGuests"], 3)
        self.assertEqual(cols["guestCount"], 3)
        self.assertEqual(cols["source_guestCount"], SOURCE_API_LIST)

    def test_lot1_columns_from_api_detail(self):
        cols = guest_count_columns_from_api({}, {"numberOfGuests": 4})
        self.assertIsNone(cols["numberOfGuests"])
        self.assertEqual(cols["guestCount"], 4)
        self.assertEqual(cols["source_guestCount"], SOURCE_API_DETAIL)

    def test_lot1_columns_conflict(self):
        cols = guest_count_columns_from_api({"numberOfGuests": 3}, {"numberOfGuests": 4})
        self.assertIsNone(cols["guestCount"])
        self.assertEqual(cols["source_guestCount"], "CONFLIT_API")
        self.assertEqual(cols["code_controle_guestCount"], CODE_CONFLICT_API)

    def test_conflict_does_not_create_canape_amount(self):
        cols = guest_count_columns_from_api({"numberOfGuests": 3}, {"numberOfGuests": 4})
        ref = {"seuil_voyageurs_preparation_canape": 3, "montant_preparation_canape": 10}
        res = calculate_canape_amount("LOG_0006", cols["guestCount"], ref)
        self.assertEqual(res.status, "A_CONTROLER")
        self.assertEqual(res.amount, 0.0)


class CanapeBusinessRuleTests(unittest.TestCase):
    def test_threshold_3_and_amount_10(self):
        ref = {"seuil_voyageurs_preparation_canape": 3, "montant_preparation_canape": 10}
        self.assertEqual(calculate_canape_amount("LOG_0006", 2, ref).amount, 0.0)
        self.assertEqual(calculate_canape_amount("LOG_0006", 3, ref).amount, 10.0)

    def test_threshold_5_and_amount_10(self):
        ref = {"seuil_voyageurs_preparation_canape": 5, "montant_preparation_canape": 10}
        self.assertEqual(calculate_canape_amount("LOG_0008", 4, ref).amount, 0.0)
        self.assertEqual(calculate_canape_amount("LOG_0008", 5, ref).amount, 10.0)

    def test_logement_non_concerne(self):
        self.assertEqual(calculate_canape_amount("LOG_0001", 8, {}).status, "NON_APPLICABLE")

    def test_guest_count_missing_on_concerned_logement_is_controlled(self):
        ref = {"seuil_voyageurs_preparation_canape": 5, "montant_preparation_canape": 10}
        res = calculate_canape_amount("LOG_0011", None, ref)
        self.assertEqual(res.status, "A_CONTROLER")
        self.assertEqual(res.amount, 0.0)

    def test_nan_guest_count_is_controlled_as_missing(self):
        ref = {"seuil_voyageurs_preparation_canape": 5, "montant_preparation_canape": 10}
        res = calculate_canape_amount("LOG_0011", math.nan, ref)
        self.assertEqual(res.status, "A_CONTROLER")
        self.assertEqual(res.amount, 0.0)


if __name__ == "__main__":
    unittest.main()
import datetime as dt
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_ref_history import (
    resolve_canape_parametres,
    resolve_commission_rate,
    resolve_management_period,
)


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



    def test_explicit_management_period_for_gere_logement(self):
        rows = [{
            "gestion_id": "GST_EXPLICIT",
            "logement_id": "LOG_GERE",
            "proprietaire_id": "PROP_1",
            "date_debut": "2026-01-01",
            "date_fin": "2026-12-31",
            "statut_gestion": "ACTIF",
        }]
        res = resolve_management_period(rows, logement_id="LOG_GERE", date_arrivee="2026-05-10")
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.value, "PROP_1")

    def test_hors_parc_without_history_remains_missing_for_history_resolver(self):
        res = resolve_management_period([], logement_id="APPARTEMENT_DIVERS", date_arrivee="2026-05-10")
        self.assertEqual(res.status, "MISSING")

    def test_single_undated_management_line_resolves_non_blocking(self):
        rows = [
            {
                "gestion_id": "GST_UNKNOWN_START",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_1",
                "date_debut": "",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
        ]
        res = resolve_management_period(rows, logement_id="LOG_1", date_arrivee="2026-05-10")
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.value, "PROP_1")
        self.assertIn("Date de debut", res.message)

    def test_dated_management_period_overrides_undated_line(self):
        rows = [
            {
                "gestion_id": "GST_UNKNOWN_START",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_OLD",
                "date_debut": "",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
            {
                "gestion_id": "GST_DATED",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_NEW",
                "date_debut": "2026-05-01",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
        ]
        res = resolve_management_period(rows, logement_id="LOG_1", date_arrivee="2026-05-10")
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.value, "PROP_NEW")
        self.assertEqual(res.row["gestion_id"], "GST_DATED")

    def test_two_undated_management_candidates_are_blocking(self):
        rows = [
            {
                "gestion_id": "GST_A",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_A",
                "date_debut": "",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
            {
                "gestion_id": "GST_B",
                "logement_id": "LOG_1",
                "proprietaire_id": "PROP_B",
                "date_debut": "",
                "date_fin": "",
                "statut_gestion": "ACTIF",
            },
        ]
        res = resolve_management_period(rows, logement_id="LOG_1", date_arrivee="2026-05-10")
        self.assertEqual(res.status, "AMBIGUOUS")

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


class CanapeParametresHistoryTests(unittest.TestCase):
    """Mission 6 — le paramètre canapé devient historisé (`ref_canape_parametres`) : un recalcul
    futur d'une réservation passée doit utiliser le seuil/montant en vigueur à SA date, jamais le
    paramètre actuel (§0/§33 de la mission)."""

    def test_evolution_future_ne_reecrit_pas_le_passe(self):
        rows = [
            {"canape_parametre_id": "CNP_1", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "2025-01-01", "date_fin": "2026-12-31", "actif": "OUI"},
            {"canape_parametre_id": "CNP_2", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "6", "montant_preparation_canape": "40",
             "date_debut": "2027-01-01", "date_fin": "", "actif": "OUI"},
        ]
        calcul_2026 = resolve_canape_parametres(rows, logement_id="LOG_1", ref_date="2026-06-15")
        calcul_2027 = resolve_canape_parametres(rows, logement_id="LOG_1", ref_date="2027-03-01")
        # Un recalcul de juin 2026 exécuté hypothétiquement en 2028 interroge la MÊME date
        # économique (2026-06-15) — le résultat est nécessairement identique, aucune notion de
        # "aujourd'hui" n'entre dans le résolveur (aucun appel à datetime.now()).
        calcul_2026_rejoue = resolve_canape_parametres(rows, logement_id="LOG_1", ref_date="2026-06-15")

        self.assertEqual(calcul_2026.status, "OK")
        self.assertEqual(calcul_2026.row["montant_preparation_canape"], "30")
        self.assertEqual(calcul_2027.status, "OK")
        self.assertEqual(calcul_2027.row["montant_preparation_canape"], "40")
        self.assertEqual(calcul_2026_rejoue.row, calcul_2026.row)

    def test_aucun_parametre_configure_est_missing_pas_une_valeur_actuelle(self):
        res = resolve_canape_parametres([], logement_id="LOG_ZZZ", ref_date="2026-06-01")
        self.assertEqual(res.status, "MISSING")
        self.assertIsNone(res.row)

    def test_chevauchement_est_ambigu(self):
        rows = [
            {"canape_parametre_id": "CNP_A", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "2026-01-01", "date_fin": "2026-06-30", "actif": "OUI"},
            {"canape_parametre_id": "CNP_B", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "6", "montant_preparation_canape": "40",
             "date_debut": "2026-05-01", "date_fin": "2026-12-31", "actif": "OUI"},
        ]
        res = resolve_canape_parametres(rows, logement_id="LOG_1", ref_date="2026-05-15")
        self.assertEqual(res.status, "AMBIGUOUS")

    def test_logement_different_nest_jamais_confondu(self):
        rows = [
            {"canape_parametre_id": "CNP_1", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ]
        res = resolve_canape_parametres(rows, logement_id="LOG_2", ref_date="2026-06-01")
        self.assertEqual(res.status, "MISSING")

    def test_periode_ouverte_depuis_lorigine_couvre_toute_date_passee(self):
        """Le backfill (migration 0058) ouvre une période sans date_debut/date_fin — elle doit
        s'appliquer à n'importe quelle date passée, préservant le comportement historique."""
        rows = [
            {"canape_parametre_id": "CNP_1", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ]
        res = resolve_canape_parametres(rows, logement_id="LOG_1", ref_date="2020-01-01")
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.row["montant_preparation_canape"], "30")

    def test_ligne_inactive_nest_pas_candidate(self):
        rows = [
            {"canape_parametre_id": "CNP_1", "logement_id": "LOG_1",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "", "date_fin": "", "actif": "NON"},
        ]
        res = resolve_canape_parametres(rows, logement_id="LOG_1", ref_date="2026-06-01")
        self.assertEqual(res.status, "MISSING")


if __name__ == "__main__":
    unittest.main()

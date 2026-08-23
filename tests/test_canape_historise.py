"""Mission 6 — le paramètre canapé (seuil/montant) devient historisé et résolu par date.

Preuve d'intégration : `build_commissions` (Lot10) utilise `ref_canape_parametres` (df_canape)
quand disponible, résolu à la date de la réservation — jamais la valeur COURANTE de
`REF_Logements`. Sans historique fourni (df_canape vide/None), le comportement historique
(colonne courante) est préservé à l'identique : aucune régression pour les appelants existants
(voir `tests/test_hors_parc_technique.py`, qui n'a jamais fourni de df_canape).
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lot10_calculer_resultats import build_commissions


PAYOUT_COLS = [
    "reservation_id", "channel_type", "statut_calcul_payout", "payout_calcule", "source_payout",
    "menage_retenu", "assiette_commission", "inclure_resultat_auto", "menage_retenu_source",
    "cout_standard_id", "cout_standard_menage_snapshot", "cout_standard_date_debut_validite",
    "cout_standard_date_fin_validite", "logement_id_snapshot", "type_logement_id_snapshot",
    "date_reference_cout_menage",
]
HH_COLS = ["reservation_hh_id", "total_percu", "menage", "commission", "taux_commission"]


def _reservation_frame(date_arrivee, guest_count=6):
    return pd.DataFrame([{
        "reservation_calc_id": f"RES-{date_arrivee}-HA-001",
        "reservation_id_hostaway": 123,
        "reservation_hh_id": None,
        "source": "HOSTAWAY_AIRBNB",
        "source_montant": "HOSTAWAY_PAYOUT",
        "montant_retenu": 100.0,
        "logement_id": "LOG_CANAPE",
        "proprietaire_id": "PROP_1",
        "date_arrivee": date_arrivee,
        "date_depart": date_arrivee,
        "nuits": 2,
        "guestCount": guest_count,
        "canal": "AIRBNB",
        "payout_calcule": 100.0,
        "menage_retenu": 20.0,
        "assiette_commission": 80.0,
    }])


def _flux_frame(date_arrivee):
    return pd.DataFrame([{
        "type_flux_id": "TYPE_FLUX_017",
        "source_pk": f"RES-{date_arrivee}-HA-001",
        "flux_id": "FLUX_1",
        "mois": date_arrivee[:7],
        "logement_id": "LOG_CANAPE",
        "proprietaire_id": "PROP_1",
    }])


def _payout_frame():
    return pd.DataFrame([{
        "reservation_id": 123, "channel_type": "AIRBNB", "statut_calcul_payout": "NORMAL",
        "payout_calcule": 100.0, "source_payout": "HOSTAWAY_PAYOUT", "menage_retenu": 20.0,
        "assiette_commission": 80.0, "inclure_resultat_auto": "OUI",
        "menage_retenu_source": "TEST", "cout_standard_id": None,
        "cout_standard_menage_snapshot": None, "cout_standard_date_debut_validite": None,
        "cout_standard_date_fin_validite": None, "logement_id_snapshot": "LOG_CANAPE",
        "type_logement_id_snapshot": None, "date_reference_cout_menage": "2026-05-10",
    }])


def _taux_frame():
    return pd.DataFrame([{
        "taux_commission_id": "TC_1", "proprietaire_id": "PROP_1", "logement_id": "LOG_CANAPE",
        "taux_commission": 0.2, "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
    }])


class CanapeHistoriseTests(unittest.TestCase):
    def _run(self, date_arrivee, df_log, df_canape):
        df_comm, _, _ = build_commissions(
            _flux_frame(date_arrivee), _reservation_frame(date_arrivee), _payout_frame(),
            pd.DataFrame(columns=HH_COLS), df_log, pd.DataFrame(), _taux_frame(), df_canape)
        return df_comm.iloc[0]

    def test_reservation_ancienne_utilise_le_montant_en_vigueur_a_sa_date(self):
        """§0/§33 de la mission : une réservation de 2026 recalculée après un changement 2027 doit
        toujours utiliser le montant EN VIGUEUR EN 2026, jamais le montant courant/futur."""
        # REF_Logements porte la valeur COURANTE (30 avant migration -> 40 après changement 2027).
        # Elle ne doit JAMAIS être lue une fois un historique disponible.
        df_log = pd.DataFrame([{"logement_id": "LOG_CANAPE", "statut_parc": "GERE",
                                "seuil_voyageurs_preparation_canape": 999,
                                "montant_preparation_canape": 999}])
        df_canape = pd.DataFrame([
            {"canape_parametre_id": "CNP_1", "logement_id": "LOG_CANAPE",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "", "date_fin": "2026-12-31", "actif": "OUI"},
            {"canape_parametre_id": "CNP_2", "logement_id": "LOG_CANAPE",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "40",
             "date_debut": "2027-01-01", "date_fin": "", "actif": "OUI"},
        ])

        ligne_2026 = self._run("2026-06-15", df_log, df_canape)
        ligne_2027 = self._run("2027-03-01", df_log, df_canape)

        self.assertEqual(ligne_2026["preparation_canape_voyageurs"], 30.0)
        self.assertEqual(ligne_2027["preparation_canape_voyageurs"], 40.0)

    def test_sans_historique_repli_sur_la_colonne_courante(self):
        """Aucune régression pour les appels existants (df_canape vide/None) : comportement
        historique inchangé, colonne courante REF_Logements toujours utilisée."""
        df_log = pd.DataFrame([{"logement_id": "LOG_CANAPE", "statut_parc": "GERE",
                                "seuil_voyageurs_preparation_canape": 5,
                                "montant_preparation_canape": 30}])

        avec_vide = self._run("2026-06-15", df_log, pd.DataFrame())
        avec_none = self._run("2026-06-15", df_log, None)

        self.assertEqual(avec_vide["preparation_canape_voyageurs"], 30.0)
        self.assertEqual(avec_none["preparation_canape_voyageurs"], 30.0)

    def test_sous_le_seuil_aucun_montant_meme_historise(self):
        df_log = pd.DataFrame([{"logement_id": "LOG_CANAPE", "statut_parc": "GERE",
                                "seuil_voyageurs_preparation_canape": 999,
                                "montant_preparation_canape": 999}])
        df_canape = pd.DataFrame([
            {"canape_parametre_id": "CNP_1", "logement_id": "LOG_CANAPE",
             "seuil_voyageurs_preparation_canape": "8", "montant_preparation_canape": "30",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ])
        ligne = self._run("2026-06-15", df_log, df_canape)
        self.assertEqual(ligne["preparation_canape_voyageurs"], 0.0)
        self.assertEqual(ligne["controle_preparation_canape"], "NON_ELIGIBLE")

    def test_logement_sans_parametre_historise_est_non_applicable(self):
        """Un logement présent dans df_canape sous un AUTRE id (aucune ligne pour LOG_CANAPE) ne
        doit jamais retomber sur la colonne courante : l'historique, une fois disponible, fait
        foi seul."""
        df_log = pd.DataFrame([{"logement_id": "LOG_CANAPE", "statut_parc": "GERE",
                                "seuil_voyageurs_preparation_canape": 5,
                                "montant_preparation_canape": 30}])
        df_canape = pd.DataFrame([
            {"canape_parametre_id": "CNP_AUTRE", "logement_id": "LOG_AUTRE",
             "seuil_voyageurs_preparation_canape": "5", "montant_preparation_canape": "30",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ])
        ligne = self._run("2026-06-15", df_log, df_canape)
        self.assertEqual(ligne["preparation_canape_voyageurs"], 0.0)
        self.assertEqual(ligne["controle_preparation_canape"], "NON_APPLICABLE")


if __name__ == "__main__":
    unittest.main()

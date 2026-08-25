"""Mission 7 — parité ligne à ligne : `build_commissions` (Lot10) délègue désormais le calcul
commission/net aux fonctions pures de `lib_commission_engine.py` pour les 3 branches de routage
(HOSTAWAY, HH, VRBO). Ce test prouve que le résultat produit par la chaîne de production est
exactement celui que donnerait un appel direct au moteur pur avec les mêmes assiette/taux/payout/
ménage — c'est la preuve de non-duplication (une seule formule, appelée 3 fois, jamais réécrite
inline une 2e fois quelque part).

Caractérisation : ces valeurs (16.0 / 64.0 pour assiette=80, taux=0.2, payout=100, ménage=20) sont
celles produites par la formule AVANT extraction (inchangée) — figées ici comme référence.
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lot10_calculer_resultats import build_commissions
from lib_commission_engine import (
    assiette_v1_paiement_direct,
    calculer_commission_conciergerie,
    calculer_net_proprietaire,
)

HH_COLS = ["reservation_hh_id", "total_percu", "menage", "commission", "taux_commission"]


def _log_frame(logement_id="LOG_R"):
    return pd.DataFrame([{"logement_id": logement_id, "statut_parc": "GERE",
                          "seuil_voyageurs_preparation_canape": None,
                          "montant_preparation_canape": None}])


def _taux_frame(logement_id="LOG_R", proprietaire_id="PROP_1", taux=0.2):
    return pd.DataFrame([{
        "taux_commission_id": "TC_1", "proprietaire_id": proprietaire_id,
        "logement_id": logement_id, "taux_commission": taux,
        "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
    }])


class PariteHostawayTests(unittest.TestCase):
    """Branche HOSTAWAY : assiette fournie par le payout amont, taux résolu par date."""

    def test_commission_et_net_identiques_au_moteur_pur(self):
        date_arrivee = "2026-06-15"
        df_flux = pd.DataFrame([{
            "type_flux_id": "TYPE_FLUX_017", "source_pk": "RES-HA-001", "flux_id": "FLUX_1",
            "mois": "2026-06", "logement_id": "LOG_R", "proprietaire_id": "PROP_1",
        }])
        df_res = pd.DataFrame([{
            "reservation_calc_id": "RES-HA-001", "reservation_id_hostaway": 123,
            "reservation_hh_id": None, "source": "HOSTAWAY_AIRBNB",
            "source_montant": "HOSTAWAY_PAYOUT", "montant_retenu": 100.0,
            "logement_id": "LOG_R", "proprietaire_id": "PROP_1",
            "date_arrivee": date_arrivee, "date_depart": date_arrivee, "nuits": 2,
            "guestCount": 2, "canal": "AIRBNB", "payout_calcule": 100.0,
            "menage_retenu": 20.0, "assiette_commission": 80.0,
        }])
        df_payout = pd.DataFrame([{
            "reservation_id": 123, "channel_type": "AIRBNB", "statut_calcul_payout": "NORMAL",
            "payout_calcule": 100.0, "source_payout": "HOSTAWAY_PAYOUT", "menage_retenu": 20.0,
            "assiette_commission": 80.0, "inclure_resultat_auto": "OUI",
            "menage_retenu_source": "TEST", "cout_standard_id": None,
            "cout_standard_menage_snapshot": None, "cout_standard_date_debut_validite": None,
            "cout_standard_date_fin_validite": None, "logement_id_snapshot": "LOG_R",
            "type_logement_id_snapshot": None, "date_reference_cout_menage": "2026-05-10",
        }])
        df_comm, _, _ = build_commissions(
            df_flux, df_res, df_payout, pd.DataFrame(columns=HH_COLS), _log_frame(),
            pd.DataFrame(), _taux_frame())
        ligne = df_comm.iloc[0]

        commission_attendue = calculer_commission_conciergerie(80.0, 0.2)
        net_attendu = calculer_net_proprietaire(100.0, 20.0, commission_attendue)
        self.assertEqual(ligne["commission_conciergerie"], commission_attendue)
        self.assertEqual(ligne["net_proprietaire"], net_attendu)
        self.assertEqual(ligne["commission_conciergerie"], 16.0)
        self.assertEqual(ligne["net_proprietaire"], 64.0)


class PariteHHTests(unittest.TestCase):
    """Branche HH : assiette = total_percu - menage (calculée dans Lot10), commission/net via le
    moteur pur — total_percu/menage BRUTS (non arrondis) passés à `calculer_net_proprietaire`,
    comportement préexistant préservé tel quel par cette extraction."""

    def test_commission_et_net_identiques_au_moteur_pur(self):
        date_arrivee = "2026-06-15"
        df_flux = pd.DataFrame([{
            "type_flux_id": "TYPE_FLUX_017", "source_pk": "RES-HH-001", "flux_id": "FLUX_2",
            "mois": "2026-06", "logement_id": "LOG_R", "proprietaire_id": "PROP_1",
        }])
        df_res = pd.DataFrame([{
            "reservation_calc_id": "RES-HH-001", "reservation_id_hostaway": None,
            "reservation_hh_id": "HH_1", "source": "DIRECT_SANS_SAISIE_HH",
            "source_montant": "SAISIE_HH", "montant_retenu": None,
            "logement_id": "LOG_R", "proprietaire_id": "PROP_1",
            "date_arrivee": date_arrivee, "date_depart": date_arrivee, "nuits": 2,
            "guestCount": 2, "canal": "DIRECT", "payout_calcule": None,
            "menage_retenu": None, "assiette_commission": None,
        }])
        df_payout = pd.DataFrame(columns=[
            "reservation_id", "channel_type", "statut_calcul_payout", "payout_calcule",
            "source_payout", "menage_retenu", "assiette_commission", "inclure_resultat_auto",
            "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
            "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
            "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
        ])
        df_hh = pd.DataFrame([{
            "reservation_hh_id": "HH_1", "total_percu": 150.0, "menage": 25.0,
            "commission": None, "taux_commission": None,
        }])
        df_comm, _, _ = build_commissions(
            df_flux, df_res, df_payout, df_hh, _log_frame(), pd.DataFrame(), _taux_frame())
        ligne = df_comm.iloc[0]

        assiette_attendue = assiette_v1_paiement_direct(150.0, 25.0)
        commission_attendue = calculer_commission_conciergerie(assiette_attendue, 0.2)
        net_attendu = calculer_net_proprietaire(150.0, 25.0, commission_attendue)
        self.assertEqual(ligne["assiette_commission"], assiette_attendue)
        self.assertEqual(ligne["commission_conciergerie"], commission_attendue)
        self.assertEqual(ligne["net_proprietaire"], net_attendu)


class PariteVRBOTests(unittest.TestCase):
    """Branche VRBO (historique clôturé) : assiette = payout - ménage (résolue), commission/net
    via le moteur pur — payout_calcule/menage_retenu déjà arrondis (comme HOSTAWAY)."""

    def test_commission_et_net_identiques_au_moteur_pur(self):
        date_arrivee = "2026-06-15"
        df_flux = pd.DataFrame([{
            "type_flux_id": "TYPE_FLUX_017", "source_pk": "RES-VRBO-001", "flux_id": "FLUX_3",
            "mois": "2026-06", "logement_id": "LOG_R", "proprietaire_id": "PROP_1",
        }])
        df_res = pd.DataFrame([{
            "reservation_calc_id": "RES-VRBO-001", "reservation_id_hostaway": None,
            "reservation_hh_id": None, "source": "HOSTAWAY_VRBO",
            "source_montant": "VRBO_HISTORIQUE", "montant_retenu": 200.0,
            "logement_id": "LOG_R", "proprietaire_id": "PROP_1",
            "date_arrivee": date_arrivee, "date_depart": date_arrivee, "nuits": 3,
            "guestCount": 2, "canal": "VRBO", "payout_calcule": 200.0,
            "menage_retenu": 30.0, "assiette_commission": None,
        }])
        df_payout = pd.DataFrame(columns=[
            "reservation_id", "channel_type", "statut_calcul_payout", "payout_calcule",
            "source_payout", "menage_retenu", "assiette_commission", "inclure_resultat_auto",
            "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
            "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
            "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
        ])
        df_comm, _, _ = build_commissions(
            df_flux, df_res, df_payout, pd.DataFrame(columns=HH_COLS), _log_frame(),
            pd.DataFrame(), _taux_frame())
        ligne = df_comm.iloc[0]

        assiette_attendue = assiette_v1_paiement_direct(200.0, 30.0)
        commission_attendue = calculer_commission_conciergerie(assiette_attendue, 0.2)
        net_attendu = calculer_net_proprietaire(200.0, 30.0, commission_attendue)
        self.assertEqual(ligne["assiette_commission"], assiette_attendue)
        self.assertEqual(ligne["commission_conciergerie"], commission_attendue)
        self.assertEqual(ligne["net_proprietaire"], net_attendu)


class UneSeuleSourceDeCalculTests(unittest.TestCase):
    """§20 : le vieux calcul inline (assiette * taux littéral) ne doit plus exister dans Lot10 —
    seul le moteur pur porte la formule."""

    def test_lot10_importe_le_moteur_et_ne_duplique_pas_la_formule(self):
        import lot10_calculer_resultats as lot10
        source = Path(lot10.__file__).read_text(encoding="utf-8")
        self.assertIn("from lib_commission_engine import", source)
        self.assertIn("calculer_commission_conciergerie", source)
        self.assertIn("calculer_net_proprietaire", source)
        self.assertIn("assiette_v1_paiement_direct", source)
        # aucune formule inline "assiette_commission ... * ... taux_commission ... .round(2)"
        # ré-écrite localement (l'ancien pattern utilisait ".round(2)" juste après la
        # multiplication assiette*taux sur les 3 branches — supprimé par cette extraction).
        self.assertNotIn(
            '"assiette_commission"] * df_ha.loc[normal_ha, "taux_commission"]', source)
        self.assertNotIn(
            'df_hh_ok["assiette_commission"] * df_hh_ok["taux_commission"]', source)
        self.assertNotIn(
            'df_vrbo["assiette_commission"] * df_vrbo["taux_commission"]', source)
        # Mission 7 bis : l'ancienne formule inline "payout - menage" (HH/VRBO) ne doit plus
        # apparaitre litteralement, remplacee par l'appel a assiette_v1_paiement_direct.
        self.assertNotIn(
            '(df_hh_ok["total_percu"] - df_hh_ok["menage"]).round(2)', source)
        self.assertNotIn(
            'df_vrbo["payout_resolu"] - df_vrbo["menage_resolu"].fillna(0.0)\n        ).round(2)',
            source)


if __name__ == "__main__":
    unittest.main()

"""Mission 6 ter — les règles versionnées (ASSIETTE_COMMISSION, CANAPE_FORMULE) sont réellement
consommées par Lot10, pas seulement déclarées en base (`ref_regles_versions`, migration 0059).

Preuve : `build_commissions` résout la version applicable à la date de CHAQUE réservation
(`resolve_regle_version`) avant d'appliquer la formule — une V2 de fixture n'affecte jamais une
date antérieure à sa date d'effet, même rejouée après coup ; l'absence de version couvrante est
fail-closed (BLOQUANT), jamais un repli silencieux vers la formule actuelle.

Sans `df_regles` fourni (ou vide) : comportement historique inchangé, comme pour le paramètre
canapé (`tests/test_canape_historise.py`) — aucune régression pour les appelants existants.
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lot10_calculer_resultats import build_commissions


HH_COLS = ["reservation_hh_id", "total_percu", "menage", "commission", "taux_commission"]


def _reservation_frame(date_arrivee, guest_count=6):
    return pd.DataFrame([{
        "reservation_calc_id": f"RES-{date_arrivee}-HA-001",
        "reservation_id_hostaway": 123,
        "reservation_hh_id": None,
        "source": "HOSTAWAY_AIRBNB",
        "source_montant": "HOSTAWAY_PAYOUT",
        "montant_retenu": 100.0,
        "logement_id": "LOG_R",
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
        "type_flux_id": "TYPE_FLUX_017", "source_pk": f"RES-{date_arrivee}-HA-001",
        "flux_id": "FLUX_1", "mois": date_arrivee[:7], "logement_id": "LOG_R",
        "proprietaire_id": "PROP_1",
    }])


def _payout_frame():
    return pd.DataFrame([{
        "reservation_id": 123, "channel_type": "AIRBNB", "statut_calcul_payout": "NORMAL",
        "payout_calcule": 100.0, "source_payout": "HOSTAWAY_PAYOUT", "menage_retenu": 20.0,
        "assiette_commission": 80.0, "inclure_resultat_auto": "OUI",
        "menage_retenu_source": "TEST", "cout_standard_id": None,
        "cout_standard_menage_snapshot": None, "cout_standard_date_debut_validite": None,
        "cout_standard_date_fin_validite": None, "logement_id_snapshot": "LOG_R",
        "type_logement_id_snapshot": None, "date_reference_cout_menage": "2026-05-10",
    }])


def _taux_frame():
    return pd.DataFrame([{
        "taux_commission_id": "TC_1", "proprietaire_id": "PROP_1", "logement_id": "LOG_R",
        "taux_commission": 0.2, "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
    }])


def _log_frame():
    return pd.DataFrame([{"logement_id": "LOG_R", "statut_parc": "GERE",
                          "seuil_voyageurs_preparation_canape": 5,
                          "montant_preparation_canape": 30}])


def _run(date_arrivee, df_regles, df_canape=None, guest_count=6):
    df_comm, _, _ = build_commissions(
        _flux_frame(date_arrivee), _reservation_frame(date_arrivee, guest_count), _payout_frame(),
        pd.DataFrame(columns=HH_COLS), _log_frame(), pd.DataFrame(), _taux_frame(), df_canape,
        df_regles)
    return df_comm.iloc[0]


class AssietteCommissionVersionneeTests(unittest.TestCase):
    def test_v1_seule_couvre_toute_date(self):
        """V1 ouverte depuis l'origine (backfill 0059) : n'importe quelle date résout V1, jamais
        de BLOQUANT — comportement de production réel, pas seulement le résolveur isolé."""
        df_regles = pd.DataFrame([
            {"regle_version_id": "R1", "rule_code": "ASSIETTE_COMMISSION", "version": "V1",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ])
        ligne = _run("2026-06-15", df_regles)
        self.assertEqual(ligne["assiette_commission"], 80.0)

    def test_sans_regles_versions_comportement_inchange(self):
        """df_regles vide/None : repli identique au comportement historique (aucune régression)."""
        ligne_none = _run("2026-06-15", None)
        ligne_vide = _run("2026-06-15", pd.DataFrame())
        self.assertEqual(ligne_none["assiette_commission"], 80.0)
        self.assertEqual(ligne_vide["assiette_commission"], 80.0)


class FailClosedReglesTests(unittest.TestCase):
    def test_assiette_sans_version_couvrante_est_bloquant(self):
        """Aucune version ne couvre la date -> BLOQUANT (sys.exit), jamais un repli silencieux
        vers une formule "actuelle" implicite."""
        df_regles = pd.DataFrame([
            {"regle_version_id": "R1", "rule_code": "ASSIETTE_COMMISSION", "version": "V1",
             "date_debut": "2020-01-01", "date_fin": "2020-12-31", "actif": "OUI"},
        ])
        with self.assertRaises(SystemExit):
            _run("2026-06-15", df_regles)

    def test_version_resolue_mais_non_implementee_est_bloquant(self):
        df_regles = pd.DataFrame([
            {"regle_version_id": "R1", "rule_code": "ASSIETTE_COMMISSION", "version": "V2_INCONNUE",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ])
        with self.assertRaises(SystemExit):
            _run("2026-06-15", df_regles)


_ASSIETTE_V1_TOUJOURS = {"regle_version_id": "R_ASSIETTE", "rule_code": "ASSIETTE_COMMISSION",
                        "version": "V1", "date_debut": "", "date_fin": "", "actif": "OUI"}


class CanapeFormuleVersionneeTests(unittest.TestCase):
    def test_v1_seule_couvre_toute_date(self):
        df_regles = pd.DataFrame([
            _ASSIETTE_V1_TOUJOURS,
            {"regle_version_id": "R1", "rule_code": "CANAPE_FORMULE", "version": "V1",
             "date_debut": "", "date_fin": "", "actif": "OUI"},
        ])
        ligne = _run("2026-06-15", df_regles, guest_count=6)
        self.assertEqual(ligne["preparation_canape_voyageurs"], 30.0)
        self.assertEqual(ligne["controle_preparation_canape"], "OK")

    def test_formule_sans_version_couvrante_est_a_controler_pas_appliquee_silencieusement(self):
        """Absence de CANAPE_FORMULE applicable -> A_CONTROLER, jamais la formule V1 appliquée
        en silence comme si de rien n'était."""
        df_regles = pd.DataFrame([
            _ASSIETTE_V1_TOUJOURS,
            {"regle_version_id": "R1", "rule_code": "CANAPE_FORMULE", "version": "V1",
             "date_debut": "2020-01-01", "date_fin": "2020-12-31", "actif": "OUI"},
        ])
        ligne = _run("2026-06-15", df_regles, guest_count=6)
        self.assertEqual(ligne["preparation_canape_voyageurs"], 0.0)
        self.assertEqual(ligne["controle_preparation_canape"], "A_CONTROLER")
        self.assertIn("CANAPE_FORMULE", ligne["source_preparation_canape"])


class RecalculHistoriqueApresV2Tests(unittest.TestCase):
    def test_v2_fixture_ne_change_jamais_une_date_anterieure(self):
        """§29/§27 de la mission : créer une V2 de fixture à partir de 2027 ne doit JAMAIS changer
        le résultat d'une date de 2026, même recalculée après l'introduction de la V2."""
        df_regles = pd.DataFrame([
            {"regle_version_id": "R1", "rule_code": "ASSIETTE_COMMISSION", "version": "V1",
             "date_debut": "", "date_fin": "2026-12-31", "actif": "OUI"},
            {"regle_version_id": "R2", "rule_code": "ASSIETTE_COMMISSION", "version": "V2_TEST",
             "date_debut": "2027-01-01", "date_fin": "", "actif": "OUI"},
        ])
        # V2_TEST n'est dans aucun dict d'implémentation -> une date 2027 est BLOQUANTE (fail
        # closed, cohérent : aucune vraie V2 n'existe encore côté code) ; 2026 reste V1, inchangé.
        avant = _run("2026-06-15", df_regles)
        self.assertEqual(avant["assiette_commission"], 80.0)
        with self.assertRaises(SystemExit):
            _run("2027-03-01", df_regles)
        # Rejoue 2026 : toujours V1, toujours identique.
        apres = _run("2026-06-15", df_regles)
        self.assertEqual(apres["assiette_commission"], avant["assiette_commission"])


if __name__ == "__main__":
    unittest.main()

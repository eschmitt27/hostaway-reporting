"""Non-régression refacturation charges propriétaires (D033/D034).

Défaut corrigé : le terme `charges_exceptionnelles_refacturees` de la formule verrouillée
D033 était absent de `montant_du_conciergerie` (lot10) et la ligne 11 de préfacture
CHARGES_EXCEPT_REFAC était figée à 0.0 (lot12).

Règle : une charge refacturable=OUI et validée alimente le bloc RÈGLEMENT propriétaire
(montant dû, reste à payer, ligne 11), jamais le revenu net d'exploitation (D034).
Éligibilité jamais inférée depuis la catégorie ou le type_flux.
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_settlements import (
    aggregate_refacturable_charges,
    eligible_refacturable_charge,
)
import lot10_calculer_resultats as lot10
import lot12_generer_factures as lot12


def _charge(**over):
    base = {
        "charge_id": "CHG-2026-05-IC-BANQUE-001",
        "mois": "2026-05",
        "montant": 30.0,
        "refacturable": "OUI",
        "statut_controle": "VALIDE",
        "logement_id": "LOG_1",
        "proprietaire_id": "PROP_1",
    }
    base.update(over)
    return base


class EligibleRefacturableChargeTests(unittest.TestCase):
    def test_refacturable_valide_avec_proprietaire_est_eligible(self):
        ok, montant, code = eligible_refacturable_charge(_charge())
        self.assertTrue(ok)
        self.assertEqual(montant, 30.0)
        self.assertEqual(code, "OK")

    def test_non_refacturable_exclu(self):
        ok, montant, code = eligible_refacturable_charge(_charge(refacturable="NON"))
        self.assertFalse(ok)
        self.assertEqual(montant, 0.0)
        self.assertEqual(code, "NON_REFACTURABLE")

    def test_refacturable_vide_exclu(self):
        ok, _montant, code = eligible_refacturable_charge(_charge(refacturable=""))
        self.assertFalse(ok)
        self.assertEqual(code, "NON_REFACTURABLE")

    def test_charge_non_valide_exclue(self):
        ok, montant, code = eligible_refacturable_charge(_charge(statut_controle="A_CONTROLER"))
        self.assertFalse(ok)
        self.assertEqual(montant, 0.0)
        self.assertEqual(code, "CHARGE_NON_VALIDE")

    def test_montant_nul_ou_negatif_exclu(self):
        for bad in (0, -5, None, "abc"):
            ok, montant, code = eligible_refacturable_charge(_charge(montant=bad))
            self.assertFalse(ok)
            self.assertEqual(code, "MONTANT_INVALIDE")

    def test_refacturable_sans_proprietaire_signale(self):
        ok, montant, code = eligible_refacturable_charge(_charge(proprietaire_id=""))
        self.assertFalse(ok)
        self.assertEqual(montant, 30.0)
        self.assertEqual(code, "REFAC_SANS_PROPRIETAIRE")

    def test_pas_inference_depuis_type_flux(self):
        # type_flux refacturable mais flag refacturable=NON -> exclu (jamais inféré)
        ok, _montant, code = eligible_refacturable_charge(
            _charge(refacturable="NON", type_flux_id="TYPE_FLUX_011")
        )
        self.assertFalse(ok)
        self.assertEqual(code, "NON_REFACTURABLE")


class AggregateRefacturableChargesTests(unittest.TestCase):
    def test_somme_par_mois_logement(self):
        charges = [
            _charge(charge_id="A", montant=30.0),
            _charge(charge_id="B", montant=20.0),
        ]
        by_log, by_prop, controls = aggregate_refacturable_charges(charges)
        self.assertEqual(by_log[("2026-05", "LOG_1")], (50.0, "PROP_1"))
        self.assertEqual(by_prop, {})
        self.assertEqual(controls, [])

    def test_proprietaire_sans_logement_route_by_prop(self):
        by_log, by_prop, controls = aggregate_refacturable_charges(
            [_charge(logement_id="", montant=40.0)]
        )
        self.assertEqual(by_log, {})
        self.assertEqual(by_prop[("2026-05", "PROP_1")], 40.0)
        self.assertEqual(controls, [])

    def test_sans_proprietaire_va_en_controle(self):
        by_log, by_prop, controls = aggregate_refacturable_charges(
            [_charge(proprietaire_id="", logement_id="", montant=15.0)]
        )
        self.assertEqual(by_log, {})
        self.assertEqual(by_prop, {})
        self.assertEqual(len(controls), 1)
        self.assertEqual(controls[0]["code_controle"], "REFAC_SANS_PROPRIETAIRE")
        self.assertEqual(controls[0]["montant"], 15.0)

    def test_non_refacturable_ignore(self):
        by_log, by_prop, controls = aggregate_refacturable_charges(
            [_charge(refacturable="NON")]
        )
        self.assertEqual((by_log, by_prop, controls), ({}, {}, []))


def _df_comm():
    return pd.DataFrame([{
        "mois": "2026-05", "logement_id": "LOG_1", "proprietaire_id": "PROP_1",
        "payout_calcule": 300.0, "menage_retenu": 50.0, "commission_conciergerie": 100.0,
        "net_proprietaire": 200.0, "preparation_canape_voyageurs": 0.0,
        "reservation_calc_id": "RES_1",
    }])


class Lot10RefacturationIntegrationTests(unittest.TestCase):
    def _reg_row(self, df_charges):
        _exploit, df_reg, _vue = lot10.build_net_proprietaire(
            _df_comm(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), df_charges,
        )
        rows = df_reg[
            (df_reg["logement_id"] == "LOG_1") & (df_reg["proprietaire_id"] == "PROP_1")
        ]
        self.assertEqual(len(rows), 1)
        return rows.iloc[0]

    def test_sans_charge_montant_du_de_base(self):
        row = self._reg_row(pd.DataFrame())
        # commission 100 + menage 50 + canape 0 + charge_fixe 0
        self.assertEqual(row["montant_du_conciergerie"], 150.0)
        self.assertEqual(row["charges_exceptionnelles_refacturees"], 0.0)

    def test_charge_refacturable_alimente_montant_du(self):
        row = self._reg_row(pd.DataFrame([_charge(montant=30.0)]))
        self.assertEqual(row["charges_exceptionnelles_refacturees"], 30.0)
        # 150 de base + 30 refac
        self.assertEqual(row["montant_du_conciergerie"], 180.0)

    def test_refac_ne_touche_pas_revenu_net_exploitation(self):
        base = self._reg_row(pd.DataFrame())
        avec = self._reg_row(pd.DataFrame([_charge(montant=30.0)]))
        # D034 : le revenu net d'exploitation est identique avec/sans refac
        self.assertEqual(
            base["net_proprietaire_apres_charge_mois"],
            avec["net_proprietaire_apres_charge_mois"],
        )
        self.assertEqual(avec["net_proprietaire_apres_charge_mois"], 200.0)

    def test_charge_non_refacturable_sans_effet(self):
        row = self._reg_row(pd.DataFrame([_charge(refacturable="NON", montant=30.0)]))
        self.assertEqual(row["montant_du_conciergerie"], 150.0)
        self.assertEqual(row["charges_exceptionnelles_refacturees"], 0.0)

    def test_charge_non_valide_sans_effet(self):
        row = self._reg_row(pd.DataFrame([_charge(statut_controle="A_CONTROLER", montant=30.0)]))
        self.assertEqual(row["montant_du_conciergerie"], 150.0)
        self.assertEqual(row["charges_exceptionnelles_refacturees"], 0.0)

    def test_plusieurs_charges_refac_meme_perimetre_cumulees(self):
        df = pd.DataFrame([
            _charge(charge_id="A", montant=30.0),
            _charge(charge_id="B", montant=20.5),
        ])
        row = self._reg_row(df)
        self.assertEqual(row["charges_exceptionnelles_refacturees"], 50.5)
        self.assertEqual(row["montant_du_conciergerie"], 200.5)

    def test_refac_sans_logement_route_en_sentinelle_hors_prefacture(self):
        df = pd.DataFrame([_charge(logement_id="", montant=40.0)])
        _exploit, df_reg, _vue = lot10.build_net_proprietaire(
            _df_comm(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), df,
        )
        sent = df_reg[df_reg["statut_reglement"] == "REFAC_SANS_LOGEMENT_A_CONTROLER"]
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent.iloc[0]["logement_id"], lot10.SENTINEL_GLOBAL)
        self.assertEqual(sent.iloc[0]["charges_exceptionnelles_refacturees"], 40.0)
        # la charge n'a pas contaminé la ligne logement affectée
        aff = df_reg[df_reg["logement_id"] == "LOG_1"].iloc[0]
        self.assertEqual(aff["charges_exceptionnelles_refacturees"], 0.0)

    def test_refac_sans_proprietaire_route_en_controle(self):
        df = pd.DataFrame([_charge(proprietaire_id="", logement_id="", montant=15.0)])
        _exploit, df_reg, _vue = lot10.build_net_proprietaire(
            _df_comm(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), df,
        )
        ctrl = df_reg[df_reg["statut_reglement"] == "REFAC_SANS_PROPRIETAIRE"]
        self.assertEqual(len(ctrl), 1)
        self.assertEqual(ctrl.iloc[0]["charges_exceptionnelles_refacturees"], 15.0)
        self.assertEqual(ctrl.iloc[0]["montant_du_conciergerie"], 0.0)


def _rec(**over):
    base = {
        "total_payout": 300.0, "total_menage": 50.0, "total_commission": 100.0,
        "total_preparation_canape": 0.0, "charge_fixe": 0.0,
        "charges_except_refac": 30.0, "revenu_net_exploitation": 200.0,
        "montant_du": 180.0, "airbnb_impute": 0.0, "reste_a_payer": 180.0,
        "acomptes": 0.0,
    }
    base.update(over)
    return base


class Lot12PrefactureLignesTests(unittest.TestCase):
    def _lines(self, rec):
        return lot12.build_facture_lignes("PREF-1", rec, "A_CONTROLER")

    def _by_type(self, lignes):
        return {l["type_ligne"]: l for l in lignes}

    # ── Structure verrouillée §17.3 : 12 lignes, STATUT ligne 12 ──

    def test_sans_canape_exactement_12_lignes_1_a_12(self):
        lignes = self._lines(_rec(total_preparation_canape=0.0))
        self.assertEqual(len(lignes), 12)
        self.assertEqual([l["ligne_num"] for l in lignes], list(range(1, 13)))
        # aucune 13e ligne
        self.assertFalse(any(l["ligne_num"] == 13 for l in lignes))

    def test_sans_canape_statut_reglement_ligne_12(self):
        lignes = self._lines(_rec(total_preparation_canape=0.0))
        self.assertEqual(lignes[-1]["type_ligne"], "STATUT_REGLEMENT")
        self.assertEqual(lignes[-1]["ligne_num"], 12)
        self.assertEqual(lignes[-1]["bloc"], "REGLEMENT")

    def test_charges_except_refac_presente_une_fois_ligne_10(self):
        lignes = self._lines(_rec(charges_except_refac=30.0))
        refac = [l for l in lignes if l["type_ligne"] == "CHARGES_EXCEPT_REFAC"]
        self.assertEqual(len(refac), 1)
        self.assertEqual(refac[0]["ligne_num"], 10)
        self.assertEqual(refac[0]["bloc"], "REGLEMENT")
        self.assertEqual(refac[0]["montant"], 30.0)

    def test_aucune_ligne_exploitation_ne_porte_la_refac(self):
        # montant refac isolé : aucune ligne EXPLOITATION ne doit valoir ce montant refac
        lignes = self._lines(_rec(charges_except_refac=777.77))
        exploit = [l for l in lignes if l["bloc"] == "EXPLOITATION"]
        self.assertTrue(all(l["type_ligne"] != "CHARGES_EXCEPT_REFAC" for l in exploit))
        self.assertTrue(all(l["montant"] != 777.77 for l in exploit))

    def test_ligne_11_zero_si_aucune_refac(self):
        by = self._by_type(self._lines(_rec(charges_except_refac=0.0)))
        self.assertEqual(by["CHARGES_EXCEPT_REFAC"]["montant"], 0.0)

    def test_refac_dans_reglement_pas_exploitation(self):
        by = self._by_type(self._lines(_rec()))
        # revenu net exploitation : bloc EXPLOITATION, n'inclut pas la refac
        self.assertEqual(by["REVENU_NET_EXPLOITATION"]["bloc"], "EXPLOITATION")
        self.assertEqual(by["REVENU_NET_EXPLOITATION"]["montant"], 200.0)
        # montant dû : bloc REGLEMENT, inclut la refac (calculée par lot10)
        self.assertEqual(by["MONTANT_DU"]["bloc"], "REGLEMENT")
        self.assertEqual(by["MONTANT_DU"]["montant"], 180.0)

    # ── Bloc REGLEMENT ancré : numéros fixes 6..12 ──

    def test_bloc_reglement_numeros_fixes(self):
        by = self._by_type(self._lines(_rec()))
        attendu = {
            "MONTANT_DU": 6, "ACOMPTE_AIRBNB": 7, "PAIEMENT_DEJA_RECU": 8,
            "RESTE_A_PAYER": 9, "CHARGES_EXCEPT_REFAC": 10,
            "ACOMPTES_PROPRIETAIRES": 11, "STATUT_REGLEMENT": 12,
        }
        for t, num in attendu.items():
            self.assertEqual(by[t]["ligne_num"], num, t)
            self.assertEqual(by[t]["bloc"], "REGLEMENT", t)

    # ── Cas avec préparation canapé : ne dérive pas le bloc REGLEMENT ──

    def test_avec_canape_ligne_supplementaire_sans_deriver_reglement(self):
        lignes = self._lines(_rec(total_preparation_canape=10.0))
        by = self._by_type(lignes)
        # canapé présent une fois, en exploitation
        canape = [l for l in lignes if l["type_ligne"] == "PREPARATION_CANAPE"]
        self.assertEqual(len(canape), 1)
        self.assertEqual(canape[0]["montant"], 10.0)
        self.assertEqual(canape[0]["bloc"], "EXPLOITATION")
        # le bloc REGLEMENT garde exactement les mêmes numéros que sans canapé
        self.assertEqual(by["MONTANT_DU"]["ligne_num"], 6)
        self.assertEqual(by["CHARGES_EXCEPT_REFAC"]["ligne_num"], 10)
        self.assertEqual(by["ACOMPTES_PROPRIETAIRES"]["ligne_num"], 11)
        self.assertEqual(by["STATUT_REGLEMENT"]["ligne_num"], 12)

    def test_canape_ne_decale_pas_la_numerotation_vs_sans_canape(self):
        sans = self._by_type(self._lines(_rec(total_preparation_canape=0.0)))
        avec = self._by_type(self._lines(_rec(total_preparation_canape=10.0)))
        for t in ("MONTANT_DU", "CHARGES_EXCEPT_REFAC", "ACOMPTES_PROPRIETAIRES",
                  "STATUT_REGLEMENT", "CHARGE_FIXE", "REVENU_NET_EXPLOITATION"):
            self.assertEqual(sans[t]["ligne_num"], avec[t]["ligne_num"], t)

    def test_charges_except_refac_unique_avec_canape(self):
        lignes = self._lines(_rec(total_preparation_canape=10.0, charges_except_refac=30.0))
        refac = [l for l in lignes if l["type_ligne"] == "CHARGES_EXCEPT_REFAC"]
        self.assertEqual(len(refac), 1)
        self.assertEqual(refac[0]["ligne_num"], 10)


if __name__ == "__main__":
    unittest.main()

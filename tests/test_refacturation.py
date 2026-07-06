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


# Structure cible obligatoire (ordre lecture propriétaire)
ORDRE_SANS_CANAPE = [
    "TOTAL_PAYOUT", "MENAGE_FACTURE", "COMMISSION_CONCIERGERIE",
    "CHARGE_FIXE", "REVENU_NET_EXPLOITATION",
    "CHARGES_EXCEPT_REFAC", "MONTANT_DU", "ACOMPTE_AIRBNB", "PAIEMENT_DEJA_RECU",
    "ACOMPTES_PROPRIETAIRES", "RESTE_A_PAYER", "STATUT_REGLEMENT",
]
ORDRE_AVEC_CANAPE = [
    "TOTAL_PAYOUT", "MENAGE_FACTURE", "COMMISSION_CONCIERGERIE",
    "PREPARATION_CANAPE", "CHARGE_FIXE", "REVENU_NET_EXPLOITATION",
    "CHARGES_EXCEPT_REFAC", "MONTANT_DU", "ACOMPTE_AIRBNB", "PAIEMENT_DEJA_RECU",
    "ACOMPTES_PROPRIETAIRES", "RESTE_A_PAYER", "STATUT_REGLEMENT",
]


class Lot12PrefactureLignesTests(unittest.TestCase):
    def _lines(self, rec):
        return lot12.build_facture_lignes("PREF-1", rec, "A_CONTROLER")

    def _pos(self, lignes, type_ligne):
        return next(i for i, l in enumerate(lignes) if l["type_ligne"] == type_ligne)

    # ── Ordre complet identique à la structure cible ──

    def test_ordre_complet_sans_canape(self):
        lignes = self._lines(_rec(total_preparation_canape=0.0))
        self.assertEqual([l["type_ligne"] for l in lignes], ORDRE_SANS_CANAPE)
        self.assertEqual([l["ligne_num"] for l in lignes], list(range(1, 13)))

    def test_ordre_complet_avec_canape(self):
        lignes = self._lines(_rec(total_preparation_canape=10.0))
        self.assertEqual([l["type_ligne"] for l in lignes], ORDRE_AVEC_CANAPE)
        self.assertEqual([l["ligne_num"] for l in lignes], list(range(1, 14)))

    # ── Comptes de lignes + statut en dernière ligne ──

    def test_sans_canape_12_lignes_statut_ligne_12(self):
        lignes = self._lines(_rec(total_preparation_canape=0.0))
        self.assertEqual(len(lignes), 12)
        self.assertEqual(lignes[-1]["type_ligne"], "STATUT_REGLEMENT")
        self.assertEqual(lignes[-1]["ligne_num"], 12)

    def test_avec_canape_13_lignes_statut_ligne_13(self):
        lignes = self._lines(_rec(total_preparation_canape=10.0))
        self.assertEqual(len(lignes), 13)
        self.assertEqual(lignes[-1]["type_ligne"], "STATUT_REGLEMENT")
        self.assertEqual(lignes[-1]["ligne_num"], 13)

    # ── Extras refacturés AVANT le montant dû (ils l'expliquent) ──

    def test_charges_except_refac_avant_montant_du(self):
        for canape in (0.0, 10.0):
            lignes = self._lines(_rec(total_preparation_canape=canape))
            self.assertLess(
                self._pos(lignes, "CHARGES_EXCEPT_REFAC"),
                self._pos(lignes, "MONTANT_DU"),
                f"canape={canape}",
            )

    # ── Acomptes / paiements déjà reçus AVANT reste à payer ──

    def test_acomptes_et_paiements_avant_reste_a_payer(self):
        for canape in (0.0, 10.0):
            lignes = self._lines(_rec(total_preparation_canape=canape))
            reste = self._pos(lignes, "RESTE_A_PAYER")
            for t in ("ACOMPTE_AIRBNB", "PAIEMENT_DEJA_RECU", "ACOMPTES_PROPRIETAIRES"):
                self.assertLess(self._pos(lignes, t), reste, f"{t} canape={canape}")

    # ── Canapé : une seule ligne, montant déjà inclus une fois dans MONTANT_DU ──

    def test_canape_ligne_unique_et_non_recomptee_dans_montant_du(self):
        rec = _rec(total_preparation_canape=10.0, montant_du=180.0)
        lignes = self._lines(rec)
        canape = [l for l in lignes if l["type_ligne"] == "PREPARATION_CANAPE"]
        self.assertEqual(len(canape), 1)
        self.assertEqual(canape[0]["montant"], 10.0)
        self.assertEqual(canape[0]["bloc"], "EXPLOITATION")
        # lot12 ne recalcule pas MONTANT_DU : le canapé n'est PAS ré-additionné ici
        montant_du = next(l for l in lignes if l["type_ligne"] == "MONTANT_DU")
        self.assertEqual(montant_du["montant"], 180.0)

    def test_canape_absent_pas_de_ligne_canape(self):
        lignes = self._lines(_rec(total_preparation_canape=0.0))
        self.assertEqual([l for l in lignes if l["type_ligne"] == "PREPARATION_CANAPE"], [])

    # ── Aucune ligne dupliquée ──

    def test_aucune_ligne_dupliquee(self):
        for canape in (0.0, 10.0):
            lignes = self._lines(_rec(total_preparation_canape=canape))
            types = [l["type_ligne"] for l in lignes]
            self.assertEqual(len(types), len(set(types)), f"doublon canape={canape}")
            nums = [l["ligne_num"] for l in lignes]
            self.assertEqual(len(nums), len(set(nums)), f"num duplique canape={canape}")

    # ── Refacturé dans REGLEMENT, jamais dans EXPLOITATION ──

    def test_refac_dans_reglement_pas_exploitation(self):
        lignes = self._lines(_rec(charges_except_refac=777.77))
        by = {l["type_ligne"]: l for l in lignes}
        self.assertEqual(by["CHARGES_EXCEPT_REFAC"]["bloc"], "REGLEMENT")
        self.assertEqual(by["CHARGES_EXCEPT_REFAC"]["montant"], 777.77)
        exploit = [l for l in lignes if l["bloc"] == "EXPLOITATION"]
        self.assertTrue(all(l["montant"] != 777.77 for l in exploit))
        # revenu net exploitation reste hors refac (D034)
        self.assertEqual(by["REVENU_NET_EXPLOITATION"]["bloc"], "EXPLOITATION")
        self.assertEqual(by["REVENU_NET_EXPLOITATION"]["montant"], 200.0)

    def test_charges_except_refac_zero_si_aucune_refac(self):
        by = {l["type_ligne"]: l for l in self._lines(_rec(charges_except_refac=0.0))}
        self.assertEqual(by["CHARGES_EXCEPT_REFAC"]["montant"], 0.0)


if __name__ == "__main__":
    unittest.main()

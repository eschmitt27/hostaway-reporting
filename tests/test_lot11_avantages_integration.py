"""Lot7C — tests d'intégration du branchement des contrôles avantages dans Lot11.

Teste `lot11_controles_coherence.controles_suivi_associe` (lecture seule) sur fixtures / DataFrames
construits en tmp. Ne lance PAS main() (pas d'écriture de MASTER_CTRL_Coherence). Aucun fichier réel.
Nécessite pandas (recette : Python312 + PYTHONPATH miniconda).
"""
import os
import sys
import tempfile
import unittest

import openpyxl
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "02_TRAVAIL"))
import lot11_controles_coherence as L  # noqa: E402
import lot7_generateur_avantages as gen  # noqa: E402

SAISIE_COLS = ["charge_id", "date_charge", "mois", "montant", "type_flux_id",
               "associe_id", "mode_paiement_id", "avantage_associe_id"]
SOURCE_COLS = ["mois", "associe_id", "type_flux_id", "type_remboursement",
               "nature", "montant", "mode_paiement_id", "lien_origine", "commentaire"]


def _make_saisie(path, charges):
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "SAISIE"
    ws.append(SAISIE_COLS)
    for c in charges:
        ws.append([c.get(k) for k in SAISIE_COLS])
    wb.save(path); wb.close()


def _make_lot7(path, source_rows=()):
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "SOURCE_SAISIE"
    ws.append(SOURCE_COLS)
    ws.append(["AAAA-MM", "instruction", "", "", "", None, "", "", ""])
    for s in source_rows:
        ws.append([s.get(k) for k in SOURCE_COLS])
    wb.save(path); wb.close()


def _calc_row(associe="PERS_WAFA", mois="2026-06", bruts=0.0, ik=0.0, dp=0.0, ch=0.0,
              ravs=0.0, rsva=0.0, nets=None, code_impact="HR"):
    nets = (bruts - ch - ravs + rsva) if nets is None else nets
    return {"pk_id": f"{associe}-{mois}", "mois": mois, "associe_id": associe,
            "avantage_brut_ik": ik, "avantage_brut_depenses_perso": dp,
            "avantages_bruts_total": bruts, "charges_payees_pour_societe": ch,
            "remboursements_associe_vers_societe": ravs,
            "remboursements_societe_vers_associe": rsva, "avantages_nets": nets,
            "code_impact": code_impact, "source_calcul": "LOT7"}


class Lot11AvantagesIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.saisie = os.path.join(self.tmp, "SAISIE_Charges_Flux.xlsx")
        self.lot7 = os.path.join(self.tmp, "MASTER_FACT_MAN_IK_Avantages.xlsx")

    def _codes(self, df_ik):
        return [e["code"] for e in L.controles_suivi_associe(df_ik, self.saisie, self.lot7)]

    # 1. Données cohérentes → aucune anomalie (calc généré depuis la SAISIE).
    def test_1_coherent_aucune_anomalie(self):
        _make_saisie(self.saisie, [
            {"charge_id": "C1", "date_charge": "2026-06-10", "montant": 100,
             "type_flux_id": "TYPE_FLUX_020", "associe_id": "", "mode_paiement_id": "PAY_001",
             "avantage_associe_id": "PERS_EWAN"}])
        _make_lot7(self.lot7)
        gen.generer(self.saisie, self.lot7, self.lot7)          # calc en phase avec la SAISIE
        df_ik = L._read_sheet(self.lot7, sheet="MASTER_CALC_AVANTAGES")
        self.assertEqual(self._codes(df_ik), [])

    # 2. avantage_associe_id présent côté charges mais absent du suivi → anomalie.
    def test_2_avantage_absent(self):
        _make_saisie(self.saisie, [
            {"charge_id": "C1", "date_charge": "2026-06-10", "montant": 100,
             "type_flux_id": "TYPE_FLUX_020", "associe_id": "", "mode_paiement_id": "PAY_001",
             "avantage_associe_id": "PERS_EWAN"}])
        _make_lot7(self.lot7)
        df_ik = pd.DataFrame([_calc_row(associe="PERS_WAFA", bruts=0.0)])  # calc non vide, mais pas EWAN
        self.assertIn("AVANTAGE_ABSENT_DU_SUIVI", self._codes(df_ik))

    # 3. SOURCE_SAISIE contient un lien_origine déjà présent dans Lot3 → anomalie.
    def test_3_source_saisie_lien_deja_lot3(self):
        _make_saisie(self.saisie, [
            {"charge_id": "C1", "date_charge": "2026-06-10", "montant": 100,
             "type_flux_id": "TYPE_FLUX_020", "associe_id": "", "mode_paiement_id": "PAY_001",
             "avantage_associe_id": "PERS_EWAN"}])
        _make_lot7(self.lot7, source_rows=[
            {"mois": "2026-06", "associe_id": "PERS_EWAN", "type_flux_id": "TYPE_FLUX_001",
             "montant": 50, "lien_origine": "C1"}])
        df_ik = pd.DataFrame([_calc_row(associe="PERS_EWAN", bruts=100, dp=100)])
        self.assertIn("SOURCE_SAISIE_LIEN_DEJA_LOT3", self._codes(df_ik))

    # 4. MASTER_CALC_AVANTAGES avec code_impact ≠ HR → anomalie.
    def test_4_code_impact_non_hr(self):
        _make_saisie(self.saisie, [])
        _make_lot7(self.lot7)
        df_ik = pd.DataFrame([_calc_row(associe="PERS_EWAN", bruts=100, dp=100, code_impact="IC")])
        self.assertIn("AVANTAGE_CODE_IMPACT_NON_HR", self._codes(df_ik))

    # 5. avantage_net incohérent → anomalie.
    def test_5_net_incoherent(self):
        _make_saisie(self.saisie, [])
        _make_lot7(self.lot7)
        df_ik = pd.DataFrame([_calc_row(associe="PERS_EWAN", bruts=100, ch=40, nets=999)])
        self.assertIn("AVANTAGE_NET_INCOHERENT", self._codes(df_ik))

    # 6. IK saisie à tort dans SAISIE_Charges_Flux → anomalie.
    def test_6_ik_dans_charges(self):
        _make_saisie(self.saisie, [
            {"charge_id": "C1", "date_charge": "2026-06-10", "montant": 75,
             "type_flux_id": "TYPE_FLUX_015", "associe_id": "PERS_EWAN",
             "mode_paiement_id": "PAY_001", "avantage_associe_id": ""}])
        _make_lot7(self.lot7)
        df_ik = pd.DataFrame([_calc_row(associe="PERS_EWAN", bruts=100, dp=100)])
        self.assertIn("IK_DANS_CHARGES_LOT3", self._codes(df_ik))

    # 7. Calc vide (état réel : suivi non régénéré) → cross-contrôles différés (INFO), pas de faux positif.
    def test_7_calc_vide_cross_differes(self):
        _make_saisie(self.saisie, [
            {"charge_id": "C1", "date_charge": "2026-06-10", "montant": 40,
             "type_flux_id": "TYPE_FLUX_004", "associe_id": "PERS_WAFA",
             "mode_paiement_id": "PAY_003", "avantage_associe_id": ""}])
        _make_lot7(self.lot7)
        codes = self._codes(pd.DataFrame())
        self.assertEqual(codes, ["SUIVI_ASSOCIE_NON_GENERE"])   # aucun CHARGE_PAYEE_NON_REPRISE à tort


if __name__ == "__main__":
    unittest.main()

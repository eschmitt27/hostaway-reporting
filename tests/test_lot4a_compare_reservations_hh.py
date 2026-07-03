"""Tests LOT4A comparateur lecture seule (SAISIE ReservationsHH vs MASTER)."""
import ast
import datetime as dt
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

import lot4a_compare_reservations_hh as cmp  # noqa: E402


TAUX_PROP_0003 = [{
    "taux_commission_id": "TX_2025_01_PROP_0003", "proprietaire_id": "PROP_0003",
    "logement_id": "", "taux_commission": "0.15",
    "date_debut": "2025-01-01", "date_fin": "", "actif": "OUI",
}]


def _saisie_oracle():
    return {
        "reservation_hh_id": "RESHH-2026-05-001", "canal_id": "CANAL_004",
        "source_financiere": "DIRECT_HA_PAYANT", "proprietaire_id": "PROP_0003",
        "logement_id": "LOG_0009", "reservation_id_hostaway": 60559486,
        "date_arrivee": dt.datetime(2026, 5, 25), "date_depart": dt.datetime(2026, 7, 19),
        "total_percu": 2343.48, "menage": 55, "montant_reverse_proprietaire": None,
        "code_impact": "HC", "comptabilisation": "NON", "statut_controle": "VALIDE",
        "niveau_anomalie": "INFO",
    }


class RoundingPrimitiveTests(unittest.TestCase):
    CASES = [2.125, 2.675, 13.275, 15.015, 144.015, 343.275]

    def test_round2_identique_pandas_float64(self):
        for x in self.CASES:
            expected = float(pd.Series([x], dtype="float64").round(2).iloc[0])
            self.assertEqual(cmp.round2(x), expected, f"divergence pandas sur {x}")

    def test_round2_identique_numpy_float64(self):
        for x in self.CASES:
            expected = float(np.round(np.float64(x), 2))
            self.assertEqual(cmp.round2(x), expected, f"divergence numpy sur {x}")

    def test_round2_differe_du_builtin_sur_cas_xx5(self):
        # preuve que round() builtin n'est PAS utilise
        self.assertNotEqual(cmp.round2(2.675), round(2.675, 2))    # 2.68 != 2.67
        self.assertNotEqual(cmp.round2(343.275), round(343.275, 2))  # 343.28 != 343.27

    def test_round2_vide(self):
        self.assertIsNone(cmp.round2(None))
        self.assertIsNone(cmp.round2(""))


class RecomputeTests(unittest.TestCase):
    def test_oracle_reshh_2026_05_001(self):
        rc = cmp.recompute(_saisie_oracle(), TAUX_PROP_0003)
        d = rc["derived"]
        self.assertEqual(rc["taux_status"], "OK")
        self.assertEqual(d["taux_commission"], 0.15)
        self.assertEqual(d["taux_commission_source"], "REF_PROPRIETAIRE")
        self.assertEqual(d["commission"], 343.27)
        self.assertEqual(d["acompte_facture"], 2343.48)
        self.assertEqual(d["mois"], "2026-05")
        self.assertEqual(d["nuits"], 55)
        self.assertEqual(d["impact_resultat_reel"], "OUI")
        self.assertEqual(d["impact_resultat_comptable"], "NON")
        self.assertEqual(d["ROW_HASH"], "RESHH-2026-05-001|CANAL_004|PROP_0003|LOG_0009|20260525|2343.48")
        self.assertEqual(d["source_module"], "LOT4_HH")
        self.assertEqual(d["source_pk"], "RESHH-2026-05-001")

    def test_equilibre_comptable(self):
        d = cmp.recompute(_saisie_oracle(), TAUX_PROP_0003)["derived"]
        self.assertEqual(d["source_acompte_facture"], "TOTAL_PERCU")

    def test_taux_logement_specifique_donne_ref_logement(self):
        taux = TAUX_PROP_0003 + [{
            "taux_commission_id": "TX_LOG", "proprietaire_id": "PROP_0003",
            "logement_id": "LOG_0009", "taux_commission": "0.12",
            "date_debut": "2025-01-01", "date_fin": "", "actif": "OUI",
        }]
        d = cmp.recompute(_saisie_oracle(), taux)["derived"]
        self.assertEqual(d["taux_commission_source"], "REF_LOGEMENT")
        self.assertEqual(d["taux_commission"], 0.12)

    def test_taux_missing(self):
        s = _saisie_oracle(); s["proprietaire_id"] = "PROP_9999"
        rc = cmp.recompute(s, TAUX_PROP_0003)
        self.assertEqual(rc["taux_status"], "MISSING")
        self.assertIsNone(rc["derived"]["commission"])
        self.assertIsNone(rc["derived"]["acompte_facture"])

    def test_taux_ambiguous(self):
        taux = TAUX_PROP_0003 + [{
            "taux_commission_id": "TX_DUP", "proprietaire_id": "PROP_0003",
            "logement_id": "", "taux_commission": "0.18",
            "date_debut": "2025-01-01", "date_fin": "", "actif": "OUI",
        }]
        rc = cmp.recompute(_saisie_oracle(), taux)
        self.assertEqual(rc["taux_status"], "AMBIGUOUS")


class CategorisationTests(unittest.TestCase):
    def _run(self, master):
        return cmp.build_rows([_saisie_oracle()], {"RESHH-2026-05-001": master}, TAUX_PROP_0003, {})

    def test_commission_null_master_ecart_historique(self):
        master = {"reservation_hh_id": "RESHH-2026-05-001", "commission": None, "date_integration": None}
        lignes, counters, statut = self._run(master)
        comm = [l for l in lignes if l["champ"] == "commission"][0]
        self.assertEqual(comm["categorie_principale"], "ECART_HISTORIQUE_ATTENDU")
        self.assertEqual(statut, "ANALYSE_TERMINEE_AVEC_ECARTS_HISTORIQUES")

    def test_date_integration_non_comparable(self):
        lignes, _, _ = self._run({"reservation_hh_id": "RESHH-2026-05-001"})
        di = [l for l in lignes if l["champ"] == "date_integration"][0]
        self.assertEqual(di["categorie_principale"], "METADONNEE_NON_COMPARABLE")

    def test_derive_coherent_quand_master_correct(self):
        master = {"reservation_hh_id": "RESHH-2026-05-001", "mois": "2026-05",
                  "commission": 343.27, "acompte_facture": 2343.48, "taux_commission": 0.15,
                  "taux_commission_source": "REF_PROPRIETAIRE", "nuits": 55,
                  "impact_resultat_reel": "OUI", "impact_resultat_comptable": "NON",
                  "ROW_HASH": "RESHH-2026-05-001|CANAL_004|PROP_0003|LOG_0009|20260525|2343.48",
                  "source_module": "LOT4_HH", "source_table": "SAISIE_ReservationsHorsHostaway",
                  "source_pk": "RESHH-2026-05-001"}
        lignes, _, _ = self._run(master)
        comm = [l for l in lignes if l["champ"] == "commission"][0]
        self.assertEqual(comm["categorie_principale"], "DERIVE_COHERENT")

    def test_acompte_positif_indicateur(self):
        lignes, _, _ = self._run({"reservation_hh_id": "RESHH-2026-05-001"})
        ac = [l for l in lignes if l["champ"] == "acompte_facture"][0]
        self.assertEqual(ac["indicateur_impact_aval"], "ACOMPTE_POSITIF_A_VERIFIER")

    def test_manuel_different(self):
        master = {"reservation_hh_id": "RESHH-2026-05-001", "total_percu": 9999.99}
        lignes, _, _ = self._run(master)
        tp = [l for l in lignes if l["champ"] == "total_percu"][0]
        self.assertEqual(tp["categorie_principale"], "MANUEL_DIFFERENT")

    def test_taux_bloquant_ligne_dediee(self):
        s = _saisie_oracle(); s["proprietaire_id"] = "PROP_9999"
        lignes, counters, statut = cmp.build_rows([s], {}, TAUX_PROP_0003, {})
        ctrl = [l for l in lignes if l["champ"] == "__TAUX_HISTORIQUE__"]
        self.assertEqual(len(ctrl), 1)
        self.assertEqual(ctrl[0]["categorie_principale"], "TAUX_BLOQUANT")
        self.assertEqual(statut, "ANALYSE_BLOQUEE_TAUX")


class PathGuardTests(unittest.TestCase):
    def test_refuse_hors_logs(self):
        for bad in ("01_SOURCES_BRUTES/x.xlsx", "02_TRAVAIL/x.xlsx",
                    "03_EXPORTS/x.csv", "05_APPLICATION/x.txt"):
            with self.assertRaises(RuntimeError):
                cmp.assert_output_allowed(cmp.ROOT / bad)

    def test_accepte_sous_logs(self):
        ok = cmp.LOGS_ROOT / "20990101T000000Z" / "rapport.csv"
        self.assertEqual(cmp.assert_output_allowed(ok), ok.resolve())


class NoMetierWriteASTTests(unittest.TestCase):
    """Scan AST : interdit les primitives dangereuses en CODE reel (docstrings/commentaires ignores)."""

    def setUp(self):
        self.tree = ast.parse(
            (ROOT / "02_TRAVAIL" / "lot4a_compare_reservations_hh.py").read_text(encoding="utf-8")
        )

    def test_pas_dappel_save_ou_to_excel(self):
        bad = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Attribute) and node.attr in ("save", "to_excel"):
                bad.append(node.attr)
        self.assertEqual(bad, [], f"appels interdits: {bad}")

    def test_pas_dimport_interdit(self):
        mods = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                mods += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                mods.append(node.module or "")
        for forbidden in ("subprocess", "win32com", "saisie_writer"):
            self.assertFalse(
                any(forbidden in m for m in mods),
                f"import interdit: {forbidden} (imports={mods})",
            )

    def test_pas_de_com_dispatch(self):
        names = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Name):
                names.append(node.id)
            elif isinstance(node, ast.Attribute):
                names.append(node.attr)
        for forbidden in ("Dispatch", "DispatchEx"):
            self.assertNotIn(forbidden, names, f"COM interdit: {forbidden}")

    def test_garde_de_chemin_existe(self):
        self.assertTrue(hasattr(cmp, "assert_output_allowed"))


@unittest.skipUnless(
    cmp.SAISIE_PATH.exists() and cmp.MASTER_PATH.exists() and cmp.REF_SETUP_PATH.exists(),
    "fichiers reels absents",
)
class RealRunReadOnlyTests(unittest.TestCase):
    def test_run_ne_modifie_pas_les_sources(self):
        fp_avant = {n: cmp.file_fingerprint(p) for n, p in {
            "s": cmp.SAISIE_PATH, "m": cmp.MASTER_PATH, "r": cmp.REF_SETUP_PATH}.items()}
        code = cmp.run_compare_existing(as_of="2026-07-02T00:00:00Z")
        fp_apres = {n: cmp.file_fingerprint(p) for n, p in {
            "s": cmp.SAISIE_PATH, "m": cmp.MASTER_PATH, "r": cmp.REF_SETUP_PATH}.items()}
        self.assertEqual(code, 0)
        self.assertEqual(fp_avant, fp_apres, "une source metier a change")


if __name__ == "__main__":
    unittest.main()

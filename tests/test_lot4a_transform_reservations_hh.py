"""Tests LOT4A transformateur --dry-run (MASTER de test, sources reelles en lecture seule)."""
import ast
import datetime as dt
import sys
import unittest
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

import lib_lot4a_reservations_hh as lib  # noqa: E402
import lot4a_transform_reservations_hh as tr  # noqa: E402


TAUX_PROP_0003 = [{
    "taux_commission_id": "TX_2025_01_PROP_0003", "proprietaire_id": "PROP_0003",
    "logement_id": "", "taux_commission": "0.15",
    "date_debut": "2025-01-01", "date_fin": "", "actif": "OUI",
}]


def _saisie(**over):
    base = {
        "reservation_hh_id": "RESHH-2026-05-001", "canal_id": "CANAL_004",
        "source_financiere": "DIRECT_HA_PAYANT", "proprietaire_id": "PROP_0003",
        "logement_id": "LOG_0009", "reservation_id_hostaway": 60559486,
        "date_arrivee": dt.datetime(2026, 5, 25), "date_depart": dt.datetime(2026, 7, 19),
        "total_percu": 2343.48, "menage": 55, "montant_reverse_proprietaire": None,
        "code_impact": "HC", "comptabilisation": "NON", "statut_controle": "VALIDE",
        "niveau_anomalie": "INFO",
    }
    base.update(over)
    return base


class AsOfTests(unittest.TestCase):
    def test_absent_refuse(self):
        with self.assertRaises(tr.AsOfError):
            tr.parse_as_of(None)
        with self.assertRaises(tr.AsOfError):
            tr.parse_as_of("")

    def test_naive_refuse(self):
        with self.assertRaises(tr.AsOfError):
            tr.parse_as_of("2026-07-02T00:00:00")

    def test_non_utc_refuse(self):
        with self.assertRaises(tr.AsOfError):
            tr.parse_as_of("2026-07-02T02:00:00+02:00")

    def test_ambigu_refuse(self):
        with self.assertRaises(tr.AsOfError):
            tr.parse_as_of("02/07/2026 minuit")

    def test_utc_canonique(self):
        self.assertEqual(tr.parse_as_of("2026-07-02T00:00:00Z"), "2026-07-02T00:00:00Z")
        # offset +00:00 == UTC -> canonique Z
        self.assertEqual(tr.parse_as_of("2026-07-02T00:00:00+00:00"), "2026-07-02T00:00:00Z")


class BuildMasterTests(unittest.TestCase):
    AS_OF = "2026-07-02T00:00:00Z"

    def test_oracle_et_equilibre(self):
        res = lib.build_master([_saisie()], TAUX_PROP_0003, self.AS_OF)
        self.assertEqual(res["statut"], "ANALYSE_TERMINEE")
        rec = res["master_rows"][0]
        self.assertEqual(rec["taux_commission"], 0.15)
        self.assertEqual(rec["commission"], 343.27)
        self.assertEqual(rec["acompte_facture"], 2343.48)
        self.assertEqual(rec["date_integration"], self.AS_OF)
        self.assertEqual(rec["date_arrivee"], "2026-05-25")
        self.assertEqual(rec["date_depart"], "2026-07-19")
        # equilibre strict au centime
        self.assertEqual(rec["source_acompte_facture"], "TOTAL_PERCU")

    def test_vue_active_filtre_valide(self):
        rows = [_saisie(), _saisie(reservation_hh_id="RESHH-2026-05-002", statut_controle="A_CONTROLER")]
        res = lib.build_master(rows, TAUX_PROP_0003, self.AS_OF)
        self.assertEqual(len(res["master_rows"]), 2)
        self.assertEqual(len(res["vue_active_rows"]), 1)
        self.assertEqual(res["vue_active_rows"][0]["reservation_hh_id"], "RESHH-2026-05-001")

    def test_bloque_taux_missing(self):
        res = lib.build_master([_saisie(proprietaire_id="PROP_9999")], TAUX_PROP_0003, self.AS_OF)
        self.assertEqual(res["statut"], "ANALYSE_BLOQUEE_TAUX")
        self.assertEqual(res["master_rows"], [])

    def test_bloque_donnees_invalides(self):
        # depart <= arrivee
        res = lib.build_master([_saisie(date_depart=dt.datetime(2026, 5, 25))], TAUX_PROP_0003, self.AS_OF)
        self.assertEqual(res["statut"], "ANALYSE_BLOQUEE_DONNEES")
        self.assertEqual(res["master_rows"], [])

    def test_bloque_pk_duplique(self):
        res = lib.build_master([_saisie(), _saisie()], TAUX_PROP_0003, self.AS_OF)
        self.assertEqual(res["statut"], "ANALYSE_BLOQUEE_DONNEES")

    def test_acompte_par_mode_paiement(self):
        cases = [
            ("PAY_001", {}, 2343.48, "TOTAL_PERCU"),
            ("PAY_004", {"montant_recupere": 120.0}, 2343.48, "TOTAL_PERCU_ASSOCIE"),
            ("PAY_003", {"montant_recupere": 130.0}, 2343.48, "TOTAL_PERCU_ASSOCIE"),
            ("PAY_002", {"montant_reverse_proprietaire": 140.0}, 2203.48, "TOTAL_PERCU_MOINS_REVERSE_ESPECES"),
            ("PAY_006", {"montant_recupere": 120.0, "montant_reverse_proprietaire": 140.0}, 0.0, "DIRECT_PROPRIETAIRE"),
        ]
        for mode, extra, expected, source in cases:
            with self.subTest(mode=mode):
                rec = lib.build_master([_saisie(mode_paiement_id=mode, **extra)], TAUX_PROP_0003, self.AS_OF)["master_rows"][0]
                self.assertEqual(rec["acompte_facture"], expected)
                self.assertEqual(rec["source_acompte_facture"], source)
                self.assertEqual(rec["mois"], "2026-05")

    def test_acompte_associe_reste_total_percu_si_recupere_inferieur(self):
        for mode in ("PAY_003", "PAY_004"):
            with self.subTest(mode=mode):
                rec = lib.build_master([
                    _saisie(mode_paiement_id=mode, montant_recupere=50.0)
                ], TAUX_PROP_0003, self.AS_OF)["master_rows"][0]
                self.assertEqual(rec["acompte_facture"], 2343.48)
                self.assertEqual(rec["source_acompte_facture"], "TOTAL_PERCU_ASSOCIE")

    def test_acompte_especes_total_moins_reverse(self):
        rec = lib.build_master([
            _saisie(mode_paiement_id="PAY_002", montant_reverse_proprietaire=343.48)
        ], TAUX_PROP_0003, self.AS_OF)["master_rows"][0]
        self.assertEqual(rec["acompte_facture"], 2000.0)
        self.assertEqual(rec["source_acompte_facture"], "TOTAL_PERCU_MOINS_REVERSE_ESPECES")

    def test_override_taux_et_menage_confirmes(self):
        rec = lib.build_master([
            _saisie(
                taux_commission_override=0.18,
                motif_override_taux_commission="Accord",
                confirmation_override_taux_commission="on",
                menage_override=70,
                motif_override_menage="Cas specifique",
                confirmation_override_menage="on",
            )
        ], TAUX_PROP_0003, self.AS_OF)["master_rows"][0]
        self.assertEqual(rec["taux_commission"], 0.18)
        self.assertEqual(rec["taux_commission_source"], "OVERRIDE_CONFIRME")
        self.assertEqual(rec["menage"], 70.0)
        self.assertEqual(rec["menage_override"], 70.0)

    def test_override_taux_decimal_canonique_confirme(self):
        for override, expected in [
            (0.18, 0.18),
            (0.005, 0.005),
            (0.0, 0.0),
            (1.0, 1.0),
        ]:
            with self.subTest(override=override):
                rec = lib.build_master([
                    _saisie(
                        taux_commission_override=override,
                        motif_override_taux_commission="Accord",
                        confirmation_override_taux_commission="on",
                    )
                ], TAUX_PROP_0003, self.AS_OF)["master_rows"][0]
                self.assertEqual(rec["taux_commission"], expected)
                self.assertEqual(rec["taux_commission_source"], "OVERRIDE_CONFIRME")

    def test_override_taux_decimal_hors_bornes_bloque(self):
        for override in (1.0001, -0.01):
            with self.subTest(override=override):
                res = lib.build_master([
                    _saisie(
                        taux_commission_override=override,
                        motif_override_taux_commission="Accord",
                        confirmation_override_taux_commission="on",
                    )
                ], TAUX_PROP_0003, self.AS_OF)
                self.assertEqual(res["statut"], "ANALYSE_BLOQUEE_TAUX")
                self.assertEqual(res["motif_blocage"], "TAUX_OVERRIDE_INVALID")
                self.assertEqual(res["anomalies_taux"][0]["statut"], "OVERRIDE_INVALID")

    def test_override_taux_aucune_division_implicite_par_100(self):
        source = (ROOT / "02_TRAVAIL" / "lib_lot4a_reservations_hh.py").read_text(encoding="utf-8")
        self.assertNotIn("override / 100", source)
        self.assertNotIn("if override > 1", source)


class AtomicWriteTests(unittest.TestCase):
    AS_OF = "2026-07-02T00:00:00Z"

    def setUp(self):
        self.run_dir = tr.LOGS_ROOT / "__pytest_atomic__"
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.run_dir, ignore_errors=True)

    def test_ecrit_schema_et_sans_temp(self):
        res = lib.build_master([_saisie()], TAUX_PROP_0003, self.AS_OF)
        final = tr._atomic_write_master(self.run_dir, res["master_rows"], res["vue_active_rows"])
        self.assertTrue(final.exists())
        self.assertFalse((self.run_dir / tr.TMP_FILENAME).exists(), "temp non nettoye")
        wb = openpyxl.load_workbook(str(final), read_only=True)
        try:
            self.assertEqual(wb.sheetnames, ["MASTER", "VUE_ACTIVE"])
            ws = wb["MASTER"]
            header = [c for c in next(ws.iter_rows(values_only=True))]
            self.assertEqual(header, lib.MASTER_COLUMNS)
            self.assertEqual(header[:34], lib.MASTER_COLUMNS[:34])
            self.assertEqual(len(header), len(lib.MASTER_COLUMNS))
        finally:
            wb.close()

    def test_pas_de_feuille_power_query(self):
        res = lib.build_master([_saisie()], TAUX_PROP_0003, self.AS_OF)
        final = tr._atomic_write_master(self.run_dir, res["master_rows"], res["vue_active_rows"])
        wb = openpyxl.load_workbook(str(final), read_only=True)
        try:
            self.assertNotIn("POWER_QUERY_CODE", wb.sheetnames)
        finally:
            wb.close()


class PathGuardTests(unittest.TestCase):
    def test_refuse_racine_metier(self):
        for bad in ("01_SOURCES_BRUTES/x.xlsx", "02_TRAVAIL/x.xlsx",
                    "03_EXPORTS/x.csv", "05_APPLICATION/x.txt"):
            with self.assertRaises(RuntimeError):
                lib.assert_output_under(lib.PROJECT_ROOT / bad, tr.LOGS_ROOT)

    def test_refuse_hors_dry_run(self):
        with self.assertRaises(RuntimeError):
            lib.assert_output_under(lib.PROJECT_ROOT / "04_LOGS" / "AUTRE" / "x", tr.LOGS_ROOT)

    def test_accepte_sous_dry_run(self):
        ok = tr.LOGS_ROOT / "20990101T000000Z" / "manifest.json"
        self.assertEqual(lib.assert_output_under(ok, tr.LOGS_ROOT), ok.resolve())


class NoDangerousPrimitiveASTTests(unittest.TestCase):
    def setUp(self):
        self.tree = ast.parse((ROOT / "02_TRAVAIL" / "lot4a_transform_reservations_hh.py").read_text(encoding="utf-8"))

    def test_pas_dimport_interdit(self):
        mods = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                mods += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                mods.append(node.module or "")
        for forbidden in ("subprocess", "win32com", "saisie_writer"):
            self.assertFalse(any(forbidden in m for m in mods), f"import interdit: {forbidden}")

    def test_pas_de_com_dispatch(self):
        names = [n.attr for n in ast.walk(self.tree) if isinstance(n, ast.Attribute)]
        names += [n.id for n in ast.walk(self.tree) if isinstance(n, ast.Name)]
        for forbidden in ("Dispatch", "DispatchEx"):
            self.assertNotIn(forbidden, names)

    def test_pas_de_write_master_mode(self):
        # Aucun ARGUMENT --write-master declare (les mentions en docstring/help = interdiction documentee, OK).
        add_arg_strings = []
        for node in ast.walk(self.tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "add_argument"):
                for a in node.args:
                    if isinstance(a, ast.Constant) and isinstance(a.value, str):
                        add_arg_strings.append(a.value)
        for s in add_arg_strings:
            self.assertNotIn("write", s.lower(), f"argument interdit declare: {s}")
        # aucun acces args.write_master
        attrs = [n.attr for n in ast.walk(self.tree) if isinstance(n, ast.Attribute)]
        self.assertNotIn("write_master", attrs)


class BlockedRunNoFileTests(unittest.TestCase):
    """Run bloque (taux) : aucun .xlsx (temp ou final), seulement rapport_anomalies + manifest."""

    def setUp(self):
        self._orig_saisie = tr.read_saisie_values
        self._orig_taux = tr.read_taux_rows
        tr.read_saisie_values = lambda: [_saisie(proprietaire_id="PROP_9999")]
        tr.read_taux_rows = lambda: TAUX_PROP_0003

    def tearDown(self):
        tr.read_saisie_values = self._orig_saisie
        tr.read_taux_rows = self._orig_taux

    def test_blocage_taux_aucun_xlsx(self):
        before = {n: lib.file_fingerprint(p) for n, p in {
            "s": lib.SAISIE_PATH, "m": lib.MASTER_PATH, "r": lib.REF_SETUP_PATH}.items()}
        code = tr.run_dry_run("2026-07-02T00:00:00Z")
        self.assertEqual(code, 0)
        # dernier dossier de run
        runs = sorted(tr.LOGS_ROOT.glob("*Z"))
        run_dir = runs[-1]
        xlsx = list(run_dir.glob("*.xlsx"))
        self.assertEqual(xlsx, [], "aucun .xlsx ne doit exister sur blocage taux")
        self.assertTrue((run_dir / "rapport_anomalies.md").exists())
        self.assertTrue((run_dir / "manifest.json").exists())
        after = {n: lib.file_fingerprint(p) for n, p in {
            "s": lib.SAISIE_PATH, "m": lib.MASTER_PATH, "r": lib.REF_SETUP_PATH}.items()}
        self.assertEqual(before, after)


@unittest.skipUnless(
    lib.SAISIE_PATH.exists() and lib.REF_SETUP_PATH.exists(),
    "fichiers reels absents",
)
class RealRunTests(unittest.TestCase):
    def test_run_reel_ne_modifie_pas_sources(self):
        before = {n: lib.file_fingerprint(p) for n, p in {
            "s": lib.SAISIE_PATH, "m": lib.MASTER_PATH, "r": lib.REF_SETUP_PATH}.items()}
        code = tr.run_dry_run("2026-07-02T00:00:00Z")
        after = {n: lib.file_fingerprint(p) for n, p in {
            "s": lib.SAISIE_PATH, "m": lib.MASTER_PATH, "r": lib.REF_SETUP_PATH}.items()}
        self.assertEqual(code, 0)
        self.assertEqual(before, after, "une source reelle a change")


if __name__ == "__main__":
    unittest.main()

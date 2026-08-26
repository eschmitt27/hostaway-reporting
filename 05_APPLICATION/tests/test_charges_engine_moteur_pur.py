"""Mission 8 — moteur Charges pur (`app/moteurs/charges_engine.py`), extrait de
`app/services/charges_impact_service.py` (désormais un simple ré-export, voir
`MOTEUR_CHARGES.md`).

Contrairement au moteur Commission (Mission 7), aucune formule n'a changé de comportement ici :
`compute_perimetre_logements`/`repartir_egal` étaient DÉJÀ purs et DÉJÀ la seule source de calcul
(un unique appelant en production, `charges_preview_service.compute_guidee`). Ce test prouve :
(1) la pureté du nouveau module (aucune dépendance sqlite3/FastAPI/pandas/fichier) ;
(2) la préuve A/B : la chaîne de production (`compute_guidee`, via le ré-export
`charges_impact_service`) produit exactement ce que donnerait un appel direct au moteur pur avec
les mêmes périmètre/montant — 0 diff, 0 duplication ;
(3) la temporalité V1/V2 de `REGLE_REPARTITION_CHARGE_COMMUNE` (une V2 de fixture 2027 ne change
jamais un calcul 2026, même rejoué après coup)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.moteurs import charges_engine as moteur  # noqa: E402
from app.services import charges_preview_service as preview  # noqa: E402


class MoteurPurTests(unittest.TestCase):
    def test_module_sans_import_sqlite_fastapi_pandas(self):
        source = Path(moteur.__file__).read_text(encoding="utf-8")
        lignes = [l.strip() for l in source.splitlines()]
        imports = [l for l in lignes if l.startswith("import ") or l.startswith("from ")]
        for terme in ("sqlite3", "fastapi", "pandas"):
            self.assertFalse(any(terme in l for l in imports), f"{terme} importe : {imports}")

    def test_module_pas_de_lecture_ecriture_fichier(self):
        source = Path(moteur.__file__).read_text(encoding="utf-8")
        for terme in ("open(", "Path(", "os.environ"):
            self.assertNotIn(terme, source)


GESTION = [
    {"logement_id": "LOG_A", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_B", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_C", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
    {"logement_id": "LOG_D", "proprietaire_id": "PROP_1", "statut_gestion": "ACTIF",
     "date_debut": "2026-01-01", "date_fin": None},
]


class PariteProductionMoteurTests(unittest.TestCase):
    """§30/§31 : `charges_preview_service.compute_guidee` (chaîne de production réelle, via le
    ré-export `charges_impact_service`) contre un appel direct au moteur pur, sur un jeu de
    scénarios représentatif : affectation directe (n=1), charge commune (n=3), deux factures
    sans contamination, résidu de centime, recalcul historique déterministe."""

    def _form(self, montant, logements, proprietaires=None, refacturable="OUI"):
        return {
            "categorie_charge_id": "CHG_017", "code_impact": "IC", "montant": str(montant),
            "impact_menage": "NON", "logements": logements,
            "proprietaires": proprietaires or [], "refacturable": refacturable,
        }

    def test_affectation_directe_un_logement_100_pourcent(self):
        refs = {"gestion_logements": GESTION, "logements": [{"logement_id": l} for l in
                ("LOG_A", "LOG_B", "LOG_C", "LOG_D")], "associes": [], "regles_versions": []}
        resultat = preview.compute_guidee(self._form(42.0, ["LOG_B"]), refs, "2026-06")
        quotes_production = resultat["reserve"]["entrees"]
        quotes_moteur = moteur.repartir_egal(42.0, ["LOG_B"])
        self.assertEqual(len(quotes_production), 1)
        self.assertEqual(quotes_production[0]["logement_id"], "LOG_B")
        self.assertEqual(quotes_production[0]["montant_refacturable"], quotes_moteur[0]["quote_part"])
        self.assertEqual(quotes_production[0]["montant_refacturable"], 42.0)

    def test_charge_commune_trois_logements_identique_au_moteur(self):
        refs = {"gestion_logements": GESTION, "logements": [{"logement_id": l} for l in
                ("LOG_A", "LOG_B", "LOG_C", "LOG_D")], "associes": [], "regles_versions": []}
        resultat = preview.compute_guidee(
            self._form(90.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        production = {e["logement_id"]: e["montant_refacturable"] for e in resultat["reserve"]["entrees"]}
        perimetre = moteur.compute_perimetre_logements(
            ["LOG_A", "LOG_B", "LOG_C"], [], "2026-06", GESTION)
        attendu = {q["logement_id"]: q["quote_part"]
                  for q in moteur.repartir_egal(90.0, perimetre["logements_finaux"])}
        self.assertEqual(production, attendu)
        self.assertEqual(production, {"LOG_A": 30.0, "LOG_B": 30.0, "LOG_C": 30.0})

    def test_deux_factures_jamais_de_contamination(self):
        refs = {"gestion_logements": GESTION, "logements": [{"logement_id": l} for l in
                ("LOG_A", "LOG_B", "LOG_C", "LOG_D")], "associes": [], "regles_versions": []}
        r_f1 = preview.compute_guidee(
            self._form(90.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        r_f2 = preview.compute_guidee(self._form(50.0, ["LOG_A", "LOG_D"]), refs, "2026-06")
        logs_f2 = {e["logement_id"] for e in r_f2["reserve"]["entrees"]}
        self.assertEqual(logs_f2, {"LOG_A", "LOG_D"})
        self.assertNotIn("LOG_B", logs_f2)
        self.assertNotIn("LOG_C", logs_f2)
        logs_f1 = {e["logement_id"] for e in r_f1["reserve"]["entrees"]}
        self.assertEqual(logs_f1, {"LOG_A", "LOG_B", "LOG_C"})

    def test_residu_centime_identique_au_moteur(self):
        refs = {"gestion_logements": GESTION, "logements": [{"logement_id": l} for l in
                ("LOG_A", "LOG_B", "LOG_C", "LOG_D")], "associes": [], "regles_versions": []}
        resultat = preview.compute_guidee(
            self._form(10.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        production = {e["logement_id"]: e["montant_refacturable"] for e in resultat["reserve"]["entrees"]}
        attendu = {q["logement_id"]: q["quote_part"]
                  for q in moteur.repartir_egal(10.0, ["LOG_A", "LOG_B", "LOG_C"])}
        self.assertEqual(production, attendu)
        self.assertEqual(sum(production.values()), 10.0)

    def test_recalcul_historique_deterministe(self):
        refs = {"gestion_logements": GESTION, "logements": [{"logement_id": l} for l in
                ("LOG_A", "LOG_B", "LOG_C", "LOG_D")], "associes": [], "regles_versions": []}
        premier = preview.compute_guidee(
            self._form(100.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        second = preview.compute_guidee(
            self._form(100.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        self.assertEqual(premier["reserve"]["entrees"], second["reserve"]["entrees"])


class TemporaliteReglesRepartitionTests(unittest.TestCase):
    """§25/§26 : V1 (2026, backfill migration 0059) reste inchangée après création d'une V2 de
    fixture applicable à partir de 2027, même recalculée après coup — la répartition reste
    REPARTIR_EGAL, la seule implémentation V1 enregistrée."""

    def _refs(self, regles_history):
        return {"gestion_logements": GESTION, "logements": [{"logement_id": l} for l in
                ("LOG_A", "LOG_B", "LOG_C", "LOG_D")], "associes": [],
                "regles_versions": regles_history}

    def _form(self, montant, logements):
        return {
            "categorie_charge_id": "CHG_017", "code_impact": "IC", "montant": str(montant),
            "impact_menage": "NON", "logements": logements, "proprietaires": [],
            "refacturable": "OUI",
        }

    def test_v1_seule_couvre_2026_meme_apres_v2_fixture_2027(self):
        regles_history = [
            {"regle_version_id": "R1", "rule_code": "REGLE_REPARTITION_CHARGE_COMMUNE",
             "version": "V1", "date_debut": "", "date_fin": "2026-12-31", "actif": "OUI"},
            {"regle_version_id": "R2", "rule_code": "REGLE_REPARTITION_CHARGE_COMMUNE",
             "version": "V2_TEST", "date_debut": "2027-01-01", "date_fin": "", "actif": "OUI"},
        ]
        refs = self._refs(regles_history)

        avant = preview.compute_guidee(self._form(90.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        self.assertEqual(avant["errors"], [])
        montants_avant = {e["logement_id"]: e["montant_refacturable"] for e in avant["reserve"]["entrees"]}
        self.assertEqual(montants_avant, {"LOG_A": 30.0, "LOG_B": 30.0, "LOG_C": 30.0})

        # 2027 : V2_TEST résolue mais non implémentée (aucune vraie V2 n'existe côté code) ->
        # fail-closed (V27), jamais un repli silencieux vers repartir_egal.
        futur = preview.compute_guidee(self._form(90.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2027-03")
        codes = {e["code"] for e in futur["errors"]}
        self.assertIn("V27_REGLE_REPARTITION_INDISPONIBLE", codes)
        self.assertEqual(futur["reserve"]["entrees"], [])
        self.assertEqual(futur["reserve"]["montant_total_refacturable"], 0.0)

        # Rejoue 2026 après l'introduction de la V2 de fixture : toujours V1, toujours identique.
        apres = preview.compute_guidee(self._form(90.0, ["LOG_A", "LOG_B", "LOG_C"]), refs, "2026-06")
        montants_apres = {e["logement_id"]: e["montant_refacturable"] for e in apres["reserve"]["entrees"]}
        self.assertEqual(montants_apres, montants_avant)


if __name__ == "__main__":
    unittest.main()

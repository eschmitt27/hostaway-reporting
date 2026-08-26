"""Mission 9 — audit du moteur Ménages internes (`lib_menage_costs.py`).

Contrairement à Commission (Mission 7) et Charges (Mission 8), l'audit montre qu'il n'y a RIEN à
extraire ni à relocaliser : `lib_menage_costs.py` (02_TRAVAIL) est DÉJÀ un moteur pur (aucun import
sqlite3/FastAPI/pandas, ni lui-même ni sa seule dépendance `lib_ref_history.py`), et DÉJÀ la seule
source de calcul, partagée correctement entre les deux mondes du projet :

- `02_TRAVAIL/lot6f_cout_complet_menages.py` (monde pandas, vue analytique gain/perte) appelle
  `resolve_internal_cleaning_cost` (dispatcheur heures/fixe par pivot de date) ;
- `05_APPLICATION/app/services/intervenant_menage_compte_service.py` (monde FastAPI, dette réelle
  intervenant) appelle `resolve_fixed_internal_cost` directement, via un `sys.path.insert` déjà
  établi pointant vers `02_TRAVAIL` (documenté dans ce fichier lui-même).

Relocaliser ce module vers `app/moteurs/` (comme pour Charges, Mission 8) inverserait la direction
de dépendance que le projet évite explicitement (`lib_db_moteur.py` : ne jamais faire dépendre
`02_TRAVAIL` de `05_APPLICATION`) — `lot6f` (monde pandas) a besoin de continuer à l'importer
directement. Décision documentée : AUCUNE relocalisation, `lib_menage_costs.py` reste à son
emplacement actuel, qui est déjà le bon.

Ce fichier complète la caractérisation déjà large de `tests/test_menage_costs.py` (pivot heures/
fixe, hiérarchie de priorité, ambiguïté, fail-closed) avec les scénarios temporels explicitement
demandés par la mission (§22-24) : changement futur de tarif sans effet rétroactif, correction
volontaire d'un tarif historique.
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_menage_costs import resolve_fixed_internal_cost


class MoteurPurTests(unittest.TestCase):
    def test_lib_menage_costs_sans_import_sqlite_fastapi_pandas(self):
        import lib_menage_costs as mod
        lignes = [l.strip() for l in Path(mod.__file__).read_text(encoding="utf-8").splitlines()]
        imports = [l for l in lignes if l.startswith("import ") or l.startswith("from ")]
        for terme in ("sqlite3", "fastapi", "pandas"):
            self.assertFalse(any(terme in l for l in imports), f"{terme} importe : {imports}")

    def test_lib_ref_history_dependance_egalement_sans_import_lourd(self):
        import lib_ref_history as mod
        lignes = [l.strip() for l in Path(mod.__file__).read_text(encoding="utf-8").splitlines()]
        imports = [l for l in lignes if l.startswith("import ") or l.startswith("from ")]
        for terme in ("sqlite3", "fastapi", "pandas"):
            self.assertFalse(any(terme in l for l in imports), f"{terme} importe : {imports}")


TARIF_2026 = {
    "cout_interne_id": "CIM_2026", "intervenant_id": "", "logement_id": "", "type_logement_id": "",
    "cout_fixe_par_menage": "30", "date_debut": "2026-01-01", "date_fin": "2026-12-31",
    "actif": "OUI",
}
TARIF_2027 = {
    "cout_interne_id": "CIM_2027", "intervenant_id": "", "logement_id": "", "type_logement_id": "",
    "cout_fixe_par_menage": "35", "date_debut": "2027-01-01", "date_fin": "",
    "actif": "OUI",
}


class TemporaliteTarifInterneTests(unittest.TestCase):
    """§22/§23 : un tarif futur (2027) ne modifie jamais un calcul 2026, même recalculé après
    l'introduction de ce tarif futur."""

    def test_2026_reste_30_apres_creation_du_tarif_2027(self):
        avant = resolve_fixed_internal_cost(
            [TARIF_2026], intervenant_id="INT_1", logement_id="LOG_1", type_logement_id="",
            ref_date="2026-06-15", nb_menages=1)
        self.assertEqual(avant.status, "OK")
        self.assertEqual(avant.total, 30.0)

        # Le tarif 2027 est désormais introduit dans le référentiel (rejeu avec les deux lignes).
        apres_2026 = resolve_fixed_internal_cost(
            [TARIF_2026, TARIF_2027], intervenant_id="INT_1", logement_id="LOG_1",
            type_logement_id="", ref_date="2026-06-15", nb_menages=1)
        self.assertEqual(apres_2026.total, 30.0)
        self.assertEqual(apres_2026.total, avant.total)

    def test_2027_utilise_le_nouveau_tarif(self):
        res = resolve_fixed_internal_cost(
            [TARIF_2026, TARIF_2027], intervenant_id="INT_1", logement_id="LOG_1",
            type_logement_id="", ref_date="2027-03-01", nb_menages=1)
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.total, 35.0)


class CorrectionRetroactiveTarifTests(unittest.TestCase):
    """§24 : une correction VOLONTAIRE d'un tarif historique de fixture peut changer un résultat
    2026 déjà calculé — comportement normal (le passé peut être corrigé exprès), pas un bug."""

    def test_correction_volontaire_du_tarif_2026_change_le_resultat(self):
        avant = resolve_fixed_internal_cost(
            [TARIF_2026], intervenant_id="INT_1", logement_id="LOG_1", type_logement_id="",
            ref_date="2026-06-15", nb_menages=1)
        self.assertEqual(avant.total, 30.0)

        tarif_corrige = {**TARIF_2026, "cout_fixe_par_menage": "32"}
        apres_correction = resolve_fixed_internal_cost(
            [tarif_corrige], intervenant_id="INT_1", logement_id="LOG_1", type_logement_id="",
            ref_date="2026-06-15", nb_menages=1)
        self.assertEqual(apres_correction.total, 32.0)
        self.assertNotEqual(apres_correction.total, avant.total)


if __name__ == "__main__":
    unittest.main()

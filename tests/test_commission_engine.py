"""Mission 7 — moteur commission pur (`lib_commission_engine.py`), extrait de
`lot10_calculer_resultats.py`. Fige le comportement de la formule (V1, ASSIETTE_COMMISSION)
avant/apres extraction : commission = assiette x taux, net = payout - menage - commission."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lib_commission_engine import (
    assiette_v1_paiement_direct,
    calculer_commission_conciergerie,
    calculer_net_proprietaire,
    plafonner_assiette_pour_commission,
)


class MoteurPurTests(unittest.TestCase):
    """§27 : aucune dependance lourde (pandas non importe par le module lui-meme)."""

    def test_module_sans_import_pandas_sqlite_fastapi(self):
        import lib_commission_engine as mod
        lignes = [l.strip() for l in Path(mod.__file__).read_text(encoding="utf-8").splitlines()]
        imports = [l for l in lignes if l.startswith("import ") or l.startswith("from ")]
        for terme in ("pandas", "sqlite3", "fastapi"):
            self.assertFalse(any(terme in l for l in imports), f"{terme} importe : {imports}")


class CommissionConciergerieTests(unittest.TestCase):
    def test_formule_nominale(self):
        self.assertEqual(calculer_commission_conciergerie(100.0, 0.19), 19.0)

    def test_arrondi_centime(self):
        self.assertEqual(calculer_commission_conciergerie(33.333, 0.20), 6.67)

    def test_assiette_nulle(self):
        self.assertEqual(calculer_commission_conciergerie(0.0, 0.19), 0.0)

    def test_taux_zero(self):
        self.assertEqual(calculer_commission_conciergerie(500.0, 0.0), 0.0)

    def test_fonctionne_sur_pandas_series(self):
        import pandas as pd
        assiette = pd.Series([100.0, 200.0])
        taux = pd.Series([0.19, 0.20])
        out = calculer_commission_conciergerie(assiette, taux)
        self.assertEqual(list(out), [19.0, 40.0])


class NetProprietaireTests(unittest.TestCase):
    def test_formule_nominale(self):
        self.assertEqual(calculer_net_proprietaire(500.0, 50.0, 85.5), 364.5)

    def test_arrondi_centime(self):
        self.assertEqual(calculer_net_proprietaire(100.333, 10.111, 17.11), 73.11)

    def test_menage_nul(self):
        self.assertEqual(calculer_net_proprietaire(300.0, 0.0, 57.0), 243.0)

    def test_fonctionne_sur_pandas_series(self):
        import pandas as pd
        payout = pd.Series([500.0, 300.0])
        menage = pd.Series([50.0, 0.0])
        commission = pd.Series([85.5, 57.0])
        out = calculer_net_proprietaire(payout, menage, commission)
        self.assertEqual(list(out), [364.5, 243.0])


class AssietteV1PaiementDirectTests(unittest.TestCase):
    """Mission 7 bis : formule extraite de HH/VRBO après audit — décision économique réelle
    (assiette = payout - ménage pour un canal de paiement direct), pas une normalisation
    technique. Figé sur les valeurs exactes déjà produites avant extraction."""

    def test_formule_nominale(self):
        self.assertEqual(assiette_v1_paiement_direct(150.0, 25.0), 125.0)

    def test_arrondi_centime(self):
        self.assertEqual(assiette_v1_paiement_direct(100.336, 10.111), 90.22)

    def test_menage_nul(self):
        self.assertEqual(assiette_v1_paiement_direct(200.0, 0.0), 200.0)

    def test_fonctionne_sur_pandas_series(self):
        import pandas as pd
        payout = pd.Series([150.0, 200.0])
        menage = pd.Series([25.0, 30.0])
        out = assiette_v1_paiement_direct(payout, menage)
        self.assertEqual(list(out), [125.0, 170.0])


class PlafonnerAssiettePourCommissionTests(unittest.TestCase):
    """Mission 14f-bis : assiette negative (menage > payout) -> commission plafonnee a 0, jamais
    l'assiette BRUTE elle-meme (conservee ailleurs pour preuve/controle Lot11). Cas reel audite :
    reservation_id=65060946, payout=11.86, menage=29.00, assiette brute=-17.14."""

    def test_cas1_assiette_positive_inchangee(self):
        # CAS 1 : payout 100, menage 20 -> assiette 80 -> comportement inchange.
        assiette = assiette_v1_paiement_direct(100.0, 20.0)
        self.assertEqual(assiette, 80.0)
        self.assertEqual(plafonner_assiette_pour_commission(assiette), 80.0)

    def test_cas2_assiette_nulle_pas_de_faux_negatif(self):
        # CAS 2 : payout 20, menage 20 -> assiette 0 -> commission 0, pas de faux negatif.
        assiette = assiette_v1_paiement_direct(20.0, 20.0)
        self.assertEqual(assiette, 0.0)
        self.assertEqual(plafonner_assiette_pour_commission(assiette), 0.0)
        self.assertEqual(calculer_commission_conciergerie(
            plafonner_assiette_pour_commission(assiette), 0.20), 0.0)

    def test_cas3_reservation_reelle_65060946(self):
        # CAS 3 : payout 11.86, menage 29.00 -> assiette brute -17.14 -> assiette retenue 0 ->
        # commission 0 (l'assiette brute elle-meme n'est JAMAIS modifiee par cette fonction).
        assiette_brute = assiette_v1_paiement_direct(11.86, 29.00)
        self.assertAlmostEqual(assiette_brute, -17.14, places=2)
        assiette_retenue = plafonner_assiette_pour_commission(assiette_brute)
        self.assertEqual(assiette_retenue, 0.0)
        self.assertEqual(calculer_commission_conciergerie(assiette_retenue, 0.20), 0.0)
        # net_proprietaire continue de refleter le vrai resultat economique negatif du sejour :
        # seule la commission de la conciergerie est plafonnee, jamais le resultat proprietaire.
        net = calculer_net_proprietaire(11.86, 29.00, 0.0)
        self.assertAlmostEqual(net, -17.14, places=2)

    def test_cas4_seule_la_ligne_negative_est_ramenee_a_zero(self):
        import pandas as pd
        # CAS 4 : plusieurs reservations, une seule negative -> seule celle-ci est ramenee a 0.
        assiette = pd.Series([80.0, -17.14, 200.0])
        out = plafonner_assiette_pour_commission(assiette)
        self.assertEqual(list(out), [80.0, 0.0, 200.0])

    def test_cas5_recalcul_identique_idempotent(self):
        # CAS 5 : recalcul identique -> meme resultat (fonction pure, aucun etat).
        assiette_brute = assiette_v1_paiement_direct(11.86, 29.00)
        r1 = plafonner_assiette_pour_commission(assiette_brute)
        r2 = plafonner_assiette_pour_commission(assiette_brute)
        self.assertEqual(r1, r2)
        self.assertEqual(plafonner_assiette_pour_commission(r1), r1)  # deja plafonnee : stable

    def test_fonctionne_sur_pandas_series(self):
        import pandas as pd
        assiette = pd.Series([80.0, -17.14, 0.0])
        out = plafonner_assiette_pour_commission(assiette)
        self.assertEqual(list(out), [80.0, 0.0, 0.0])


if __name__ == "__main__":
    unittest.main()

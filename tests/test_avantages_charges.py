"""Lot7 — agrégation avantages issus des charges (réplique testable du PQ). Preuve « une seule fois »."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

import lib_avantages as av


def _charge(cid, montant, associe=None, avantage=None, tf=None, mois="2026-06"):
    return {"charge_id": cid, "montant": montant, "associe_id": associe,
            "avantage_associe_id": avantage, "type_flux_id": tf, "mois": mois}


class AvantagesChargesTests(unittest.TestCase):
    def test_avantage_explicite_attribue_une_fois(self):
        charges = [_charge("C1", 100.0, associe=None, avantage="PERS_EWAN")]
        agg = av.aggregate_avantages(charges)
        assert agg[("PERS_EWAN", "2026-06")]["avantage_brut"] == 100.0
        assert agg[("PERS_EWAN", "2026-06")]["nb_charges"] == 1

    def test_seconde_execution_pas_de_duplication(self):
        # Même charge présente deux fois (rejeu) → comptée une seule fois (idempotence).
        charges = [_charge("C1", 100.0, avantage="PERS_EWAN"),
                   _charge("C1", 100.0, avantage="PERS_EWAN")]
        agg = av.aggregate_avantages(charges)
        assert agg[("PERS_EWAN", "2026-06")]["avantage_brut"] == 100.0
        assert agg[("PERS_EWAN", "2026-06")]["nb_charges"] == 1

    def test_avantage_distinct_du_paiement(self):
        # Charge banque pro (associe_id paiement vide) mais avantage pour EWAN → attribué à EWAN.
        charges = [_charge("C2", 50.0, associe=None, avantage="PERS_EWAN", tf="TYPE_FLUX_020")]
        agg = av.aggregate_avantages(charges)
        assert ("PERS_EWAN", "2026-06") in agg
        assert agg[("PERS_EWAN", "2026-06")]["avantage_brut"] == 50.0

    def test_type_flux_002_avantage_perso_sans_flag(self):
        # Parité PQ : TYPE_FLUX_002 (dépense perso) = avantage même sans flag explicite, par associe_id.
        charges = [_charge("C3", 30.0, associe="PERS_WAFA", tf="TYPE_FLUX_002")]
        agg = av.aggregate_avantages(charges)
        assert agg[("PERS_WAFA", "2026-06")]["avantage_brut"] == 30.0

    def test_charge_non_avantage_ignoree(self):
        # Charge société normale (TF020) sans flag avantage → pas un avantage.
        charges = [_charge("C4", 40.0, associe=None, avantage=None, tf="TYPE_FLUX_020")]
        assert av.aggregate_avantages(charges) == {}

    def test_pas_double_attribution_meme_charge(self):
        # Charge flaggée avantage ET TF002 → comptée une seule fois (pas deux attributions).
        charges = [_charge("C5", 60.0, associe="PERS_WAFA", avantage="PERS_EWAN", tf="TYPE_FLUX_002")]
        agg = av.aggregate_avantages(charges)
        total = sum(v["avantage_brut"] for v in agg.values())
        assert total == 60.0  # pas 120
        # Bénéficiaire = flag explicite (EWAN), prioritaire sur associe paiement (WAFA)
        assert ("PERS_EWAN", "2026-06") in agg

    def test_avantage_pour_helper(self):
        charges = [_charge("C6", 25.0, avantage="PERS_EWAN")]
        assert av.avantage_pour(charges, "PERS_EWAN", "2026-06") == 25.0
        assert av.avantage_pour(charges, "PERS_WAFA", "2026-06") == 0.0


if __name__ == "__main__":
    unittest.main()

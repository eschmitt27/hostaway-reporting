"""Lot12 réserve (lecture préparatoire) + Lot11 contrôles impacts charges."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

import lib_charges_reserve as lr
import lib_controles_impacts as lc


def _res(cid, log, prop, montant, mois="2026-06", statut="EN_ATTENTE", rid=None):
    return {"reserve_id": rid or f"{cid}-RES-001", "charge_id": cid, "mois": mois,
            "logement_id": log, "proprietaire_id": prop, "montant_refacturable": montant,
            "statut_traitement": statut}


class ReserveLot12Tests(unittest.TestCase):
    def test_propositions_en_attente_par_proprietaire_mois(self):
        rows = [_res("C1", "L1", "P1", 60.0, rid="C1-RES-001"),
                _res("C1", "L2", "P1", 40.0, rid="C1-RES-002"),
                _res("C2", "L3", "P2", 20.0, rid="C2-RES-001")]
        props = lr.propositions_pour_prefacture(rows, "P1", "2026-06")
        assert len(props) == 2
        assert lr.montant_propositions(props) == 100.0

    def test_pas_de_double_proposition_meme_quote_part(self):
        rows = [_res("C1", "L1", "P1", 50.0, rid="C1-RES-001"),
                _res("C1", "L1", "P1", 50.0, rid="C1-RES-001")]  # même reserve_id
        props = lr.propositions_pour_prefacture(rows, "P1", "2026-06")
        assert len(props) == 1  # dédup par reserve_id

    def test_etats_futurs_ne_cassent_pas(self):
        rows = [_res("C1", "L1", "P1", 50.0, statut="APPLIQUE"),
                _res("C2", "L2", "P1", 30.0, statut="EN_ATTENTE", rid="C2-RES-001")]
        props = lr.propositions_pour_prefacture(rows, "P1", "2026-06")
        assert len(props) == 1  # seuls EN_ATTENTE proposés ; APPLIQUE lu sans casser

    def test_prefacture_inchangee(self):
        assert lr.prefacture_inchangee(150.0, 150.0) is True

    def test_lecture_reelle_vide(self):
        src = ROOT / "01_SOURCES_BRUTES" / "Charges" / "SAISIE_Charges_Impacts.xlsx"
        assert lr.charger_reserve(src) == []


class Lot11ControlesTests(unittest.TestCase):
    def test_affectations_somme_ok_et_ko(self):
        aff = [{"charge_id": "C1", "quote_part": 60.0}, {"charge_id": "C1", "quote_part": 40.0}]
        assert lc.ctrl_affectations_somme(aff, {"C1": 100.0}) == []
        aff_ko = [{"charge_id": "C2", "quote_part": 60.0}]
        anos = lc.ctrl_affectations_somme(aff_ko, {"C2": 100.0})
        assert anos and anos[0]["code"] == "AFFECTATION_SOMME_INCOHERENTE"

    def test_reserve_somme(self):
        res = [{"charge_id": "C1", "montant_refacturable": 50.0}, {"charge_id": "C1", "montant_refacturable": 50.0}]
        assert lc.ctrl_reserve_somme(res, {"C1": 100.0}) == []

    def test_menage_hors_reserve(self):
        menage = [{"charge_id": "CM"}]
        reserve = [{"charge_id": "CM", "montant_refacturable": 10.0}]
        anos = lc.ctrl_menage_hors_reserve(menage, reserve)
        assert anos and anos[0]["code"] == "MENAGE_DANS_RESERVE"

    def test_avantage_unique(self):
        charges = [{"charge_id": "C1", "avantage_associe_id": "PERS_EWAN"},
                   {"charge_id": "C1", "avantage_associe_id": "PERS_EWAN"}]
        # Deux lignes même charge_id → doublon détecté
        anos = lc.ctrl_avantage_unique(charges)
        assert anos and anos[0]["code"] == "AVANTAGE_DUPLIQUE"

    def test_pas_de_charge_recreee(self):
        aff = [{"affectation_id": "C1-AFF-001", "charge_id": "C1"}]
        assert lc.ctrl_pas_de_charge_recreee(aff, {"C1"}) == []       # affectation_id distinct des charges
        aff_ko = [{"affectation_id": "C1", "charge_id": "C1"}]        # affectation_id = charge réelle
        anos = lc.ctrl_pas_de_charge_recreee(aff_ko, {"C1"})
        assert anos and anos[0]["code"] == "AFFECTATION_RECREEE_COMME_CHARGE"

    def test_charge_id_valide(self):
        rows = [{"charge_id": "C1"}, {"charge_id": "CX"}]
        anos = lc.ctrl_charge_id_valide(rows, {"C1"})
        assert anos and anos[0]["code"] == "IMPACT_CHARGE_ID_INCONNU"


if __name__ == "__main__":
    unittest.main()

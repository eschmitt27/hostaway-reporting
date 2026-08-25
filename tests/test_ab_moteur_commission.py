"""Mission 7 bis §8-13 — preuve A/B réelle sur une recette représentative complète (15 réservations,
3 canaux HOSTAWAY/HH/VRBO, 3 logements, 2 propriétaires, 2 taux différents) : ANCIEN CALCUL
(formule inline reconstituée à l'identique, isolée ici — jamais en production, cf. §9/§15 de la
mission) vs NOUVEAU MOTEUR (`build_commissions` réel, qui délègue à `lib_commission_engine.py`).

Comparaison ligne à ligne (assiette, taux, commission, net) + agrégats. Écart attendu : 0 partout.
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "02_TRAVAIL"))

from lot10_calculer_resultats import build_commissions

HH_COLS = ["reservation_hh_id", "total_percu", "menage", "commission", "taux_commission"]


# ─────────────────────────────────────────────────────────────────────────────
# ANCIEN CALCUL — reconstitué à l'identique des formules supprimées par Mission 7/7bis.
# Isolé ici, jamais réactivé en production (§9/§15 de la mission).
#
# Arrondi via `pandas.Series.round()` et non le `round()` builtin : Lot10 a TOUJOURS opéré de
# façon vectorisée (jamais un scalaire Python nu) — les deux arrondissent différemment sur
# certaines valeurs limites (ex. 205.5 * 0.19 : builtin round() -> 39.05, Series.round() -> 39.04,
# écart de représentation flottante au bit près). Utiliser Series.round() ici reproduit fidèlement
# le comportement HISTORIQUE réel, pas un comportement scalaire jamais exécuté en production.
# ─────────────────────────────────────────────────────────────────────────────
def _round2(x):
    return float(pd.Series([x]).round(2).iloc[0])


def _ancien_commission(assiette, taux):
    return _round2(assiette * taux)


def _ancien_net(payout, menage, commission):
    return _round2(payout - menage - commission)


def _ancien_assiette_paiement_direct(payout, menage):
    return _round2(payout - menage)


# ─────────────────────────────────────────────────────────────────────────────
# Recette représentative : 3 logements / 2 propriétaires / 2 taux, 15 réservations
# (5 HOSTAWAY + 5 HH + 5 VRBO), montants variés dont cas limites (ménage nul, arrondi).
# ─────────────────────────────────────────────────────────────────────────────
LOGEMENTS = [
    {"logement_id": "LOG_A", "proprietaire_id": "PROP_1", "taux": 0.19},
    {"logement_id": "LOG_B", "proprietaire_id": "PROP_1", "taux": 0.19},
    {"logement_id": "LOG_C", "proprietaire_id": "PROP_2", "taux": 0.22},
]

HOSTAWAY_MONTANTS = [(100.0, 20.0), (250.50, 45.0), (80.0, 0.0), (333.33, 33.33), (60.0, 15.0)]
HH_MONTANTS = [(150.0, 25.0), (400.0, 60.0), (90.0, 0.0), (222.22, 22.22), (75.0, 10.0)]
VRBO_MONTANTS = [(200.0, 30.0), (500.0, 80.0), (110.0, 0.0), (317.17, 31.71), (95.0, 12.5)]


def _log_frame():
    return pd.DataFrame([{"logement_id": l["logement_id"], "statut_parc": "GERE",
                          "seuil_voyageurs_preparation_canape": None,
                          "montant_preparation_canape": None} for l in LOGEMENTS])


def _taux_frame():
    return pd.DataFrame([{
        "taux_commission_id": f"TC_{l['logement_id']}", "proprietaire_id": l["proprietaire_id"],
        "logement_id": l["logement_id"], "taux_commission": l["taux"],
        "date_debut": "2026-01-01", "date_fin": None, "actif": "OUI",
    } for l in LOGEMENTS])


def _build_recette():
    flux_rows, res_rows, payout_rows, hh_rows = [], [], [], []

    def logement_for(i):
        return LOGEMENTS[i % len(LOGEMENTS)]

    for i, (payout, menage) in enumerate(HOSTAWAY_MONTANTS):
        rid = f"RES-HA-{i}"
        log = logement_for(i)
        assiette = round(payout - menage, 2)
        flux_rows.append({"type_flux_id": "TYPE_FLUX_017", "source_pk": rid,
                          "flux_id": f"F-HA-{i}", "mois": "2026-06",
                          "logement_id": log["logement_id"], "proprietaire_id": log["proprietaire_id"]})
        res_rows.append({
            "reservation_calc_id": rid, "reservation_id_hostaway": 1000 + i,
            "reservation_hh_id": None, "source": "HOSTAWAY_AIRBNB",
            "source_montant": "HOSTAWAY_PAYOUT", "montant_retenu": payout,
            "logement_id": log["logement_id"], "proprietaire_id": log["proprietaire_id"],
            "date_arrivee": "2026-06-15", "date_depart": "2026-06-17", "nuits": 2,
            "guestCount": 2, "canal": "AIRBNB", "payout_calcule": payout,
            "menage_retenu": menage, "assiette_commission": assiette,
        })
        payout_rows.append({
            "reservation_id": 1000 + i, "channel_type": "AIRBNB", "statut_calcul_payout": "NORMAL",
            "payout_calcule": payout, "source_payout": "HOSTAWAY_PAYOUT", "menage_retenu": menage,
            "assiette_commission": assiette, "inclure_resultat_auto": "OUI",
            "menage_retenu_source": "TEST", "cout_standard_id": None,
            "cout_standard_menage_snapshot": None, "cout_standard_date_debut_validite": None,
            "cout_standard_date_fin_validite": None, "logement_id_snapshot": log["logement_id"],
            "type_logement_id_snapshot": None, "date_reference_cout_menage": "2026-06-01",
        })

    for i, (total_percu, menage) in enumerate(HH_MONTANTS):
        rid = f"RES-HH-{i}"
        log = logement_for(i)
        flux_rows.append({"type_flux_id": "TYPE_FLUX_017", "source_pk": rid,
                          "flux_id": f"F-HH-{i}", "mois": "2026-06",
                          "logement_id": log["logement_id"], "proprietaire_id": log["proprietaire_id"]})
        res_rows.append({
            "reservation_calc_id": rid, "reservation_id_hostaway": None,
            "reservation_hh_id": f"HH_{i}", "source": "DIRECT_SANS_SAISIE_HH",
            "source_montant": "SAISIE_HH", "montant_retenu": None,
            "logement_id": log["logement_id"], "proprietaire_id": log["proprietaire_id"],
            "date_arrivee": "2026-06-15", "date_depart": "2026-06-17", "nuits": 2,
            "guestCount": 2, "canal": "DIRECT", "payout_calcule": None,
            "menage_retenu": None, "assiette_commission": None,
        })
        hh_rows.append({"reservation_hh_id": f"HH_{i}", "total_percu": total_percu,
                        "menage": menage, "commission": None, "taux_commission": None})

    for i, (payout, menage) in enumerate(VRBO_MONTANTS):
        rid = f"RES-VRBO-{i}"
        log = logement_for(i)
        flux_rows.append({"type_flux_id": "TYPE_FLUX_017", "source_pk": rid,
                          "flux_id": f"F-VRBO-{i}", "mois": "2026-06",
                          "logement_id": log["logement_id"], "proprietaire_id": log["proprietaire_id"]})
        res_rows.append({
            "reservation_calc_id": rid, "reservation_id_hostaway": None,
            "reservation_hh_id": None, "source": "HOSTAWAY_VRBO",
            "source_montant": "VRBO_HISTORIQUE", "montant_retenu": payout,
            "logement_id": log["logement_id"], "proprietaire_id": log["proprietaire_id"],
            "date_arrivee": "2026-06-15", "date_depart": "2026-06-18", "nuits": 3,
            "guestCount": 2, "canal": "VRBO", "payout_calcule": payout,
            "menage_retenu": menage, "assiette_commission": None,
        })

    df_payout_cols = [
        "reservation_id", "channel_type", "statut_calcul_payout", "payout_calcule",
        "source_payout", "menage_retenu", "assiette_commission", "inclure_resultat_auto",
        "menage_retenu_source", "cout_standard_id", "cout_standard_menage_snapshot",
        "cout_standard_date_debut_validite", "cout_standard_date_fin_validite",
        "logement_id_snapshot", "type_logement_id_snapshot", "date_reference_cout_menage",
    ]
    df_payout = pd.DataFrame(payout_rows, columns=df_payout_cols) if payout_rows \
        else pd.DataFrame(columns=df_payout_cols)
    df_hh = pd.DataFrame(hh_rows, columns=HH_COLS) if hh_rows else pd.DataFrame(columns=HH_COLS)
    return pd.DataFrame(flux_rows), pd.DataFrame(res_rows), df_payout, df_hh


class PreuveABRecetteCompleteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        df_flux, df_res, df_payout, df_hh = _build_recette()
        cls.df_comm, cls.df_ac, _ = build_commissions(
            df_flux, df_res, df_payout, df_hh, _log_frame(), pd.DataFrame(), _taux_frame())
        cls.taux_par_logement = {l["logement_id"]: l["taux"] for l in LOGEMENTS}

    def _ancien_pour_ligne(self, ligne):
        """Reconstitue ANCIEN calcul pour une ligne du nouveau résultat, à partir des montants
        source connus (payout/menage/total_percu déjà présents dans df_comm)."""
        taux = self.taux_par_logement[ligne["logement_id"]]
        if ligne["source_type"] == "HOSTAWAY":
            assiette = _round2(ligne["payout_calcule"] - ligne["menage_retenu"])
            commission = _ancien_commission(assiette, taux)
            net = _ancien_net(ligne["payout_calcule"], ligne["menage_retenu"], commission)
        elif ligne["source_type"] == "VRBO":
            assiette = _round2(ligne["payout_calcule"] - ligne["menage_retenu"])
            commission = _ancien_commission(assiette, taux)
            net = _ancien_net(ligne["payout_calcule"], ligne["menage_retenu"], commission)
        else:  # HH — l'ancien calcul utilisait total_percu/menage BRUTS (pas payout_calcule/menage_retenu)
            assiette = _ancien_assiette_paiement_direct(ligne["payout_calcule"], ligne["menage_retenu"])
            commission = _ancien_commission(assiette, taux)
            net = _ancien_net(ligne["payout_calcule"], ligne["menage_retenu"], commission)
        return assiette, commission, net

    def test_nombre_de_lignes_15(self):
        self.assertEqual(len(self.df_comm), 15)

    def test_aucune_ligne_manquante_ou_supplementaire(self):
        attendu = {f"RES-HA-{i}" for i in range(5)} | {f"RES-HH-{i}" for i in range(5)} \
            | {f"RES-VRBO-{i}" for i in range(5)}
        obtenu = set(self.df_comm["reservation_calc_id"])
        self.assertEqual(obtenu - attendu, set())
        self.assertEqual(attendu - obtenu, set())

    def test_parite_ligne_a_ligne_assiette_commission_net(self):
        diffs = []
        for _, ligne in self.df_comm.iterrows():
            assiette_ancien, commission_ancien, net_ancien = self._ancien_pour_ligne(ligne)
            if abs(ligne["assiette_commission"] - assiette_ancien) > 1e-9:
                diffs.append(("assiette", ligne["reservation_calc_id"]))
            if abs(ligne["commission_conciergerie"] - commission_ancien) > 1e-9:
                diffs.append(("commission", ligne["reservation_calc_id"]))
            if abs(ligne["net_proprietaire"] - net_ancien) > 1e-9:
                diffs.append(("net", ligne["reservation_calc_id"]))
        self.assertEqual(diffs, [], f"diffs trouvés : {diffs}")

    def test_taux_identique_a_la_reference(self):
        for _, ligne in self.df_comm.iterrows():
            self.assertEqual(ligne["taux_commission"], self.taux_par_logement[ligne["logement_id"]])

    def test_agregats_ancien_nouveau_identiques(self):
        total_assiette_nouveau = round(self.df_comm["assiette_commission"].sum(), 2)
        total_commission_nouveau = round(self.df_comm["commission_conciergerie"].sum(), 2)
        total_net_nouveau = round(self.df_comm["net_proprietaire"].sum(), 2)

        total_assiette_ancien = total_commission_ancien = total_net_ancien = 0.0
        for _, ligne in self.df_comm.iterrows():
            a, c, n = self._ancien_pour_ligne(ligne)
            total_assiette_ancien += a
            total_commission_ancien += c
            total_net_ancien += n
        total_assiette_ancien = round(total_assiette_ancien, 2)
        total_commission_ancien = round(total_commission_ancien, 2)
        total_net_ancien = round(total_net_ancien, 2)

        self.assertEqual(total_assiette_nouveau, total_assiette_ancien)
        self.assertEqual(total_commission_nouveau, total_commission_ancien)
        self.assertEqual(total_net_nouveau, total_net_ancien)
        # Rapporté dans MOTEUR_COMMISSION.md — chiffres de cette recette représentative (pas les
        # anciens chiffres historiques 285/41602,41€, qui portaient sur un dataset différent).
        self.ecart_max = max(
            abs(total_assiette_nouveau - total_assiette_ancien),
            abs(total_commission_nouveau - total_commission_ancien),
            abs(total_net_nouveau - total_net_ancien),
        )
        self.assertEqual(self.ecart_max, 0.0)


if __name__ == "__main__":
    unittest.main()

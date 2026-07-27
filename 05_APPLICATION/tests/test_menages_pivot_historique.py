"""Pivot historique du coût de ménage interne — frontières, ancrées sur la décision D101.

`D101 — Méthode interne selon période` (DECISIONS_METIER.md, VALIDÉ le 2026-06-18) :

    Pivot 2026-06.
    ≤ 2026-05 : INTERNE_HEURES_M04        = nb_heures  × taux horaire (PARAM_004)
    ≥ 2026-06 : INTERNE_STANDARD_PARAMETRE = nb_menages × forfait REF_Couts_Menage_Interne

`lib_menage_costs.PIVOT_FIXED_COST = 2026-06-01` applique exactement cette décision.

⚠️ Une consigne reçue le 2026-07-27 demandait de basculer le pivot au **1er mai 2026**, au motif
que le moteur aurait dévié. La vérification montre l'inverse : le moteur est conforme à D101, et
avancer le pivot recalculerait **mai 2026** — le mois qui porte les données réelles (factures
Aissata / Mounir, heures Imène / Kheira) — avec l'autre méthode. Le pivot n'a donc PAS été modifié
et l'arbitrage est inscrit au handoff.

Ces tests figent le comportement documenté : si le pivot est un jour déplacé, ils échouent et
forcent à trancher explicitement plutôt qu'à dériver en silence.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

import app.config as cfg

MOTEUR_DIR = Path(cfg.PROJECT_ROOT) / "02_TRAVAIL"


@pytest.fixture(scope="module")
def couts():
    """Importe le moteur de coûts. Il n'exige pas pandas — seulement `lib_ref_history`."""
    if not (MOTEUR_DIR / "lib_menage_costs.py").exists():
        pytest.skip(f"Moteur absent de cette racine : {MOTEUR_DIR}")
    ajoute = str(MOTEUR_DIR) not in sys.path
    if ajoute:
        sys.path.insert(0, str(MOTEUR_DIR))
    try:
        import lib_menage_costs
        return lib_menage_costs
    except ImportError as exc:                       # pragma: no cover - dépend de l'environnement
        pytest.skip(f"lib_menage_costs non importable : {exc}")


TAUX = [{
    "intervenant_id": "INT_TEST", "taux_horaire": 10.0,
    "date_debut_validite": "2026-01-01", "date_fin_validite": None, "actif": "OUI",
}]

FORFAITS = [{
    "cout_interne_id": "CI_TEST", "type_logement_id": "TYPE_001",
    "cout_fixe_par_menage": 30.0,
    "date_debut_validite": "2026-01-01", "date_fin_validite": None, "actif": "OUI",
}]


def _resoudre(couts, ref_date):
    return couts.resolve_internal_cleaning_cost(
        ref_date=ref_date, intervenant_id="INT_TEST", logement_id="LOG_TEST",
        type_logement_id="TYPE_001", nb_menages=2, nb_heures=3,
        hourly_rows=TAUX, fixed_rows=FORFAITS)


def test_le_pivot_est_celui_de_la_decision_d101(couts):
    assert couts.PIVOT_FIXED_COST == dt.date(2026, 6, 1)


# ── Frontières demandées ─────────────────────────────────────────────────────

def test_30_avril_2026_utilise_les_heures(couts):
    r = _resoudre(couts, "2026-04-30")
    assert r.status == "OK"
    assert r.total == pytest.approx(30.0)            # 3 h × 10 €
    assert r.rate == pytest.approx(10.0)


def test_1er_mai_2026_utilise_encore_les_heures(couts):
    """Frontière contestée : D101 place la bascule au 1er juin, pas au 1er mai."""
    r = _resoudre(couts, "2026-05-01")
    assert r.status == "OK"
    assert r.total == pytest.approx(30.0)
    assert r.rate == pytest.approx(10.0)


def test_31_mai_2026_utilise_encore_les_heures(couts):
    r = _resoudre(couts, "2026-05-31")
    assert r.status == "OK"
    assert r.total == pytest.approx(30.0)


def test_1er_juin_2026_bascule_sur_le_forfait(couts):
    r = _resoudre(couts, "2026-06-01")
    assert r.status == "OK"
    assert r.total == pytest.approx(60.0)            # 2 ménages × 30 €
    assert r.rate is None or r.rate == 0


# ── Robustesse ───────────────────────────────────────────────────────────────

def test_date_absente_refusee_sans_repli(couts):
    """Aucune date par défaut n'est inventée : une date absente est une anomalie explicite."""
    r = couts.resolve_internal_cleaning_cost(
        ref_date=None, intervenant_id="INT_TEST", logement_id="LOG_TEST",
        type_logement_id="TYPE_001", nb_menages=2, nb_heures=3,
        hourly_rows=TAUX, fixed_rows=FORFAITS)
    assert r.status == "MISSING"
    assert r.total is None


def test_heures_absentes_avant_le_pivot_ne_produisent_pas_de_zero(couts):
    """Avant le pivot, sans heures, le coût est ABSENT — jamais 0,00 € inventé."""
    r = couts.resolve_internal_cleaning_cost(
        ref_date="2026-05-15", intervenant_id="INT_TEST", logement_id="LOG_TEST",
        type_logement_id="TYPE_001", nb_menages=2, nb_heures=None,
        hourly_rows=TAUX, fixed_rows=FORFAITS)
    assert r.status == "MISSING"
    assert r.total is None


def test_taux_historique_absent_ne_produit_pas_de_zero(couts):
    """Un taux hors période de validité ne doit pas se replier sur le taux courant."""
    r = couts.resolve_internal_cleaning_cost(
        ref_date="2025-11-15", intervenant_id="INT_TEST", logement_id="LOG_TEST",
        type_logement_id="TYPE_001", nb_menages=2, nb_heures=3,
        hourly_rows=TAUX, fixed_rows=FORFAITS)
    assert r.status != "OK"
    assert r.total is None


def test_aucun_mois_passe_ne_change_de_methode(couts):
    """Garde-fou de non-régression : la méthode appliquée par mois est figée par D101."""
    attendu = {
        "2026-03-15": "heures", "2026-04-30": "heures",
        "2026-05-01": "heures", "2026-05-31": "heures",
        "2026-06-01": "forfait", "2026-07-15": "forfait",
    }
    for jour, methode in attendu.items():
        r = _resoudre(couts, jour)
        assert r.status == "OK", f"{jour} : {r.status} {r.message}"
        obtenu = "heures" if r.total == pytest.approx(30.0) else "forfait"
        assert obtenu == methode, f"{jour} : méthode {obtenu}, attendue {methode}"

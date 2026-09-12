"""§53-70 — un KPI qui ne dit pas d'où il vient ne se vérifie pas.

Les quatre chiffres du bandeau du relevé s'affichaient nus : « Commission totale 2 191,57 € ».
Somme de quoi, sur quel périmètre, recalculée ou reprise du moteur ? Un chiffre qu'on ne sait pas
reconstituer ne se contrôle pas — et un chiffre qu'on ne peut pas contrôler finit par ne plus être
cru.

La formule est déclarée dans le SERVICE qui calcule le chiffre, jamais dans le gabarit : écrite à
l'écran, elle aurait dérivé du calcul à la première modification, et aurait menti sans que rien ne
le signale.
"""
from __future__ import annotations

import pytest

from app.services import proprietaires_reglements_service as svc


def test_chaque_kpi_declare_sa_formule_et_sa_source():
    for cle, kpi in svc.FORMULES_KPI.items():
        assert kpi.get("libelle", "").strip(), f"{cle} sans libellé"
        assert kpi.get("formule", "").strip(), f"{cle} sans formule"
        assert kpi.get("source", "").strip(), f"{cle} sans source"


def test_les_formules_citent_les_colonnes_reellement_sommees():
    """La formule doit nommer la colonne du moteur, pas la paraphraser."""
    assert "total_commission_mois" in svc.FORMULES_KPI["commission_totale"]["formule"]
    assert "net_proprietaire_apres_charge_mois" in svc.FORMULES_KPI["net_total"]["formule"]
    assert "reste_a_payer_conciergerie" in svc.FORMULES_KPI["reste_total"]["formule"]


def test_les_kpi_du_moteur_disent_qu_ils_ne_sont_pas_recalcules():
    """C'est la règle du module : ne JAMAIS recalculer une commission ni un net."""
    for cle in ("commission_totale", "net_total"):
        assert "recalcul" in svc.FORMULES_KPI[cle]["source"].lower()


@pytest.mark.parametrize("cle", ["commission_totale", "net_total", "reste_total"])
def test_le_tableau_de_bord_expose_la_formule_avec_le_chiffre(cle):
    """Le chiffre et sa formule voyagent ensemble : l'écran ne peut pas afficher l'un sans l'autre."""
    resume = svc.load_dashboard()["summary"] if "summary" in svc.load_dashboard() \
        else svc.load_dashboard()
    formules = resume.get("formules_kpi") or {}
    assert cle in formules, f"{cle} affiché sans sa formule"
    assert cle in resume, f"{cle} absent du résumé"


def test_aucune_formule_orpheline():
    """Une formule sans chiffre serait une promesse non tenue."""
    resume = svc.load_dashboard()
    resume = resume.get("summary", resume)
    for cle in svc.FORMULES_KPI:
        assert cle in resume, f"formule déclarée pour {cle}, mais le chiffre n'est pas calculé"

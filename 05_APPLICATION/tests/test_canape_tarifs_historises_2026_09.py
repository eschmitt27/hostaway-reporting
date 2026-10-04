"""Supplément canapé — changement de tarif à une date d'effet, sans réécrire le passé.

Besoin (2026-10-05) : à compter du 01/09/2026, le supplément canapé passe à 15 € pour deux logements ;
un troisième reste à 10 €. Le mécanisme est GÉNÉRIQUE (`canape_gestion_service.changer_parametres`) :
aucune règle propre à un logement ni à un propriétaire. Ces tests reproduisent la forme exacte des
données réelles — une période ouverte « depuis toujours » (`date_debut` vide), issue du rattrapage de
la migration 0058 — avec des logements fictifs.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import canape_gestion_service as canape
from app.services import referentiel_admin_service as adm

_TRAVAIL = Path(__file__).resolve().parents[2] / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
from lib_canape import calculate_canape_amount  # noqa: E402
from lib_ref_history import resolve_canape_parametres  # noqa: E402


def _ligne_canape(lid, seuil, montant):
    return {"canape_parametre_id": f"CNP_{lid}", "logement_id": lid,
            "seuil_voyageurs_preparation_canape": seuil, "montant_preparation_canape": montant,
            "date_debut": "", "date_fin": "", "actif": "OUI",
            "commentaire": "Backfill migration 0058 (valeur courante au moment de la migration)"}


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer(
        tmp_db,
        logements=[{"logement_id": lid, "nom_court": lid, "actif": "OUI", "statut_parc": "GERE"}
                   for lid in ("LOG_T3A", "LOG_T3B", "LOG_STUDIO")],
        canape=[_ligne_canape("LOG_T3A", "5", "10"), _ligne_canape("LOG_T3B", "5", "10"),
                _ligne_canape("LOG_STUDIO", "3", "10")])
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _toutes(db):
    # `ref_canape_parametres` est une table NATIVE (hors catalogue du classeur) : on la lit comme le
    # fait le service, jamais par `fixtures_referentiel.lignes` (limité aux onglets du catalogue).
    return adm.lignes("ref_canape_parametres", db_path=db)


def _rows(db, lid):
    return sorted((r for r in _toutes(db) if r["logement_id"] == lid), key=lambda r: r["date_debut"])


def _montant(db, lid, arrivee, voyageurs):
    """Ce que le moteur applique à un séjour : paramètre RÉSOLU À LA DATE D'ARRIVÉE, puis formule."""
    res = resolve_canape_parametres(_toutes(db), logement_id=lid, ref_date=arrivee)
    ligne = res.row if res.status == "OK" else None
    return calculate_canape_amount(lid, voyageurs, ligne).amount


def test_le_changement_ferme_l_ancienne_periode_la_veille_et_ouvre_la_nouvelle(ref):
    res = canape.changer_parametres("LOG_T3A", 5, 15, "2026-09-01", acteur="test",
                                    justification="nouveau tarif au 01/09/2026", db_path=ref)
    assert res["ok"], res
    ancienne, nouvelle = _rows(ref, "LOG_T3A")
    assert (ancienne["date_debut"], ancienne["date_fin"], float(ancienne["montant_preparation_canape"])) \
        == ("", "2026-08-31", 10.0)
    assert (nouvelle["date_debut"], nouvelle["date_fin"], float(nouvelle["montant_preparation_canape"])) \
        == ("2026-09-01", "", 15.0)
    assert nouvelle["seuil_voyageurs_preparation_canape"] == "5"      # le seuil ne bouge pas


def test_avant_la_date_d_effet_l_ancien_tarif_a_partir_de_la_date_le_nouveau(ref):
    canape.changer_parametres("LOG_T3A", 5, 15, "2026-09-01", justification="x", db_path=ref)
    assert _montant(ref, "LOG_T3A", "2026-08-31", 5) == 10.0          # dernier jour de l'ancien tarif
    assert _montant(ref, "LOG_T3A", "2026-09-01", 5) == 15.0          # premier jour du nouveau
    assert _montant(ref, "LOG_T3A", "2025-03-10", 6) == 10.0          # historique ancien intact
    assert _montant(ref, "LOG_T3A", "2026-12-24", 5) == 15.0
    assert _montant(ref, "LOG_T3A", "2026-09-10", 4) == 0.0           # sous le seuil : pas de supplément


def test_deux_logements_changent_un_troisieme_reste_a_10_sans_nouvelle_version(ref):
    for lid in ("LOG_T3A", "LOG_T3B"):
        assert canape.changer_parametres(lid, 5, 15, "2026-09-01", justification="x", db_path=ref)["ok"]
    assert len(_rows(ref, "LOG_STUDIO")) == 1                          # aucune version inutile
    assert _montant(ref, "LOG_STUDIO", "2026-09-15", 3) == 10.0
    assert _montant(ref, "LOG_STUDIO", "2026-08-15", 3) == 10.0
    assert _montant(ref, "LOG_T3B", "2026-09-15", 5) == 15.0


def test_aucune_ligne_close_n_est_reecrite_par_un_second_changement(ref):
    canape.changer_parametres("LOG_T3A", 5, 15, "2026-09-01", justification="x", db_path=ref)
    canape.changer_parametres("LOG_T3A", 5, 18, "2027-01-01", justification="x", db_path=ref)
    premiere, deuxieme, troisieme = _rows(ref, "LOG_T3A")
    assert (premiere["date_fin"], premiere["montant_preparation_canape"]) == ("2026-08-31", "10")
    assert (deuxieme["date_fin"], float(deuxieme["montant_preparation_canape"])) == ("2026-12-31", 15.0)
    assert float(troisieme["montant_preparation_canape"]) == 18.0


def test_le_changement_est_journalise_comme_correction_retroactive_avec_sa_justification(ref):
    canape.changer_parametres("LOG_T3A", 5, 15, "2026-09-01", acteur="mission",
                              justification="nouveau tarif au 01/09/2026", db_path=ref)
    evts = [e for e in adm.historique_evenements("ref_canape_parametres", db_path=ref)]
    assert evts, "aucun événement de journal"
    assert {e["action"] for e in evts} == {"CORRECTION_RETROACTIVE"}   # la date d'effet est passée
    assert all(e["acteur"] == "mission" for e in evts)


def test_un_changement_futur_n_est_pas_une_correction_retroactive(ref):
    canape.changer_parametres("LOG_T3A", 5, 15, "2099-01-01", db_path=ref)
    assert {e["action"] for e in adm.historique_evenements("ref_canape_parametres", db_path=ref)} \
        == {"CHANGEMENT_PARAMETRES_CANAPE"}


def test_aucune_exception_par_logement_n_est_codee_dans_le_chemin_du_tarif_canape():
    """Le tarif se règle par la donnée, jamais par une exception pour un logement ou un propriétaire :
    ni dans le service d'historisation, ni dans la formule, ni dans la résolution datée, ni dans le
    calcul des commissions."""
    racine = Path(__file__).resolve().parents[2]
    interdits = re.compile(r"\bLOG_00(06|08|13)\b|\bPROP_00(06|08|09)\b")
    chemins = [racine / "05_APPLICATION" / "app" / "services" / "canape_gestion_service.py",
               racine / "05_APPLICATION" / "app" / "services" / "impact_preview_service.py",
               racine / "05_APPLICATION" / "app" / "services" / "referentiel_admin_service.py",
               racine / "02_TRAVAIL" / "lib_canape.py", racine / "02_TRAVAIL" / "lib_ref_history.py",
               racine / "02_TRAVAIL" / "lot10_calculer_resultats.py"]
    fautifs = [str(c.relative_to(racine)) for c in chemins
               if interdits.search(c.read_text(encoding="utf-8", errors="ignore"))]
    assert not fautifs, fautifs

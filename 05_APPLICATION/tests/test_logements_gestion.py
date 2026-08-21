"""Cycle de vie d'un logement existant : modifier / archiver / réactiver / changer_proprietaire /
changer_taux_commission — validations + historisation.

Le référentiel est SEMÉ EN SQLITE : depuis la migration 0051, ces services écrivent dans les tables
`ref_*` et n'ouvrent plus `REF_Setup.xlsm`. Les règles métier vérifiées ici sont inchangées — seule
la destination de l'écriture a changé.
"""
import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import logements_gestion_service as svc

# Correspondance onglet historique → table, pour que les tests se lisent comme avant.
TABLES = {
    "REF_Logements": "ref_logements",
    "REF_Gestion_Logements_Hist": "ref_gestion_logements_hist",
    "REF_Taux_Commission": "ref_taux_commission",
}


@pytest.fixture
def ref(tmp_db, monkeypatch):
    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _lignes(db_path, sheet):
    return fx.lignes(db_path, TABLES[sheet])


def test_modifier_met_a_jour_les_champs_descriptifs(ref):
    res = svc.modifier("LOG_A1", {"nom_court": "A1-bis", "ville": "NOUVELLE_VILLE"})
    assert res["ok"], res
    logs = _lignes(ref, "REF_Logements")
    assert logs[0]["nom_court"] == "A1-bis"
    assert logs[0]["ville"] == "NOUVELLE_VILLE"
    assert logs[0]["logement_id"] == "LOG_A1"        # jamais touché


def test_modifier_logement_inconnu_refuse(ref):
    res = svc.modifier("LOG_ZZZ", {"nom_court": "X"})
    assert res["ok"] is False and res["code"] == svc.E_LOGEMENT_INCONNU


def test_modifier_type_inconnu_refuse(ref):
    res = svc.modifier("LOG_A1", {"type_logement_id": "TYPE_ZZZ"})
    assert res["ok"] is False


def test_modifier_refuse_si_referentiel_non_initialise(tmp_db):
    """Fail-closed : sans référentiel importé, le service REFUSE — il ne se rabat pas sur le
    classeur et n'écrit pas dans une base vide.

    Remplace l'ancien test des flags `CHARGES_REAL_WRITE_*` : ces flags protégeaient l'écriture
    dans un CLASSEUR SOURCE réel. L'écriture va désormais dans `app.db`, la base de l'application ;
    la garde qui compte est celle-ci.
    """
    res = svc.modifier("LOG_A1", {"nom_court": "X"}, db_path=tmp_db)
    assert res["ok"] is False and res["code"] == svc.E_REFERENTIEL_ABSENT


# ── archiver / reactiver ──────────────────────────────────────────────────

def test_archiver_desactive_et_cloture_la_gestion(ref):
    res = svc.archiver("LOG_A1", "2026-06-30")
    assert res["ok"], res
    logs = {r["logement_id"]: r for r in _lignes(ref, "REF_Logements")}
    assert logs["LOG_A1"]["actif"] == "NON" and logs["LOG_A1"]["statut_parc"] == "RETIRE"
    gest = [r for r in _lignes(ref, "REF_Gestion_Logements_Hist") if r["logement_id"] == "LOG_A1"]
    assert gest[0]["date_fin"] == "2026-06-30" and gest[0]["statut_gestion"] == "RETIRE"


def test_archiver_deja_archive_refuse(ref):
    res = svc.archiver("LOG_INACTIF", "2026-06-30")
    assert res["ok"] is False and res["code"] == svc.E_DEJA_ARCHIVE


def test_archiver_date_invalide_refusee(ref):
    res = svc.archiver("LOG_A1", "30/06/2026")
    assert res["ok"] is False and res["code"] == svc.E_DATE_INVALIDE


def test_reactiver_reactive_et_ouvre_un_rattachement(ref):
    res = svc.reactiver("LOG_INACTIF", "2026-07-01", "PROP_B")
    assert res["ok"], res
    logs = {r["logement_id"]: r for r in _lignes(ref, "REF_Logements")}
    assert logs["LOG_INACTIF"]["actif"] == "OUI" and logs["LOG_INACTIF"]["statut_parc"] == "GERE"
    gest = [r for r in _lignes(ref, "REF_Gestion_Logements_Hist")
            if r["logement_id"] == "LOG_INACTIF"]
    assert len(gest) == 2                            # ancienne conservée + nouvelle
    nouvelle = [g for g in gest if not g["date_fin"]][0]
    assert nouvelle["proprietaire_id"] == "PROP_B" and nouvelle["date_debut"] == "2026-07-01"


def test_reactiver_deja_actif_refuse(ref):
    res = svc.reactiver("LOG_A1", "2026-07-01", "PROP_A")
    assert res["ok"] is False and res["code"] == svc.E_DEJA_ACTIF


def test_reactiver_proprietaire_inconnu_refuse(ref):
    res = svc.reactiver("LOG_INACTIF", "2026-07-01", "PROP_ZZ")
    assert res["ok"] is False and res["code"] == svc.E_PROP_INCONNU


# ── changer_proprietaire ──────────────────────────────────────────────────

def test_changer_proprietaire_cloture_et_ouvre(ref):
    res = svc.changer_proprietaire("LOG_A1", "PROP_B", "2026-07-01")
    assert res["ok"], res
    gest = [r for r in _lignes(ref, "REF_Gestion_Logements_Hist") if r["logement_id"] == "LOG_A1"]
    assert len(gest) == 2
    ancienne = [g for g in gest if g["date_fin"]][0]
    assert ancienne["proprietaire_id"] == "PROP_A" and ancienne["date_fin"] == "2026-06-30"
    nouvelle = [g for g in gest if not g["date_fin"]][0]
    assert nouvelle["proprietaire_id"] == "PROP_B" and nouvelle["date_debut"] == "2026-07-01"


def test_changer_proprietaire_ne_modifie_jamais_la_ligne_close(ref):
    svc.changer_proprietaire("LOG_A1", "PROP_B", "2026-07-01")
    svc.changer_proprietaire("LOG_A1", "PROP_C", "2026-08-01")
    gest = [r for r in _lignes(ref, "REF_Gestion_Logements_Hist") if r["logement_id"] == "LOG_A1"]
    assert len(gest) == 3
    closes = sorted([g for g in gest if g["date_fin"]], key=lambda g: g["date_debut"])
    assert closes[0]["proprietaire_id"] == "PROP_A" and closes[0]["date_fin"] == "2026-06-30"
    assert closes[1]["proprietaire_id"] == "PROP_B" and closes[1]["date_fin"] == "2026-07-31"


def test_changer_proprietaire_inconnu_refuse(ref):
    res = svc.changer_proprietaire("LOG_A1", "PROP_ZZ", "2026-07-01")
    assert res["ok"] is False and res["code"] == svc.E_PROP_INCONNU


def test_changer_proprietaire_manquant_refuse(ref):
    res = svc.changer_proprietaire("LOG_A1", "", "2026-07-01")
    assert res["ok"] is False and res["code"] == svc.E_PROP_MANQUANT


# ── changer_taux_commission ───────────────────────────────────────────────

def test_changer_taux_cloture_ancien_et_ouvre_nouveau(ref):
    res = svc.changer_taux_commission("LOG_A1", 0.15, "2026-07-01")
    assert res["ok"], res
    taux = [r for r in _lignes(ref, "REF_Taux_Commission") if r["logement_id"] == "LOG_A1"]
    assert len(taux) == 2
    ancien = [t for t in taux if t["date_fin"]][0]
    assert float(ancien["taux_commission"]) == 0.19 and ancien["date_fin"] == "2026-06-30"
    nouveau = [t for t in taux if not t["date_fin"]][0]
    assert float(nouveau["taux_commission"]) == 0.15 and nouveau["date_debut"] == "2026-07-01"
    assert nouveau["proprietaire_id"] == "PROP_A"     # hérité du rattachement actif


def test_changer_taux_grain_logement_pas_de_regression_sur_taux_historique(ref):
    """Une ligne close (date_fin déjà renseignée) n'est jamais modifiée par un nouveau
    changement — seule la ligne active (date_fin vide) est close."""
    svc.changer_taux_commission("LOG_A1", 0.15, "2026-07-01")
    svc.changer_taux_commission("LOG_A1", 0.12, "2026-08-01")
    taux = [r for r in _lignes(ref, "REF_Taux_Commission") if r["logement_id"] == "LOG_A1"]
    assert len(taux) == 3
    initial = [t for t in taux if float(t["taux_commission"]) == 0.19][0]
    assert initial["date_fin"] == "2026-06-30"        # jamais retouché par le 2e changement


def test_changer_taux_invalide_refuse(ref):
    res = svc.changer_taux_commission("LOG_A1", 1.5, "2026-07-01")
    assert res["ok"] is False and res["code"] == svc.E_TAUX_INVALIDE


def test_changer_taux_logement_inconnu_refuse(ref):
    res = svc.changer_taux_commission("LOG_ZZZ", 0.15, "2026-07-01")
    assert res["ok"] is False and res["code"] == svc.E_LOGEMENT_INCONNU


def test_changer_taux_refuse_une_periode_incoherente(ref):
    """Une clôture antérieure au début de la période courante est refusée : elle produirait une
    période négative, que la résolution datée ne saurait pas interpréter.

    Remplace l'ancien test du write-guard « hors racine recette », qui protégeait l'écriture d'un
    FICHIER. L'écriture étant désormais en base, l'invariant à défendre est la cohérence des
    périodes historisées.
    """
    res = svc.changer_taux_commission("LOG_A1", 0.15, "2025-01-01", db_path=ref)
    assert res["ok"] is False and res["code"] == svc.E_PERIODE_INCOHERENTE
    taux = [r for r in _lignes(ref, "REF_Taux_Commission") if r["logement_id"] == "LOG_A1"]
    assert len(taux) == 1                             # aucune écriture

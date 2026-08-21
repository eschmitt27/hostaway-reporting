"""Création d'un logement au référentiel : validations + écriture SQLite.

Le référentiel est SEMÉ EN SQLITE : depuis la migration 0051, ce service écrit dans les tables
`ref_*` et n'ouvre plus `REF_Setup.xlsm`. Les validations vérifiées ici sont inchangées.
"""
import pytest

import app.config as cfg
import fixtures_referentiel as fx
from app.services import logements_creation_service as svc

FORM = {"logement_id": "LOG_NEW", "nom_logement_officiel": "Fictif Nouveau", "nom_court": "NEW",
        "adresse": "1 rue Test", "ville": "RECETTE", "type_logement_id": "TYPE_001",
        "proprietaire_id": "PROP_A", "date_debut": "2026-06-01", "actif": "OUI"}

TABLES = {
    "REF_Logements": "ref_logements",
    "REF_Gestion_Logements_Hist": "ref_gestion_logements_hist",
}


@pytest.fixture
def ref(tmp_db, monkeypatch):
    """Même jeu qu'avant : LOG_A1 déjà présent, PROP_A/PROP_B actifs, TYPE_001 connu."""
    fx.semer(
        tmp_db,
        logements=[{"logement_id": "LOG_A1", "hostaway_listing_id": "900001",
                    "nom_logement_officiel": "Fictif A1", "nom_court": "A1", "adresse": "1 rue",
                    "ville": "RECETTE", "type_logement_id": "TYPE_001", "sur_hostaway": "OUI",
                    "actif": "OUI", "statut_parc": "GERE", "commentaire": "",
                    "forfait_logiciel_consommables_mensuel": "0"}],
        gestion=[{"gestion_id": "GST_1", "logement_id": "LOG_A1", "proprietaire_id": "PROP_A",
                  "date_debut": "2026-01-01", "date_fin": "", "statut_gestion": "ACTIF",
                  "source": "FICTIF", "commentaire": ""}],
        proprietaires=[{"proprietaire_id": pid, "nom_proprietaire": f"Nom {pid}", "actif": "OUI"}
                       for pid in ("PROP_A", "PROP_B")],
        types=[{"type_logement_id": "TYPE_001", "type_logement": "STUDIO"}],
    )
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    return tmp_db


def _lignes(db_path, sheet):
    """Lignes de la table, sous forme de dicts (l'ordre des colonnes n'a plus de sens en base)."""
    return fx.lignes(db_path, TABLES[sheet])


def test_referentiels_exposent_les_options(ref):
    r = svc.referentiels()
    assert r["status"] == "OK"
    assert {p["proprietaire_id"] for p in r["proprietaires"]} == {"PROP_A", "PROP_B"}
    assert "LOG_A1" in r["logements"]


def test_creation_ajoute_logement_et_rattachement_date(ref):
    res = svc.creer(dict(FORM))
    assert res["ok"], res
    logs = _lignes(ref, "REF_Logements")
    assert len(logs) == 2
    nouveau = next(l for l in logs if l["logement_id"] == "LOG_NEW")
    assert nouveau["nom_court"] == "NEW"
    gest = _lignes(ref, "REF_Gestion_Logements_Hist")
    assert len(gest) == 2
    rattachement = next(g for g in gest if g["logement_id"] == "LOG_NEW")
    assert rattachement["proprietaire_id"] == "PROP_A"
    assert rattachement["date_debut"] == "2026-06-01"
    assert rattachement["statut_gestion"] == "ACTIF"


def test_identifiant_deja_utilise_refuse(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, logement_id="LOG_A1"))]
    assert svc.E_ID_EXISTANT in codes


def test_identifiant_obligatoire(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, logement_id=""))]
    assert svc.E_ID_MANQUANT in codes


def test_nom_obligatoire(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, nom_logement_officiel="", nom_court=""))]
    assert svc.E_NOM_MANQUANT in codes


def test_proprietaire_inconnu_refuse(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, proprietaire_id="PROP_ZZ"))]
    assert svc.E_PROP_INCONNU in codes


def test_proprietaire_obligatoire(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, proprietaire_id=""))]
    assert svc.E_PROP_MANQUANT in codes


def test_date_invalide_refusee(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, date_debut="15/06/2026"))]
    assert svc.E_DATE_INVALIDE in codes


def test_type_inconnu_refuse(ref):
    codes = [e["code"] for e in svc.valider(dict(FORM, type_logement_id="TYPE_ZZ"))]
    assert svc.E_TYPE_INCONNU in codes


def test_creation_refusee_si_referentiel_non_initialise(tmp_db):
    """Fail-closed : sans référentiel importé, la création REFUSE — elle ne se rabat pas sur le
    classeur et n'amorce pas un référentiel vide par une saisie isolée.

    Remplace les anciens tests des flags d'écriture et du write-guard : tous deux protégeaient
    l'écriture d'un FICHIER source. L'écriture va désormais dans `app.db`.
    """
    res = svc.creer(dict(FORM), db_path=tmp_db)
    assert res["ok"] is False and res["code"] == svc.E_REFERENTIEL_ABSENT


def test_logement_inactif_cree_un_rattachement_retire(ref):
    assert svc.creer(dict(FORM, actif="NON"))["ok"]
    gest = _lignes(ref, "REF_Gestion_Logements_Hist")
    assert next(g for g in gest if g["logement_id"] == "LOG_NEW")["statut_gestion"] == "RETIRE"

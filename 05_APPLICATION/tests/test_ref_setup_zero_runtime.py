"""§20 — `REF_Setup.xlsm` : ZÉRO dépendance au runtime.

Le référentiel vit dans les tables `ref_*` (0029) et s'administre depuis l'application (0051). Le
classeur n'est plus qu'un IMPORT INITIAL et une archive : l'application, ses écrans et son CRUD
doivent fonctionner sans lui.

La preuve est ACTIVE : le chemin configuré pointe vers un fichier inexistant ET toute tentative
d'ouvrir un classeur nommé `REF_Setup` fait échouer le test immédiatement, quel que soit le chemin
de code emprunté. Même principe que `test_master_ctrl_coherence_zero_runtime.py`.

CE QUI RESTE HORS PÉRIMÈTRE DE CE TEST : le lecteur de SAISIE des charges
(`saisie_charges_reader`) et les flux de simulation « écriture réelle sur COPIE » lisent encore un
classeur. Ils relèvent du chantier Charges / Saisies, pas du référentiel.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
import fixtures_referentiel as fx


@pytest.fixture(autouse=True)
def _referentiel_sans_classeur(tmp_db, tmp_path, monkeypatch):
    """Référentiel SQLite alimenté, classeur RENDU INDISPONIBLE et INTERDIT."""
    import openpyxl

    fx.semer_parc_standard(tmp_db)
    monkeypatch.setattr(cfg, "REF_SETUP", tmp_path / "REF_Setup_ABSENT.xlsm")

    original = openpyxl.load_workbook

    def garde(chemin, *a, **kw):
        if "REF_Setup" in Path(chemin).name:
            raise AssertionError(
                f"REF_Setup ouvert au runtime : {chemin}. Le référentiel est en base ; le classeur "
                "ne doit servir qu'à l'import initial.")
        return original(chemin, *a, **kw)

    monkeypatch.setattr(openpyxl, "load_workbook", garde)
    return tmp_db


# ── Lecture ────────────────────────────────────────────────────────────────

def test_referentiel_service_lit_sans_classeur(tmp_db):
    from app.services import referentiel_service as ref

    assert ref.disponible(db_path=tmp_db) is True
    logements = ref.logements(db_path=tmp_db)
    assert {l["logement_id"] for l in logements} == {"LOG_A1", "LOG_INACTIF"}
    assert ref.nom_proprietaire("PROP_A", db_path=tmp_db)


def test_lecteur_hh_lit_sans_classeur(tmp_db):
    from app.readers import ref_setup_hh_reader as hh

    assert {l["logement_id"] for l in hh.get_all_logements_hh(db_path=tmp_db)} == {
        "LOG_A1", "LOG_INACTIF"}
    assert hh.get_gestion_hist(db_path=tmp_db)
    assert hh.get_all_proprietaires_hh(db_path=tmp_db)


def test_libelles_banque_sans_classeur(tmp_db):
    """Les libellés d'affichage du module Banque viennent du référentiel SQLite."""
    from app.services import banques_controle_service as bq

    bq.vider_cache()
    assert bq.libelle_proprietaire("PROP_A")
    bq.vider_cache()


def test_noms_proprietaires_menages_sans_classeur(tmp_db):
    from app.readers import menages_reader

    menages_reader.vider_cache()
    noms = menages_reader.noms_proprietaires()
    assert noms.get("PROP_A")
    menages_reader.vider_cache()


# ── Écriture (CRUD parc) ───────────────────────────────────────────────────

def test_crud_logement_sans_classeur(tmp_db):
    """Le cycle de vie complet d'un logement s'exécute sans jamais ouvrir le classeur."""
    from app.services import logements_creation_service as crea
    from app.services import logements_gestion_service as gest

    assert crea.creer({"logement_id": "LOG_Z9", "nom_court": "Z9", "proprietaire_id": "PROP_A",
                       "date_debut": "2026-02-01", "type_logement_id": "TYPE_001"},
                      db_path=tmp_db)["ok"]
    assert gest.modifier("LOG_Z9", {"ville": "AILLEURS"}, db_path=tmp_db)["ok"]
    assert gest.changer_proprietaire("LOG_Z9", "PROP_B", "2026-05-01", db_path=tmp_db)["ok"]
    assert gest.changer_taux_commission("LOG_Z9", 0.17, "2026-05-01", db_path=tmp_db)["ok"]
    assert gest.archiver("LOG_Z9", "2026-09-30", db_path=tmp_db)["ok"]

    histo = gest.historique("LOG_Z9", db_path=tmp_db)
    closes = [g for g in histo["gestion"] if g["date_fin"]]
    assert len(closes) == 2                      # la période initiale ET celle du changement


def test_administration_ecrit_sans_classeur(tmp_db):
    from app.services import referentiel_admin_service as adm

    res = adm.creer_ligne("ref_proprietaires",
                          {"proprietaire_id": "PROP_NEW", "nom_proprietaire": "Nouveau",
                           "actif": "OUI"}, db_path=tmp_db)
    assert res["ok"], res
    assert adm.basculer_activation("ref_proprietaires", "PROP_NEW", False,
                                   db_path=tmp_db)["ok"]
    assert adm.ligne("ref_proprietaires", "PROP_NEW", db_path=tmp_db)["actif"] == "NON"


# ── Écrans ─────────────────────────────────────────────────────────────────

def test_ecrans_repondent_sans_classeur(client, tmp_db):
    for url in ("/administration/referentiels",
                "/administration/referentiels/ref_logements",
                "/administration/referentiels/ref_gestion_logements_hist",
                "/logements"):
        r = client.get(url)
        assert r.status_code == 200, (url, r.status_code)


# ── Fail-closed : jamais de repli sur le classeur ──────────────────────────

def test_referentiel_absent_refuse_sans_se_rabattre(tmp_path, monkeypatch):
    """Base vide + classeur présent sur le disque : le service REFUSE quand même.

    C'est le cœur de la règle §18. Un repli « SQLite sinon Excel » ferait ici disparaître le
    problème en silence, et le référentiel réapparaîtrait sans que personne ne l'ait importé.
    """
    import sqlite3

    from app.db.connection import apply_migrations
    from app.services import logements_creation_service as crea

    base = tmp_path / "vide.db"
    apply_migrations(base)
    monkeypatch.setattr(cfg, "DB_PATH", base)

    res = crea.creer({"logement_id": "LOG_X", "nom_court": "X", "proprietaire_id": "PROP_A",
                      "date_debut": "2026-01-01"}, db_path=base)
    assert res["ok"] is False
    assert res["code"] == crea.E_REFERENTIEL_ABSENT

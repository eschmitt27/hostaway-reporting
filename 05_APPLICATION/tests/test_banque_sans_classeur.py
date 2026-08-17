"""La Banque fonctionne sans aucun classeur — et le dit quand elle n'a pas de données.

Deux affirmations distinctes, souvent confondues :

  1. AVEC des données en base et SANS classeur, tout fonctionne normalement. C'est la preuve que la
     migration est faite : rien ne lit plus le fichier, pas même en repli.
  2. SANS données en base, chaque écran annonce un état NOMMÉ. C'est la preuve qu'aucun repli n'a été
     laissé en place : un service qui se rabattrait sur le classeur afficherait, après un changement
     de banque, les mouvements de l'ancienne comme s'ils étaient courants.

Le classeur est rendu introuvable en pointant la configuration sur un chemin inexistant : plus fiable
que de supposer qu'il est absent de la machine, et sans rien déplacer sur le disque.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import app.config as cfg
import fixtures_banque as fx
from app.readers import banques_reader as reader
from app.readers import controles_detail_reader as detail
from app.services import banque_adaptateur_moteur as adaptateur
from app.services import banque_attentes_service as att
from app.services import banque_classification_service as cls
from app.services import banque_vues_service as vues
from app.services import banques_controle_service as ctrl
from app.services import banques_controle_writer as writer
from app.services import calculs_pipeline_service as pipeline


@pytest.fixture(autouse=True)
def sans_classeur(tmp_db, tmp_path, monkeypatch):
    """Aucun classeur bancaire atteignable, quelle que soit la machine."""
    introuvable = tmp_path / "AUCUN_CLASSEUR" / "BANQUE_LOT8_IMPORT.xlsx"
    monkeypatch.setattr(cfg, "MASTER_BANQUE", introuvable)
    reader.vider_cache()
    ctrl.vider_cache()
    detail.vider_cache()
    yield introuvable
    reader.vider_cache()
    ctrl.vider_cache()
    detail.vider_cache()


@pytest.fixture
def banque_peuplee(tmp_db):
    fx.construire(tmp_db, mouvements=[
        fx.mouvement("MVT-SC-0001", "2026-04-02", "VIR PLATEFORME G12345678", 1200.0, "CREDIT",
                     tiers=att.TIERS_PLATEFORME,
                     statut_classification=cls.CLASS_RAPPROCHEMENT_REQUIS),
        fx.mouvement("MVT-SC-0002", "2026-04-05", "PRELEVEMENT A QUALIFIER", 60.0, "DEBIT",
                     statut_controle=cls.ST_A_CONTROLER,
                     statut_classification=cls.CLASS_A_ENVOYER_IA),
        fx.mouvement("MVT-SC-0003", "2026-04-09", "FRAIS TENUE DE COMPTE", 4.50, "DEBIT",
                     categorie="FRAIS_BANCAIRES"),
    ], controles=[
        fx.controle("MVT-SC-0002", "IA_CONFIANCE_INSUFFISANTE", "Aucune regle deterministe"),
    ], attentes=[
        fx.attente("MVT-SC-0001", 1200.0, att.ATTENTE_EXPORT_PLATEFORME,
                   tiers=att.TIERS_PLATEFORME, reference="G12345678"),
    ])
    reader.vider_cache()
    ctrl.vider_cache()
    return tmp_db


# ── 1. Avec des données, sans classeur : tout marche ────────────────────────────────────────────

def test_le_classeur_est_bien_introuvable(sans_classeur):
    assert not Path(cfg.MASTER_BANQUE).exists()


def test_ecrans_banque(banque_peuplee, client):
    for url in ("/banques-caisse", "/banques-caisse/a-classer"):
        r = client.get(url)
        assert r.status_code == 200, url
        assert "BANQUE_LOT8_IMPORT" not in r.text, f"{url} nomme encore le classeur"


def test_lecture_des_mouvements(banque_peuplee):
    src = reader.mouvements()
    assert src.etat.disponible and src.etat.nb_lignes == 3
    assert "xlsx" not in src.etat.fichier.lower()


def test_controles_et_file_ia(banque_peuplee, tmp_db):
    assert len(vues.controles_a_controler(db_path=tmp_db)) == 1
    assert len(vues.a_classer_par_ia(db_path=tmp_db)) == 1


def test_attentes_lot8c(banque_peuplee, tmp_db):
    lignes = vues.attentes_plateforme(db_path=tmp_db)
    assert len(lignes) == 1
    assert lignes[0]["montant_banque"] == 1200.0
    assert lignes[0]["reference_airbnb"] == "G12345678"


def test_controle_detail_banque_non_classee(banque_peuplee):
    lignes = detail.banque_non_classees("2026-04")
    assert [l["mouvement_id"] for l in lignes] == ["MVT-SC-0001"]


def test_decision_humaine_appliquee(banque_peuplee, tmp_db):
    """Un mouvement A_CONTROLER passe EN_COURS, et la vue en tient compte sans rien réécrire.

    La transition suit le cycle métier existant (A_CONTROLER → EN_COURS) : ce test vérifie que la
    décision est prise en compte sans classeur, pas qu'on peut sauter une étape du cycle.
    """
    opaque = ctrl.id_opaque("MVT-SC-0002")
    ctrl.enregistrer_decision(opaque, statut_controle="EN_COURS", categorie="FRAIS_BANCAIRES",
                              db_path=tmp_db)
    res = writer.appliquer_decisions(db_path=tmp_db)
    assert res["statut"] == "SUCCES"
    ligne = next(l for l in vues.mouvements_normalises(db_path=tmp_db)
                 if l["mouvement_id"] == "MVT-SC-0002")
    assert ligne["statut_controle"] == "EN_COURS"
    assert ligne["source_classification"] == vues.SOURCE_DECISION_HUMAINE
    assert ligne["regle_id_appliquee"], "la proposition du moteur reste lisible"


def test_adaptateur_moteur_fabrique_le_classeur(banque_peuplee, tmp_db, tmp_path):
    """Le classeur du moteur est produit à la demande, ailleurs, et ne devient pas une source."""
    cible = tmp_path / "ws" / "02_TRAVAIL" / "Lot8_Banque" / "BANQUE_LOT8_IMPORT.xlsx"
    gen = adaptateur.ecrire_classeur_moteur(cible, db_path=tmp_db)
    assert gen["ok"] and gen["nb_mouvements"] == 3
    assert cible.exists() and not Path(cfg.MASTER_BANQUE).exists()

    import openpyxl
    wb = openpyxl.load_workbook(cible, read_only=True)
    try:
        # Les quatre onglets que lot11 lit dans un seul bloc : s'il en manque un, il conclut
        # « Banque non disponible » avec un code retour 0, donc sans rien signaler.
        for onglet in (adaptateur.ONGLET_NORM, adaptateur.ONGLET_CTRL,
                       adaptateur.ONGLET_CLOTURE, adaptateur.ONGLET_RAPPRO_PLATEFORME):
            assert onglet in wb.sheetnames, onglet
    finally:
        wb.close()


def test_pipeline_declare_le_dataset_pas_un_fichier(banque_peuplee):
    entree = pipeline._entree_banque()
    assert entree["existe"] is True
    assert entree["nb_mouvements"] == 3
    assert "xlsx" not in entree["fichier"].lower()


# ── 2. Sans données : un état nommé, jamais un repli ────────────────────────────────────────────

def test_etat_nomme_quand_la_base_est_vide(tmp_db):
    assert vues.etat(db_path=tmp_db) == vues.ETAT_NON_INITIALISEE
    assert reader.mouvements().etat.etat == reader.ETAT_NON_INITIALISEE


def test_ecrans_sans_donnees_ne_plantent_pas(client):
    for url in ("/banques-caisse", "/banques-caisse/a-classer"):
        assert client.get(url).status_code == 200, url


def test_adaptateur_refuse_de_fabriquer_un_classeur_vide(tmp_db, tmp_path):
    """Un classeur vide ferait conclure au moteur « aucune anomalie bancaire »."""
    gen = adaptateur.ecrire_classeur_moteur(tmp_path / "vide.xlsx", db_path=tmp_db)
    assert gen["ok"] is False and gen["code"] == vues.ETAT_NON_INITIALISEE
    assert not (tmp_path / "vide.xlsx").exists()


def test_pipeline_signale_le_dataset_non_initialise(tmp_db):
    entree = pipeline._entree_banque()
    assert entree["existe"] is False
    assert entree["etat"] == vues.ETAT_NON_INITIALISEE
    assert "importez" in entree["message"].lower()

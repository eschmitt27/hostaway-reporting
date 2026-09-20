"""Maintenance de phase de TEST : remise à zéro des factures fournisseurs issues des PDF.

Ce script existe pour remesurer l'extraction sur un corpus propre tant que l'application n'est pas
en production. Ce qui est vérifié ici, ce sont ses GARDE-FOUS — car il lève volontairement un
verrou métier (le mois clôturé) et ne doit rien lever d'autre :

· il s'arrête si une facture fournisseur n'est pas À CONTRÔLER (validée, réglée…) ;
· il s'arrête si une facture a produit une écriture, une dette ou un règlement ;
· il ne touche ni aux factures propriétaires, ni aux référentiels, ni aux mappings logements ;
· il ne modifie AUCUN statut de clôture, et le verrou « mois clôturé » reprend ses droits ensuite.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

import app.config as cfg
from app.db.connection import get_db
from app.services import factures_service as fact

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))
maintenance = pytest.importorskip("maintenance_reset_factures_pdf_test")

MOIS_CLOS = "2026-03"


@pytest.fixture(autouse=True)
def _ecriture_activee(monkeypatch):
    for drapeau in ("FACTURES_REAL_WRITE_ENABLED", "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED",
                    "ECRITURE_OPERATIONNELLE_ENABLED"):
        monkeypatch.setattr(cfg, drapeau, True, raising=False)


def _facture(db, *, ref, source="PDF_EXTRACTION", statut=fact.ST_A_CONTROLER,
             date_facture=f"{MOIS_CLOS}-31"):
    conn = get_db(db)
    try:
        opaque = f"FAC-{ref}"
        conn.execute(
            "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
            "facture_ref_source, date_facture, montant_ttc, statut, source, justificatif) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (opaque, "INT_0004", ref, ref, date_facture, 100.0, statut, source, f"{ref}.pdf"))
        conn.execute("INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, "
                     "type_ligne, montant_ttc, description) VALUES (?,?,?,?,?)",
                     (f"FLM-{ref}", opaque, "MENAGE_EXTERNE", 100.0, "ligne provisoire"))
        conn.commit()
    finally:
        conn.close()
    return opaque


@pytest.fixture()
def base(tmp_db):
    """Un mois clôturé, et une facture de test qui tombe dedans."""
    conn = get_db(tmp_db)
    try:
        conn.execute("INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
                     "VALUES (?,?,?)", (MOIS_CLOS, "CLOTURE", "TEST"))
        conn.commit()
    finally:
        conn.close()
    return tmp_db


def _compte(db, table):
    conn = get_db(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def test_le_verrou_mois_cloture_bloque_la_suppression_normale(base):
    """La règle métier NORMALE reste ce qu'elle est : hors maintenance, on ne supprime pas."""
    opaque = _facture(base, ref="NORMALE")
    res = fact.supprimer(opaque, motif="essai", acteur="test", db_path=base)
    assert res["ok"] is False and res["code"] == fact.E_MOIS_CLOTURE


def test_la_maintenance_supprime_les_factures_de_test_meme_en_mois_cloture(base, tmp_path):
    _facture(base, ref="PDF1")
    _facture(base, ref="PDF2", date_facture="2026-08-31")
    res = maintenance.executer(db_path=base, dossier_sauvegardes=tmp_path / "bck")

    assert res["ok"] is True and res["nb_supprimees"] == 2 and res["restant"] == 0
    assert _compte(base, "facture_lignes_menage") == 0, "les artefacts provisoires partent aussi"
    assert Path(res["sauvegarde"]).exists(), "sauvegarde préalable"
    assert Path(res["journal"]).exists(), "journal de l'opération, la trace en base disparaissant"


def test_la_maintenance_ne_touche_pas_aux_factures_saisies_a_la_main(base, tmp_path):
    _facture(base, ref="PDF1")
    manuelle = _facture(base, ref="MANUELLE", source="SAISIE")
    maintenance.executer(db_path=base, dossier_sauvegardes=tmp_path / "bck")
    assert fact.charger(manuelle, db_path=base) is not None


def test_la_maintenance_s_arrete_devant_une_facture_validee(base, tmp_path):
    _facture(base, ref="PDF1")
    _facture(base, ref="VALIDEE", statut=fact.ST_VALIDEE)
    res = maintenance.executer(db_path=base, dossier_sauvegardes=tmp_path / "bck")

    assert res["ok"] is False and res["code"] == "FACTURE_NON_SUPPRIMABLE"
    assert _compte(base, "factures") == 2, "aucune suppression : on s'arrête avant d'écrire"
    assert Path(res["sauvegarde"]).exists()


def test_la_maintenance_s_arrete_devant_une_consequence_comptable(base, tmp_path, monkeypatch):
    opaque = _facture(base, ref="PDF1")
    monkeypatch.setattr(fact, "consequences_constatees",
                        lambda o, db_path=None: {"ecritures": [{"journal": "ACHATS"}],
                                                 "nb_reglements": 0, "reversible": False})
    res = maintenance.executer(db_path=base, dossier_sauvegardes=tmp_path / "bck")
    assert res["ok"] is False and res["code"] == "CONSEQUENCES_CONSTATEES"
    assert fact.charger(opaque, db_path=base) is not None


def test_la_maintenance_ne_modifie_aucune_cloture_et_rend_le_verrou(base, tmp_path):
    _facture(base, ref="PDF1")
    maintenance.executer(db_path=base, dossier_sauvegardes=tmp_path / "bck")

    conn = get_db(base)
    try:
        statut = conn.execute("SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois = ?",
                              (MOIS_CLOS,)).fetchone()[0]
    finally:
        conn.close()
    assert statut == "CLOTURE", "aucun statut de clôture n'est touché"

    # Le verrou métier est rendu : une suppression normale redevient impossible.
    opaque = _facture(base, ref="APRES")
    res = fact.supprimer(opaque, motif="essai", acteur="test", db_path=base)
    assert res["ok"] is False and res["code"] == fact.E_MOIS_CLOTURE


def test_le_script_refuse_sans_confirmation_explicite():
    assert maintenance.main([]) == 2, "une maintenance de test ne se déclenche jamais par défaut"

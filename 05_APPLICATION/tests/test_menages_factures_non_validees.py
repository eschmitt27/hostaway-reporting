"""Une facture fournisseur non validée n'a AUCUN effet économique (mission §A + §B + §C).

Règle métier vérifiée ici, de bout en bout :

    facture reçue (A_CONTROLER)  -> visible au rapprochement, anomalies visibles, impact 0
    facture validée (VALIDEE...)  -> entre dans le calcul économique

Le défaut corrigé était réel : les requêtes `facture_lignes_menage JOIN factures` de lot6d/6e/6f ne
filtraient que `type_ligne`. Une ligne d'une facture A_CONTROLER traversait donc lot6e/lot6f jusqu'à
`menages_cout_complet`, puis ressortait en TYPE_FLUX_018/019 dans lot9 — une charge économique née
d'un document que personne n'avait accepté.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

import app.config as cfg

_TRAVAIL = Path(cfg.APP_ROOT).parent / "02_TRAVAIL"
if str(_TRAVAIL) not in sys.path:
    sys.path.insert(0, str(_TRAVAIL))

from app.services import menages_backfill_historique_service as bf   # noqa: E402

CLASSEUR_EXTERNES = (Path(cfg.APP_ROOT).parent / "02_TRAVAIL" / "Lot6c_MenagesExternes"
                     / "MASTER_FACT_MEN_MenagesExternes.xlsx")

# Référence figée du classeur legacy (mesurée sur le fichier réel, cf. mission §B1).
REF_LIGNES = 13
REF_VALIDE = 12
REF_TOTAL_ECONOMIQUE = 2381.00
REF_SHA256 = "f92eaee4f3cfa76daee3f367ef35a98950356f907f8ca5419b28912f18191792"

classeur_reel = pytest.mark.skipif(not CLASSEUR_EXTERNES.exists(),
                                   reason="classeur legacy Lot6c absent d'un checkout propre")


def _facture(conn, *, ref, statut, logement, montant, date_facture="2026-07-15"):
    """Insère une facture + sa ligne ménage externe. Aucun service : on teste la REQUÊTE SQL."""
    fid = f"FAC-{ref}"
    conn.execute(
        "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, "
        "date_facture, montant_ttc, statut, source, version) "
        "VALUES (?,?,?,?,?,?,'TEST',1)",
        (fid, "FOU-1", ref, date_facture, montant, statut))
    conn.execute(
        "INSERT INTO facture_lignes_menage (ligne_id_opaque, facture_id_opaque, type_ligne, "
        "logement_id, montant_ttc, source) VALUES (?,?,'MENAGE_EXTERNE',?,?, 'TEST')",
        (f"LIG-{ref}", fid, logement, montant))
    conn.commit()
    return fid


def _lignes_economiques(db_path):
    """Ce que le lecteur unique de lot9 retient comme économiquement réel."""
    return bf.menages_externes_economiques(db_path=db_path)


# ── §A — le filtre par statut ───────────────────────────────────────────────────────────────────

def test_01_facture_a_controler_n_est_pas_economique(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _facture(conn, ref="A1", statut="A_CONTROLER", logement="LOG_0003", montant=36.0)
    conn.close()
    assert _lignes_economiques(tmp_db) == []


def test_02_facture_validee_est_economique(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _facture(conn, ref="V1", statut="VALIDEE", logement="LOG_0002", montant=260.0)
    conn.close()
    lignes = _lignes_economiques(tmp_db)
    assert [l["montant_ligne_ttc"] for l in lignes] == [260.0]
    assert lignes[0]["origine"] == "FACTURE_VALIDEE"


def test_03_facture_reglee_reste_economique(tmp_db):
    """Une facture PAYÉE a forcément été validée : la filtrer sur VALIDEE seule la ferait
    disparaître du résultat alors que la charge est bien réelle."""
    conn = sqlite3.connect(str(tmp_db))
    _facture(conn, ref="R1", statut="REGLEE", logement="LOG_0002", montant=100.0)
    _facture(conn, ref="P1", statut="PARTIELLEMENT_REGLEE", logement="LOG_0006", montant=50.0)
    conn.close()
    assert sorted(l["montant_ligne_ttc"] for l in _lignes_economiques(tmp_db)) == [50.0, 100.0]


def test_04_statuts_moteur_et_application_restent_synchronises():
    """Le moteur (02_TRAVAIL) ne peut pas importer l'application : il recopie la liste des statuts
    comptables. Ce test est le verrou qui empêche les deux copies de diverger en silence."""
    import lib_db_moteur as dbm
    from app.services.factures_service import STATUTS_COMPTABLES

    assert set(dbm.STATUTS_FACTURE_COMPTABLES) == set(STATUTS_COMPTABLES)


def test_05_le_fragment_sql_ne_retient_que_les_statuts_comptables():
    import lib_db_moteur as dbm

    fragment = dbm.filtre_sql_factures_comptables("f")
    assert "f.statut IN (" in fragment
    assert "'A_CONTROLER'" not in fragment
    for statut in dbm.STATUTS_FACTURE_COMPTABLES:
        assert f"'{statut}'" in fragment


def test_06_melange_validee_et_a_controler_ne_retient_que_la_validee(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _facture(conn, ref="MIX-OK", statut="VALIDEE", logement="LOG_0002", montant=200.0)
    _facture(conn, ref="MIX-KO", statut="A_CONTROLER", logement="LOG_0003", montant=36.0)
    conn.close()
    lignes = _lignes_economiques(tmp_db)
    assert [l["montant_ligne_ttc"] for l in lignes] == [200.0]


# ── §B — backfill historique figé des ménages externes ──────────────────────────────────────────

@classeur_reel
def test_07_backfill_importe_les_treize_lignes_historiques(tmp_db):
    r = bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                        mois_autorises=["2026-05"], statut_cloture="CLOTURE")
    assert r["ok"] is True
    assert r["nb_lignes"] == REF_LIGNES
    assert r["nb_valide"] == REF_VALIDE
    assert r["nb_non_valide"] == REF_LIGNES - REF_VALIDE
    assert r["source_hash"] == REF_SHA256


@classeur_reel
def test_08_les_douze_validees_reproduisent_l_impact_legacy(tmp_db):
    bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                    mois_autorises=["2026-05"], statut_cloture="CLOTURE")
    lignes = [l for l in _lignes_economiques(tmp_db) if l["origine"] == "HISTORIQUE_FIGE"]
    assert len(lignes) == REF_VALIDE
    total = round(sum(float(l["montant_ligne_ttc"] or 0) for l in lignes), 2)
    assert total == REF_TOTAL_ECONOMIQUE


@classeur_reel
def test_09_la_ligne_non_validee_est_conservee_mais_non_economique(tmp_db):
    """L'historique n'est jamais amputé : la ligne A_CONTROLER legacy est stockée, simplement pas
    retenue par le lecteur économique — exactement le comportement de l'ancien `men_valide`."""
    bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                    mois_autorises=["2026-05"], statut_cloture="CLOTURE")
    conn = sqlite3.connect(str(tmp_db))
    try:
        stockees = conn.execute("SELECT COUNT(*) FROM menages_externes_historique").fetchone()[0]
        non_valide = conn.execute(
            "SELECT COUNT(*) FROM menages_externes_historique WHERE statut_source <> 'VALIDE'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert stockees == REF_LIGNES
    assert non_valide == REF_LIGNES - REF_VALIDE
    economiques = [l for l in _lignes_economiques(tmp_db) if l["origine"] == "HISTORIQUE_FIGE"]
    assert len(economiques) == REF_VALIDE


@classeur_reel
def test_10_backfill_idempotent(tmp_db):
    for _ in range(2):
        bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                        mois_autorises=["2026-05"], statut_cloture="CLOTURE")
    conn = sqlite3.connect(str(tmp_db))
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM menages_externes_historique").fetchone()[0] == REF_LIGNES
    finally:
        conn.close()


@classeur_reel
def test_11_backfill_ne_touche_pas_la_cloture(tmp_db):
    """Un import d'historique n'est pas un recalcul : il ne rouvre pas le mois."""
    conn = sqlite3.connect(str(tmp_db))
    conn.execute("INSERT INTO ref_cloture_mensuelle (mois, statut_mois, import_id) "
                 "VALUES ('2026-05','CLOTURE','TEST')")
    conn.commit()
    conn.close()

    bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                    mois_autorises=["2026-05"], statut_cloture="CLOTURE")

    conn = sqlite3.connect(str(tmp_db))
    try:
        assert conn.execute(
            "SELECT statut_mois FROM ref_cloture_mensuelle WHERE mois='2026-05'"
        ).fetchone()[0] == "CLOTURE"
    finally:
        conn.close()


@classeur_reel
def test_12_backfill_ne_fabrique_aucune_facture(tmp_db):
    """§B2 : l'historique legacy ne doit JAMAIS créer de faux objets métier dans le cycle
    fournisseur — sinon un écran de validation proposerait de valider des factures fantômes."""
    bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                    mois_autorises=["2026-05"], statut_cloture="CLOTURE")
    conn = sqlite3.connect(str(tmp_db))
    try:
        assert conn.execute("SELECT COUNT(*) FROM factures").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM facture_lignes_menage").fetchone()[0] == 0
    finally:
        conn.close()


# ── §C — interface unique ───────────────────────────────────────────────────────────────────────

@classeur_reel
def test_13_interface_unique_fusionne_historique_et_factures_validees(tmp_db):
    bf.importer_externes_historique(chemin_classeur=CLASSEUR_EXTERNES, db_path=tmp_db,
                                    mois_autorises=["2026-05"], statut_cloture="CLOTURE")
    conn = sqlite3.connect(str(tmp_db))
    _facture(conn, ref="NEW", statut="VALIDEE", logement="LOG_0002", montant=90.0)
    _facture(conn, ref="ATT", statut="A_CONTROLER", logement="LOG_0003", montant=36.0)
    conn.close()

    lignes = _lignes_economiques(tmp_db)
    origines = {l["origine"] for l in lignes}
    assert origines == {"HISTORIQUE_FIGE", "FACTURE_VALIDEE"}
    assert len(lignes) == REF_VALIDE + 1
    total = round(sum(float(l["montant_ligne_ttc"] or 0) for l in lignes), 2)
    assert total == round(REF_TOTAL_ECONOMIQUE + 90.0, 2)
    # Le contrat de sortie est identique quelle que soit l'origine : lot9 ne fait pas la différence.
    for l in lignes:
        assert set(l) >= {"source_pk", "mois", "logement_id", "montant_ligne_ttc",
                          "type_flux_id", "sens", "code_impact"}
        assert l["type_flux_id"] == "TYPE_FLUX_014"


def test_14_aucune_facture_aucune_ligne_economique(tmp_db):
    """Base sans historique ni facture : le lecteur rend une liste vide, jamais une erreur."""
    assert _lignes_economiques(tmp_db) == []

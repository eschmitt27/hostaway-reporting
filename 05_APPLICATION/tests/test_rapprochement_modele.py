"""APP-3F — Modèle de rapprochement et machine à états (commit 1).

Aucun paiement, aucun virement, aucune donnée bancaire sensible. Version optimiste, historique
append-only, aucune suppression physique.
"""
import pytest

from app.services import rapprochement_reglements_service as rap

REG = "REG-test3f01"


def _r(db):
    return rap.creer_ou_charger(REG, db_path=db)


def test_01_creation_non_rapproche(tmp_db):
    r = _r(tmp_db)
    assert r["statut"] == rap.ST_NON_RAPPROCHE and r["rapprochement_id_opaque"].startswith("RAP-")


def test_02_creer_ou_charger_idempotent(tmp_db):
    r1 = _r(tmp_db); r2 = _r(tmp_db)
    assert r1["id"] == r2["id"]


def _proposer(db, r, mvt="MVT-abc0000000"):
    return rap.enregistrer_proposition(
        r, mvt, criteres=["montant_exact", "sens_sortant"], score=2, mouvement_empreinte="emp1",
        ecart_montant=0.0, ecart_jours=1, version_attendue=r["version"], db_path=db)


def test_03_proposition_disponible(tmp_db):
    r = _r(tmp_db)
    r = _proposer(tmp_db, r)
    assert r["statut"] == rap.ST_PROPOSITION_DISPONIBLE and r["mouvement_id_opaque"] == "MVT-abc0000000"
    assert r["score_explicable"] == 2


def test_04_passage_a_controler_sans_candidat_refuse(tmp_db):
    r = _r(tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.passer_a_controler(r, version_attendue=r["version"], db_path=tmp_db)


def test_05_passage_a_controler(tmp_db):
    r = _proposer(tmp_db, _r(tmp_db))
    r = rap.passer_a_controler(r, version_attendue=r["version"], db_path=tmp_db)
    assert r["statut"] == rap.ST_A_CONTROLER


def test_06_confirmation_reglement_non_paye_refusee(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.confirmer(r, reglement_paye=False, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)


def test_07_confirmation_mouvement_disparu_refusee(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.confirmer(r, reglement_paye=True, mouvement_present=False, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)


def test_08_confirmation_mouvement_entrant_refusee(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=False,
                      version_attendue=r["version"], db_path=tmp_db)


def test_09_confirmation_humaine(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    r = rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)
    assert r["statut"] == rap.ST_RAPPROCHE and r["decision"] == "CONFIRME"


def test_10_double_confirmation_refusee(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    r = rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)   # RAPPROCHE -> RAPPROCHE interdit


def test_11_reglement_deja_rapproche_bloque_unicite(tmp_db):
    """Un 2e mouvement confirmé sur le même règlement viole l'index d'unicité actif."""
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                  version_attendue=r["version"], db_path=tmp_db)
    # un second rapprochement actif pour le même REG est refusé par l'index partiel
    import sqlite3
    from app.db.connection import get_db
    conn = get_db(tmp_db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO rapprochements_reglements (rapprochement_id_opaque, releve_id_opaque) "
                     "VALUES (?,?)", ("RAP-dup0000000", REG))
        conn.commit()
    conn.close()


def test_12_mouvement_deja_utilise_bloque_unicite(tmp_db):
    """Deux règlements ne peuvent pas confirmer le même mouvement (index partiel RAPPROCHE)."""
    import sqlite3
    from app.db.connection import get_db
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db), mvt="MVT-shared00000"), db_path=tmp_db)
    rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                  version_attendue=r["version"], db_path=tmp_db)
    r2 = rap.creer_ou_charger("REG-autre02", db_path=tmp_db)
    r2 = rap.enregistrer_proposition(r2, "MVT-shared00000", criteres=["x"], score=1,
                                     mouvement_empreinte="e", ecart_montant=0.0, ecart_jours=0,
                                     version_attendue=r2["version"], db_path=tmp_db)
    r2 = rap.passer_a_controler(r2, version_attendue=r2["version"], db_path=tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.confirmer(r2, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r2["version"], db_path=tmp_db)


def test_13_ecartement_sans_motif_refuse(tmp_db):
    r = _proposer(tmp_db, _r(tmp_db))
    with pytest.raises(rap.RapprochementRefuse):
        rap.ecarter(r, "", version_attendue=r["version"], db_path=tmp_db)


def test_14_ecartement(tmp_db):
    r = _proposer(tmp_db, _r(tmp_db))
    r = rap.ecarter(r, "montant différent", version_attendue=r["version"], db_path=tmp_db)
    assert r["statut"] == rap.ST_ECARTE and r["motif"] == "montant différent"


def test_15_anomalie(tmp_db):
    r = _r(tmp_db)
    r = rap.signaler_anomalie(r, "plusieurs candidats ambigus", version_attendue=r["version"], db_path=tmp_db)
    assert r["statut"] == rap.ST_ANOMALIE


def test_16_reouverture_sans_motif_refusee(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    r = rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.rouvrir(r, "", version_attendue=r["version"], db_path=tmp_db)


def test_17_reouverture_avec_motif(tmp_db):
    r = rap.passer_a_controler(_proposer(tmp_db, _r(tmp_db)), db_path=tmp_db)
    r = rap.confirmer(r, reglement_paye=True, mouvement_present=True, sens_sortant=True,
                      version_attendue=r["version"], db_path=tmp_db)
    r = rap.rouvrir(r, "erreur de mouvement", version_attendue=r["version"], db_path=tmp_db)
    assert r["statut"] == rap.ST_ROUVERT


def test_18_annulation_sans_motif_refusee(tmp_db):
    r = _r(tmp_db)
    with pytest.raises(rap.RapprochementRefuse):
        rap.annuler(r, "", version_attendue=r["version"], db_path=tmp_db)


def test_19_version_obsolete_refusee(tmp_db):
    r = _r(tmp_db)
    _proposer(tmp_db, r)
    with pytest.raises(rap.RapprochementRefuse):
        rap.signaler_anomalie(r, "x", version_attendue=r["version"], db_path=tmp_db)   # version périmée


def test_20_transition_rejouee_refusee(tmp_db):
    r = _proposer(tmp_db, _r(tmp_db))
    with pytest.raises(rap.RapprochementRefuse):
        rap.enregistrer_proposition(r, "MVT-x", criteres=[], score=0, mouvement_empreinte="e",
                                    ecart_montant=0.0, ecart_jours=0, version_attendue=r["version"],
                                    db_path=tmp_db)   # PROPOSITION_DISPONIBLE -> lui-même interdit


def test_21_historique_append_only(tmp_db):
    from app.db.connection import get_db
    r = _proposer(tmp_db, _r(tmp_db))
    conn = get_db(tmp_db)
    n = conn.execute("SELECT COUNT(*) FROM rapprochement_evenements WHERE rapprochement_id_opaque=?",
                     (r["rapprochement_id_opaque"],)).fetchone()[0]
    conn.close()
    assert n >= 2   # CREATION + TRANSITION


def test_22_aucune_suppression_physique(tmp_db):
    r = _r(tmp_db)
    r = rap.annuler(r, "abandon", version_attendue=r["version"], db_path=tmp_db)
    assert rap.charger(r["rapprochement_id_opaque"], db_path=tmp_db) is not None   # toujours là, ANNULE


def test_23_aucun_champ_bancaire_dans_le_modele(tmp_db):
    r = _proposer(tmp_db, _r(tmp_db))
    for interdit in ("iban", "rib", "bic", "numero_compte", "compte", "libelle_brut"):
        assert interdit not in r


def test_24_migration_0014_idempotente(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "m14.db"
    apply_migrations(db); apply_migrations(db)
    conn = get_db(db)
    tables = {x[0] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"rapprochements_reglements", "rapprochement_evenements"} <= tables


def test_25_mention_non_preuve_bancaire():
    assert "ne constitue ni un ordre de paiement ni une preuve bancaire certifiée" in rap.MENTION

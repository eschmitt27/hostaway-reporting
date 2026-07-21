"""APP-3E — Association historisée fournisseur ↔ logement.

Distincte de l'intervenant ménage du moteur (lecture seule) et de l'affectation flat par charge.
Aucun montant. Version optimiste, historique append-only, aucune suppression physique.
"""
import pytest

from app.services import fournisseur_rattachements_service as fl
from app.services import fournisseurs_referentiel_service as frs


def _fournisseur(db, nom="Maintenance SARL", statut_actif=True):
    f = frs.creer(nom, "MAINTENANCE", db_path=db)
    if not statut_actif:
        f = frs.desactiver(f, version_attendue=f["version"], db_path=db)
    return f


def test_01_creation_association(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                    date_debut="2026-01-01", db_path=tmp_db)
    assert a["association_id_opaque"].startswith("FLG-") and a["statut"] == "ACTIF"
    assert a["date_fin"] is None


def test_02_logement_vide_refuse(tmp_db):
    f = _fournisseur(tmp_db)
    with pytest.raises(fl.AssociationRefusee):
        fl.associer(f["fournisseur_id_opaque"], "", date_debut="2026-01-01", db_path=tmp_db)


def test_03_type_prestation_invalide_refuse(tmp_db):
    f = _fournisseur(tmp_db)
    with pytest.raises(fl.AssociationRefusee):
        fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="ZZZ",
                    date_debut="2026-01-01", db_path=tmp_db)


def test_04_fournisseur_inconnu_refuse(tmp_db):
    with pytest.raises(fl.AssociationRefusee):
        fl.associer("FRS-0000000000", "LOG1", date_debut="2026-01-01", db_path=tmp_db)


def test_05_fournisseur_inactif_refuse(tmp_db):
    f = _fournisseur(tmp_db, statut_actif=False)
    with pytest.raises(fl.AssociationRefusee):
        fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)


def test_06_chevauchement_periode_ouverte_refuse(tmp_db):
    f = _fournisseur(tmp_db)
    fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                date_debut="2026-01-01", db_path=tmp_db)
    with pytest.raises(fl.AssociationRefusee):
        fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                    date_debut="2026-02-01", db_path=tmp_db)


def test_07_fermeture_periode(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    a2 = fl.fermer(a, "2026-06-30", version_attendue=a["version"], db_path=tmp_db)
    assert a2["date_fin"] == "2026-06-30"


def test_08_fermeture_deja_fermee_refuse(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    a = fl.fermer(a, "2026-06-30", version_attendue=a["version"], db_path=tmp_db)
    with pytest.raises(fl.AssociationRefusee):
        fl.fermer(a, "2026-07-30", version_attendue=a["version"], db_path=tmp_db)


def test_09_reouverture_apres_fermeture_autorisee(tmp_db):
    """Après fermeture, une nouvelle période peut être ouverte (plus de chevauchement)."""
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                    date_debut="2026-01-01", db_path=tmp_db)
    fl.fermer(a, "2026-06-30", version_attendue=a["version"], db_path=tmp_db)
    a2 = fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                     date_debut="2026-07-01", db_path=tmp_db)
    assert a2["date_fin"] is None


def test_10_changer_fournisseur_ferme_ancien_ouvre_nouveau(tmp_db):
    f1 = _fournisseur(tmp_db, nom="Ancien")
    f2 = _fournisseur(tmp_db, nom="Nouveau")
    a1 = fl.associer(f1["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                     date_debut="2026-01-01", db_path=tmp_db)
    a2 = fl.changer_fournisseur("LOG1", a1, f2["fournisseur_id_opaque"], date_bascule="2026-07-01",
                                type_prestation="MAINTENANCE", db_path=tmp_db)
    ancien = fl.charger_par_opaque(a1["association_id_opaque"], db_path=tmp_db)
    assert ancien["date_fin"] == "2026-07-01"
    assert a2["fournisseur_id_opaque"] == f2["fournisseur_id_opaque"] and a2["date_fin"] is None


def test_11_version_obsolete_refusee(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    fl.fermer(a, "2026-06-30", version_attendue=a["version"], db_path=tmp_db)
    with pytest.raises(fl.AssociationRefusee):
        fl.desactiver(a, version_attendue=a["version"], db_path=tmp_db)   # version périmée


def test_12_desactivation(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    a2 = fl.desactiver(a, version_attendue=a["version"], db_path=tmp_db)
    assert a2["statut"] == "INACTIF"


def test_13_aucune_suppression_physique(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    fl.desactiver(a, version_attendue=a["version"], db_path=tmp_db)
    assert fl.charger_par_opaque(a["association_id_opaque"], db_path=tmp_db) is not None


def test_14_historique_append_only(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    fl.fermer(a, "2026-06-30", version_attendue=a["version"], db_path=tmp_db)
    hist = fl.historique(a["association_id_opaque"], db_path=tmp_db)
    assert len(hist) >= 2   # CREATION + FERMETURE


def test_15_lister_par_logement(tmp_db):
    f = _fournisseur(tmp_db)
    fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MAINTENANCE",
                date_debut="2026-01-01", db_path=tmp_db)
    fl.associer(f["fournisseur_id_opaque"], "LOG1", type_prestation="MENAGE",
                date_debut="2026-01-01", db_path=tmp_db)
    rows = fl.lister_par_logement("LOG1", db_path=tmp_db)
    assert len(rows) == 2


def test_16_aucun_montant_dans_le_modele(tmp_db):
    f = _fournisseur(tmp_db)
    a = fl.associer(f["fournisseur_id_opaque"], "LOG1", date_debut="2026-01-01", db_path=tmp_db)
    for champ_interdit in ("montant", "cout", "prix", "total"):
        assert champ_interdit not in a


def test_17_migration_0013_idempotente(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "m13.db"
    apply_migrations(db); apply_migrations(db)
    conn = get_db(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"fournisseur_rattachements", "fournisseur_rattachement_evenements"} <= tables

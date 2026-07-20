"""APP-3D — Référentiel fournisseur minimal. Aucune donnée bancaire, aucune écriture réelle."""
import pytest

from app.services import fournisseurs_referentiel_service as frs


def test_01_creation_fournisseur(tmp_db):
    f = frs.creer("Ménage Pro SARL", "MENAGE", acteur="t", db_path=tmp_db)
    assert f["fournisseur_id_opaque"].startswith("FRS-") and f["statut"] == "ACTIF"


def test_02_nom_vide_refuse(tmp_db):
    with pytest.raises(frs.FournisseurRefuse):
        frs.creer("", "MENAGE", db_path=tmp_db)


def test_03_type_invalide_refuse(tmp_db):
    with pytest.raises(frs.FournisseurRefuse):
        frs.creer("X", "TYPE_INCONNU", db_path=tmp_db)


def test_04_modification(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    f2 = frs.modifier(f, nom="B", acteur="t", version_attendue=f["version"], db_path=tmp_db)
    assert f2["nom"] == "B" and f2["version"] == 2


def test_05_desactivation(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    f2 = frs.desactiver(f, acteur="t", version_attendue=f["version"], db_path=tmp_db)
    assert f2["statut"] == "INACTIF"


def test_06_reactivation(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    f = frs.desactiver(f, version_attendue=f["version"], db_path=tmp_db)
    f = frs.reactiver(f, version_attendue=f["version"], db_path=tmp_db)
    assert f["statut"] == "ACTIF"


def test_07_desactivation_double_refusee(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    f = frs.desactiver(f, version_attendue=f["version"], db_path=tmp_db)
    with pytest.raises(frs.FournisseurRefuse):
        frs.desactiver(f, version_attendue=f["version"], db_path=tmp_db)


def test_08_recherche_doublons_insensible_casse(tmp_db):
    frs.creer("Ménage Pro SARL", "MENAGE", db_path=tmp_db)
    dups = frs.rechercher_doublons("MÉNAGE pro sarl", db_path=tmp_db)
    assert len(dups) == 1


def test_09_aucune_suppression_physique(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    frs.desactiver(f, version_attendue=f["version"], db_path=tmp_db)
    # toujours present, juste inactif (actif=1 en base, statut=INACTIF)
    tous = frs.lister(db_path=tmp_db)
    assert any(x["fournisseur_id_opaque"] == f["fournisseur_id_opaque"] for x in tous)


def test_10_historique_conserve(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    frs.modifier(f, commentaire="x", version_attendue=f["version"], db_path=tmp_db)
    hist = frs.historique(f["fournisseur_id_opaque"], db_path=tmp_db)
    assert len(hist) >= 2   # CREATION + MODIFICATION


def test_11_version_obsolete_refusee(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    frs.modifier(f, nom="B", version_attendue=f["version"], db_path=tmp_db)
    with pytest.raises(frs.FournisseurRefuse):
        frs.modifier(f, nom="C", version_attendue=f["version"], db_path=tmp_db)   # version perimee


def test_12_aucun_champ_bancaire_dans_le_modele(tmp_db):
    f = frs.creer("A", "AUTRE", db_path=tmp_db)
    for champ_interdit in ("iban", "compte_bancaire", "numero_carte", "rib"):
        assert champ_interdit not in f


def test_13_route_liste_200(client):
    r = client.get("/referentiel-fournisseurs")
    assert r.status_code == 200


def test_14_route_creation_puis_liste(client, tmp_db):
    r = client.post("/referentiel-fournisseurs/creer", data={"nom": "Test SARL", "type": "MENAGE"})
    r2 = client.get("/referentiel-fournisseurs")
    assert "Test SARL" in r2.text


def test_15_route_404_inconnu(client):
    r = client.get("/referentiel-fournisseurs/FRS-0000000000")
    assert r.status_code == 404


def test_16_headers_securite(client):
    r = client.get("/referentiel-fournisseurs")
    assert r.headers.get("cache-control") == "no-store"


def test_17_migration_0010_cree_tables(tmp_path):
    from app.db.connection import apply_migrations, get_db
    db = tmp_path / "m10.db"
    apply_migrations(db)
    conn = get_db(db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert {"fournisseurs", "fournisseur_evenements"} <= tables

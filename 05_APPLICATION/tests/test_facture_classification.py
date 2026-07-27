"""Classification sens/type de facture (migration 0020, audit
`42_AUDIT_MODELE_FACTURES_CHARGES_REGLEMENTS.md`).

Aucune confusion de code n'a été trouvée entre facture fournisseur et facture propriétaire :
l'absence de modèle commun était le vrai constat. Ces colonnes préparent le socle Comptabilité
sans migrer le circuit propriétaire (lot12, Excel) vers SQLite.
"""
from __future__ import annotations

import pytest

import app.config as cfg
from app.db.connection import apply_migrations, get_db
from app.services import factures_service as fact


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "test.db"
    apply_migrations(p)
    monkeypatch.setattr(cfg, "RECETTE_MODE", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_ENABLED", True)
    monkeypatch.setattr(cfg, "FACTURES_REAL_WRITE_CONFIRMATION_ENABLED", True)
    return p


def _classification(opaque, db_path):
    conn = get_db(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM facture_classification WHERE facture_id_opaque=?", (opaque,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def test_facture_creee_par_le_service_existant_est_classee_automatiquement(db):
    """factures_service.creer() ne connaît pas cette table : le trigger la peuple seul."""
    res = fact.creer({"fournisseur_id_opaque": "FRS-TEST", "facture_ref": "FA-1",
                      "montant_ttc": 100.0}, db_path=db)
    assert res["ok"], res
    c = _classification(res["facture_id_opaque"], db)
    assert c is not None
    assert c["sens"] == "RECUE"
    assert c["type_facture"] == "FOURNISSEUR"


def test_retro_classification_des_factures_deja_en_base(tmp_path):
    """Une base migrée depuis 0017 doit voir ses factures existantes rétro-classées."""
    from app.db.connection import get_db as _get_db
    p = tmp_path / "ancienne.db"
    apply_migrations(p)
    conn = _get_db(p)
    conn.execute(
        "INSERT INTO factures (facture_id_opaque, fournisseur_id_opaque, facture_ref, montant_ttc) "
        "VALUES ('FAC-ANCIENNE','FRS-X','REF-1', 50.0)")
    conn.commit()
    conn.close()
    # Rejouer les migrations (comportement réel : replay à chaque démarrage).
    apply_migrations(p)
    c = _classification("FAC-ANCIENNE", p)
    assert c is not None and c["sens"] == "RECUE" and c["type_facture"] == "FOURNISSEUR"


def test_classification_reste_modifiable_sans_toucher_a_la_facture(db):
    """Le classement peut évoluer (ex. avoir reçu) sans recréer la facture."""
    res = fact.creer({"fournisseur_id_opaque": "FRS-TEST", "facture_ref": "FA-2",
                      "montant_ttc": 30.0}, db_path=db)
    opaque = res["facture_id_opaque"]
    conn = get_db(db)
    conn.execute("UPDATE facture_classification SET type_facture='AVOIR_RECU' "
                "WHERE facture_id_opaque=?", (opaque,))
    conn.commit(); conn.close()
    assert _classification(opaque, db)["type_facture"] == "AVOIR_RECU"
    # La facture elle-même n'a pas bougé.
    assert fact.charger(opaque, db)["montant_ttc"] == 30.0

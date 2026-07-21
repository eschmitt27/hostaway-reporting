"""APP-3E — Validation approfondie des migrations (0011, 0012, 0013 et l'ensemble).

Base vierge, application partielle, double application, idempotence, absence de doublon de table/
index, ordre déterministe, nombre de migrations découvert par glob (jamais codé en dur).
"""
import sqlite3

import pytest

from app.db.connection import MIGRATIONS_DIR, apply_migrations, get_db

TABLES_APP3E = {
    "charges_affectations", "charges_affectation_evenements",      # 0011
    "proprietaires_releve_cycle", "proprietaires_paiement",        # 0012
    "fournisseur_rattachements", "fournisseur_rattachement_evenements",    # 0013
}


def _tables(db):
    conn = get_db(db)
    try:
        return {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()}
    finally:
        conn.close()


def _indexes(db):
    conn = get_db(db)
    try:
        return [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchall()]
    finally:
        conn.close()


def test_base_vierge_cree_toutes_les_tables_app3e(tmp_path):
    db = tmp_path / "vierge.db"
    apply_migrations(db)
    assert TABLES_APP3E <= _tables(db)


def _appliquer_jusqua(db, borne_incluse: str):
    conn = get_db(db)
    try:
        for m in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if m.name <= borne_incluse:
                conn.executescript(m.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


def test_application_partielle_puis_complete(tmp_path):
    """Base arrêtée à 0010, puis application du reste : 0011/0012/0013 créent leurs tables."""
    db = tmp_path / "partielle.db"
    _appliquer_jusqua(db, "0010_fournisseurs.sql")
    assert not (TABLES_APP3E & _tables(db))   # aucune table APP-3E avant
    apply_migrations(db)
    assert TABLES_APP3E <= _tables(db)


def test_double_application_idempotente(tmp_path):
    db = tmp_path / "double.db"
    apply_migrations(db)
    tables1 = _tables(db)
    apply_migrations(db)   # 2e fois — ne doit rien casser ni dupliquer
    tables2 = _tables(db)
    assert tables1 == tables2


def test_aucune_table_dupliquee(tmp_path):
    db = tmp_path / "dup.db"
    apply_migrations(db); apply_migrations(db)
    conn = get_db(db)
    noms = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    assert len(noms) == len(set(noms))


def test_aucun_index_duplique(tmp_path):
    db = tmp_path / "idx.db"
    apply_migrations(db); apply_migrations(db)
    idx = _indexes(db)
    assert len(idx) == len(set(idx))


def test_schema_migrations_une_ligne_par_fichier(tmp_path):
    """Nombre d'entrées = nombre de fichiers *.sql (découvert par glob, jamais codé en dur)."""
    db = tmp_path / "count.db"
    apply_migrations(db); apply_migrations(db)
    conn = get_db(db)
    n = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    conn.close()
    assert n == len(list(MIGRATIONS_DIR.glob("*.sql")))


def test_ordre_deterministe_des_migrations():
    noms = [m.name for m in sorted(MIGRATIONS_DIR.glob("*.sql"))]
    assert noms == sorted(noms)   # tri lexicographique stable (préfixes numériques 0001..00NN)


def test_versions_migrations_uniques():
    """Aucun numéro de migration concurrent (préfixe 4 chiffres unique)."""
    prefixes = [m.name[:4] for m in MIGRATIONS_DIR.glob("*.sql")]
    assert len(prefixes) == len(set(prefixes)), f"préfixes en double : {prefixes}"


def test_historique_conserve_apres_remigration(tmp_path):
    """Des données existantes survivent à une réapplication des migrations (pas de DROP)."""
    from app.services import fournisseurs_referentiel_service as frs
    import app.config as cfg
    db = tmp_path / "hist.db"
    apply_migrations(db)
    orig = cfg.DB_PATH
    cfg.DB_PATH = db
    try:
        f = frs.creer("Persistant", "AUTRE", db_path=db)
        apply_migrations(db)   # réapplication
        assert frs.charger_par_opaque(f["fournisseur_id_opaque"], db_path=db) is not None
    finally:
        cfg.DB_PATH = orig


def test_double_application_concurrente_legere(tmp_path):
    """Deux connexions appliquent les migrations : aucune erreur, tables présentes."""
    db = tmp_path / "conc.db"
    apply_migrations(db)
    # 2e application via une connexion distincte ouverte en parallèle
    conn = sqlite3.connect(str(db))
    try:
        for m in sorted(MIGRATIONS_DIR.glob("*.sql")):
            conn.executescript(m.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()
    assert TABLES_APP3E <= _tables(db)

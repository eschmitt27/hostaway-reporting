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
    """Deux applications successives des migrations : aucune erreur, tables présentes.

    La version d'origine rejouait les fichiers `.sql` BRUTS sur une base déjà migrée. Cette
    prémisse — « rejouer tout l'historique est sans effet » — a cessé d'être vraie le jour où des
    migrations de RECONSTRUCTION y sont entrées : `0060` recrée `charges` avec une liste de colonnes
    figée et un `INSERT … SELECT *`, qui casse dès que la table gagne une colonne (ce qu'a fait la
    migration 0076). C'est d'ailleurs exactement pour cette raison qu'`apply_migrations` ne rejoue
    plus que les fichiers MANQUANTS.

    Le test exerce donc désormais le point d'entrée réel, qui est aussi le seul que l'application
    utilise — et vérifie que le rejouer est sans effet, ce qui est la vraie garantie attendue.
    """
    db = tmp_path / "conc.db"
    apply_migrations(db)
    avant = _tables(db)
    apply_migrations(db)          # seconde application : doit être un no-op silencieux
    apply_migrations(db)
    assert TABLES_APP3E <= _tables(db)
    assert _tables(db) == avant, "une réapplication ne doit ni créer ni supprimer de table"


def test_rejouer_les_fichiers_bruts_n_est_pas_un_contrat_supporte(tmp_path):
    """Documente la limite : les fichiers de migration ne sont PAS tous rejouables isolément.

    Ce test ne dénonce pas un défaut — il fige une frontière. Une migration de reconstruction est
    légitime ; ce qui ne l'est pas, c'est de la rejouer sur une base déjà transformée. Passer par
    `apply_migrations` est le contrat ; lire ce test évite de croire l'inverse.
    """
    db = tmp_path / "brut.db"
    apply_migrations(db)
    conn = sqlite3.connect(str(db))
    try:
        colonnes = {r[1] for r in conn.execute("PRAGMA table_info(charges)")}
        assert "affectable_menage" in colonnes, "0076 doit avoir ajouté la colonne"
        contenu = (MIGRATIONS_DIR / "0060_durcissement_sqlite_final.sql").read_text(
            encoding="utf-8")
        # `INSERT INTO charges_new SELECT *` vers une liste de colonnes figée : le rejeu échoue,
        # et c'est cohérent — ce n'est pas une opération que l'application effectue.
        with pytest.raises(sqlite3.OperationalError):
            conn.executescript(contenu)
    finally:
        conn.close()

"""APP-0 — Migrations SQLite idempotentes."""
from pathlib import Path
from app.db.connection import apply_migrations, get_db

EXPECTED_TABLES = {
    "schema_migrations",
    "audit_events",
    "pipeline_runs",
    "snapshots",
    "screen_states",
    "drafts",
    "periods",
    "saisie_hh_writes",
    "menage_overrides",
    "saisie_charges_writes",   # 0004 — journal des tentatives d'écriture des charges (APP-3b)
    "menages_recalcul_runs",   # 0005 — journal des recalculs ménages (APP-2b)
    "banque_overrides",        # 0006 — journal des décisions bancaires (APP-4B)
    "banque_controle_runs",    # 0006 — runs d'enregistrement sur copie (APP-4B)
    "controles_suivi",             # 0007 — journal du suivi humain des contrôles (APP-5B)
    "controles_suivi_historique",  # 0007 — historique append-only du suivi (APP-5B)
    "controles_runs",              # 0007 — runs de recalcul moteur sur copies (APP-5B)
    "clotures_mensuelles",     # 0008 — suivi humain de la clôture mensuelle (APP-5C)
    "cloture_evenements",      # 0008 — historique append-only des événements de clôture (APP-5C)
    "cloture_elements",        # 0008 — snapshot des contrôles à la clôture (APP-5C)
    "cloture_documents",       # 0008 — métadonnées de preuves de clôture (APP-5C)
    "proprietaires_releves",             # 0009 — suivi humain des relevés propriétaires (APP-3D)
    "proprietaires_releve_evenements",   # 0009 — historique append-only des relevés (APP-3D)
    "fournisseurs",             # 0010 — référentiel fournisseur minimal (APP-3D)
    "fournisseur_evenements",   # 0010 — historique append-only fournisseurs (APP-3D)
    "charges_affectations",             # 0011 — affectation logique des charges (APP-3E)
    "charges_affectation_evenements",   # 0011 — historique append-only des affectations (APP-3E)
    "proprietaires_releve_cycle",   # 0012 — cycle de préparation du relevé, snapshot/dérive (APP-3E)
    "proprietaires_paiement",       # 0012 — préparation des règlements, sans virement (APP-3E)
    "fournisseur_rattachements",            # 0013 — association historisée fournisseur↔logement (APP-3E)
    "fournisseur_rattachement_evenements",  # 0013 — historique append-only de l'association (APP-3E)
    "rapprochements_reglements",   # 0014 — rapprochement déclaratif règlement↔mouvement (APP-3F)
    "rapprochement_evenements",    # 0014 — historique append-only du rapprochement (APP-3F)
    "banque_imports",                       # 0015 — journal des imports bancaires (module Banque)
    "banque_rapprochements",                # 0015 — rapprochement mouvement↔objet métier (module Banque)
    "banque_rapprochement_evenements",      # 0015 — historique append-only du rapprochement (module Banque)
}


def _get_tables(db_path: Path) -> set[str]:
    conn = get_db(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def test_migration_creates_all_tables(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    tables = _get_tables(db)
    assert EXPECTED_TABLES == tables, f"Tables manquantes : {EXPECTED_TABLES - tables}"


def test_migration_is_idempotent(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    apply_migrations(db)  # 2ème fois — ne doit pas planter
    tables = _get_tables(db)
    assert EXPECTED_TABLES == tables


def test_schema_migrations_has_version(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        row = conn.execute("SELECT version FROM schema_migrations WHERE version='0001'").fetchone()
        assert row is not None
    finally:
        conn.close()


def test_wal_mode(tmp_path):
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
    finally:
        conn.close()


def test_periods_mirror_not_authoritative(tmp_path):
    """La table periods est un miroir applicatif — elle accepte des valeurs de statut libres."""
    db = tmp_path / "test.db"
    apply_migrations(db)
    conn = get_db(db)
    try:
        conn.execute(
            "INSERT INTO periods (year_month, statut_mirror) VALUES ('2024-01', 'OUVERT')"
        )
        conn.commit()
        row = conn.execute("SELECT statut_mirror FROM periods WHERE year_month='2024-01'").fetchone()
        assert row[0] == "OUVERT"
    finally:
        conn.close()

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
    "banque_suggestion_decisions",          # 0016 — décisions sur les suggestions (module Banque)
    "fournisseur_details",                  # 0017 — champs métier étendus fournisseur
    "factures",                             # 0017 — factures fournisseurs (dette + pièce)
    "facture_evenements",                   # 0017 — historique append-only des factures
    "reglements_fournisseurs",              # 0017 — règlements fournisseurs (paiement effectué)
    "reglement_repartitions",               # 0017 — ventilation d'un règlement (paiement groupé)
    "calculs_runs",                         # 0018 — runs de pipeline de calcul
    "calculs_run_lots",                     # 0018 — un enregistrement par lot exécuté
    "calculs_indicateurs",                  # 0018 — indicateurs métier relevés par run
    "calculs_sauvegardes",                  # 0018 — sauvegardes des sorties (rollback)
    "cloture_statuts",                      # 0018 — statut mensuel applicatif
    "cloture_statut_evenements",            # 0018 — historique des transitions de clôture
    "menages",                              # 0019 — cycle de vie opérationnel du ménage unitaire
    "menage_evenements",                    # 0019 — historique append-only des ménages
    "fournisseur_menage_qualification",     # 0019 — qualification ménage du référentiel Fournisseurs
    "facture_classification",               # 0020 — sens/type de facture (audit Factures/Charges)
    "plan_comptable",                       # 0021 — socle Comptabilité
    "ecritures",                            # 0021
    "ecriture_lignes",                      # 0021
    "ecriture_evenements",                  # 0021
    "facture_lignes",                       # 0022 — lignes de facture (multi-charges/multi-logements)
    "mapping_categorie_compte",              # 0023 — mapping catégorie de charge -> compte (A_CONTROLER)
    "operations_caisse",                     # 0023 — journal CAISSE, cas sans objet existant
    "operations_diverses",                   # 0023 — OD, objet avec ses propres lignes
    "od_lignes",                              # 0023
    "periodes_comptables",                    # 0023 — périodes comptables (distinctes clôture pilotage)
    "periode_evenements",                     # 0023
    "mapping_comptable_regles",                 # 0024 — mappings comptables historisés
    "ecriture_ligne_ventilation",                # 0024 — ventilation analytique par ligne d'écriture
    "mouvements_tresorerie_proprietaires",              # 0025 — trésorerie propriétaires
    "mouvements_tresorerie_proprietaires_evenements",   # 0025 — historique append-only
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

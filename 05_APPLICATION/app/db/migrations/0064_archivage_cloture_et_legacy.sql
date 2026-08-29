-- Migration 0064 — Archive économique des mois clôturés + classification legacy (mission 15).
--
-- `mois_archive_reglement` : niveau agrégé (mois x logement) du bloc RÉGLEMENT Lot10, figé au
-- moment réel de la clôture (`cloture_archivage_service.archiver_mois`). Le niveau réservation
-- réutilise `reservations_historique_cloture` (migration 0034) — pas de duplication.
CREATE TABLE IF NOT EXISTS mois_archive_reglement (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                            TEXT NOT NULL,
    logement_id                     TEXT,
    proprietaire_id                 TEXT,
    total_commission_mois           REAL,
    total_menage_mois               REAL,
    total_preparation_canape_mois   REAL,
    charges_exceptionnelles_refacturees REAL,
    montant_du_conciergerie         REAL,
    reste_a_payer_conciergerie      REAL,
    fige_le                        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mois_archive_reglement_unique
    ON mois_archive_reglement(mois, logement_id, proprietaire_id);

-- Classification explicite des mois "clôturés" pour lesquels AUCUNE archive économique
-- authentique n'a jamais été prise au moment réel de la clôture (les 17 mois legacy antérieurs à
-- cette mécanique — mission 14g/15). Jamais déduite automatiquement d'une absence d'archive
-- (un mois tout juste clos, pas encore archivé, n'est PAS un mois legacy) : classification
-- toujours EXPLICITE, un mois à la fois, avec motif.
CREATE TABLE IF NOT EXISTS mois_classification_legacy (
    mois              TEXT PRIMARY KEY,
    classification    TEXT NOT NULL DEFAULT 'LEGACY_SANS_ARCHIVE_ORIGINE',
    motif             TEXT NOT NULL,
    acteur            TEXT,
    date_classification TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Correction historique explicite (mission 15, partie D) : une correction reste possible mais
-- jamais silencieuse — ancien état + nouvel état + justification + auteur, jamais un DELETE/UPDATE
-- en place sur `reservations_historique_cloture` (dont le contrat d'upsert-sans-suppression est
-- déjà garanti par migration 0034 ; ce journal couvre les corrections qui doivent malgré tout
-- pouvoir REMPLACER une ligne figée, action distincte et tracée).
CREATE TABLE IF NOT EXISTS reservations_historique_corrections (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    cle_historisation     TEXT NOT NULL,
    mois                  TEXT NOT NULL,
    avant_json            TEXT NOT NULL,
    apres_json            TEXT NOT NULL,
    justification         TEXT NOT NULL,
    acteur                TEXT,
    date_correction       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0064');

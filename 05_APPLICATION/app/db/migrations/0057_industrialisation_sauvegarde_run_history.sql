-- Migration 0057 — Industrialisation socle technique : sauvegarde app.db + historique de runs centralisé.
--
-- CE QUI EXISTE DÉJÀ, ET N'EST PAS DUPLIQUÉ
-- `snapshots` (0001) sauvegarde déjà des FICHIERS (masters Excel moteur) avec manifeste sha256.
-- `calculs_runs`/`calculs_sauvegardes` (0018) trace déjà les runs de calcul avec sauvegarde/
-- restauration de fichiers. `lot10_runs`/`lot12_runs` ont déjà un dataset "actif" unique
-- (CURRENT), un nouveau run étant créé non-actif puis promu (CANDIDATE→CURRENT) seulement après
-- validation — le patron demandé par la mission existe déjà pour ces deux lots.
--
-- CE QUI MANQUAIT
-- Aucun mécanisme ne sauvegarde `app.db` ELLE-MÊME (le fichier SQLite, pas un fichier Excel)
-- avant une opération risquée dessus (migration de schéma notamment — `apply_migrations()` n'a
-- aujourd'hui aucune protection). Aucune vue centralisée ne trace TOUS les types d'opérations
-- (Hostaway/Banque/migration/recalcul) sous un même statut — chaque sous-système garde sa propre
-- table, ce qui reste voulu (pas de réécriture), mais rien ne les regroupe pour une vue
-- d'ensemble simple.

CREATE TABLE IF NOT EXISTS sauvegardes_base (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sauvegarde_id_opaque TEXT NOT NULL UNIQUE,          -- BCK-xxxx
    operation           TEXT NOT NULL,                  -- ex. MIGRATION, RECALCUL_GLOBAL, IMPORT_HOSTAWAY
    chemin_fichier      TEXT NOT NULL,                  -- copie physique de app.db
    git_commit          TEXT,                           -- HEAD au moment de la sauvegarde
    database_hash       TEXT NOT NULL,                  -- sha256 de la copie
    taille_octets       INTEGER NOT NULL,
    validation_status   TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (validation_status IN ('PENDING', 'VALIDE', 'CORROMPU')),
    date_creation       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_sauvegardes_base_operation ON sauvegardes_base(operation, date_creation);

-- Historique centralisé des runs (Phase 3/6). Ne remplace aucune table de run existante
-- (calculs_runs, lot10_runs, lot12_runs, hostaway_extractions, banque_imports restent la source de
-- vérité DÉTAILLÉE de leur propre lot) — sert de vue d'ensemble légère pour l'observabilité et le
-- rollback explicite, alimentée volontairement par les opérations qui l'utilisent, pas
-- rétroactivement par les lots existants (mission : ne pas tout refactorer).
CREATE TABLE IF NOT EXISTS run_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id_opaque       TEXT NOT NULL UNIQUE,           -- RUNH-xxxx
    operation           TEXT NOT NULL,
    statut              TEXT NOT NULL DEFAULT 'STARTED'
        CHECK (statut IN ('STARTED', 'VALIDATING', 'SUCCESS', 'FAILED', 'ROLLED_BACK')),
    sauvegarde_id_opaque TEXT,                          -- référence sauvegardes_base, si applicable
    duree_s             REAL,
    erreur              TEXT,
    acteur              TEXT,
    date_debut          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_fin            TEXT
);
CREATE INDEX IF NOT EXISTS idx_run_history_operation ON run_history(operation, date_debut);
CREATE INDEX IF NOT EXISTS idx_run_history_statut ON run_history(statut);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0057');

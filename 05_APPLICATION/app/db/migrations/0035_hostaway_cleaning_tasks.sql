-- Migration 0035 — CleaningTasks Hostaway (Lot6a) : couche RAW SQLite.
--
-- Additive comme 0017→0034. Aucun ALTER TABLE.
--
-- Prépare le chemin API H6 → RAW SQLite (mission Ménages), sans exiger de relancer H6 en réel : le
-- dataset legacy existant (`MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`) peut alimenter cette
-- couche via un adaptateur de reprise, à titre de migration/parité — pas comme source canonique.
--
-- Registre d'extraction SÉPARÉ de `hostaway_extractions` (0034) : CleaningTasks vient d'un
-- endpoint différent (/v1/tasks, segmenté par listing, sujet à ses propres 429 — cf H6), avec son
-- propre cycle de vie. Fusionner les deux registres masquerait un échec CleaningTasks derrière un
-- succès Reservations, ou l'inverse.
CREATE TABLE IF NOT EXISTS hostaway_cleaning_tasks_extractions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id   TEXT NOT NULL UNIQUE,   -- HCT-xxxx
    run_id          TEXT,
    mode            TEXT NOT NULL,          -- API|FIXTURE|REPRISE_EXCEL
    date_debut      TEXT NOT NULL,
    date_fin        TEXT,
    statut          TEXT NOT NULL DEFAULT 'EN_COURS',
        -- EN_COURS|SUCCES|PARTIEL|ECHEC — PARTIEL si un segment listing a été plafonné/429.
    nb_taches       INTEGER NOT NULL DEFAULT 0,
    segments_plafonnes TEXT,               -- CSV de listingMapId dont le segment a atteint 500 (D065)
    message         TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_hct_statut ON hostaway_cleaning_tasks_extractions(statut, date_debut);

-- Tâches brutes telles que l'API /v1/tasks les rend, avant toute résolution logement/mois (H6 :
-- cost systématiquement NULL — comptage uniquement, jamais valorisation, cf lot6a).
CREATE TABLE IF NOT EXISTS hostaway_cleaning_tasks (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id     TEXT NOT NULL,
    task_id           TEXT NOT NULL,
    reservation_id    TEXT,
    listing_map_id    TEXT,
    title             TEXT,
    status            TEXT,
    can_start_from    TEXT,
    assignee_user_id  TEXT,
    extrait_le        TEXT,
    row_hash          TEXT,
    FOREIGN KEY (extraction_id) REFERENCES hostaway_cleaning_tasks_extractions(extraction_id)
);
CREATE INDEX IF NOT EXISTS idx_hct_tasks_extraction ON hostaway_cleaning_tasks(extraction_id);
CREATE INDEX IF NOT EXISTS idx_hct_tasks_reservation ON hostaway_cleaning_tasks(reservation_id);
CREATE INDEX IF NOT EXISTS idx_hct_tasks_listing ON hostaway_cleaning_tasks(listing_map_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0035');

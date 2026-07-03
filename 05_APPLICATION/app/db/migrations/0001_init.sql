-- Migration 0001 — journal applicatif
-- Aucune valeur métier autoritaire. Source de vérité = Excel/moteur Python.

CREATE TABLE IF NOT EXISTS schema_migrations (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    version   TEXT NOT NULL UNIQUE,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS audit_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL DEFAULT (datetime('now')),
    action     TEXT NOT NULL,
    details    TEXT,
    user_label TEXT NOT NULL DEFAULT 'local'
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL DEFAULT (datetime('now')),
    script_name TEXT NOT NULL,
    dry_run     INTEGER NOT NULL DEFAULT 1,
    status      TEXT NOT NULL,   -- 'DRY_RUN' | 'OK' | 'ERROR'
    duration_ms INTEGER,
    output      TEXT,
    error       TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL DEFAULT (datetime('now')),
    type       TEXT NOT NULL,    -- 'MANUEL' | 'PERIODIQUE' | 'AVANT_CLOTURE' | 'APRES_CLOTURE' | 'AVANT_REOUVERTURE'
    path       TEXT NOT NULL,    -- chemin absolu du répertoire snapshot
    manifest   TEXT NOT NULL,    -- JSON [{file, sha256, size}]
    verified   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS screen_states (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL DEFAULT (datetime('now')),
    screen     TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drafts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL DEFAULT (datetime('now')),
    screen     TEXT NOT NULL,
    key        TEXT NOT NULL,
    value_json TEXT NOT NULL,
    UNIQUE(screen, key)
);

CREATE TABLE IF NOT EXISTS periods (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month    TEXT NOT NULL UNIQUE,  -- 'YYYY-MM'
    statut_mirror TEXT NOT NULL DEFAULT 'OUVERT',
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0001');

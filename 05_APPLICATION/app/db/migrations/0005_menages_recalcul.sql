-- Migration 0005 — journal des recalculs ménages (APP-2b)
-- Journal applicatif uniquement : trace des runs de recalcul (mode copies / réel).
-- Aucune valeur métier autoritaire. Source de vérité = MASTER Excel produits par le moteur.

CREATE TABLE IF NOT EXISTS menages_recalcul_runs (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    pipeline_code        TEXT NOT NULL DEFAULT 'menages_recalcul',
    periode              TEXT,                 -- 'YYYY-MM'
    mode                 TEXT NOT NULL,        -- 'COPIES' | 'REEL'
    statut               TEXT NOT NULL,        -- PREPARE|EN_COURS|SUCCES|ECHEC|PARTIEL|VERROUILLE|ANNULE|BLOQUE
    date_debut           TEXT,
    date_fin             TEXT,
    duree_secondes       REAL,
    snapshot_id          INTEGER,
    git_head             TEXT,
    workspace_path       TEXT,
    sha256_sources_avant TEXT,                 -- JSON {fichier: sha}
    sha256_sorties_avant TEXT,                 -- JSON {fichier: sha}
    sha256_sorties_apres TEXT,                 -- JSON {fichier: sha}
    comparaison_json     TEXT,                 -- JSON {rapprochement:{avant,apres}, reel_intact}
    etapes_json          TEXT,                 -- JSON [{name, statut, rc, ...}]
    erreur_code          TEXT,
    erreur_resume        TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0005');

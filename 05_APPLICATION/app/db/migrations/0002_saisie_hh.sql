-- Migration 0002 — journal écriture SAISIE HH contrôlée (APP-2b)
-- Journal applicatif uniquement. Aucune valeur métier autoritaire.
-- Seule source de vérité : SAISIE_ReservationsHorsHostaway.xlsx

CREATE TABLE IF NOT EXISTS saisie_hh_writes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL DEFAULT (datetime('now')),
    pk           TEXT NOT NULL,
    mois         TEXT NOT NULL,
    snapshot_id  INTEGER,
    sha256_avant TEXT,
    sha256_apres TEXT,
    ligne_cible  INTEGER,
    statut       TEXT NOT NULL,
    details      TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0002');

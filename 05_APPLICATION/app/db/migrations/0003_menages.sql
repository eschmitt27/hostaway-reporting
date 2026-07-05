-- Migration 0003 — journal outrepassage ménages (APP-2)
-- Journal applicatif uniquement. Aucune valeur métier autoritaire.
-- Seule source de vérité : MASTER_CTRL_Rapprochement_Menages.xlsx

CREATE TABLE IF NOT EXISTS menage_overrides (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    mois           TEXT NOT NULL,
    logement_id    TEXT NOT NULL,
    intervenant_id TEXT NOT NULL,
    motif          TEXT NOT NULL,
    statut_override TEXT NOT NULL DEFAULT 'JUSTIFIE',
    UNIQUE(mois, logement_id, intervenant_id)
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0003');

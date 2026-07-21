-- Migration 0011 — affectation logique des charges (APP-3E).
-- Le montant, le coût, la répartition et le caractère refacturé restent exclusivement calculés par
-- le moteur (sources charges réelles) — jamais recalculés ici. Cette table journalise UNIQUEMENT
-- l'affectation humaine d'une charge à un fournisseur/logement/propriétaire/mois/statut, jamais un
-- nouveau montant. Append-only ; aucune suppression physique ; version optimiste.

CREATE TABLE IF NOT EXISTS charges_affectations (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    affectation_id_opaque TEXT NOT NULL UNIQUE,   -- CHA-xxxx
    charge_id             TEXT NOT NULL,          -- identifiant charge source (charge_id métier)
    source_empreinte      TEXT,                   -- empreinte (hash) de la ligne source au moment de l'affectation
    fournisseur_id_opaque TEXT,                   -- FRS-xxxx, NULL si non affecté
    logement_id           TEXT,
    proprietaire_id       TEXT,
    mois                  TEXT,                   -- AAAA-MM
    nature                TEXT,
    refacturable          INTEGER,                -- 0/1/NULL (non statué)
    justificatif_logique  TEXT,
    statut_controle       TEXT NOT NULL DEFAULT 'A_CONTROLER',   -- A_CONTROLER|CONFORME|ANOMALIE
    commentaire           TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1,
    actif                 INTEGER NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_charges_affectations_charge_actif
    ON charges_affectations(charge_id) WHERE actif = 1;
CREATE INDEX IF NOT EXISTS idx_charges_affectations_opaque ON charges_affectations(affectation_id_opaque);
CREATE INDEX IF NOT EXISTS idx_charges_affectations_fournisseur ON charges_affectations(fournisseur_id_opaque);
CREATE INDEX IF NOT EXISTS idx_charges_affectations_prop_mois ON charges_affectations(proprietaire_id, mois);

-- Historique append-only de toute modification d'affectation.
CREATE TABLE IF NOT EXISTS charges_affectation_evenements (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    affectation_id_opaque  TEXT NOT NULL,
    type_evenement         TEXT NOT NULL,   -- CREATION|MODIFICATION
    commentaire            TEXT,
    date_evenement         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                 TEXT
);
CREATE INDEX IF NOT EXISTS idx_charges_affectation_evenements_aff
    ON charges_affectation_evenements(affectation_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0011');

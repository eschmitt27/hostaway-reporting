-- Migration 0013 — association historisée fournisseur ↔ logement (APP-3E).
-- Concept DISTINCT et non redondant :
--   * `fournisseurs` (0010) = référentiel fournisseur applicatif minimal (qui est le fournisseur) ;
--   * `intervenant_id` / REF_Intervenants (moteur, REF_Setup, LECTURE SEULE) = prestataire ménage
--     rattaché par le moteur (mois × logement × intervenant), jamais géré par l'application ;
--   * cette table = relation DURABLE "tel fournisseur intervient sur tel logement" sur une période
--     (ex. société de maintenance affectée à un logement du 01/2026 au 06/2026), avec type de
--     prestation. Elle ne porte AUCUN montant (la vérité des montants reste le moteur) et ne
--     duplique pas le rapprochement ménage du moteur.
-- Historisation : une période active par (fournisseur, logement, type_prestation) à la fois ;
-- fermeture par date_fin ; append-only pour l'historique ; version optimiste ; aucune suppression.

CREATE TABLE IF NOT EXISTS fournisseur_rattachements (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    association_id_opaque  TEXT NOT NULL UNIQUE,   -- FLG-xxxx
    fournisseur_id_opaque  TEXT NOT NULL,          -- FRS-xxxx (référentiel fournisseur)
    logement_id            TEXT NOT NULL,          -- clé logique logement existante (jamais un id SQLite)
    type_prestation        TEXT NOT NULL DEFAULT 'AUTRE',   -- MENAGE|MAINTENANCE|FOURNITURE|AUTRE
    date_debut             TEXT NOT NULL,          -- AAAA-MM-JJ
    date_fin               TEXT,                   -- NULL = période ouverte
    statut                 TEXT NOT NULL DEFAULT 'ACTIF',   -- ACTIF|INACTIF
    motif                  TEXT,
    date_creation          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                 TEXT,
    version                INTEGER NOT NULL DEFAULT 1
);
-- Au plus une période OUVERTE (date_fin IS NULL) par (fournisseur, logement, type_prestation).
CREATE UNIQUE INDEX IF NOT EXISTS idx_fournisseur_rattachements_periode_ouverte
    ON fournisseur_rattachements(fournisseur_id_opaque, logement_id, type_prestation)
    WHERE date_fin IS NULL AND statut = 'ACTIF';
CREATE INDEX IF NOT EXISTS idx_fournisseur_rattachements_logement ON fournisseur_rattachements(logement_id);
CREATE INDEX IF NOT EXISTS idx_fournisseur_rattachements_fournisseur ON fournisseur_rattachements(fournisseur_id_opaque);

-- Historique append-only de tous les événements de l'association.
CREATE TABLE IF NOT EXISTS fournisseur_rattachement_evenements (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    association_id_opaque  TEXT NOT NULL,
    type_evenement         TEXT NOT NULL,   -- CREATION|FERMETURE|DESACTIVATION|MODIFICATION
    ancien_statut          TEXT,
    nouveau_statut         TEXT,
    commentaire            TEXT,
    date_evenement         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                 TEXT
);
CREATE INDEX IF NOT EXISTS idx_fournisseur_rattachement_evenements_assoc
    ON fournisseur_rattachement_evenements(association_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0013');

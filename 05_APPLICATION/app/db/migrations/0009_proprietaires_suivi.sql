-- Migration 0009 — suivi humain des RELEVÉS/PRÉFACTURES PROPRIÉTAIRES (APP-3D).
-- La vérité financière reste exclusivement dans le moteur (MASTER_CALC_NetProprietaire,
-- MASTER_CALC_Commissions, MASTER_FACT_Proprietaires) — jamais recalculée ni écrite ici. Cette
-- migration journalise UNIQUEMENT le statut de préparation humaine d'un relevé propriétaire×mois
-- (brouillon, décision, statut de facturation, commentaires, historique), jamais une nouvelle
-- vérité financière, jamais une facture légale définitive. Append-only ; aucune suppression
-- physique ; version optimiste ; une seule ligne active par (propriétaire, mois).

CREATE TABLE IF NOT EXISTS proprietaires_releves (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    releve_id_opaque        TEXT NOT NULL UNIQUE,   -- identifiant public opaque REG-xxxx
    proprietaire_id         TEXT NOT NULL,          -- id métier propriétaire, jamais exposé en clair dans l'URL
    mois                    TEXT NOT NULL,           -- AAAA-MM
    statut_facturation      TEXT NOT NULL DEFAULT 'NON_CONCERNE',
        -- NON_CONCERNE|A_FACTURER|FACTURE|AVOIR_A_EMETTRE|AVOIR_EMIS
    date_creation           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_preparation        TEXT,
    date_validation         TEXT,
    date_reouverture        TEXT,
    commentaire_validation  TEXT,
    justification_reouverture TEXT,
    cree_par                TEXT,
    valide_par              TEXT,
    version                 INTEGER NOT NULL DEFAULT 1,
    actif                   INTEGER NOT NULL DEFAULT 1   -- 1 = ligne courante du (propriétaire, mois), 0 = historisée
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_proprietaires_releves_prop_mois_actif
    ON proprietaires_releves(proprietaire_id, mois) WHERE actif = 1;
CREATE INDEX IF NOT EXISTS idx_proprietaires_releves_opaque ON proprietaires_releves(releve_id_opaque);

-- Historique append-only de tous les événements du relevé (transitions, actions).
CREATE TABLE IF NOT EXISTS proprietaires_releve_evenements (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    releve_id_opaque TEXT NOT NULL,
    type_evenement   TEXT NOT NULL,      -- CREATION|TRANSITION|COMMENTAIRE|EXPORT
    ancien_statut    TEXT,
    nouveau_statut   TEXT,
    commentaire      TEXT,
    date_evenement   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur           TEXT
);
CREATE INDEX IF NOT EXISTS idx_proprietaires_releve_evenements_releve
    ON proprietaires_releve_evenements(releve_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0009');

-- Migration 0012 — cycle de préparation du relevé (snapshot/dérive) et suivi de paiement (APP-3E).
-- Deux dimensions d'état INDÉPENDANTES du statut de facturation existant (proprietaires_releves,
-- migration 0009), une ligne par relevé (releve_id_opaque, clé logique — pas de FK physique,
-- cohérent avec le reste du schéma applicatif) :
--   1. proprietaires_releve_cycle : préparation humaine du relevé. Le snapshot est figé au passage
--      A_VALIDER puis comparé à l'état courant pour détecter une dérive — jamais un recalcul.
--   2. proprietaires_paiement : déclaration humaine de paiement au propriétaire. AUCUNE écriture
--      bancaire, AUCUN virement, AUCUN IBAN. MARQUE_COMME_PAYE est une déclaration, pas une preuve
--      bancaire.
-- Tables séparées plutôt qu'ALTER TABLE sur proprietaires_releves : le runner de migrations
-- (`apply_migrations`) réexécute le script complet de chaque fichier à chaque appel — un
-- `ALTER TABLE ADD COLUMN` casserait l'idempotence (`CREATE TABLE IF NOT EXISTS` reste la seule
-- forme sûre avec ce runner).

CREATE TABLE IF NOT EXISTS proprietaires_releve_cycle (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    releve_id_opaque    TEXT NOT NULL UNIQUE,
    etat_cycle          TEXT NOT NULL DEFAULT 'NON_DEMARRE',
        -- NON_DEMARRE|EN_PREPARATION|BLOQUE|A_VALIDER|VALIDE|ROUVERT|ANNULE
    snapshot_json        TEXT,
    snapshot_empreinte    TEXT,
    date_snapshot         TEXT,
    motif_annulation      TEXT,
    date_creation        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version              INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_proprietaires_releve_cycle_opaque
    ON proprietaires_releve_cycle(releve_id_opaque);

CREATE TABLE IF NOT EXISTS proprietaires_paiement (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    releve_id_opaque            TEXT NOT NULL UNIQUE,
    statut_paiement              TEXT NOT NULL DEFAULT 'NON_PREPARE',
        -- NON_PREPARE|A_CONTROLER|PRET_A_PAYER|MARQUE_COMME_PAYE|BLOQUE|ANNULE|ROUVERT
    reference_interne_paiement  TEXT,
    date_paiement                TEXT,
    motif_paiement                TEXT,
    date_creation                TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version                      INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_proprietaires_paiement_opaque
    ON proprietaires_paiement(releve_id_opaque);

-- Événements append-only communs aux deux machines (type_evenement distingue
-- CYCLE_TRANSITION | DERIVE_DETECTEE | PAIEMENT_TRANSITION) — réutilise la table d'événements
-- existante plutôt qu'une nouvelle table parallèle par dimension.
-- (proprietaires_releve_evenements existe déjà, migration 0009 — aucune structure à créer ici.)

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0012');

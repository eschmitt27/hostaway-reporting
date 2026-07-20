-- Migration 0010 — référentiel fournisseur minimal (APP-3D).
-- Aucun module fournisseur n'existait (audit confirmé : /fournisseurs gère des CHARGES, pas des
-- fournisseurs — aucune table, aucun id, aucune fiche). Référentiel minimal, lecture/écriture
-- SQLite isolée uniquement. Aucun IBAN, aucune donnée bancaire, aucune donnée personnelle sensible.
-- Aucune écriture dans un fichier métier réel. Append-only pour l'historique ; désactivation
-- logique uniquement (jamais de suppression physique) ; version optimiste.

CREATE TABLE IF NOT EXISTS fournisseurs (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    fournisseur_id_opaque TEXT NOT NULL UNIQUE,   -- identifiant public opaque FRS-xxxx
    nom                  TEXT NOT NULL,
    type                 TEXT NOT NULL,            -- MENAGE|MAINTENANCE|FOURNITURE|AUTRE
    statut               TEXT NOT NULL DEFAULT 'ACTIF',  -- ACTIF|INACTIF
    commentaire          TEXT,
    date_creation        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version              INTEGER NOT NULL DEFAULT 1,
    actif                INTEGER NOT NULL DEFAULT 1   -- 1 = ligne courante, 0 = historisée (jamais supprimée)
);
CREATE INDEX IF NOT EXISTS idx_fournisseurs_opaque ON fournisseurs(fournisseur_id_opaque);
CREATE INDEX IF NOT EXISTS idx_fournisseurs_nom ON fournisseurs(nom);

-- Historique append-only des modifications (jamais de suppression physique de ligne).
CREATE TABLE IF NOT EXISTS fournisseur_evenements (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    fournisseur_id_opaque TEXT NOT NULL,
    type_evenement        TEXT NOT NULL,   -- CREATION|MODIFICATION|DESACTIVATION|REACTIVATION
    ancien_statut         TEXT,
    nouveau_statut        TEXT,
    commentaire           TEXT,
    date_evenement        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT
);
CREATE INDEX IF NOT EXISTS idx_fournisseur_evenements_fournisseur ON fournisseur_evenements(fournisseur_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0010');

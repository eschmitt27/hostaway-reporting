-- Migration 0041 — Constats Lot11 (contrôles de cohérence transverses), reprise SQLite.
--
-- Additive comme 0017→0040. Aucun ALTER TABLE.
--
-- POURQUOI UNE REPRISE, PAS UNE MIGRATION DE LOT11
-- Lot11 (`lot11_controles_coherence.py`) n'est pas migré cette mission (banque + réservations +
-- ménages combinés, hors périmètre). L'application ne doit néanmoins jamais lire
-- `MASTER_CTRL_Coherence.xlsx` directement (mission Ménages, Bloc B §8) : `controles_lot11_adapter.
-- reprendre()` lit ce classeur UNE FOIS (reprise, même statut que `hostaway_adaptateurs.reprendre`)
-- et alimente cette table ; `menages_reader.controles_lot11()` ne lit plus que SQLite.
--
-- Mêmes colonnes que le MASTER produit par `lot11_controles_coherence._ctrl()` — reprises telles
-- quelles, aucune règle de contrôle recalculée ici.
CREATE TABLE IF NOT EXISTS controles_lot11_constats (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ctrl_pk           TEXT NOT NULL,
    source_module     TEXT,
    source_table      TEXT,
    source_pk         TEXT,
    code_controle     TEXT,
    severity          TEXT,
    message           TEXT,
    impact_facture    TEXT,
    statut_resolution TEXT,
    commentaire       TEXT,
    date_reprise      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_lot11_module ON controles_lot11_constats(source_module);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0041');

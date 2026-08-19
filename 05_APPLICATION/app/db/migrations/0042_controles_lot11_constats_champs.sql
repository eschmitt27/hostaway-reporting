-- Migration 0042 — Constats Lot11 : champs manquants (mois, logement, période).
--
-- Additive comme 0017→0041. Aucun ALTER TABLE — extension 1-1 de `controles_lot11_constats`
-- (0041), comme `fournisseur_menage_qualification` (0019) l'est de `fournisseurs`.
--
-- POURQUOI UNE TABLE SÉPARÉE, PAS UN ALTER
-- `controles_lot11_constats` (0041) ne reprenait qu'un sous-ensemble des colonnes produites par
-- `lot11_controles_coherence._ctrl()` — insuffisant pour filtrer un constat par mois (ex. compter
-- CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE d'un mois donné, `controles_runner_service`).
-- Les migrations sont rejouées à chaque démarrage et SQLite n'a pas d'ADD COLUMN IF NOT EXISTS.
CREATE TABLE IF NOT EXISTS controles_lot11_constats_champs (
    ctrl_pk           TEXT PRIMARY KEY,
    mois              TEXT,
    logement_id       TEXT,
    proprietaire_id   TEXT,
    reservation_id    TEXT,
    document_id       TEXT,
    date_detection    TEXT
);
CREATE INDEX IF NOT EXISTS idx_lot11_champs_mois ON controles_lot11_constats_champs(mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0042');

-- Migration 0046 — DASHBOARD_MOIS Lot11, persisté (nécessaire à Lot12).
--
-- Additive comme 0017→0045. Aucun ALTER TABLE.
--
-- `controles_lot11_service.construire()` (0045) calculait déjà ce dashboard en mémoire mais ne le
-- persistait pas — rien n'en avait besoin. Lot12 (préfactures) lit ce dashboard pour la gate de
-- facturation par mois (`facturation_lot12_ok`) : il faut donc le stocker. Un instantané unique
-- (DELETE+INSERT), comme `controles_lot11_constats` — le dashboard reflète toujours le DERNIER
-- run réussi, jamais un historique à cumuler.
CREATE TABLE IF NOT EXISTS controles_lot11_dashboard_mois (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                 TEXT NOT NULL,
    mois                   TEXT NOT NULL,
    nb_bloquants_ouverts   INTEGER,
    nb_a_controler_ouverts INTEGER,
    nb_info                INTEGER,
    statut_mois_banque     TEXT,
    cloture_possible       TEXT,
    facturation_lot12_ok   TEXT
);
CREATE INDEX IF NOT EXISTS idx_lot11_dashboard_mois ON controles_lot11_dashboard_mois(mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0046');

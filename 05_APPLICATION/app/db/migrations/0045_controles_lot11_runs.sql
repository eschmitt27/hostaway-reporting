-- Migration 0045 — Traçabilité des runs Lot11 (moteur SQLite natif).
--
-- Additive comme 0017→0044. Aucun ALTER TABLE.
--
-- Depuis cette mission, `controles_lot11_constats`/`_champs` (0041/0042) ne sont plus alimentées
-- uniquement par une reprise du classeur `MASTER_CTRL_Coherence.xlsx` (`controles_lot11_adapter`) :
-- `controles_lot11_service.construire()` (nouveau) recalcule directement depuis les chaînes déjà
-- SQLite (flux_unifies, lot10_*, reservations_resolues, hostaway_payouts/anomalies, banque,
-- REF_Setup) et écrit dans les MÊMES tables. Cette table trace CHAQUE tentative (reprise classeur ou
-- calcul SQLite natif) : run_id, source, statut, compteurs — jamais un remplacement des constats
-- avant que le run entier n'ait réussi (le DELETE+INSERT de 0041/0042 reste dans une seule
-- transaction, engagée seulement après un calcul complet sans exception).
CREATE TABLE IF NOT EXISTS controles_lot11_runs (
    run_id           TEXT PRIMARY KEY,
    source           TEXT NOT NULL,   -- SQLITE_NATIF | REPRISE_CLASSEUR
    statut           TEXT NOT NULL,   -- SUCCES | ECHEC
    nb_constats      INTEGER,
    nb_bloquants     INTEGER,
    nb_a_controler   INTEGER,
    nb_info          INTEGER,
    erreur_code      TEXT,
    erreur_message   TEXT,
    date_calcul      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_lot11_runs_date ON controles_lot11_runs(date_calcul);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0045');

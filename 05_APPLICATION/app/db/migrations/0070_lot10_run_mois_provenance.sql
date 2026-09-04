-- Migration 0070 — Provenance par mois d'un run Lot10 (mission « arrêter le backfill historique
-- ménages » — remplace le mécanisme mixte d'une mission précédente, jamais appliqué en production).
--
-- `lot10_runs` trace le run dans son ensemble (périmètre, statut, activation) mais ne dit rien de
-- la manière dont CHAQUE mois qu'il contient a été traité. Décision produit : on ne cherche plus à
-- reconstruire l'historique ménage des mois clôturés. Un mois est donc soit :
--   RECALCULE       — mois OUVERT, calculé depuis les sources SQLite courantes.
--   EXCLU_PERIMETRE — mois CLOTURE, absent du run par construction : jamais recalculé, jamais
--                     recopié depuis une archive ou un run antérieur.
--
-- Cette table ne duplique aucun résultat économique (déjà dans lot10_commissions/lot10_resultats/
-- lot10_net_exploitation/lot10_net_reglement/lot10_net_vue_mois) : elle trace uniquement le
-- périmètre réellement traité, pour pouvoir prouver après coup qu'un mois clôturé a bien été
-- ignoré plutôt que recalculé.
--
-- `source_run_id`/`source_archive_id` restent au schéma (toujours NULL en pratique aujourd'hui) :
-- une future décision produit pourrait réintroduire une reprise d'archive explicite sans nouvelle
-- migration.

CREATE TABLE IF NOT EXISTS lot10_run_mois_provenance (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    mois              TEXT NOT NULL,
    classification    TEXT NOT NULL,   -- OUVERT | CLOTURE
    mode_traitement   TEXT NOT NULL,   -- RECALCULE | EXCLU_PERIMETRE
    source_run_id     TEXT,            -- réservé, non utilisé par le mécanisme actuel
    source_archive_id TEXT,            -- réservé, non utilisé par le mécanisme actuel
    date_traitement   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_lot10_provenance_run ON lot10_run_mois_provenance(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_lot10_provenance_run_mois
    ON lot10_run_mois_provenance(run_id, mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0070');

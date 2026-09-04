-- Migration 0070 — Provenance par mois d'un run Lot10 (mission « Lot10 mixte : préserver les mois
-- clôturés et recalculer uniquement les mois ouverts »).
--
-- `lot10_runs` trace le run dans son ensemble (périmètre, statut, activation) mais ne dit rien de
-- la manière dont CHAQUE mois qu'il contient a été obtenu. Un run Lot10 mixte combine désormais
-- trois origines par mois :
--   RECALCULE            — mois OUVERT, calculé depuis les sources SQLite courantes.
--   ARCHIVE_AUTHENTIQUE  — mois CLOTURE avec un snapshot pris à la clôture (`mois_archive_reglement`,
--                          migration 0064) : repris tel quel, jamais recalculé.
--   LEGACY_FIGE          — mois CLOTURE classé `LEGACY_SANS_ARCHIVE_ORIGINE` (`mois_classification_
--                          legacy`, mission 16) : aucune archive contemporaine n'existe, les lignes
--                          du dernier run Lot10 reconnu comme référence sont recopiées à l'identique.
--
-- Cette table ne duplique aucun résultat économique (déjà dans lot10_commissions/lot10_resultats/
-- lot10_net_exploitation/lot10_net_reglement/lot10_net_vue_mois) : elle trace uniquement la
-- décision prise pour chaque mois, avec sa source, pour pouvoir prouver après coup qu'un mois
-- clôturé n'a pas été recalculé — jamais une étiquette ARCHIVE_AUTHENTIQUE sur une simple copie
-- (LEGACY_FIGE) : un run L10-xxx antérieur au mécanisme d'archivage à la clôture n'est jamais une
-- archive contemporaine authentique, même s'il est la seule source figée disponible.

CREATE TABLE IF NOT EXISTS lot10_run_mois_provenance (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    mois              TEXT NOT NULL,
    classification    TEXT NOT NULL,   -- OUVERT | CLOTURE_ARCHIVE_AUTHENTIQUE | LEGACY_SANS_ARCHIVE_ORIGINE
    mode_traitement   TEXT NOT NULL,   -- RECALCULE | ARCHIVE_AUTHENTIQUE | LEGACY_FIGE
    source_run_id     TEXT,            -- run Lot10 d'origine si LEGACY_FIGE
    source_archive_id TEXT,            -- référence mois_archive_reglement si ARCHIVE_AUTHENTIQUE
    date_traitement   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_lot10_provenance_run ON lot10_run_mois_provenance(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_lot10_provenance_run_mois
    ON lot10_run_mois_provenance(run_id, mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0070');

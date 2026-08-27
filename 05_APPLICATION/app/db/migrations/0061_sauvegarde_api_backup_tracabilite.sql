-- Migration 0061 — Traçabilité renforcée des sauvegardes (mission 14 — activation réelle contrôlée).
--
-- `backup_service.sauvegarder()` passait par `PRAGMA wal_checkpoint(FULL)` + `shutil.copy2()`.
-- Bascule vers `sqlite3.Connection.backup()` (déjà utilisé ailleurs dans ce projet, cf.
-- `menages_recalcul_service.py`, `controles_runner_service.py`) : API officielle, backup cohérent
-- même en présence d'écritures WAL non checkpointées, sans fenêtre de course entre checkpoint et
-- copie de fichier.
--
-- Table séparée plutôt qu'un `ALTER TABLE sauvegardes_base ADD COLUMN` : les migrations sont
-- rejouées à chaque démarrage et SQLite n'a pas d'ADD COLUMN IF NOT EXISTS (même raison que
-- 0012/0017/0019/0020/0028/0032/0033/0034/0042/0053) — un ALTER planterait au second passage.
--   source_hash    : sha256 de la base SOURCE au moment de la sauvegarde (distinct de
--                    sauvegardes_base.database_hash, qui reste le hash de la COPIE) — permet de
--                    détecter une dérive entre l'état lu et l'état effectivement sauvegardé.
--   schema_version : dernière version de schema_migrations présente dans la SOURCE au moment de
--                    la sauvegarde (traçabilité : sur quel schéma portait cette sauvegarde).

CREATE TABLE IF NOT EXISTS sauvegardes_base_tracabilite (
    sauvegarde_id_opaque TEXT PRIMARY KEY REFERENCES sauvegardes_base(sauvegarde_id_opaque),
    source_hash          TEXT NOT NULL,
    schema_version       TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0061');

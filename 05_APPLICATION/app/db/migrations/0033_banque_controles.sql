-- Migration 0033 — Contrôles bancaires et signaux de classification, en SQLite.
--
-- Additive comme 0017→0032 : uniquement des CREATE TABLE / INDEX IF NOT EXISTS.
--
-- CE QUI MANQUAIT ENCORE
-- 0032 a apporté les mouvements et leur classification. Deux informations que l'application affiche
-- vivaient toujours dans le classeur seul :
--   1. les CONSTATS DE CONTRÔLE par mouvement (onglet `CTRL_A_CONTROLER`) — ce que le moteur
--      demande de vérifier humainement, avec son code et sa sévérité ;
--   2. le NIVEAU D'ANOMALIE et les CODES D'ANOMALIE (colonnes de `NORM_Banque`).
--
-- POURQUOI UNE TABLE COMPAGNE PLUTÔT QUE DEUX COLONNES EN PLUS
-- Les migrations sont rejouées à chaque démarrage : un `ALTER TABLE ADD COLUMN` échouerait au
-- second passage. `banque_classification_signaux` complète donc `banque_classifications` par
-- strictement ce qui lui manque, avec la même clé — exactement le motif déjà retenu pour
-- `banque_import_source` en 0032.

-- Un constat de contrôle porte sur UN mouvement, et peut coexister avec d'autres sur le même
-- mouvement : un remboursement ambigu s'ajoute au constat de la règle appliquée, il ne le remplace
-- pas. La clé n'est donc pas le mouvement mais le triplet (mouvement, exécution, code).
CREATE TABLE IF NOT EXISTS banque_controles (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque   TEXT NOT NULL,
    classification_run_id TEXT NOT NULL,
    origine               TEXT NOT NULL,   -- IMPORT|CLASSIFICATION : à quelle étape le constat naît
    code_controle         TEXT NOT NULL,
    severite              TEXT NOT NULL,
    description           TEXT,
    statut_controle       TEXT NOT NULL DEFAULT 'A_CONTROLER',
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_bq_ctrl_mvt ON banque_controles(mouvement_id_opaque);
CREATE INDEX IF NOT EXISTS idx_bq_ctrl_run ON banque_controles(classification_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bq_ctrl_unique
    ON banque_controles(mouvement_id_opaque, classification_run_id, code_controle);

-- Extension 1-1 de `banque_classifications`.
--
-- `niveau_anomalie` n'est PAS déductible de `niveau_risque` seul : un libellé de remboursement le
-- lève aussi, sans changer le risque de la règle. Le stocker évite que l'affichage le recalcule à
-- sa façon — deux dérivations d'une même règle finissent toujours par diverger.
CREATE TABLE IF NOT EXISTS banque_classification_signaux (
    mouvement_id_opaque   TEXT NOT NULL,
    classification_run_id TEXT NOT NULL,
    niveau_anomalie       TEXT,   -- vide, ou A_CONTROLER
    codes_anomalie        TEXT,   -- codes issus de l'import (doublon probable, notamment)
    PRIMARY KEY (mouvement_id_opaque, classification_run_id)
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0033');

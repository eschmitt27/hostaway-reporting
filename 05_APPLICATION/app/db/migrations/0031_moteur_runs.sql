-- Migration 0031 — Journal des runs moteur, écrit DÈS LE DÉMARRAGE.
--
-- Additive comme 0017→0030 : uniquement des CREATE TABLE / INDEX IF NOT EXISTS.
--
-- LE DÉFAUT CORRIGÉ
-- `lot1_hostaway_extract` n'écrivait son entrée de journal qu'à la toute fin. Le run du 2026-08-17
-- a téléchargé 1542 réservations, écrit sept masters, puis a été interrompu pendant l'étape des
-- tâches ménage : `MASTER_RUN_Log` n'en porte AUCUNE trace. Un run qui réussit l'essentiel puis
-- échoue devient invisible — c'est le pire des cas, parce que rien ne signale qu'il faut vérifier.
--
-- POURQUOI DE NOUVELLES TABLES ET PAS calculs_runs / pipeline_runs
--   `pipeline_runs` (0001) est une ligne unique par script, sans étapes ni volumétries.
--   `calculs_runs` (0018) porte `mois NOT NULL` et décrit l'exécution d'une CHAÎNE de lots pour un
--   mois donné. Un run d'extraction n'est pas rattaché à un mois, et son unité de découpe est
--   l'ÉTAPE (réservations, payouts, tâches ménage), pas le lot.
-- Ces tables complètent les deux autres, elles ne les remplacent pas. Le vocabulaire de statut est
-- repris de `calculs_runs` (EN_COURS / SUCCES / ECHEC) plutôt que réinventé ; seuls PARTIEL et
-- INTERROMPU sont ajoutés, parce qu'aucun des existants ne les exprimait.

CREATE TABLE IF NOT EXISTS moteur_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL UNIQUE,          -- RUN-<lot>-<horodatage>
    lot             TEXT NOT NULL,                 -- lot1_hostaway_extract, lot8a_banque_import, ...
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    statut          TEXT NOT NULL DEFAULT 'EN_COURS',
        -- EN_COURS   : ligne écrite au démarrage, avant tout téléchargement
        -- SUCCES     : toutes les étapes en SUCCES
        -- PARTIEL    : au moins une étape en SUCCES et au moins une en ECHEC
        -- ECHEC      : aucune étape aboutie
        -- INTERROMPU : statut déduit a posteriori — le run n'a jamais été clos
    declencheur     TEXT NOT NULL DEFAULT 'MANUEL',   -- MANUEL|AUTO|ORCHESTRATEUR
    arguments       TEXT,                          -- ligne de commande, pour rejouer à l'identique
    pid             INTEGER,                       -- permet de reconnaître un run orphelin
    hote            TEXT,
    code_version    TEXT,                          -- HEAD au moment du run, si disponible
    nb_etapes       INTEGER NOT NULL DEFAULT 0,
    nb_etapes_ok    INTEGER NOT NULL DEFAULT 0,
    nb_etapes_ko    INTEGER NOT NULL DEFAULT 0,
    duree_s         REAL,
    erreur_resume   TEXT
);
CREATE INDEX IF NOT EXISTS idx_moteur_runs_lot ON moteur_runs(lot, started_at);
CREATE INDEX IF NOT EXISTS idx_moteur_runs_statut ON moteur_runs(statut);

-- Une ligne par étape, écrite À LA FIN DE L'ÉTAPE. Une étape absente de cette table alors que le
-- run est terminé signifie qu'elle n'a jamais été atteinte — information utile en soi.
CREATE TABLE IF NOT EXISTS moteur_run_etapes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    etape           TEXT NOT NULL,                 -- RESERVATIONS|PAYOUTS|LISTINGS|DETAILS|FEES|
                                                   -- FINANCE_FIELDS|ANOMALIES|CLEANING_TASKS
    ordre           INTEGER NOT NULL DEFAULT 0,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    statut          TEXT NOT NULL,                 -- SUCCES|ECHEC|IGNOREE
    nb_lus          INTEGER,
    nb_ecrits       INTEGER,
    position        TEXT,                          -- dernier offset/curseur atteint
    tentatives      INTEGER NOT NULL DEFAULT 0,
    http_status     INTEGER,                       -- renseigné quand l'échec vient de l'API
    erreur          TEXT,
    sorties         TEXT                           -- JSON : [{fichier, lignes, sha256}]
);
CREATE INDEX IF NOT EXISTS idx_moteur_run_etapes_run ON moteur_run_etapes(run_id, ordre);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0031');

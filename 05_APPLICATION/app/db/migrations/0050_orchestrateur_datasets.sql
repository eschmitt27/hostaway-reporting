-- Migration 0050 — Orchestrateur : état des DATASETS, verrous, runs globaux.
--
-- Additive comme 0017→0049. Aucun ALTER TABLE.
--
-- CE QUE CETTE MIGRATION N'AJOUTE PAS
-- Aucun quatrième registre de runs. L'orchestrateur réutilise `moteur_runs`/`moteur_run_etapes`
-- (0031), dont le vocabulaire couvre déjà exactement ce dont il a besoin : statuts EN_COURS /
-- SUCCES / PARTIEL / ECHEC / INTERROMPU, `declencheur` MANUEL|AUTO|ORCHESTRATEUR, `pid`/`hote` pour
-- reconnaître un run orphelin, et une étape par lot avec son erreur. `pipeline_runs` (journal de
-- scripts) et `calculs_runs` (recalcul mensuel sur copie, avec prévisualisation et rollback)
-- gardent leurs responsabilités propres et ne sont pas absorbés.
--
-- CE QUI MANQUAIT VRAIMENT
-- 1. L'état d'un DATASET (et non d'un run) : quel est le dernier calcul valide de `flux_unifies`,
--    de quand date-t-il, et est-il encore à jour compte tenu de ses dépendances amont.
-- 2. Un verrou, pour que deux recalculs du même pipeline ne s'exécutent pas en parallèle.
--
-- LA FRAÎCHEUR EST FONDÉE SUR LES RUNS, JAMAIS SUR LE mtime D'UN FICHIER
-- `calcule_le` est la date du calcul qui a produit le dataset, et `source_run_id` le run qui l'a
-- produit. Un dataset devient A_RECALCULER quand un dataset AMONT a été recalculé après lui — c'est
-- une comparaison entre deux calculs, pas entre deux fichiers. Un Lot10 calculé sur un ancien Lot9
-- ne peut donc pas s'afficher « à jour ».
CREATE TABLE IF NOT EXISTS orchestrateur_datasets (
    dataset          TEXT PRIMARY KEY,   -- identifiant logique du nœud du DAG (HOSTAWAY_RAW, LOT10…)
    statut           TEXT NOT NULL,      -- JAMAIS_CALCULE|A_JOUR|A_RECALCULER|EN_COURS|ECHEC|PARTIEL
    calcule_le       TEXT,               -- horodatage du dernier calcul RÉUSSI
    source_run_id    TEXT,               -- run (moteur_runs.run_id) qui a produit ce dataset
    declencheur      TEXT,               -- MANUEL|AUTO|ORCHESTRATEUR
    nb_lignes        INTEGER,            -- volumétrie constatée, pour l'affichage
    detail           TEXT,               -- JSON libre (compteurs du service appelé)
    erreur_code      TEXT,
    erreur_message   TEXT,
    maj_le           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_orch_datasets_statut ON orchestrateur_datasets(statut);

-- Journal append-only des transitions d'un dataset : permet de dire POURQUOI un dataset est dans
-- l'état où il est (recalculé, invalidé par un amont, échoué), sans écraser l'historique.
CREATE TABLE IF NOT EXISTS orchestrateur_dataset_evenements (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset       TEXT NOT NULL,
    run_id        TEXT,
    statut_avant  TEXT,
    statut_apres  TEXT NOT NULL,
    motif         TEXT,                  -- RECALCUL|INVALIDATION_AMONT|ECHEC|REPRISE_INTERROMPU
    detail        TEXT,
    horodatage    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_orch_evt_dataset ON orchestrateur_dataset_evenements(dataset, id);

-- VERROU (§36) — un bail, pas un verrou éternel.
--
-- `expire_le` est indispensable : un processus tué n'a aucune chance de libérer son verrou, et un
-- verrou sans expiration bloquerait le pipeline jusqu'à intervention humaine. Le bail est donc
-- renouvelé pendant le run et considéré comme caduc au-delà. `portee` permet de ne verrouiller que
-- ce qui doit l'être : deux actualisations réellement indépendantes ne se bloquent pas.
CREATE TABLE IF NOT EXISTS orchestrateur_verrous (
    portee       TEXT PRIMARY KEY,       -- PIPELINE_GLOBAL, ou le nom d'un dataset
    run_id       TEXT NOT NULL,
    detenu_par   TEXT,                   -- hôte/pid, pour diagnostiquer un verrou resté pris
    pris_le      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    expire_le    TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0050');

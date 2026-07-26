-- Migration 0018 — pilotage des calculs (runs de pipeline) et clôture mensuelle.
--
-- Journal applicatif : ce que l'application a lancé, dans quel ordre, avec quel résultat.
-- La VÉRITÉ des résultats métier reste dans les fichiers produits par les moteurs (Lot9/Lot10/…) ;
-- ces tables n'en sont que la traçabilité et les indicateurs comparés d'un run à l'autre.
--
-- La clôture RÉELLE reste `REF_Cloture_Mensuelle` (REF_Setup.xlsm, moteur, D024), jamais écrite par
-- l'application : `cloture_statuts` ci-dessous journalise la préparation/validation applicative,
-- exactement comme la migration 0008 le fait déjà pour la checklist humaine.

CREATE TABLE IF NOT EXISTS calculs_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id_opaque       TEXT NOT NULL UNIQUE,      -- RUN-xxxx
    mois                TEXT NOT NULL,             -- AAAA-MM
    mode                TEXT NOT NULL,             -- RECETTE|REEL
    statut              TEXT NOT NULL DEFAULT 'PREPARE',
        -- PREPARE|EN_COURS|SUCCES|ECHEC|RESTAURE|ANNULE
    token_previsualisation TEXT,
    empreinte_entrees   TEXT,                      -- hash des entrées au moment de la prévisualisation
    racine              TEXT,                      -- racine projet utilisée (data_recette en recette)
    interpreteur        TEXT,                      -- interpréteur des lots réellement utilisé
    lots_demandes       TEXT,                      -- JSON : liste ordonnée des lots
    nb_lots_reussis     INTEGER NOT NULL DEFAULT 0,
    nb_lots_echoues     INTEGER NOT NULL DEFAULT 0,
    duree_totale_s      REAL,
    erreur_resume       TEXT,
    acteur              TEXT,
    date_debut          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_fin            TEXT
);
CREATE INDEX IF NOT EXISTS idx_calculs_runs_mois ON calculs_runs(mois, date_debut);

-- Un enregistrement par lot exécuté dans un run. stdout/stderr conservés tronqués (jamais un
-- chemin absolu de la machine dans le résumé affiché : le nettoyage se fait à la lecture).
CREATE TABLE IF NOT EXISTS calculs_run_lots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id_opaque       TEXT NOT NULL,
    ordre               INTEGER NOT NULL,
    lot                 TEXT NOT NULL,
    statut              TEXT NOT NULL,             -- ATTENDU|EN_COURS|SUCCES|ECHEC|IGNORE|TIMEOUT
    code_retour         INTEGER,
    duree_s             REAL,
    stdout_extrait      TEXT,
    stderr_extrait      TEXT,
    sorties_json        TEXT,                      -- JSON : fichiers attendus -> présent/absent
    message             TEXT,
    date_debut          TEXT,
    date_fin            TEXT
);
CREATE INDEX IF NOT EXISTS idx_calculs_run_lots ON calculs_run_lots(run_id_opaque, ordre);

-- Indicateurs métier relevés à la fin d'un run, pour la comparaison avant/après.
CREATE TABLE IF NOT EXISTS calculs_indicateurs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id_opaque       TEXT NOT NULL,
    mois                TEXT NOT NULL,
    indicateur          TEXT NOT NULL,             -- ca, commissions, charges, net_proprietaire, …
    valeur              REAL,
    nb_lignes           INTEGER,
    date_releve         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_calculs_indicateurs ON calculs_indicateurs(mois, indicateur, run_id_opaque);

-- Sauvegardes des sorties avant exécution (permet la restauration si la chaîne devient incohérente).
CREATE TABLE IF NOT EXISTS calculs_sauvegardes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id_opaque       TEXT NOT NULL,
    fichier             TEXT NOT NULL,             -- chemin relatif à la racine du projet
    sauvegarde          TEXT NOT NULL,             -- chemin de la copie
    sha256_avant        TEXT,
    restaure            INTEGER NOT NULL DEFAULT 0,
    date_sauvegarde     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_calculs_sauvegardes ON calculs_sauvegardes(run_id_opaque);

-- Statut mensuel applicatif (préparation/validation humaine de la clôture).
CREATE TABLE IF NOT EXISTS cloture_statuts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                TEXT NOT NULL UNIQUE,      -- AAAA-MM
    statut              TEXT NOT NULL DEFAULT 'OUVERTE',
        -- OUVERTE|EN_CALCUL|A_CONTROLER|BLOQUEE|VALIDEE|CLOTUREE|ROUVERTE
    run_id_opaque       TEXT,                      -- dernier run de référence
    commentaire         TEXT,
    acteur              TEXT,
    date_modification   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version             INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS cloture_statut_evenements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                TEXT NOT NULL,
    ancien_statut       TEXT,
    nouveau_statut      TEXT NOT NULL,
    commentaire         TEXT,
    acteur              TEXT,
    date_evenement      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_cloture_statut_evt ON cloture_statut_evenements(mois, date_evenement);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0018');

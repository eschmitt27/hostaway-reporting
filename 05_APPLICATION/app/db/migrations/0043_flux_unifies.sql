-- Lot9 — flux économique unifié (SQLite), remplace la lecture applicative de MASTER_CALC_Flux.xlsx.
--
-- Reprend les 22 colonnes du classeur legacy (`02_TRAVAIL/lot9_construire_flux.py::COLS`), sans le
-- compteur positionnel `n` du `flux_id` legacy (`FLUX-{mois}-{code_impact}-{module}-{n:04d}` —
-- instable si l'ordre ou le filtrage change). `flux_id` est ici dérivé de `ROW_HASH`, lui-même
-- dérivé de `source_pk` (clé métier stable de chaque module) : `flux_id` ne bouge donc jamais
-- pour une même ligne source, même si d'autres lignes apparaissent/disparaissent avant elle.
--
-- Remplacement intégral à chaque construction (DELETE + INSERT) — un instantané du dernier calcul,
-- pas un historique à cumuler (même statut que `controles_lot11_constats`, 0041).
CREATE TABLE IF NOT EXISTS flux_unifies (
    flux_id                         TEXT PRIMARY KEY,
    row_hash                        TEXT NOT NULL,
    source_module                   TEXT NOT NULL,
    source_table                    TEXT NOT NULL,
    source_pk                       TEXT,
    date_flux                       TEXT,
    mois                            TEXT,
    logement_id                     TEXT,
    proprietaire_id                 TEXT,
    associe_id                      TEXT,
    type_flux_id                    TEXT,
    sens                            TEXT,
    montant                         REAL,
    code_impact                     TEXT,
    inclure_resultat_reel           TEXT,
    inclure_resultat_comptable      TEXT,
    inclure_resultat_hors_compta    TEXT,
    statut_controle                 TEXT,
    niveau_anomalie                 TEXT,
    code_anomalie                   TEXT,
    commentaire                     TEXT,
    date_integration                TEXT,
    run_id                          TEXT,
    date_calcul                     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_flux_unifies_mois ON flux_unifies(mois);
CREATE INDEX IF NOT EXISTS idx_flux_unifies_module ON flux_unifies(source_module, source_table);
CREATE INDEX IF NOT EXISTS idx_flux_unifies_logement ON flux_unifies(logement_id);

CREATE TABLE IF NOT EXISTS flux_unifies_runs (
    run_id                TEXT PRIMARY KEY,
    date_calcul           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    nb_res                INTEGER,
    nb_men                INTEGER,
    nb_bnq                INTEGER,
    nb_chg                INTEGER,
    nb_gpm                INTEGER,
    nb_total              INTEGER,
    nb_doublons           INTEGER,
    statut                TEXT,
    detail                TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0043');

-- Migration 0038 — Sorties SQLite des Lots 6a (comptage)/6b/6d/6e/6f.
--
-- Additive comme 0017→0037. Aucun ALTER TABLE.
--
-- POURQUOI PAS UN REGISTRE DE DATASETS (comme reservations_datasets, 0034)
-- Ce registre existe pour les réservations parce qu'un mois CLOS y est immuable et que deux étapes
-- (CALCULEES/RESOLUES) doivent rester comparables dans le temps. Aucun de ces deux besoins n'existe
-- ici : chaque table ci-dessous est un calcul dérivé, intégralement remplacé à chaque run
-- (DELETE + INSERT), comme un cache. La fraîcheur (mission §33) tient sur deux colonnes portées par
-- la ligne elle-même : `run_id`/`date_calcul` — pas de mtime de fichier, pas de table séparée.
--
-- GRAIN CONSERVÉ : une table par sortie PRINCIPALE de chaque Lot (celle que le Lot suivant ou
-- l'application consomme réellement). Les onglets de RÉSUMÉ (RESUME_LOGEMENT, RESUME_INTERVENANT,
-- CONTROLES...) des classeurs legacy sont des agrégats dérivables de ces tables : ne pas les
-- dupliquer en SQLite évite un modèle qui diverge de sa propre source.

-- ═══ LOT 6A — comptage CleaningTasks enrichi (résolution logement/mois) ══════════════════════════
CREATE TABLE IF NOT EXISTS menages_taches_enrichies (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id               TEXT NOT NULL,
    row_hash              TEXT,
    mois                  TEXT,
    logement_id           TEXT,
    proprietaire_id       TEXT,
    listing_map_id        TEXT,
    reservation_id        TEXT,
    scheduled_date        TEXT,
    title                 TEXT,
    status                TEXT,
    statut_menage         TEXT,
    type_ligne_menage_id  TEXT,
    type_ligne_menage_lib TEXT,
    compte_comme_menage   TEXT,
    cost                  REAL,
    h6_note               TEXT,
    statut_controle       TEXT,
    niveau_anomalie       TEXT,
    code_anomalie         TEXT,
    extrait_le            TEXT,
    date_integration      TEXT,
    run_id                TEXT,
    date_calcul           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mte_mois_logement ON menages_taches_enrichies(mois, logement_id);
CREATE INDEX IF NOT EXISTS idx_mte_task ON menages_taches_enrichies(task_id);

-- ═══ LOT 6B — déclarations internes normalisées (dépivot Google Sheet) ══════════════════════════
CREATE TABLE IF NOT EXISTS menages_declarations_internes (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                        TEXT,
    annee                       TEXT,
    mois_saisie                 TEXT,
    appartement_source          TEXT,
    nom_appartement             TEXT,
    logement_id                 TEXT,
    intervenant_source          TEXT,
    intervenant_id              TEXT,
    nom_intervenant             TEXT,
    type_intervenant            TEXT,
    nb_menages                  INTEGER,
    nb_heures                   REAL,
    cout_lavage_attribue        REAL,
    lavage_non_attribuable_mois REAL,
    statut_controle             TEXT,
    code_controle               TEXT,
    source_url                  TEXT,
    date_extraction             TEXT,
    row_hash                    TEXT,
    run_id                      TEXT,
    date_calcul                 TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mdi_mois_logement ON menages_declarations_internes(mois, logement_id);
CREATE INDEX IF NOT EXISTS idx_mdi_intervenant ON menages_declarations_internes(intervenant_id);

-- ═══ LOT 6D — rapprochement Tasks Hostaway ↔ déclarations/factures ══════════════════════════════
CREATE TABLE IF NOT EXISTS menages_rapprochement (
    id                                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                                TEXT,
    nom_appartement                     TEXT,
    logement_id                         TEXT,
    proprietaire_id                     TEXT,
    intervenant_id                      TEXT,
    nom_intervenant                     TEXT,
    type_intervenant                    TEXT,
    source_mapping_hostaway             TEXT,
    nb_menages_tasks_hostaway_completed INTEGER,
    nb_menages_declares_externe         INTEGER,
    nb_menages_declares_interne_m04     INTEGER,
    total_menages_declares              INTEGER,
    ecart                               INTEGER,
    statut_controle                     TEXT,
    code_controle                       TEXT,
    commentaire                         TEXT,
    run_id                              TEXT,
    date_calcul                         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mrap_mois_logement ON menages_rapprochement(mois, logement_id);

-- ═══ LOT 6E — écart gain/perte vs coût standard ═════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS menages_gainperte (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                   TEXT,
    nom_appartement        TEXT,
    logement_id            TEXT,
    type_logement_id       TEXT,
    type_logement_libelle  TEXT,
    intervenant_id         TEXT,
    nom_intervenant        TEXT,
    type_intervenant       TEXT,
    nb_menages             INTEGER,
    nb_heures              REAL,
    cout_standard_unitaire REAL,
    cout_standard_total    REAL,
    methode_cout_reel      TEXT,
    cout_reel_unitaire     REAL,
    cout_reel_total        REAL,
    ecart_total            REAL,
    statut_ecart           TEXT,
    statut_controle        TEXT,
    code_controle          TEXT,
    commentaire            TEXT,
    run_id                 TEXT,
    date_calcul            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mgp_mois_logement ON menages_gainperte(mois, logement_id);

-- ═══ LOT 6F — coût complet ménage (avec quote-parts) ════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS menages_cout_complet (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    mois                            TEXT,
    logement_id                     TEXT,
    nom_appartement                 TEXT,
    proprietaire_id                 TEXT,
    type_logement_id                TEXT,
    intervenant_id                  TEXT,
    nom_intervenant                 TEXT,
    type_intervenant                TEXT,
    nb_menages                      INTEGER,
    cout_standard_total             REAL,
    cout_direct_total                REAL,
    quote_part_local                REAL,
    quote_part_courses              REAL,
    quote_part_lavage               REAL,
    quote_part_consommables         REAL,
    quote_part_autres_charges_menage REAL,
    cout_complet_total              REAL,
    cout_complet_unitaire           REAL,
    ecart_vs_standard_total         REAL,
    ecart_unitaire                  REAL,
    methode                         TEXT,
    cout_interne_ref_id             TEXT,
    cout_interne_priorite           INTEGER,
    controle_cout_interne           TEXT,
    statut_ecart                    TEXT,
    statut_controle                 TEXT,
    code_controle                   TEXT,
    commentaire                     TEXT,
    run_id                          TEXT,
    date_calcul                     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_mcc_mois_logement ON menages_cout_complet(mois, logement_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0038');

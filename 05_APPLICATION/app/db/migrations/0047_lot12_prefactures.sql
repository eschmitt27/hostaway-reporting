-- Migration 0047 — Lot12, préfactures propriétaires (SQLite natif).
--
-- Additive comme 0017→0046. Aucun ALTER TABLE.
--
-- Reprend le grain et les colonnes des 5 onglets de `MASTER_FACT_Proprietaires.xlsx` produits par
-- `02_TRAVAIL/lot12_generer_factures.py` (Module 10). RÈGLE FONDAMENTALE INCHANGÉE : ces tables ne
-- portent QUE des PRÉFACTURES (statut_generation toujours PREFACTURE_CONTROLE) — jamais une
-- facture propriétaire ÉMISE. La facturation-propriétaire existante (`factures_service`,
-- `comptabilite_ecritures_service` via `ventes_lot12_adapter_service`) reste l'unique source de
-- vérité comptable ; ces tables ne sont lues par AUCUN générateur d'écriture.
--
-- Un run par construction (DELETE+INSERT dans une transaction unique, comme `lot10_runs` : dataset
-- versionné, `actif` désigne le dernier calcul réussi, un run raté ne remplace jamais le dernier
-- valide).
CREATE TABLE IF NOT EXISTS lot12_runs (
    run_id           TEXT PRIMARY KEY,
    statut           TEXT NOT NULL,   -- SUCCES | ECHEC
    actif            INTEGER NOT NULL DEFAULT 1,
    nb_entetes       INTEGER,
    nb_lignes        INTEGER,
    nb_controle      INTEGER,
    nb_dashboard     INTEGER,
    nb_a_controler   INTEGER,
    erreur_code      TEXT,
    erreur_message   TEXT,
    date_calcul      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_lot12_runs_actif ON lot12_runs(actif) WHERE actif = 1;

-- FACT_FACTURE_ENTETE — grain PRÉFACTURE (mois × propriétaire × logement).
CREATE TABLE IF NOT EXISTS lot12_prefactures_entete (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                 TEXT NOT NULL,
    facture_id             TEXT NOT NULL,   -- PREF-AAAA-MM-PROP-LOG-NNN
    mois                   TEXT,
    proprietaire_id        TEXT,
    nom_proprietaire       TEXT,
    adresse_proprietaire   TEXT,
    logement_id            TEXT,
    nom_logement           TEXT,
    periode_debut          TEXT,
    periode_fin            TEXT,
    nb_reservations        INTEGER,
    total_exploitation_net REAL,
    total_reglement_du     REAL,
    reste_a_payer          REAL,
    credit_a_traiter       REAL,
    mode_facturation       TEXT,
    statut_facture         TEXT,
    statut_generation      TEXT,   -- toujours PREFACTURE_CONTROLE
    balises                TEXT,
    date_generation        TEXT,
    UNIQUE (run_id, facture_id)
);
CREATE INDEX IF NOT EXISTS idx_lot12_entete_run ON lot12_prefactures_entete(run_id, mois);

-- FACT_FACTURE_LIGNES — 12 lignes par préfacture (13 si préparation canapé).
CREATE TABLE IF NOT EXISTS lot12_prefactures_lignes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    facture_id   TEXT NOT NULL,
    ligne_num    INTEGER NOT NULL,
    type_ligne   TEXT,
    libelle      TEXT,
    montant      REAL,
    bloc         TEXT,   -- EXPLOITATION | REGLEMENT
    commentaire  TEXT,
    UNIQUE (run_id, facture_id, ligne_num)
);
CREATE INDEX IF NOT EXISTS idx_lot12_lignes_run ON lot12_prefactures_lignes(run_id, facture_id);

-- CONTROLE_MENSUEL — grain mois × propriétaire × logement, inclut GLOBAL_NON_AFFECTE (D-LOT12-07,
-- contrôle uniquement, jamais en facture).
CREATE TABLE IF NOT EXISTS lot12_controle_mensuel (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                   TEXT NOT NULL,
    mois                     TEXT,
    proprietaire_id          TEXT,
    logement_id              TEXT,
    total_payout             REAL,
    total_menage             REAL,
    total_commission         REAL,
    total_preparation_canape REAL,
    charge_fixe              REAL,
    charges_except_refac     REAL,
    revenu_net_exploitation  REAL,
    montant_du               REAL,
    acomptes                 REAL,
    airbnb_impute            REAL,
    reste_a_payer            REAL,
    credit_a_traiter         REAL,
    nb_reservations          INTEGER,
    statut                   TEXT,
    code_anomalie            TEXT
);
CREATE INDEX IF NOT EXISTS idx_lot12_controle_run ON lot12_controle_mensuel(run_id, mois);

-- DASHBOARD_FACTURATION — grain mois × propriétaire.
CREATE TABLE IF NOT EXISTS lot12_dashboard_facturation (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                TEXT NOT NULL,
    mois                  TEXT,
    proprietaire_id       TEXT,
    nb_logements          INTEGER,
    nb_bloquants_mois     INTEGER,
    nb_a_controler_mois   INTEGER,
    facturation_lot12_ok  TEXT,
    mode_facturation      TEXT,
    statut_facture        TEXT,
    balises_non_resolues  TEXT
);
CREATE INDEX IF NOT EXISTS idx_lot12_dashboard_run ON lot12_dashboard_facturation(run_id, mois);

-- A_CONTROLER — réservations exclues + anomalies transverses de facturation.
CREATE TABLE IF NOT EXISTS lot12_a_controler (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id               TEXT NOT NULL,
    mois                 TEXT,
    proprietaire_id      TEXT,
    logement_id          TEXT,
    reservation          TEXT,
    code_anomalie        TEXT,
    severite             TEXT,
    impact_facturation   TEXT,
    message              TEXT
);
CREATE INDEX IF NOT EXISTS idx_lot12_ac_run ON lot12_a_controler(run_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0047');

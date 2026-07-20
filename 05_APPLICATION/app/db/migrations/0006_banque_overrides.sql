-- Migration 0006 — journal des décisions bancaires APP-4B (contrôle & catégorisation).
-- Journal applicatif UNIQUEMENT : décisions humaines, historique, versions. Ce n'est PAS la
-- vérité métier. La vérité reste Excel (BANQUE_LOT8_IMPORT.xlsx). SQLite journalise, ne fait
-- jamais autorité, et ne masque jamais une anomalie moteur.

-- Décisions par mouvement (une ligne ACTIVE par mouvement ; les précédentes restent en historique).
CREATE TABLE IF NOT EXISTS banque_overrides (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque   TEXT NOT NULL,        -- identifiant public opaque (aucune donnée de compte)
    mouvement_id_interne  TEXT NOT NULL,        -- correspondance interne vers la ligne NORM_Banque
    categorie_validee     TEXT,                 -- décision humaine (distincte de la proposition)
    type_flux_id          TEXT,
    proprietaire_id       TEXT,
    logement_id           TEXT,
    reservation_id        TEXT,
    facture_id            TEXT,
    statut_controle       TEXT NOT NULL,        -- A_CONTROLER|EN_COURS|CONTROLE|RAPPROCHE|IGNORE|ROUVERT
    commentaire           TEXT,
    justification         TEXT,                 -- obligatoire si statut IGNORE
    proposition_categorie TEXT,                 -- proposition moteur préservée (jamais écrasée)
    proposition_ia        TEXT,                 -- proposition IA préservée
    date_action           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    auteur                TEXT,
    source_action         TEXT NOT NULL DEFAULT 'APP',
    actif                 INTEGER NOT NULL DEFAULT 1,   -- 1 = décision courante, 0 = historique
    version               INTEGER NOT NULL DEFAULT 1,
    run_id                INTEGER                        -- run d'enregistrement-copie associé
);
CREATE INDEX IF NOT EXISTS idx_banque_overrides_mvt ON banque_overrides(mouvement_id_opaque, actif);

-- Runs d'enregistrement-copie / relance moteur sur copie (traçabilité).
CREATE TABLE IF NOT EXISTS banque_controle_runs (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    mode                 TEXT NOT NULL,        -- 'COPIES' | 'REEL'
    statut               TEXT NOT NULL,        -- SUCCES|ECHEC|BLOQUE|VERROUILLE
    workspace_path       TEXT,
    snapshot_id          INTEGER,
    nb_overrides         INTEGER,
    reel_intact          INTEGER,
    sha256_avant         TEXT,
    sha256_apres         TEXT,
    controles_avant_json TEXT,
    controles_apres_json TEXT,
    erreur_code          TEXT,
    erreur_resume        TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0006');

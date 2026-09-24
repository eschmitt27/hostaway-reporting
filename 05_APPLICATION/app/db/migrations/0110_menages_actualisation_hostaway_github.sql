-- Migration 0110 — « Actualiser les ménages » par le workflow GitHub des tâches de ménage, et
-- traitement des mois clôturés qui reçoivent de nouvelles données.
--
-- Numérotée 0110 (et non 0100) : la PR parallèle « Mission 2 — saisie des charges » peut apporter
-- ses propres migrations. Deux fichiers portant la même version s'excluraient silencieusement
-- (`schema_migrations` n'enregistre que le numéro).
--
-- 1. UNE LIGNE PAR ACTUALISATION DEMANDÉE
-- L'actualisation est asynchrone : le clic déclenche le run, l'écran suit son état. Cette table est
-- l'état partagé entre le clic, le travail de fond et l'écran — et la preuve, après coup, de ce qui
-- a été demandé, par qui, quel run l'a servi, quel artifact a été lu et ce qui a été activé.
-- AUCUN SECRET : ni jeton, ni en-tête, ni URL signée.
CREATE TABLE IF NOT EXISTS menages_actualisations_hostaway (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    refresh_id         TEXT NOT NULL UNIQUE,          -- identifiant interne
    request_id         TEXT NOT NULL UNIQUE,          -- MEN-REFRESH-… transmis au workflow
    acteur             TEXT NOT NULL,
    mois_affiche       TEXT,
    demande_le         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    etat               TEXT NOT NULL DEFAULT 'LANCEMENT'
        CHECK (etat IN ('LANCEMENT', 'EN_ATTENTE', 'EXTRACTION', 'RECUPERATION',
                        'RAPPROCHEMENT', 'TERMINE', 'ECHEC', 'ANNULE')),
    -- 1 tant que l'actualisation n'est pas terminée, NULL ensuite. L'index unique partiel ci-dessous
    -- garantit qu'il n'en existe JAMAIS deux actives : un double clic, deux onglets ou deux requêtes
    -- simultanées se heurtent à la base, pas à une vérification applicative contournable.
    active             INTEGER DEFAULT 1 CHECK (active IS NULL OR active = 1),
    github_run_id      TEXT,
    github_statut      TEXT,                          -- queued / in_progress / completed
    github_conclusion  TEXT,                          -- success / failure / cancelled / timed_out…
    maj_le             TEXT,
    termine_le         TEXT,
    code_erreur        TEXT,
    erreur             TEXT,                          -- message technique, jamais un secret
    artifact_id        TEXT,
    artifact_traite    INTEGER NOT NULL DEFAULT 0,
    extraction_id      TEXT,                          -- extraction Cleaning Tasks préparée
    nb_taches          INTEGER,
    synchronisation_faite INTEGER NOT NULL DEFAULT 0, -- jeu activé ET pipeline Ménages passé
    resume             TEXT                           -- phrases courtes, pour l'écran
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_menages_actualisation_active
    ON menages_actualisations_hostaway(active) WHERE active = 1;
CREATE INDEX IF NOT EXISTS idx_menages_actualisation_demande
    ON menages_actualisations_hostaway(demande_le);

-- 2. MOIS CLÔTURÉS QUI REÇOIVENT DE NOUVELLES DONNÉES
-- Le signalement existait (`menages_changements_mois_clotures`, 0071) ; la décision n'avait pas de
-- place. Elle en a une : rouvrir le mois, ou classer le signalement sans réouverture — les deux avec
-- un motif, un acteur et une date. Rien n'est effacé.
ALTER TABLE menages_changements_mois_clotures ADD COLUMN quantite INTEGER;
ALTER TABLE menages_changements_mois_clotures ADD COLUMN decision TEXT;        -- REOUVERT | CLASSE
ALTER TABLE menages_changements_mois_clotures ADD COLUMN decision_le TEXT;
ALTER TABLE menages_changements_mois_clotures ADD COLUMN decision_par TEXT;
ALTER TABLE menages_changements_mois_clotures ADD COLUMN decision_motif TEXT;

-- Journal des réouvertures de mois décidées depuis l'écran Ménages. Une réouverture ne reclôt
-- jamais d'elle-même : le mois passe en EN_CONTROLE (« à contrôler / à reclore ») et y reste jusqu'à
-- une nouvelle clôture par le parcours de clôture.
CREATE TABLE IF NOT EXISTS mois_reouvertures (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    mois               TEXT NOT NULL,
    statut_avant       TEXT NOT NULL,
    statut_apres       TEXT NOT NULL,
    motif              TEXT NOT NULL CHECK (length(trim(motif)) > 0),
    acteur             TEXT NOT NULL,
    reouvert_le        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    recalcul_statut    TEXT,                          -- SUCCES | ECHEC | NON_LANCE
    recalcul_message   TEXT
);
CREATE INDEX IF NOT EXISTS idx_mois_reouvertures_mois ON mois_reouvertures(mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0110');

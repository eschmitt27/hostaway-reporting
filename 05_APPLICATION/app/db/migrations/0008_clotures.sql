-- Migration 0008 — suivi humain de la CLÔTURE MENSUELLE (APP-5C).
-- La clôture RÉELLE reste exclusivement pilotée par REF_Cloture_Mensuelle (REF_Setup.xlsm, moteur,
-- D024) : statuts OUVERT/EN_CONTROLE/CLOTURE, jamais écrits par l'application. Ce module journalise
-- UNIQUEMENT la préparation et la validation HUMAINE (checklist, revue des contrôles APP-5B, décision
-- de passage), jamais une nouvelle vérité, jamais une clôture réelle. Append-only ; aucune suppression
-- physique ; version optimiste ; une seule ligne active par mois dans clotures_mensuelles.

CREATE TABLE IF NOT EXISTS clotures_mensuelles (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    cloture_id_opaque      TEXT NOT NULL UNIQUE,  -- identifiant public opaque CLO-xxxx
    mois                   TEXT NOT NULL,         -- AAAA-MM
    statut                 TEXT NOT NULL,         -- NON_DEMARREE|EN_PREPARATION|A_VALIDER|VALIDEE|ROUVERTE|ARCHIVEE
    date_creation           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_preparation        TEXT,
    date_validation         TEXT,
    date_reouverture        TEXT,
    commentaire_validation  TEXT,
    justification_reouverture TEXT,
    cree_par               TEXT,
    valide_par             TEXT,
    version                INTEGER NOT NULL DEFAULT 1,
    actif                  INTEGER NOT NULL DEFAULT 1   -- 1 = ligne courante du mois, 0 = historisée
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_clotures_mois_actif ON clotures_mensuelles(mois) WHERE actif = 1;
CREATE INDEX IF NOT EXISTS idx_clotures_opaque ON clotures_mensuelles(cloture_id_opaque);

-- Historique append-only de tous les événements de la clôture (transitions, actions).
CREATE TABLE IF NOT EXISTS cloture_evenements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cloture_id_opaque TEXT NOT NULL,
    type_evenement  TEXT NOT NULL,      -- CREATION|TRANSITION|COMMENTAIRE|PREUVE_AJOUTEE|EXPORT
    ancien_statut   TEXT,
    nouveau_statut  TEXT,
    commentaire     TEXT,
    preuve          TEXT,               -- référence logique de preuve (jamais un chemin réel)
    date_evenement  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur          TEXT,
    correlation_id  TEXT
);
CREATE INDEX IF NOT EXISTS idx_cloture_evenements_cloture ON cloture_evenements(cloture_id_opaque);

-- Snapshot logique des contrôles au passage en A_VALIDER (jamais de copie de fichier métier réel).
CREATE TABLE IF NOT EXISTS cloture_elements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cloture_id_opaque TEXT NOT NULL,
    ctrl_opaque     TEXT NOT NULL,      -- identifiant CTRL opaque APP-5B (jamais l'id moteur brut)
    code_controle   TEXT NOT NULL,
    entite_type     TEXT,               -- BANQUE|MENAGES_EXT|COMMISSIONS|RESERVATIONS|...
    entite_opaque   TEXT,               -- entité opaque (MVT-/CTRL-/... jamais un identifiant brut)
    severite        TEXT,               -- BLOQUANT|A_CONTROLER|INFO
    statut_moteur   TEXT,               -- PRESENTE|ABSENTE (anomalie moteur au moment du snapshot)
    statut_humain   TEXT,               -- statut de suivi APP-5B au moment du snapshot
    bloque_cloture  INTEGER NOT NULL DEFAULT 0,
    date_snapshot   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    actif           INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_cloture_elements_cloture ON cloture_elements(cloture_id_opaque, actif);

-- Métadonnées de preuves associées à la clôture (jamais le fichier lui-même, jamais de chemin réel).
CREATE TABLE IF NOT EXISTS cloture_documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id_opaque TEXT NOT NULL UNIQUE,  -- identifiant public opaque DOC-xxxx
    cloture_id_opaque TEXT NOT NULL,
    nom_logique     TEXT NOT NULL,       -- nom d'affichage, jamais un chemin
    type_document   TEXT,                -- PREUVE|EXPORT|COMMENTAIRE_JOINT
    chemin_logique  TEXT,                -- toujours sanitisé (<PROJECT_ROOT>\...), jamais un chemin réel
    hash            TEXT,
    taille          INTEGER,
    date_creation   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    actif           INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_cloture_documents_cloture ON cloture_documents(cloture_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0008');

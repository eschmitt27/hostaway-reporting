-- Migration 0015 — import bancaire applicatif + rapprochement multi-objets (module Banque, suite
-- APP-4A/4B/3F). Deux journaux applicatifs distincts, jamais une nouvelle vérité :
--   - banque_imports : traçabilité d'un import (fichier, compteurs, acteur) — la vérité du contenu
--     reste NORM_Banque (BANQUE_LOT8_IMPORT.xlsx), écrit par le writer atomique, jamais ici.
--   - banque_rapprochements : généralise le patron déjà éprouvé de la migration 0014
--     (rapprochements_reglements : identifiants opaques, critères explicables, statuts, historique
--     append-only) à N types d'objets métier (réservation, charge, fournisseur, associé...), avec
--     montant rapproché pour supporter le partiel et le multiple — jamais une relation 1-1 forcée.
-- Aucune donnée bancaire brute (IBAN, libellé brut, compte) n'est stockée ici : uniquement des
-- identifiants opaques déjà utilisés ailleurs dans l'application (MVT-xxxx).

CREATE TABLE IF NOT EXISTS banque_imports (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    import_id                TEXT NOT NULL UNIQUE,   -- IMP-xxxx (opaque, aucune donnée de compte)
    nom_fichier_origine      TEXT NOT NULL,           -- nom de fichier uniquement, jamais un chemin
    compte_id_opaque         TEXT,                    -- CPT-xxxx
    nb_lignes_lues           INTEGER NOT NULL DEFAULT 0,
    nb_valides               INTEGER NOT NULL DEFAULT 0,
    nb_doublons_certains     INTEGER NOT NULL DEFAULT 0,
    nb_doublons_probables    INTEGER NOT NULL DEFAULT 0,
    nb_invalides             INTEGER NOT NULL DEFAULT 0,
    total_debit              REAL,
    total_credit             REAL,
    statut                   TEXT NOT NULL,          -- SUCCES|ECHEC
    erreur_resume            TEXT,
    acteur                   TEXT,
    date_import              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_banque_imports_date ON banque_imports(date_import);

-- Rapprochement mouvement bancaire <-> objet métier. Plusieurs lignes actives possibles par
-- mouvement (couverture multi-objets) et par objet (plusieurs mouvements réglant un même objet) :
-- le contrôle porte sur la SOMME des montants rapprochés actifs, jamais sur un couple unique.
CREATE TABLE IF NOT EXISTS banque_rapprochements (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    rapprochement_id_opaque  TEXT NOT NULL UNIQUE,   -- BRP-xxxx
    mouvement_id_opaque      TEXT NOT NULL,          -- MVT-xxxx
    type_objet               TEXT NOT NULL,
        -- RESERVATION|PAYOUT_PLATEFORME|CHARGE_FOURNISSEUR|REGLEMENT_CHARGE|
        -- REVERSEMENT_PROPRIETAIRE|REMBOURSEMENT_ASSOCIE|REMBOURSEMENT_VOYAGEUR|
        -- MOUVEMENT_INTERNE|NON_IDENTIFIE
    objet_id                 TEXT,                   -- identifiant métier de l'objet, NULL si NON_IDENTIFIE
    montant_rapproche        REAL NOT NULL,
    statut                   TEXT NOT NULL DEFAULT 'PROPOSE',   -- PROPOSE|CONFIRME|REFUSE|ANNULE
    source                   TEXT NOT NULL DEFAULT 'MANUEL',    -- AUTO|MANUEL
    score_explicable         INTEGER,
    criteres_json            TEXT,
    commentaire              TEXT,
    date_creation            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                   TEXT,
    version                  INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_banque_rappro_mvt ON banque_rapprochements(mouvement_id_opaque, statut);
CREATE INDEX IF NOT EXISTS idx_banque_rappro_objet ON banque_rapprochements(type_objet, objet_id, statut);

-- Historique append-only des événements de rapprochement (créé, confirmé, refusé, annulé).
CREATE TABLE IF NOT EXISTS banque_rapprochement_evenements (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    rapprochement_id_opaque  TEXT NOT NULL,
    type_evenement           TEXT NOT NULL,   -- CREATION|CONFIRMATION|REFUS|ANNULATION
    ancien_statut            TEXT,
    nouveau_statut           TEXT,
    commentaire              TEXT,
    date_evenement           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                   TEXT
);
CREATE INDEX IF NOT EXISTS idx_banque_rappro_evt ON banque_rapprochement_evenements(rapprochement_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0015');

-- Migration 0026 — décisions humaines de classement pour la file A_ENVOYER_IA (module Banque).
-- Distincte de `banque_suggestion_decisions` (migration 0016, décisions mouvement<->objet de
-- rapprochement) : ici la décision porte sur mouvement<->catégorie, aucun objet métier impliqué.
-- Aucune IA externe, aucun appel réseau, aucune modification du moteur de classification.
-- Historique append-only : la ligne la plus récente pour un mouvement donné (ORDER BY id DESC)
-- fait foi ; les précédentes restent conservées, jamais supprimées ni écrasées.
-- Aucune donnée bancaire brute ici : uniquement des identifiants opaques (MVT-xxxx) déjà utilisés
-- ailleurs, jamais un libellé bancaire brut complet, un IBAN ou un numéro de compte.

CREATE TABLE IF NOT EXISTS banque_classement_decisions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_opaque          TEXT NOT NULL UNIQUE,   -- BCD-xxxx
    mouvement_id_opaque      TEXT NOT NULL,          -- MVT-xxxx
    ancienne_categorie       TEXT,
    nouvelle_categorie       TEXT,
    type_decision            TEXT NOT NULL,
        -- CATEGORISER|MAINTENIR_A_CONTROLER|NON_CLASSE|REPORTER
    justification            TEXT,
    anomalie_moteur          TEXT,
    future_regle             TEXT,
    date_decision            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                   TEXT,
    actif                    INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_banque_classement_mvt
    ON banque_classement_decisions(mouvement_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0026');

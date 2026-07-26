-- Migration 0016 — décisions sur les suggestions de rapprochement bancaire (module Banque, suite
-- migration 0015). Les suggestions elles-mêmes ne sont PAS stockées : elles sont recalculées à la
-- demande à partir des données réelles (mouvements + objets candidats). Seules les DÉCISIONS
-- humaines sont persistées, pour deux raisons :
--   - une suggestion refusée ne doit pas réapparaître tant que ni le mouvement ni l'objet candidat
--     n'ont changé (empreinte : on mémorise l'état au moment du refus) ;
--   - l'historique de ce qui a été proposé/accepté/refusé doit rester auditable.
-- Aucune donnée bancaire brute ici : uniquement des identifiants opaques (MVT-xxxx) déjà utilisés
-- ailleurs, jamais un IBAN, un libellé brut ou un numéro de compte.

CREATE TABLE IF NOT EXISTS banque_suggestion_decisions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque      TEXT NOT NULL,          -- MVT-xxxx
    type_objet               TEXT NOT NULL,
    objet_id                 TEXT NOT NULL,
    decision                 TEXT NOT NULL,          -- REFUSEE|IGNOREE_TEMPORAIREMENT|ACCEPTEE
    empreinte_candidat       TEXT NOT NULL,          -- hash (mouvement + objet) au moment de la décision
    montant_propose          REAL,
    score                    INTEGER,
    niveau                   TEXT,                   -- EXACT|PROBABLE|FAIBLE
    commentaire              TEXT,
    acteur                   TEXT,
    date_decision            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
-- Une seule décision courante par (mouvement, objet) : la plus récente fait foi (ORDER BY id DESC).
CREATE INDEX IF NOT EXISTS idx_banque_sugg_mvt
    ON banque_suggestion_decisions(mouvement_id_opaque, type_objet, objet_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0016');

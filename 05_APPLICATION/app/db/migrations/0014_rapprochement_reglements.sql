-- Migration 0014 — rapprochement déclaratif règlement propriétaire ↔ mouvement bancaire (APP-3F).
-- LECTURE SEULE côté bancaire : ce journal ne stocke QUE des identifiants opaques et une empreinte
-- du mouvement — jamais d'IBAN/RIB/BIC/numéro de compte, jamais le libellé bancaire brut, jamais le
-- mouvement complet, jamais un chemin de fichier. La vérité des montants reste le moteur ; la vérité
-- des mouvements reste le fichier bancaire, jamais modifié. « Rapprochement déclaratif interne — ne
-- constitue ni un ordre de paiement ni une preuve bancaire certifiée. »
-- Version optimiste ; historique append-only ; aucune suppression physique.

CREATE TABLE IF NOT EXISTS rapprochements_reglements (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    rapprochement_id_opaque  TEXT NOT NULL UNIQUE,   -- RAP-xxxx
    releve_id_opaque         TEXT NOT NULL,          -- REG-xxxx (règlement APP-3E)
    mouvement_id_opaque      TEXT,                   -- MVT-xxxx (mouvement bancaire masqué), NULL tant qu'aucun candidat sélectionné
    statut                   TEXT NOT NULL DEFAULT 'NON_RAPPROCHE',
        -- NON_RAPPROCHE|PROPOSITION_DISPONIBLE|A_CONTROLER|RAPPROCHE|ECARTE|ANOMALIE|ROUVERT|ANNULE
    criteres_json            TEXT,                   -- critères satisfaits (explicable), jamais un score opaque
    score_explicable         INTEGER,                -- nombre de critères satisfaits (0..N), facultatif
    mouvement_empreinte      TEXT,                   -- hash du mouvement au moment de la proposition (détection disparition/modif)
    ecart_montant            REAL,                   -- montant déclaré (moteur) - montant candidat ; informatif
    ecart_jours              INTEGER,                -- |date déclaration - date mouvement| en jours ; informatif
    decision                 TEXT,                   -- CONFIRME|ECARTE|ANOMALIE (décision humaine)
    commentaire              TEXT,
    motif                    TEXT,
    date_creation            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                   TEXT,
    version                  INTEGER NOT NULL DEFAULT 1
);
-- Au plus un rapprochement actif (non ANNULE) par règlement.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rapprochements_releve_actif
    ON rapprochements_reglements(releve_id_opaque) WHERE statut <> 'ANNULE';
-- Un mouvement confirmé ne peut servir qu'à un seul rapprochement RAPPROCHE.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rapprochements_mouvement_confirme
    ON rapprochements_reglements(mouvement_id_opaque) WHERE statut = 'RAPPROCHE';
CREATE INDEX IF NOT EXISTS idx_rapprochements_opaque ON rapprochements_reglements(rapprochement_id_opaque);

-- Historique append-only de tous les événements du rapprochement.
CREATE TABLE IF NOT EXISTS rapprochement_evenements (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    rapprochement_id_opaque  TEXT NOT NULL,
    type_evenement           TEXT NOT NULL,   -- CREATION|TRANSITION|PROPOSITION|COMMENTAIRE|EXPORT
    ancien_statut            TEXT,
    nouveau_statut           TEXT,
    commentaire              TEXT,
    date_evenement           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                   TEXT
);
CREATE INDEX IF NOT EXISTS idx_rapprochement_evenements_rap
    ON rapprochement_evenements(rapprochement_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0014');

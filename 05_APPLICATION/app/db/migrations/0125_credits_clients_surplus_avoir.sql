-- Migration 0125 — Crédit client issu du SURPLUS d'un avoir émis (2026-10-03).
--
-- Un avoir émis qui dépasse la créance restante du client ne laisse plus son surplus au crédit du
-- 411 : le surplus est reclassé 411 D / 419700 C (auxiliaire = client) et enregistré comme un
-- crédit client (origine SURPLUS_AVOIR, référence = l'avoir). Une seule source de vérité pour les
-- crédits clients : 419700 + `credits_clients`. Reconstruction de la table pour élargir le CHECK.

DROP TRIGGER IF EXISTS trg_credits_clients_sans_suppression;

CREATE TABLE credits_clients_0125 (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    credit_id_opaque    TEXT NOT NULL UNIQUE,                    -- CRD-xxxx
    proprietaire_id     TEXT NOT NULL,
    origine             TEXT NOT NULL CHECK (origine IN ('REVERSEMENT_AIRBNB', 'REPRISE_SOLDE',
                                                        'SURPLUS_AVOIR')),
    mode_origine        TEXT NOT NULL CHECK (mode_origine IN ('BANQUE', 'JUSTIFIE')),
    date_origine        TEXT NOT NULL,
    mois                TEXT,
    montant_initial     REAL NOT NULL CHECK (montant_initial > 0),
    reference           TEXT,                                    -- référence Airbnb, libellé
    justification       TEXT,
    compte_source       TEXT,                                    -- mode JUSTIFIE seulement
    statut              TEXT NOT NULL DEFAULT 'EN_ATTENTE_ORIGINE'
        CHECK (statut IN ('EN_ATTENTE_ORIGINE', 'DISPONIBLE', 'ANNULE')),
    mouvement_origine   TEXT,                                    -- mouvement bancaire rapproché
    ecriture_origine    TEXT,                                    -- écriture … / 419100
    lettrage_origine    TEXT,
    cree_par            TEXT NOT NULL,
    cree_le             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version             INTEGER NOT NULL DEFAULT 1,
    CHECK (mode_origine <> 'JUSTIFIE'
           OR (compte_source IS NOT NULL AND length(trim(COALESCE(justification, ''))) > 0))
);
INSERT INTO credits_clients_0125 SELECT * FROM credits_clients;
DROP TABLE credits_clients;
ALTER TABLE credits_clients_0125 RENAME TO credits_clients;
CREATE INDEX IF NOT EXISTS idx_credits_clients_proprietaire ON credits_clients(proprietaire_id);
CREATE TRIGGER IF NOT EXISTS trg_credits_clients_sans_suppression
BEFORE DELETE ON credits_clients
BEGIN
    SELECT RAISE(ABORT, 'Suppression interdite : un crédit s''annule, il ne se supprime pas.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0125');

-- Migration 0123 — Crédit client repris de l'ancienne structure (2026-10-03).
--
-- Un crédit client peut désormais avoir pour origine une REPRISE DE SOLDE : un solde créditeur
-- dû au propriétaire par l'ancienne structure, que la nouvelle société s'engage à honorer. Ce n'est
-- ni un paiement encaissé, ni une facture, ni une réduction, ni un acompte, ni un mouvement
-- bancaire. Comptabilisation (cf. `credits_clients_service.creer_reprise_solde`) :
--     467100 Ancienne structure  D  /  419100 Crédit client (auxiliaire = propriétaire)  C
--     654000 Perte sur créance irrécouvrable  D  /  467100 Ancienne structure  C
--
-- `credits_clients` porte un CHECK sur `origine` : SQLite ne sait pas élargir un CHECK, la table
-- est reconstruite à l'identique (données, index, déclencheur), avec la nouvelle origine.
-- Les deux comptes manquants sont ajoutés au plan comptable existant (aucun plan parallèle).

DROP TRIGGER IF EXISTS trg_credits_clients_sans_suppression;

CREATE TABLE credits_clients_0123 (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    credit_id_opaque    TEXT NOT NULL UNIQUE,                    -- CRD-xxxx
    proprietaire_id     TEXT NOT NULL,
    origine             TEXT NOT NULL CHECK (origine IN ('REVERSEMENT_AIRBNB', 'REPRISE_SOLDE')),
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
INSERT INTO credits_clients_0123 SELECT * FROM credits_clients;
DROP TABLE credits_clients;
ALTER TABLE credits_clients_0123 RENAME TO credits_clients;
CREATE INDEX IF NOT EXISTS idx_credits_clients_proprietaire ON credits_clients(proprietaire_id);
CREATE TRIGGER IF NOT EXISTS trg_credits_clients_sans_suppression
BEFORE DELETE ON credits_clients
BEGIN
    SELECT RAISE(ABORT, 'Suppression interdite : un crédit s''annule, il ne se supprime pas.');
END;

INSERT OR IGNORE INTO plan_comptable (compte, libelle, type_compte, actif, auxiliaire_autorise,
                                      auxiliaire_mode, commentaire)
VALUES ('467100', 'Ancienne structure — reprise des soldes', 'PASSIF', 1, 0, 'NONE',
        'Reprise des soldes clients de l''ancienne structure (migration 0123)'),
       ('654000', 'Pertes sur créances irrécouvrables', 'CHARGE', 1, 0, 'NONE',
        'Créance sur l''ancienne structure considérée perdue (migration 0123)');

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0123');

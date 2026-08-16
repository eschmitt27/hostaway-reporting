-- Migration 0030 — Compte propriétaire global et allocations FIFO.
--
-- Additive comme 0017→0029 : uniquement des CREATE TABLE / CREATE INDEX IF NOT EXISTS.
--
-- CE QUI N'EST PAS CRÉÉ ICI, ET POURQUOI
-- Aucune table de « paiement propriétaire », aucune table de « facture », aucune table de
-- « crédit ». Elles existent déjà ou n'ont pas lieu d'être :
--   - la créance est portée par `factures_proprietaires` (0027) ;
--   - le paiement reçu et la somme à reverser sont portés par
--     `mouvements_tresorerie_proprietaires` (0025), qui distingue déjà le sens et la nature ;
--   - le CRÉDIT (acompte) n'est PAS un objet : c'est la part d'une source financière qui n'a pas
--     trouvé de facture à solder. Le stocker en créerait un second exemplaire, qui divergerait.
--     Il se déduit, comme tous les soldes de cette application.
--
-- CE QUI MANQUAIT
-- Le lien entre l'argent et les factures. `creances_dettes_service._imputations()` retournait 0.0
-- en dur faute de pouvoir répondre à « quelle facture ce paiement a-t-il soldé ». C'est ce que
-- `proprietaire_allocations` enregistre.
--
-- DÉRIVÉ *ET* JOURNALISÉ
-- Une allocation FIFO est entièrement déterminée par ses entrées : elle est donc recalculable, et
-- un recalcul remplace intégralement les allocations du propriétaire concerné. Elles sont malgré
-- tout stockées, pour trois raisons : expliquer un solde sans refaire le calcul, permettre l'audit
-- d'un état passé via `proprietaire_recalculs`, et rendre visible une divergence si les entrées
-- changent. Le journal des recalculs, lui, ne s'efface jamais.

CREATE TABLE IF NOT EXISTS proprietaire_allocations (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    allocation_id_opaque  TEXT NOT NULL UNIQUE,      -- ALO-xxxx
    proprietaire_id       TEXT NOT NULL,
    recalcul_id           TEXT NOT NULL,             -- recalcul qui a produit cette allocation
    source_type           TEXT NOT NULL,
        -- PAIEMENT|AVOIR|REVERSEMENT
        -- PAIEMENT    : mouvement PROPRIETAIRE_VERS_SOCIETE validé — argent réellement reçu.
        -- AVOIR       : facture d'avoir émise — réduit ce que doit le propriétaire.
        -- REVERSEMENT : mouvement SOCIETE_VERS_PROPRIETAIRE validé, utilisé en COMPENSATION.
        --               Une allocation de ce type diminue le virement net, elle n'encaisse rien.
    source_ref            TEXT NOT NULL,             -- mouvement_opaque ou facture_id_opaque (avoir)
    source_date           TEXT NOT NULL,             -- date qui a servi à ordonner les sources
    facture_id_opaque     TEXT NOT NULL,             -- facture soldée, totalement ou partiellement
    montant_alloue        REAL NOT NULL,
    rang_fifo             INTEGER NOT NULL,          -- ordre de consommation, pour rejouer le calcul
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_alloc_proprietaire ON proprietaire_allocations(proprietaire_id);
CREATE INDEX IF NOT EXISTS idx_alloc_facture ON proprietaire_allocations(facture_id_opaque);
CREATE INDEX IF NOT EXISTS idx_alloc_source ON proprietaire_allocations(source_type, source_ref);
CREATE INDEX IF NOT EXISTS idx_alloc_recalcul ON proprietaire_allocations(recalcul_id);

-- Journal append-only des recalculs. `empreinte_entrees` est l'empreinte des factures et sources
-- utilisées : deux recalculs de même empreinte doivent produire les mêmes allocations. C'est le
-- test de déterminisme, et c'est aussi ce qui permet de dire « les entrées ont changé depuis ».
CREATE TABLE IF NOT EXISTS proprietaire_recalculs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    recalcul_id           TEXT NOT NULL UNIQUE,      -- RCL-xxxx
    proprietaire_id       TEXT NOT NULL,
    horodatage            TEXT NOT NULL,
    empreinte_entrees     TEXT NOT NULL,
    empreinte_allocations TEXT NOT NULL,
    nb_factures           INTEGER NOT NULL DEFAULT 0,
    nb_sources            INTEGER NOT NULL DEFAULT 0,
    nb_allocations        INTEGER NOT NULL DEFAULT 0,
    montant_alloue        REAL NOT NULL DEFAULT 0,
    creance_restante      REAL NOT NULL DEFAULT 0,
    credit_restant        REAL NOT NULL DEFAULT 0,
    declencheur           TEXT NOT NULL DEFAULT 'MANUEL'   -- MANUEL|AUTO
);
CREATE INDEX IF NOT EXISTS idx_recalcul_proprietaire
    ON proprietaire_recalculs(proprietaire_id, horodatage);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0030');

-- Migration 0020 — classification des factures (audit `42_AUDIT_MODELE_FACTURES_CHARGES_REGLEMENTS`).
--
-- La table `factures` (0017) ne modélisait que les factures fournisseurs reçues. Aucune confusion
-- de code n'a été trouvée entre facture fournisseur et facture propriétaire (celle-ci reste
-- entièrement portée par lot12, Excel, jamais en SQLite) : l'audit a identifié une ABSENCE de
-- modèle commun, pas un bug. Ces colonnes préparent le terrain pour le premier socle Comptabilité
-- (Mission 3), qui doit savoir classer une facture, sans migrer lot12 vers SQLite.
--
-- Table SÉPARÉE plutôt qu'ALTER TABLE : même raison que 0017 (migrations rejouées à chaque
-- démarrage, pas d'ADD COLUMN IF NOT EXISTS en SQLite).

CREATE TABLE IF NOT EXISTS facture_classification (
    facture_id_opaque TEXT PRIMARY KEY,
    sens              TEXT NOT NULL DEFAULT 'RECUE',
        -- RECUE|EMISE
    type_facture      TEXT NOT NULL DEFAULT 'FOURNISSEUR',
        -- FOURNISSEUR|PROPRIETAIRE|VOYAGEUR|AVOIR_RECU|AVOIR_EMIS
    date_modification TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Rétro-classification : toutes les factures déjà en base sont des factures fournisseurs reçues
-- (seul circuit qui existait avant cette migration).
INSERT OR IGNORE INTO facture_classification (facture_id_opaque, sens, type_facture)
SELECT facture_id_opaque, 'RECUE', 'FOURNISSEUR' FROM factures;

-- Toute facture créée APRÈS cette migration (par factures_service.creer(), qui ne connaît pas
-- cette table) reçoit automatiquement la classification par défaut — sans exiger de modifier le
-- service existant.
CREATE TRIGGER IF NOT EXISTS trg_facture_classification_defaut
AFTER INSERT ON factures
BEGIN
    INSERT OR IGNORE INTO facture_classification (facture_id_opaque)
    VALUES (NEW.facture_id_opaque);
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0020');

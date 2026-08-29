-- Migration 0063 — Ajoute CHARGE_REFACTUREE au CHECK(type_ligne) de factures_proprietaires_lignes
-- (mission 15).
--
-- Avant cette migration, une facture ne portait qu'UNE ligne agrégée 'CHARGES_EXCEPT_REFAC' pour
-- toutes les charges refacturées du mois. Désormais, chaque décision d'imputation individuelle
-- (`charges_refacturation_service.imputer`) produit sa PROPRE ligne 'CHARGE_REFACTUREE' — le
-- propriétaire doit voir exactement quelle charge il paie, pas un total masqué (mission 15,
-- partie Q). 'CHARGES_EXCEPT_REFAC' reste une valeur autorisée pour compatibilité ascendante des
-- factures déjà émises avant cette migration (jamais réécrites).
--
-- SQLite n'a pas d'ALTER TABLE ... DROP/MODIFY CONSTRAINT : reconstruction de table, même schéma
-- que la migration 0055 qui a créé ce CHECK.
PRAGMA foreign_keys=OFF;

CREATE TABLE factures_proprietaires_lignes_new (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ligne_id_opaque       TEXT NOT NULL UNIQUE,
    facture_id_opaque     TEXT NOT NULL,
    numero_ligne          INTEGER NOT NULL,
    type_ligne            TEXT NOT NULL CHECK (type_ligne IN (
        'COMMISSION_CONCIERGERIE', 'MENAGE_FACTURE', 'PREPARATION_CANAPE',
        'CHARGE_FIXE', 'CHARGES_EXCEPT_REFAC', 'CHARGE_REFACTUREE')),
    libelle               TEXT NOT NULL,
    montant               REAL NOT NULL,
    objet_source_type     TEXT,
    objet_source_ref      TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (facture_id_opaque) REFERENCES factures_proprietaires(facture_id_opaque)
);
INSERT INTO factures_proprietaires_lignes_new (
    id, ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant,
    objet_source_type, objet_source_ref, date_creation)
SELECT id, ligne_id_opaque, facture_id_opaque, numero_ligne, type_ligne, libelle, montant,
       objet_source_type, objet_source_ref, date_creation
FROM factures_proprietaires_lignes;
DROP TABLE factures_proprietaires_lignes;
ALTER TABLE factures_proprietaires_lignes_new RENAME TO factures_proprietaires_lignes;

CREATE INDEX IF NOT EXISTS idx_fprl_facture ON factures_proprietaires_lignes(facture_id_opaque);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fprl_ordre
    ON factures_proprietaires_lignes(facture_id_opaque, numero_ligne);

PRAGMA foreign_keys=ON;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0063');

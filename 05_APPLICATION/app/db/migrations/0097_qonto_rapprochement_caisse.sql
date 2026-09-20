-- Rapprochement Qonto : nature d'une transaction, et transferts Banque → Caisse.
--
-- Trois ajouts, aucun effet comptable :
--   1. la catégorie de flux que le titulaire a lui-même posée dans Qonto, remontée en colonne ;
--   2. la nature déduite d'une transaction, à côté de son statut de traitement ;
--   3. les transferts internes Banque → Caisse nés d'un retrait d'espèces.

-- ── 1. Catégorie de flux Qonto ────────────────────────────────────────────────────────────────
-- `cashflow_category.name` est une information HUMAINE : c'est le titulaire du compte qui range
-- lui-même un mouvement dans Qonto (« Apport en compte courant d'associé »…). C'est donc une
-- preuve utilisable par le moteur de suggestions, pas une devinette de notre part. Elle dormait
-- dans la charge utile brute ; elle devient une colonne pour être requêtable.
ALTER TABLE qonto_transactions_raw ADD COLUMN categorie_flux TEXT;

UPDATE qonto_transactions_raw
   SET categorie_flux = json_extract(charge_utile_json, '$.cashflow_category.name')
 WHERE categorie_flux IS NULL;

-- ── 2. Nature déduite ─────────────────────────────────────────────────────────────────────────
-- `statut_local` dit OÙ EN EST le traitement (à rapprocher, à contrôler, en attente…).
-- `nature` dit DE QUOI IL S'AGIT (retrait d'espèces, apport, encaissement…). Deux questions
-- différentes : les mélanger dans une colonne obligerait à choisir laquelle perdre.
ALTER TABLE qonto_transactions_statut_local ADD COLUMN nature TEXT;
ALTER TABLE qonto_transactions_statut_local ADD COLUMN nature_motif TEXT;

-- ── 3. Transferts Banque → Caisse ─────────────────────────────────────────────────────────────
-- Un retrait au distributeur n'est ni une charge ni un produit : l'argent change de poche. La
-- banque baisse, la caisse monte, le résultat ne bouge pas. Aucune décision humaine n'est requise
-- sur sa nature — ce qui ne veut pas dire qu'il est acquis d'avance : tant que Qonto le donne
-- `pending`, le transfert est PROVISOIRE et ne compte pas dans l'encaisse.
--
-- `qonto_transaction_uuid` est UNIQUE : le double comptage est rendu impossible par le schéma, et
-- pas seulement évité par du code prudent. Rejouer une synchronisation ne peut pas créer un
-- second transfert pour le même retrait.
--
-- AUCUNE ÉCRITURE COMPTABLE N'EST PRODUITE ICI, et c'est délibéré. La caisse comptable existe
-- déjà (`operations_caisse`, dont la validation génère une écriture au 530000) : y déverser
-- automatiquement un retrait reviendrait à comptabiliser sans contrôle humain. Cette table porte
-- le fait de trésorerie ; la comptabilisation reste une décision, pour plus tard.
CREATE TABLE IF NOT EXISTS caisse_transferts_banque (
    transfert_id            TEXT PRIMARY KEY,           -- TRF-XXXXXXXX
    qonto_transaction_uuid  TEXT NOT NULL UNIQUE
        REFERENCES qonto_transactions_raw(qonto_transaction_uuid),
    montant                 REAL NOT NULL,
    devise                  TEXT NOT NULL DEFAULT 'EUR',
    date_operation          TEXT,
    etat                    TEXT NOT NULL,              -- PROVISOIRE | CONFIRME | ANNULE
    motif_etat              TEXT,
    libelle_source          TEXT,
    cree_le                 TEXT NOT NULL,
    maj_le                  TEXT NOT NULL,
    confirme_le             TEXT,
    annule_le               TEXT
);

CREATE INDEX IF NOT EXISTS idx_caisse_transferts_etat
    ON caisse_transferts_banque(etat);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0097');

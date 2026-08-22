-- Migration 0056 — Correction : retire le CHECK(sens) ajouté à tort en 0055.
--
-- ERREUR IDENTIFIÉE (mission durcissement Phase 1, rule §6 : « ne ferme pas un domaine si les
-- valeurs réellement utilisées ne sont pas exhaustivement connues ») : `banques_controles_
-- catalogue.py` détecte volontairement un `sens` hors domaine (`test_sens_incoherent_detecte`,
-- valeur "INCONNU") comme une ANOMALIE CONTRÔLABLE — un mouvement bancaire brut corrompu ou mal
-- extrait doit pouvoir être INSÉRÉ puis SIGNALÉ par l'écran de contrôle, jamais bloqué à l'entrée.
-- Le CHECK ajouté en 0055 empêchait cette insertion et cassait ce mécanisme existant.
--
-- Ne modifie jamais une migration existante (0055 reste telle quelle, historique immuable) :
-- corrige en recréant `banque_mouvements` une nouvelle fois, sans le CHECK sur `sens`. Toutes les
-- autres colonnes/index/valeurs par défaut restent identiques à 0055.
--
-- `banque_classifications` (FK vers banque_mouvements, ajoutée en 0055) n'est pas touchée : la FK
-- ne portait que sur `mouvement_id_opaque`, jamais sur `sens`.

PRAGMA foreign_keys=OFF;

CREATE TABLE banque_mouvements_new (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque     TEXT NOT NULL UNIQUE,
    import_id               TEXT NOT NULL,
    bank_account_id         TEXT NOT NULL,
    external_transaction_id TEXT,
    date_operation          TEXT NOT NULL,
    date_valeur             TEXT,
    sens                    TEXT NOT NULL,   -- DEBIT|CREDIT attendus ; hors domaine = anomalie
                                              -- CONTRÔLABLE (banques_controles_catalogue.py,
                                              -- C_SENS_INCOHERENT), jamais rejetée à l'insertion.
    montant                 REAL NOT NULL,
    devise                  TEXT NOT NULL DEFAULT 'EUR',
    libelle_brut            TEXT NOT NULL,
    contrepartie_brute      TEXT,
    fingerprint             TEXT NOT NULL,
    ligne_source            INTEGER,
    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
INSERT INTO banque_mouvements_new (
    id, mouvement_id_opaque, import_id, bank_account_id, external_transaction_id,
    date_operation, date_valeur, sens, montant, devise, libelle_brut, contrepartie_brute,
    fingerprint, ligne_source, created_at)
SELECT id, mouvement_id_opaque, import_id, bank_account_id, external_transaction_id,
       date_operation, date_valeur, sens, montant, devise, libelle_brut, contrepartie_brute,
       fingerprint, ligne_source, created_at
FROM banque_mouvements;
DROP TABLE banque_mouvements;
ALTER TABLE banque_mouvements_new RENAME TO banque_mouvements;

CREATE INDEX IF NOT EXISTS idx_bq_mvt_compte ON banque_mouvements(bank_account_id, date_operation);
CREATE INDEX IF NOT EXISTS idx_bq_mvt_import ON banque_mouvements(import_id);
CREATE INDEX IF NOT EXISTS idx_bq_mvt_fingerprint ON banque_mouvements(bank_account_id, fingerprint);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bq_mvt_external
    ON banque_mouvements(bank_account_id, external_transaction_id)
    WHERE external_transaction_id IS NOT NULL AND external_transaction_id <> '';

PRAGMA foreign_keys=ON;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0056');

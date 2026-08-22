-- Migration 0055 — Durcissement SQLite Phase 1 : FK et CHECK sur 4 tables déjà créées.
--
-- SQLite n'autorise pas d'ajouter une FK ou un CHECK à une table existante sans la recréer
-- (CREATE TABLE ... ; INSERT INTO ... SELECT ... ; DROP TABLE ... ; ALTER TABLE ... RENAME TO ...).
-- Idempotent par construction du projet : `apply_migrations()` court-circuite dès que
-- `schema_migrations` porte la version du dernier fichier (voir app/db/connection.py) — ce bloc ne
-- s'exécute donc qu'une seule fois, jamais rejoué au second passage.
--
-- Colonnes, valeurs par défaut, index : repris à l'identique des migrations 0027/0032. Aucune
-- colonne renommée ou supprimée, aucun index perdu. Les `id` (AUTOINCREMENT) sont réattribués à la
-- copie — vérifié par grep qu'aucun code ni aucune migration ne référence ces `id` bruts (seules
-- les clés opaques `*_id_opaque`/`mouvement_id_opaque` sont utilisées partout).
--
-- Domaines fermés par CHECK vérifiés EXHAUSTIFS dans le code avant fermeture (mission §6/§7) :
--   banque_mouvements.sens         : constantes "DEBIT"/"CREDIT" (banques_import_service.py)
--   factures_proprietaires.statut  : ST_BROUILLON/VALIDE/EMIS/ANNULE (factures_proprietaires_service.py:58)
--   factures_proprietaires.type_document : TYPE_FACTURE/TYPE_AVOIR (factures_proprietaires_service.py:61)
--   factures_proprietaires_lignes.type_ligne : TYPES_FACTURABLES, 5 valeurs (factures_proprietaires_service.py:30-36)
--
-- FK ajoutées vérifiées sans risque d'orphelin par lecture des sites d'écriture réels :
--   banque_classifications.mouvement_id_opaque       -> banque_mouvements.mouvement_id_opaque
--     (banque_classification_service.py : les lignes insérées proviennent toujours d'une lecture
--      préalable de banque_mouvements dans la même fonction, sous BEGIN IMMEDIATE)
--   factures_proprietaires_lignes.facture_id_opaque  -> factures_proprietaires.facture_id_opaque
--     (factures_proprietaires_service.py : la facture est créée juste avant ses lignes, même
--      transaction)
--
-- app.db réelle reste en 0016 : cette migration n'y a jamais été jouée et ne le sera pas ici.

PRAGMA foreign_keys=OFF;

-- ── 1. banque_mouvements : CHECK(sens) ───────────────────────────────────────

CREATE TABLE banque_mouvements_new (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque     TEXT NOT NULL UNIQUE,
    import_id               TEXT NOT NULL,
    bank_account_id         TEXT NOT NULL,
    external_transaction_id TEXT,
    date_operation          TEXT NOT NULL,
    date_valeur             TEXT,
    sens                    TEXT NOT NULL CHECK (sens IN ('DEBIT', 'CREDIT')),
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

-- ── 2. banque_classifications : FK -> banque_mouvements ─────────────────────

CREATE TABLE banque_classifications_new (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque     TEXT NOT NULL,
    classification_run_id   TEXT NOT NULL,
    regle_id                TEXT,
    categorie               TEXT,
    tiers_detecte           TEXT,
    type_flux_id            TEXT,
    code_impact             TEXT,
    source_economique       TEXT,
    statut_controle         TEXT,
    statut_classification   TEXT,
    niveau_risque           TEXT,
    rapprochement_requis    TEXT,
    date_classification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (mouvement_id_opaque) REFERENCES banque_mouvements(mouvement_id_opaque)
);
INSERT INTO banque_classifications_new (
    id, mouvement_id_opaque, classification_run_id, regle_id, categorie, tiers_detecte,
    type_flux_id, code_impact, source_economique, statut_controle, statut_classification,
    niveau_risque, rapprochement_requis, date_classification)
SELECT id, mouvement_id_opaque, classification_run_id, regle_id, categorie, tiers_detecte,
       type_flux_id, code_impact, source_economique, statut_controle, statut_classification,
       niveau_risque, rapprochement_requis, date_classification
FROM banque_classifications;
DROP TABLE banque_classifications;
ALTER TABLE banque_classifications_new RENAME TO banque_classifications;

CREATE INDEX IF NOT EXISTS idx_bq_class_mvt ON banque_classifications(mouvement_id_opaque);
CREATE INDEX IF NOT EXISTS idx_bq_class_run ON banque_classifications(classification_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bq_class_unique
    ON banque_classifications(mouvement_id_opaque, classification_run_id);

-- ── 3. factures_proprietaires : CHECK(statut), CHECK(type_document) ─────────

CREATE TABLE factures_proprietaires_new (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque     TEXT NOT NULL UNIQUE,
    numero_facture        TEXT,
    type_document         TEXT NOT NULL DEFAULT 'FACTURE' CHECK (type_document IN ('FACTURE', 'AVOIR')),
    facture_origine       TEXT,
    proprietaire_id       TEXT NOT NULL,
    logement_id           TEXT NOT NULL,
    mois                  TEXT NOT NULL,
    devise                TEXT NOT NULL DEFAULT 'EUR',
    montant_total         REAL NOT NULL DEFAULT 0,
    statut                TEXT NOT NULL DEFAULT 'BROUILLON'
        CHECK (statut IN ('BROUILLON', 'VALIDE', 'EMIS', 'ANNULE')),
    source_calcul         TEXT,
    snapshot_json         TEXT,
    snapshot_hash         TEXT,
    document_nom          TEXT,
    document_hash         TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_facture          TEXT,
    date_validation       TEXT,
    date_emission         TEXT,
    date_annulation       TEXT,
    motif_annulation      TEXT,
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);
INSERT INTO factures_proprietaires_new (
    id, facture_id_opaque, numero_facture, type_document, facture_origine, proprietaire_id,
    logement_id, mois, devise, montant_total, statut, source_calcul, snapshot_json, snapshot_hash,
    document_nom, document_hash, date_creation, date_facture, date_validation, date_emission,
    date_annulation, motif_annulation, acteur, version)
SELECT id, facture_id_opaque, numero_facture, type_document, facture_origine, proprietaire_id,
       logement_id, mois, devise, montant_total, statut, source_calcul, snapshot_json, snapshot_hash,
       document_nom, document_hash, date_creation, date_facture, date_validation, date_emission,
       date_annulation, motif_annulation, acteur, version
FROM factures_proprietaires;
DROP TABLE factures_proprietaires;
ALTER TABLE factures_proprietaires_new RENAME TO factures_proprietaires;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fpr_grain
    ON factures_proprietaires(mois, proprietaire_id, logement_id, type_document)
    WHERE statut <> 'ANNULE';
CREATE UNIQUE INDEX IF NOT EXISTS idx_fpr_numero
    ON factures_proprietaires(numero_facture) WHERE numero_facture IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fpr_statut ON factures_proprietaires(statut);
CREATE INDEX IF NOT EXISTS idx_fpr_proprietaire ON factures_proprietaires(proprietaire_id, mois);

-- ── 4. factures_proprietaires_lignes : FK -> factures_proprietaires, CHECK(type_ligne) ──

CREATE TABLE factures_proprietaires_lignes_new (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ligne_id_opaque       TEXT NOT NULL UNIQUE,
    facture_id_opaque     TEXT NOT NULL,
    numero_ligne          INTEGER NOT NULL,
    type_ligne            TEXT NOT NULL CHECK (type_ligne IN (
        'COMMISSION_CONCIERGERIE', 'MENAGE_FACTURE', 'PREPARATION_CANAPE',
        'CHARGE_FIXE', 'CHARGES_EXCEPT_REFAC')),
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

-- ── 5. Index supplémentaires (aucune recréation nécessaire) ──────────────────
-- `idx_charges_logement`/`idx_reshh_logement` existent déjà (migration 0052). Seul `proprietaire_id`
-- n'était pas indexé. Colonnes FK-métier (référentiel Excel historique, "non FK SQL" par décision
-- documentée en 0025/0027/0052) laissées sans FK réelle mais désormais indexées pour permettre une
-- validation applicative rapide (existence dans ref_proprietaires) sans scan complet.
CREATE INDEX IF NOT EXISTS idx_charges_proprietaire ON charges(proprietaire_id);
CREATE INDEX IF NOT EXISTS idx_reshh_proprietaire ON reservations_hors_hostaway(proprietaire_id);

PRAGMA foreign_keys=ON;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0055');

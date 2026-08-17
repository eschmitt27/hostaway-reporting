-- Migration 0032 — Mouvements bancaires bruts et classification, en SQLite.
--
-- Additive comme 0017→0031 : uniquement des CREATE TABLE / INDEX IF NOT EXISTS.
--
-- CE QUI EXISTAIT DÉJÀ, ET N'EST PAS DUPLIQUÉ
-- L'audit des tables Banque a trouvé tout l'appareil de DÉCISION, mais aucune table de données :
--   `banque_imports` (0015)               journal des imports
--   `banque_overrides` (0006)             décisions humaines de catégorisation
--   `banque_classement_decisions` (0026)  décisions de classement A_ENVOYER_IA
--   `banque_rapprochements` (0015)        rapprochement mouvement ↔ objet métier
--   `banque_suggestion_decisions` (0016)  décisions sur les suggestions
--   `ref_banque_regles` (0029)            règles de classification
-- Toutes référencent un `mouvement_id_opaque`… qui ne vivait nulle part en base. Les 541
-- mouvements n'existaient que dans l'onglet `NORM_Banque` d'un classeur. C'est cette pièce
-- manquante que la présente migration ajoute — rien d'autre.
--
-- LE BRUT EST IMMUABLE
-- `banque_mouvements` porte ce que la banque a envoyé, tel quel. Aucun statut, aucune catégorie,
-- aucune décision : ces informations vivent dans les tables ci-dessus, qui existent déjà. Séparer
-- le fait de son interprétation permet de rejouer une classification sans jamais réécrire la
-- source, et de comparer deux classifications sur la même donnée.

CREATE TABLE IF NOT EXISTS banque_mouvements (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque     TEXT NOT NULL UNIQUE,   -- MVT-xxxx, clé applicative stable
    import_id               TEXT NOT NULL,          -- import qui a introduit la ligne
    bank_account_id         TEXT NOT NULL,
        -- Rend possible la future banque SANS mélanger les mouvements anciens : au cut-over, le
        -- nouveau compte est un contexte distinct, pas une reprise de solde.
    external_transaction_id TEXT,                   -- identifiant banque, quand il est fourni
    date_operation          TEXT NOT NULL,
    date_valeur             TEXT,
    sens                    TEXT NOT NULL,          -- DEBIT|CREDIT
    montant                 REAL NOT NULL,          -- toujours signé selon `sens`
    devise                  TEXT NOT NULL DEFAULT 'EUR',
    libelle_brut            TEXT NOT NULL,          -- jamais réinterprété
    contrepartie_brute      TEXT,
    fingerprint             TEXT NOT NULL,          -- empreinte de déduplication
    ligne_source            INTEGER,                -- n° de ligne dans l'export, pour remonter à la source
    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_bq_mvt_compte ON banque_mouvements(bank_account_id, date_operation);
CREATE INDEX IF NOT EXISTS idx_bq_mvt_import ON banque_mouvements(import_id);
CREATE INDEX IF NOT EXISTS idx_bq_mvt_fingerprint ON banque_mouvements(bank_account_id, fingerprint);

-- Déduplication — deux garanties de nature différente.
--
-- 1. L'identifiant fourni par la banque fait foi quand il existe : deux lignes portant le même
--    `external_transaction_id` sur le même compte SONT le même mouvement. Contrainte dure.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bq_mvt_external
    ON banque_mouvements(bank_account_id, external_transaction_id)
    WHERE external_transaction_id IS NOT NULL AND external_transaction_id <> '';
--
-- 2. À défaut, l'empreinte sert d'INDICE, pas de vérité : un même montant, à la même date, avec le
--    même libellé peut être DEUX vrais mouvements — un double prélèvement existe. L'index n'est
--    donc pas unique, et le service tranche : rejet du doublon certain, `A_CONTROLER` sinon.
--    Fusionner silencieusement ferait perdre un mouvement réel.

-- Extension 1-1 de `banque_imports`, qui ne porte pas ces champs.
--
-- SQLite ne permet pas d'ajouter une colonne dans une migration rejouée à chaque démarrage
-- (`ADD COLUMN` échouerait au second passage). Plutôt que de recréer une table d'import
-- concurrente, on complète l'existante par une table portant strictement ce qui lui manque. La clé
-- est la même : `import_id`.
CREATE TABLE IF NOT EXISTS banque_import_source (
    import_id       TEXT PRIMARY KEY,
    bank_account_id TEXT NOT NULL,
    source_type     TEXT NOT NULL,   -- HISTORIQUE|MENSUEL|INCREMENTAL|API
    source_filename TEXT,
    source_sha256   TEXT,            -- empreinte du fichier reçu ; vide pour une source API
    date_min        TEXT,
    date_max        TEXT,
    nb_lignes       INTEGER NOT NULL DEFAULT 0,
    nb_inseres      INTEGER NOT NULL DEFAULT 0,
    nb_doublons     INTEGER NOT NULL DEFAULT 0,
    nb_a_controler  INTEGER NOT NULL DEFAULT 0,
    date_import     TEXT NOT NULL,
    run_id          TEXT,            -- run moteur associé (migration 0031)
    statut          TEXT NOT NULL DEFAULT 'IMPORTE'
);
CREATE INDEX IF NOT EXISTS idx_bq_import_source_compte
    ON banque_import_source(bank_account_id, date_import);

-- Résultat de la classification. Séparé du brut À DESSEIN : rejouer les règles ne doit jamais
-- réécrire ce que la banque a envoyé. Une ligne par mouvement et par exécution de classification.
CREATE TABLE IF NOT EXISTS banque_classifications (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id_opaque     TEXT NOT NULL,
    classification_run_id   TEXT NOT NULL,   -- exécution qui a produit cette ligne
    regle_id                TEXT,            -- règle appliquée, vide si aucune n'a matché
    categorie               TEXT,
    tiers_detecte           TEXT,
    type_flux_id            TEXT,
    code_impact             TEXT,
    source_economique       TEXT,
    statut_controle         TEXT,            -- VALIDE|A_CONTROLER
    statut_classification   TEXT,            -- CLASSE|RAPPROCHEMENT_REQUIS|A_ENVOYER_IA
    niveau_risque           TEXT,
    rapprochement_requis    TEXT,
    date_classification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_bq_class_mvt ON banque_classifications(mouvement_id_opaque);
CREATE INDEX IF NOT EXISTS idx_bq_class_run ON banque_classifications(classification_run_id);
-- Une seule classification COURANTE par mouvement et par exécution.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bq_class_unique
    ON banque_classifications(mouvement_id_opaque, classification_run_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0032');

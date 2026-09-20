-- Couche RAW Qonto — ce que l'API a répondu, tel quel. AUCUNE comptabilité ici.
--
-- Ces trois tables sont volontairement ISOLÉES du module comptable : elles ne référencent aucune
-- table métier (charges, mouvements_bancaires, factures, règlements…) et aucune table métier ne
-- les référence. Rien dans cette couche ne vaut rapprochement, règlement, créance ou dette : c'est
-- une photographie de la banque, que le futur module de rapprochement viendra LIRE.
--
-- Le préfixe `qonto_` est la frontière : un test refuse qu'un service de synchronisation écrive
-- ailleurs que dans une table portant ce préfixe.

-- ── Comptes bancaires Qonto ───────────────────────────────────────────────────────────────────
-- Un compte par ligne, remplacé à chaque synchronisation (le solde est une valeur d'instant, pas
-- un historique : l'historique des soldes se reconstruit depuis `settled_balance` des mouvements).
--
-- L'IBAN est stocké EN CLAIR parce que c'est la donnée source et que le rapprochement futur en
-- dépendra (c'est la clé d'appariement d'un virement). Il ne doit jamais être affiché entier :
-- l'UI le masque (`FR76 **** **** 1234`). C'est une donnée de l'entreprise, pas un secret
-- d'authentification — les identifiants d'API, eux, ne sont JAMAIS écrits en base.
CREATE TABLE IF NOT EXISTS qonto_accounts (
    qonto_account_id          TEXT PRIMARY KEY,   -- `id` (UUID) côté Qonto
    slug                      TEXT,
    nom                       TEXT,               -- `name`
    devise                    TEXT,               -- `currency`
    statut                    TEXT,               -- `status` : active / closed
    iban                      TEXT,
    bic                       TEXT,
    -- `balance` = solde comptable ; `authorized_balance` = solde disponible au sens Qonto
    -- (comptable moins les opérations autorisées non encore réglées). Les deux sont conservés,
    -- en unité monétaire ET en centimes : les centimes sont la valeur exacte, le flottant n'est
    -- là que pour l'affichage.
    solde                     REAL,
    solde_cents               INTEGER,
    solde_autorise            REAL,
    solde_autorise_cents      INTEGER,
    compte_principal          INTEGER NOT NULL DEFAULT 0,  -- `main`
    compte_externe            INTEGER NOT NULL DEFAULT 0,  -- `is_external_account`
    maj_qonto                 TEXT,               -- `updated_at` tel que Qonto le date
    premiere_recuperation     TEXT NOT NULL,      -- première fois que NOUS avons vu ce compte
    derniere_synchronisation  TEXT NOT NULL       -- dernière synchronisation réussie
);

-- ── Mouvements Qonto, bruts ───────────────────────────────────────────────────────────────────
-- Deux identifiants coexistent chez Qonto : `id` (UUID technique) et `transaction_id` (référence
-- stable et lisible). Les DEUX sont uniques ici : c'est la garantie « jamais de doublon », quel
-- que soit celui des deux que le futur rapprochement choisira comme clé.
--
-- Une ligne n'est JAMAIS supprimée. Une réponse partielle de l'API (une page manquante, un filtre
-- de statut, une panne au milieu) ne prouve pas qu'un mouvement a disparu — elle prouve seulement
-- qu'il n'était pas dans CETTE réponse.
CREATE TABLE IF NOT EXISTS qonto_transactions_raw (
    qonto_transaction_uuid    TEXT PRIMARY KEY,   -- `id`
    transaction_id            TEXT NOT NULL UNIQUE, -- `transaction_id`
    qonto_account_id          TEXT NOT NULL REFERENCES qonto_accounts(qonto_account_id),

    montant                   REAL,               -- `amount` (toujours positif : le sens est `side`)
    montant_cents             INTEGER,            -- `amount_cents` — valeur exacte
    devise                    TEXT,               -- `currency`
    montant_local             REAL,               -- `local_amount` (devise d'origine si change)
    montant_local_cents       INTEGER,
    devise_locale             TEXT,               -- `local_currency`

    sens                      TEXT,               -- `side` : debit / credit
    statut                    TEXT,               -- `status` : completed / pending / declined / reversed
    type_operation            TEXT,               -- `operation_type` : transfer / card / income / …

    libelle                   TEXT,               -- `label`
    reference                 TEXT,               -- `reference` (motif du virement)
    note                      TEXT,               -- `note` saisie dans Qonto
    contrepartie              TEXT,               -- `clean_counterparty_name`

    -- Les quatre dates que Qonto expose. Aucune n'est « la » date : `emitted_at` est l'émission,
    -- `settled_at` le règlement effectif (absent tant que l'opération est en attente).
    emis_le                   TEXT,               -- `emitted_at`
    regle_le                  TEXT,               -- `settled_at`
    cree_le                   TEXT,               -- `created_at`
    maj_le                    TEXT,               -- `updated_at` côté Qonto

    -- Utiles au rapprochement futur, sans interprétation ici.
    solde_apres               REAL,               -- `settled_balance` — solde après ce mouvement
    solde_apres_cents         INTEGER,
    categorie                 TEXT,               -- `category` (catégorie Qonto, pas un plan comptable)
    tva_montant               REAL,               -- `vat_amount` déclaré dans Qonto
    tva_taux                  REAL,               -- `vat_rate`
    carte_4_derniers          TEXT,               -- `card_last_digits` (déjà tronqué par Qonto)
    justificatifs_nb          INTEGER NOT NULL DEFAULT 0,  -- longueur de `attachment_ids`
    justificatif_requis       INTEGER NOT NULL DEFAULT 0,  -- `attachment_required`
    operation_externe         INTEGER NOT NULL DEFAULT 0,  -- `is_external_transaction`

    -- Réponse complète, telle quelle. C'est ce qui fait de cette table une couche RAW : un champ
    -- que nous n'avions pas anticipé reste récupérable sans réinterroger l'API. Aucun identifiant
    -- d'authentification n'y figure — la charge utile décrit un mouvement, pas une connexion.
    charge_utile_json         TEXT NOT NULL,
    -- SHA-256 de la charge utile canonique : dit si Qonto a modifié le mouvement depuis la
    -- dernière fois. Empreinte identique → rien à réécrire.
    empreinte                 TEXT NOT NULL,

    premiere_recuperation     TEXT NOT NULL,
    derniere_maj              TEXT NOT NULL,
    dernier_sync_run_id       TEXT
);

CREATE INDEX IF NOT EXISTS idx_qonto_tx_compte
    ON qonto_transactions_raw(qonto_account_id);
CREATE INDEX IF NOT EXISTS idx_qonto_tx_regle_le
    ON qonto_transactions_raw(regle_le);
CREATE INDEX IF NOT EXISTS idx_qonto_tx_statut
    ON qonto_transactions_raw(statut);
CREATE INDEX IF NOT EXISTS idx_qonto_tx_reference
    ON qonto_transactions_raw(reference);

-- ── Journal des synchronisations ──────────────────────────────────────────────────────────────
-- Trace ce qui a été tenté et ce qui en est sorti. AUCUN secret : ni login, ni clé, ni en-tête
-- d'authentification. Un échec est décrit par un code et un message métier, jamais par l'URL
-- complète ni par la requête émise.
CREATE TABLE IF NOT EXISTS qonto_sync_runs (
    sync_run_id               TEXT PRIMARY KEY,
    demarre_le                TEXT NOT NULL,
    termine_le                TEXT,
    statut                    TEXT NOT NULL,      -- EN_COURS / SUCCES / ECHEC
    comptes_vus               INTEGER NOT NULL DEFAULT 0,
    pages_lues                INTEGER NOT NULL DEFAULT 0,
    transactions_vues         INTEGER NOT NULL DEFAULT 0,
    transactions_creees       INTEGER NOT NULL DEFAULT 0,
    transactions_mises_a_jour INTEGER NOT NULL DEFAULT 0,
    transactions_inchangees   INTEGER NOT NULL DEFAULT 0,
    code_erreur               TEXT,
    message_erreur            TEXT
);

CREATE INDEX IF NOT EXISTS idx_qonto_sync_runs_demarre
    ON qonto_sync_runs(demarre_le);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0095');

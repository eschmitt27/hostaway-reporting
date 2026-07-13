-- Migration 0004 — journal des tentatives d'écriture réelle des charges (APP-3b)
-- Journal applicatif UNIQUEMENT. Aucune valeur métier autoritaire n'est stockée ici.
-- Source de vérité : SAISIE_Charges_Flux.xlsx / SAISIE_Charges_Impacts.xlsx (Excel).
--
-- On journalise la TENTATIVE, pas la charge : ni montant, ni logement, ni propriétaire, ni
-- payload — seulement de quoi reconstituer APRÈS COUP ce que la machine a fait aux fichiers.
-- C'est le point qui manquait pour qu'un rollback critique laisse une trace exploitable.
--
-- `statut` = verdict MÉTIER (9 valeurs, ci-dessous). `code` = détail TECHNIQUE (E_*).
-- Les deux ne sont jamais confondus.

CREATE TABLE IF NOT EXISTS saisie_charges_writes (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id         TEXT NOT NULL,          -- uuid4 : identifie la tentative
    token_previsualisation TEXT,                   -- token du dry-run, si disponible
    charge_id              TEXT,                   -- définitif, ou demandé si refus avant résolution
    charge_id_source       TEXT,                   -- 'FOURNI' | 'GENERE' | 'DEMANDE'

    debut_utc              TEXT NOT NULL,
    fin_utc                TEXT,

    -- Verdict métier : REFUSE_FLAGS | REFUSE_VERROU | REFUSE_VALIDATION | REFUSE_CHARGE_ID
    --                | ECHEC_PREPARATION | SUCCES | ROLLBACK_REUSSI | ROLLBACK_CRITIQUE
    --                | ETAT_INCOHERENT
    statut                 TEXT NOT NULL,
    code                   TEXT,                   -- code technique (E_VERROU_DEJA_PRIS, …)
    details                TEXT,                   -- message lisible

    -- Cibles : NOM de fichier seulement (jamais un chemin temporaire, jamais un chemin sensible)
    cible_saisie           TEXT,
    cible_impacts          TEXT,

    sha256_saisie_avant    TEXT,
    sha256_saisie_apres    TEXT,
    sha256_impacts_avant   TEXT,
    sha256_impacts_apres   TEXT,

    fichiers_remplaces     TEXT,                   -- JSON : noms de fichiers
    rollback_tente         INTEGER NOT NULL DEFAULT 0,
    rollback_reussi        INTEGER,                -- NULL si non tenté
    fichiers_non_restaures TEXT,                   -- JSON : état critique, seule voie de retour

    verrou_pid             INTEGER,                -- détenteur du verrou (nous, ou le bloqueur)
    verrou_hostname        TEXT,
    residus                TEXT,                   -- JSON : temporaires/sauvegardes non nettoyés

    git_head               TEXT,                   -- best-effort, jamais bloquant
    app_pid                INTEGER,
    hostname               TEXT
);

CREATE INDEX IF NOT EXISTS idx_saisie_charges_writes_charge_id
    ON saisie_charges_writes (charge_id);
CREATE INDEX IF NOT EXISTS idx_saisie_charges_writes_token
    ON saisie_charges_writes (token_previsualisation);
CREATE INDEX IF NOT EXISTS idx_saisie_charges_writes_transaction
    ON saisie_charges_writes (transaction_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0004');

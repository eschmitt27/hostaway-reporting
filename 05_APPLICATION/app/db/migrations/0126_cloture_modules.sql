-- 0126 — Clôture PAR MODULE d'un mois (réservations, ménages, charges, banque, factures clients,
-- créances et dettes, comptabilité).
--
-- CE QUI EST STOCKÉ — et RIEN d'autre : qu'un module est clôturé pour un mois, par qui, quand, et
-- l'historique de ses réouvertures. Les anomalies, elles, ne sont JAMAIS copiées ici : un bloqueur est
-- recalculé à chaque affichage depuis son module d'origine (voir `cloture_modules_service`), si bien
-- qu'il disparaît tout seul quand le vrai problème est corrigé. Une table de « problèmes de clôture »
-- divergerait au premier incident : on n'en crée pas.
--
-- UN MODULE CLÔTURÉ EST VERROUILLÉ POUR DE VRAI : les services de chaque domaine lisent cette table
-- (`cloture_verrous_service`) et refusent leurs écritures sur le mois. Les données restent consultables.
--
-- `cloture_modules` : état COURANT d'un (mois, module). `cloture_modules_evenements` : journal
-- APPEND-ONLY de chaque clôture et de chaque réouverture — aucune trace de clôture n'est jamais
-- supprimée ni modifiée (déclencheurs ci-dessous).
--
-- Additive : aucune table existante modifiée, aucune donnée existante touchée.

CREATE TABLE IF NOT EXISTS cloture_modules (
    mois                       TEXT NOT NULL,             -- AAAA-MM
    module                     TEXT NOT NULL,             -- RESERVATIONS|MENAGES|CHARGES|BANQUE|FACTURES_CLIENTS|CREANCES|COMPTABILITE
    cloture_id_opaque          TEXT NOT NULL,             -- clôture mensuelle de rattachement (CLO-xxxx)
    statut                     TEXT NOT NULL CHECK (statut IN ('CLOS', 'ROUVERT')),
    date_cloture               TEXT,                      -- dernière clôture du module (heure locale du poste)
    acteur_cloture             TEXT,
    commentaire_cloture        TEXT,
    date_reouverture           TEXT,                      -- dernière réouverture
    acteur_reouverture         TEXT,
    justification_reouverture  TEXT,
    nb_clotures                INTEGER NOT NULL DEFAULT 0,
    nb_reouvertures            INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mois, module)
);
CREATE INDEX IF NOT EXISTS idx_cloture_modules_cloture ON cloture_modules(cloture_id_opaque);

CREATE TABLE IF NOT EXISTS cloture_modules_evenements (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    mois               TEXT NOT NULL,
    module             TEXT NOT NULL,
    cloture_id_opaque  TEXT NOT NULL,
    type_evenement     TEXT NOT NULL CHECK (type_evenement IN ('CLOTURE_MODULE', 'REOUVERTURE_MODULE')),
    ancien_statut      TEXT,
    nouveau_statut     TEXT,
    commentaire        TEXT,                              -- justification d'une réouverture, commentaire d'une clôture
    resume             TEXT,                              -- ce que la clôture a constaté (ex. « aucun bloqueur »)
    date_evenement     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur             TEXT
);
CREATE INDEX IF NOT EXISTS idx_cloture_modules_evt ON cloture_modules_evenements(mois, module);

-- Journal APPEND-ONLY : ni correction ni suppression d'une trace de clôture.
CREATE TRIGGER IF NOT EXISTS trg_cloture_modules_evt_no_update
BEFORE UPDATE ON cloture_modules_evenements
BEGIN
    SELECT RAISE(ABORT, 'CLOTURE_MODULE_TRACE : l''historique de clôture des modules est en ajout seul.');
END;

CREATE TRIGGER IF NOT EXISTS trg_cloture_modules_evt_no_delete
BEFORE DELETE ON cloture_modules_evenements
BEGIN
    SELECT RAISE(ABORT, 'CLOTURE_MODULE_TRACE : l''historique de clôture des modules ne s''efface pas.');
END;

-- Aucun module d'un mois antérieur au début de la comptabilité V1 ne se clôture (même règle que la
-- clôture mensuelle, migration 0120). Sans cutover appliqué, la sous-requête vaut NULL : rien n'est bloqué.
CREATE TRIGGER IF NOT EXISTS trg_cloture_modules_mois_v1
BEFORE INSERT ON cloture_modules
WHEN NEW.mois < (SELECT substr(valeur, 1, 7) FROM parametres_societe_facturation
                  WHERE cle = 'V1_ACCOUNTING_START_DATE')
BEGIN
    SELECT RAISE(ABORT, 'CLOTURE_AVANT_V1 : un mois antérieur au début de la comptabilité V1 ne se clôture pas.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0126');

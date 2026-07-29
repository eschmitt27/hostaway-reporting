-- Migration 0023 — Cœur Comptabilité : VENTES, CAISSE, OD, mappings, périodes.
--
-- Suite du socle `0021` (ACHATS + BANQUE). Additive uniquement : aucune table de 0021/0022 n'est
-- modifiée. Le plan de comptes reste PROVISOIRE (cf. cadrage `43`/`46`) — les comptes ajoutés ici
-- (530000, 467000, 706000) ne sont pas un plan comptable définitif, seulement ce qui est nécessaire
-- pour que CAISSE/OD/VENTES fonctionnent sans improviser un compte au moment de générer une écriture.

INSERT OR IGNORE INTO plan_comptable (compte, libelle, type_compte, auxiliaire_autorise, commentaire) VALUES
    ('530000', 'Caisse',                                   'ACTIF',   0, 'Compte de caisse unique (recette : un seul compte fictif)'),
    ('467000', 'Associés — comptes courants',               'PASSIF',  1, 'Auxiliaire = associe_id ; avances/dépenses personnelles/remboursements'),
    ('706000', 'Prestations de services (commissions)',      'PRODUIT', 0, 'Crédité par VENTES — source Lot12, montant_du_conciergerie. Mapping non arbitré au-delà de ce compte générique.');

-- ── Mapping catégorie de charge → compte (0046) : configurable, jamais figé ──────────────────────
-- Une ligne par catégorie connue de REF_Categories_Charges. `statut` reste A_CONTROLER tant qu'un
-- arbitrage métier n'a pas validé le compte cible : `606000` reste le compte réellement utilisé par
-- generer_ecriture_achat tant qu'aucune ligne VALIDE ne le remplace (aucune bascule automatique).
CREATE TABLE IF NOT EXISTS mapping_categorie_compte (
    categorie_charge_id TEXT PRIMARY KEY,
    compte               TEXT NOT NULL DEFAULT '606000',
    statut                TEXT NOT NULL DEFAULT 'A_CONTROLER',   -- A_CONTROLER|VALIDE
    commentaire           TEXT,
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- ── Opérations de caisse : objet métier distinct de l'écriture CAISSE ────────────────────────────
-- Même principe que `banque_rapprochements` pour BANQUE : l'écriture ne fait que traduire un objet
-- déjà qualifié. Ne remplace jamais un règlement fournisseur (moyen=CAISSE) qui reste la source pour
-- un paiement fournisseur en espèces ; sert les cas SANS objet existant (encaissement, remboursement
-- associé en espèces).
CREATE TABLE IF NOT EXISTS operations_caisse (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id_opaque  TEXT NOT NULL UNIQUE,      -- CAI-xxxx
    type_operation       TEXT NOT NULL,
        -- ENCAISSEMENT|REMBOURSEMENT_ASSOCIE|AUTRE
    date_operation       TEXT NOT NULL,
    montant              REAL NOT NULL,
    tiers_type           TEXT,                       -- ASSOCIE|PROPRIETAIRE|AUTRE
    tiers_id             TEXT,                        -- auxiliaire (associe_id, proprietaire_id...)
    piece                TEXT,
    commentaire          TEXT,
    statut               TEXT NOT NULL DEFAULT 'ENREGISTREE',   -- ENREGISTREE|ANNULEE
    acteur               TEXT,
    date_creation        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version              INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_operations_caisse_statut ON operations_caisse(statut);

-- ── Opérations diverses (OD) : objet métier avec ses propres lignes équilibrées ──────────────────
-- Contrairement à ACHATS/BANQUE/CAISSE/VENTES (dérivées d'un objet métier déjà typé), une OD est par
-- nature libre (ajustement, ouverture, reclassement...) : elle porte donc ses propres lignes, du
-- même contrat que `ecriture_lignes`, équilibrées AVANT toute écriture ODIVERSES générée.
CREATE TABLE IF NOT EXISTS operations_diverses (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    od_id_opaque        TEXT NOT NULL UNIQUE,        -- OD-xxxx
    type_operation       TEXT NOT NULL,
        -- AJUSTEMENT|OUVERTURE|REMBOURSEMENT_ASSOCIE|PAIEMENT_PERSONNEL_ASSOCIE|
        -- HORS_COMPTA_REGULARISE|CORRECTION|RECLASSEMENT
    date_operation       TEXT NOT NULL,
    libelle              TEXT NOT NULL,
    justification        TEXT,
    statut               TEXT NOT NULL DEFAULT 'BROUILLON',   -- BROUILLON|VALIDEE|ANNULEE
    acteur               TEXT,
    date_creation        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version              INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_operations_diverses_statut ON operations_diverses(statut);

CREATE TABLE IF NOT EXISTS od_lignes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    od_id_opaque  TEXT NOT NULL,
    ligne_num     INTEGER NOT NULL,
    compte        TEXT NOT NULL,
    auxiliaire    TEXT,
    debit         REAL NOT NULL DEFAULT 0,
    credit        REAL NOT NULL DEFAULT 0,
    commentaire   TEXT
);
CREATE INDEX IF NOT EXISTS idx_od_lignes_od ON od_lignes(od_id_opaque);

-- ── Périodes comptables (distinctes de la clôture applicative du pilotage des calculs) ───────────
CREATE TABLE IF NOT EXISTS periodes_comptables (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    periode            TEXT NOT NULL UNIQUE,         -- AAAA-MM
    statut             TEXT NOT NULL DEFAULT 'OUVERTE',
        -- OUVERTE|EN_CONTROLE|VALIDEE|CLOTUREE|ROUVERTE
    date_creation      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version            INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS periode_evenements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    periode           TEXT NOT NULL,
    type_evenement    TEXT NOT NULL,     -- TRANSITION
    ancien_statut     TEXT,
    nouveau_statut    TEXT,
    commentaire       TEXT,
    resume_json       TEXT,              -- compteurs/totaux au moment de la transition (clôture)
    date_evenement    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur            TEXT
);
CREATE INDEX IF NOT EXISTS idx_periode_evenements_periode ON periode_evenements(periode);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0023');

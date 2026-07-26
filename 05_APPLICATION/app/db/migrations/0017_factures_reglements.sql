-- Migration 0017 — Factures fournisseurs et règlements (module Fournisseurs/Factures/Règlements).
--
-- Quatre objets DISTINCTS, jamais confondus (décision de cadrage 32_AUDIT_FOURNISSEURS_FACTURES) :
--   Facture      = la dette et la pièce fournisseur          -> table `factures` (ici)
--   Charge       = l'impact économique/analytique            -> SAISIE_Charges (Excel, inchangé)
--   Règlement    = le paiement effectué                      -> `reglements_fournisseurs` (ici)
--   Rapprochement= le lien règlement <-> mouvement bancaire  -> `banque_rapprochements` (0015)
--
-- Une charge n'est JAMAIS créée ici : le module Factures enregistre le lien vers une charge créée
-- par le parcours Charges existant (`charge_id`), il ne contourne aucun contrôle Charges.
-- Aucune donnée bancaire brute, aucun IBAN, aucune donnée personnelle sensible.

-- ── Fournisseurs : champs métier additionnels ───────────────────────────────
-- Table SÉPARÉE plutôt qu'ALTER TABLE : le runner de migrations rejoue tous les fichiers .sql à
-- chaque démarrage, et SQLite n'a pas d'`ADD COLUMN IF NOT EXISTS` — un ALTER planterait au second
-- boot. La table 0010 reste donc intacte ; ces champs la complètent en 1-1.
CREATE TABLE IF NOT EXISTS fournisseur_details (
    fournisseur_id_opaque TEXT PRIMARY KEY,
    raison_sociale        TEXT,
    contact               TEXT,
    email                 TEXT,
    telephone             TEXT,
    adresse               TEXT,
    numero_entreprise     TEXT,
    assujetti_tva         INTEGER NOT NULL DEFAULT 0,
    conditions_paiement   TEXT,
    prestataire_menage    INTEGER NOT NULL DEFAULT 0,
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- ── Factures ────────────────────────────────────────────────────────────────
-- `montant_regle` et `solde_restant` ne sont PAS stockés : ils sont recalculés depuis les
-- répartitions de règlements, pour ne jamais dériver de la vérité (cf. service).
CREATE TABLE IF NOT EXISTS factures (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque     TEXT NOT NULL UNIQUE,     -- FAC-xxxx
    fournisseur_id_opaque TEXT NOT NULL,            -- FRS-xxxx
    facture_ref           TEXT NOT NULL,            -- référence portée par la pièce fournisseur
    date_facture          TEXT,
    date_echeance         TEXT,
    montant_ht            REAL,
    montant_tva           REAL,
    montant_ttc           REAL NOT NULL,
    devise                TEXT NOT NULL DEFAULT 'EUR',
    statut                TEXT NOT NULL DEFAULT 'BROUILLON',
        -- BROUILLON|A_CONTROLER|VALIDEE|PARTIELLEMENT_REGLEE|REGLEE|ANNULEE|LITIGE
    charge_id             TEXT,                     -- lien vers la charge économique (jamais créée ici)
    justificatif          TEXT,                     -- nom de fichier uniquement, jamais un chemin absolu
    source                TEXT NOT NULL DEFAULT 'SAISIE',   -- SAISIE|PDF|CSV|XLSX
    empreinte             TEXT,                     -- anti-doublon (fournisseur+ref+montant+date)
    commentaire           TEXT,
    date_import           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);
-- Doublon CERTAIN : même fournisseur + même référence. Interdit au niveau du schéma.
CREATE UNIQUE INDEX IF NOT EXISTS idx_factures_fournisseur_ref
    ON factures(fournisseur_id_opaque, facture_ref) WHERE statut <> 'ANNULEE';
CREATE INDEX IF NOT EXISTS idx_factures_statut ON factures(statut);
CREATE INDEX IF NOT EXISTS idx_factures_echeance ON factures(date_echeance);
-- Une charge ne peut être rattachée qu'à UNE facture (jamais deux charges pour la même facture,
-- jamais deux factures sur la même charge).
CREATE UNIQUE INDEX IF NOT EXISTS idx_factures_charge
    ON factures(charge_id) WHERE charge_id IS NOT NULL AND statut <> 'ANNULEE';

CREATE TABLE IF NOT EXISTS facture_evenements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque TEXT NOT NULL,
    type_evenement    TEXT NOT NULL,   -- CREATION|MODIFICATION|VALIDATION|ANNULATION|LITIGE|CHARGE_LIEE|REGLEMENT
    ancien_statut     TEXT,
    nouveau_statut    TEXT,
    commentaire       TEXT,
    date_evenement    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur            TEXT
);
CREATE INDEX IF NOT EXISTS idx_facture_evenements ON facture_evenements(facture_id_opaque);

-- ── Règlements fournisseurs ─────────────────────────────────────────────────
-- Un règlement peut couvrir PLUSIEURS factures (paiement groupé) : la ventilation vit dans
-- `reglement_repartitions`. Le montant du règlement est la somme de ses répartitions.
CREATE TABLE IF NOT EXISTS reglements_fournisseurs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    reglement_id_opaque   TEXT NOT NULL UNIQUE,     -- REG-xxxx
    fournisseur_id_opaque TEXT NOT NULL,
    date_reglement        TEXT NOT NULL,
    montant               REAL NOT NULL,
    moyen                 TEXT NOT NULL,
        -- BANQUE|CAISSE|PERSONNEL_ASSOCIE|ACOMPTE|AVOIR|REMBOURSEMENT_FOURNISSEUR
    compte                TEXT,
    mouvement_id_opaque   TEXT,                     -- MVT-xxxx si déjà rapproché (informatif)
    statut                TEXT NOT NULL DEFAULT 'ENREGISTRE',  -- ENREGISTRE|RAPPROCHE|ANNULE
    commentaire           TEXT,
    acteur                TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version               INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_reglements_fournisseur
    ON reglements_fournisseurs(fournisseur_id_opaque, statut);

-- Ventilation d'un règlement sur une ou plusieurs factures (paiement groupé, paiement partiel).
CREATE TABLE IF NOT EXISTS reglement_repartitions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    reglement_id_opaque TEXT NOT NULL,
    facture_id_opaque   TEXT NOT NULL,
    montant             REAL NOT NULL,
    date_creation       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_repartitions_reglement ON reglement_repartitions(reglement_id_opaque);
CREATE INDEX IF NOT EXISTS idx_repartitions_facture ON reglement_repartitions(facture_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0017');

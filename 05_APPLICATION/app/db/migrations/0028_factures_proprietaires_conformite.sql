-- Migration 0028 — Conformité des factures propriétaires + préparation facturation électronique.
--
-- Additive comme 0017→0027 : uniquement des CREATE TABLE IF NOT EXISTS, aucun ALTER sur les tables
-- existantes (SQLite n'a pas d'ADD COLUMN IF NOT EXISTS et les migrations sont rejouées à chaque
-- démarrage — même raison que 0017/0020/0022).
--
-- `factures_proprietaires_conformite` porte les données réglementaires figées à l'émission :
-- identités complètes, type de client, régime de TVA appliqué, période de prestation, conditions
-- de règlement. Elles sont volontairement SÉPARÉES de l'en-tête : l'en-tête décrit l'objet métier
-- (qui, quoi, combien), la conformité décrit ce qui rend le document opposable. Une facture peut
-- exister en BROUILLON sans être conforme.
--
-- Les colonnes `electronic_invoice_*` préparent la facturation électronique : elles restent NULL
-- tant qu'aucune plateforme n'est choisie. Aucune logique de transmission n'existe dans ce lot —
-- l'objectif est de ne pas avoir à remodeler la table le jour venu.

CREATE TABLE IF NOT EXISTS factures_proprietaires_conformite (
    facture_id_opaque       TEXT PRIMARY KEY,

    -- ── Nature de l'opération ────────────────────────────────────────────────────────────────
    nature_operation        TEXT NOT NULL DEFAULT 'PRESTATION_DE_SERVICES',
        -- PRESTATION_DE_SERVICES uniquement a ce jour : le projet ne vend aucun bien.
    adresse_livraison       TEXT NOT NULL DEFAULT 'NON_APPLICABLE',

    -- ── Période de prestation (distincte de la date d'émission) ──────────────────────────────
    periode_debut           TEXT,
    periode_fin             TEXT,

    -- ── Émetteur, figé ───────────────────────────────────────────────────────────────────────
    emetteur_denomination   TEXT,
    emetteur_forme_juridique TEXT,
    emetteur_capital        TEXT,
    emetteur_siren          TEXT,
    emetteur_siret          TEXT,
    emetteur_rcs            TEXT,
    emetteur_adresse_siege  TEXT,
    emetteur_tva_intra      TEXT,
    emetteur_contact        TEXT,
    emetteur_coordonnees_paiement TEXT,

    -- ── Client, figé ─────────────────────────────────────────────────────────────────────────
    -- Le type conditionne les mentions : une clause B2B n'a pas à figurer sur une facture
    -- adressée a un particulier.
    type_client             TEXT NOT NULL DEFAULT 'A_CONTROLER',
        -- PARTICULIER|PROFESSIONNEL|A_CONTROLER
    client_denomination     TEXT,
    client_adresse          TEXT,
    client_adresse_facturation TEXT,
    client_siren            TEXT,
    client_tva_intra        TEXT,
    numero_bon_commande     TEXT,

    -- ── TVA ──────────────────────────────────────────────────────────────────────────────────
    -- Vocabulaire aligné sur la décision D083 déjà en vigueur pour les prestataires.
    regime_tva              TEXT NOT NULL DEFAULT 'A_CONTROLER',
        -- FRANCHISE_TVA|ASSUJETTI_TVA|EXONERATION_AUTRE|A_CONTROLER
    mention_tva             TEXT,
    total_ht                REAL NOT NULL DEFAULT 0,
    total_tva               REAL NOT NULL DEFAULT 0,
    total_ttc               REAL NOT NULL DEFAULT 0,

    -- ── Conditions de règlement ──────────────────────────────────────────────────────────────
    date_echeance           TEXT,
    delai_paiement_jours    INTEGER,
    conditions_escompte     TEXT,
    taux_penalites_retard   TEXT,
    indemnite_recouvrement  TEXT,

    -- ── Facturation électronique (préparation, aucun envoi) ──────────────────────────────────
    electronic_invoice_status        TEXT NOT NULL DEFAULT 'NON_APPLICABLE',
        -- NON_APPLICABLE|A_TRANSMETTRE|TRANSMISE|REJETEE
    electronic_invoice_provider      TEXT,
    electronic_invoice_external_id   TEXT,
    electronic_invoice_format        TEXT,
    electronic_invoice_sent_at       TEXT,
    electronic_invoice_received_status TEXT,

    date_creation           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_fpr_conf_type_client
    ON factures_proprietaires_conformite(type_client);
CREATE INDEX IF NOT EXISTS idx_fpr_conf_einvoice
    ON factures_proprietaires_conformite(electronic_invoice_status);

-- Détail unitaire des lignes. La table 0027 ne porte qu'un montant : une facture opposable doit
-- pouvoir présenter quantité et prix unitaire. Table séparée pour rester additive ; une ligne sans
-- entrée ici est une ligne au forfait (quantité 1), ce qui reste le cas courant.
CREATE TABLE IF NOT EXISTS factures_proprietaires_lignes_detail (
    ligne_id_opaque   TEXT PRIMARY KEY,
    quantite          REAL NOT NULL DEFAULT 1,
    prix_unitaire_ht  REAL,
    total_ht          REAL,
    taux_tva          REAL NOT NULL DEFAULT 0,
    montant_tva       REAL NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0028');

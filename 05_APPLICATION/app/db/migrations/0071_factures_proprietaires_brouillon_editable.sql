-- Migration 0071 — Facture propriétaire BROUILLON éditable + instantané des réservations.
--
-- ADDITIVE UNIQUEMENT, comme 0017→0070 : aucun ALTER TABLE. SQLite rejoue toutes les migrations à
-- chaque démarrage et n'a pas d'`ADD COLUMN IF NOT EXISTS` — toute donnée nouvelle sur une table
-- existante passe donc par une table COMPAGNON, jamais par une modification de la table d'origine.
--
-- DÉCISION D'ARCHITECTURE PORTÉE PAR CETTE MIGRATION
-- Jusqu'ici une facture était le reflet exact et figé d'un calcul Lot10/Lot12 : son total devait
-- réconcilier `montant_du_conciergerie` au centime près. Un BROUILLON devient maintenant un
-- DOCUMENT ÉDITABLE. Trois montants coexistent, et il faut pouvoir les distinguer pour toujours :
--
--   TOTAL_SOURCE_CALCULE  montant issu de Lot10/Lot12 AU MOMENT de la création de la facture.
--                         Figé définitivement ici. Jamais recalculé, jamais réécrit.
--   TOTAL_FACTURE         somme VIVE de `factures_proprietaires_lignes.montant`. Bouge à chaque
--                         ajout/modification/suppression de ligne.
--   AJUSTEMENT_MANUEL     TOTAL_FACTURE − TOTAL_SOURCE_CALCULE. Dérivé, jamais stocké.
--
-- Lot10/Lot12 restent la source de vérité du CALCUL et ne sont JAMAIS écrits par l'édition d'une
-- facture. Éditer une facture change le DOCUMENT, pas le calcul — d'où la nécessité de conserver
-- ici, et seulement ici, la valeur d'origine.

-- ── Entête : montant source figé ────────────────────────────────────────────────────────────────
-- Compagne de `factures_proprietaires` (0027), qu'on ne peut pas étendre par ALTER.
-- Une facture sans ligne ici est une facture antérieure à cette migration : son total source est
-- alors réputé égal à son `montant_total` (aucun ajustement manuel n'existait avant).
CREATE TABLE IF NOT EXISTS factures_proprietaires_meta (
    facture_id_opaque     TEXT PRIMARY KEY,
    total_source_calcule  REAL NOT NULL DEFAULT 0,  -- gelé à la création/régénération, jamais touché
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Reprise des factures antérieures. À CET INSTANT PRÉCIS, et seulement à cet instant,
-- `montant_total` EST le total calculé : l'édition manuelle n'existait pas avant cette migration,
-- donc aucun ajustement ne peut encore s'y cacher. Figer la valeur ici est le seul moyen de rendre
-- l'écart mesurable pour ces factures ; sans cela, leur « total source » suivrait éternellement
-- leur total vivant et l'ajustement afficherait toujours zéro.
-- `INSERT OR IGNORE` : un rejeu de migration ne réécrit jamais une valeur déjà figée.
INSERT OR IGNORE INTO factures_proprietaires_meta (facture_id_opaque, total_source_calcule)
    SELECT facture_id_opaque, montant_total FROM factures_proprietaires;

-- ── Lignes : mémoire du montant calculé d'origine ───────────────────────────────────────────────
-- L'ORIGINE d'une ligne (CALCULEE / MANUELLE) reste DÉRIVÉE de `objet_source_type` (0027) : non
-- NULL = CALCULEE. Aucune colonne redondante n'est ajoutée pour cela.
-- En revanche, si l'utilisateur MODIFIE le montant d'une ligne CALCULEE, la valeur d'origine serait
-- perdue et l'écart deviendrait irreconstituable. Cette table la conserve, écrite UNE SEULE FOIS,
-- à la première modification (INSERT OR IGNORE) — jamais réécrite ensuite.
CREATE TABLE IF NOT EXISTS factures_proprietaires_lignes_provenance (
    ligne_id_opaque        TEXT PRIMARY KEY,
    facture_id_opaque      TEXT NOT NULL,
    montant_source_initial REAL NOT NULL,
    objet_source_type      TEXT,
    objet_source_ref       TEXT,
    date_creation          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_fprlp_facture
    ON factures_proprietaires_lignes_provenance(facture_id_opaque);

-- ── Lignes de type CHARGE : lien vers la charge canonique ───────────────────────────────────────
-- Une ligne CHARGE ajoutée sur un BROUILLON crée AUSSI une charge réelle, via
-- `charges_saisie_service.creer()` — jamais par un INSERT direct dans `charges`. Ce lien permet
-- (a) d'afficher l'impact résultat/comptabilité sur la fiche, (b) d'annuler la charge si la ligne
-- est supprimée, (c) de prouver qu'aucun second mécanisme de création de charge n'existe.
CREATE TABLE IF NOT EXISTS factures_proprietaires_lignes_charge (
    ligne_id_opaque    TEXT PRIMARY KEY,
    facture_id_opaque  TEXT NOT NULL,
    charge_id          TEXT NOT NULL,
    code_impact        TEXT NOT NULL,             -- IC|HC|HR (vocabulaire flux_unifie_service)
    date_creation      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_fprlc_facture
    ON factures_proprietaires_lignes_charge(facture_id_opaque);
CREATE INDEX IF NOT EXISTS idx_fprlc_charge
    ON factures_proprietaires_lignes_charge(charge_id);

-- ── Réservations de la période : INSTANTANÉ, pas une jointure ───────────────────────────────────
-- Audit exécuté sur une COPIE de la base réelle avant d'écrire cette table :
--   * `factures_proprietaires.source_calcul` contient le `facture_id` Lot12 (ex.
--     'PREF-2026-08-PROP_0001-LOG_0001'), qui n'est PAS porté par un run : la même valeur existe
--     dans 4 runs Lot12 distincts, avec des `nb_reservations` différents (18, 36, 9, 9). Une
--     jointure dessus n'est donc pas déterministe.
--   * `lot12_runs` ne référence aucun run Lot10 : il n'existe aucun chemin
--     facture → run Lot12 → run Lot10 → réservations.
--   * `reservations_resolues` est régénérée par dataset (plusieurs `dataset_id` cohabitent, un seul
--     `actif=1`) : elle reflète l'état COURANT, qui change à chaque extraction Hostaway.
-- Conclusion : aucune jointure stable n'existe. L'instantané est donc obligatoire. Il est peuplé
-- UNE FOIS à la création du BROUILLON et jamais re-dérivé : une extraction Hostaway ultérieure ne
-- peut plus modifier ce que la facture affiche.
--
-- Purement INFORMATIF : cette table n'entre dans AUCUN total. Elle ne porte ni commission, ni
-- ménage, ni montant dû — seulement le payout, tel qu'il était au moment du calcul source.
CREATE TABLE IF NOT EXISTS factures_proprietaires_reservations (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque  TEXT NOT NULL,
    reservation_id     TEXT NOT NULL,
    guest_name         TEXT,                      -- NULL si la source ne le porte pas
    check_in           TEXT,
    check_out          TEXT,
    nights             INTEGER,
    guest_count        INTEGER,
    payout             REAL,
    plateforme         TEXT,
    source_run_id      TEXT,                      -- run Lot10 réellement lu au moment du gel
    date_creation      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fprres_grain
    ON factures_proprietaires_reservations(facture_id_opaque, reservation_id);

-- Reprise des factures antérieures. Elles n'ont jamais eu d'instantané : sans reprise, leur section
-- « Réservations de la période » resterait vide pour toujours. On fige donc ici l'état du run Lot10
-- ACTIF au moment de la migration.
-- HONNÊTETÉ SUR CE QUE VAUT CETTE REPRISE : ce n'est pas nécessairement le run qui a servi à créer
-- ces factures (aucun chemin ne relie une facture à un run Lot10 — c'est précisément ce qui a
-- imposé cette table). C'est la meilleure approximation disponible, et elle est figée dès
-- maintenant : à partir d'ici, ces lignes ne bougeront plus. `guest_name` reste NULL — il n'est
-- extractible que du payload Hostaway en Python, pas en SQL ; l'écran affiche alors la référence de
-- réservation, jamais un nom inventé.
INSERT OR IGNORE INTO factures_proprietaires_reservations
    (facture_id_opaque, reservation_id, check_in, check_out, nights, guest_count, payout,
     plateforme, source_run_id)
SELECT f.facture_id_opaque, c.reservation_id_hostaway, c.date_arrivee, c.date_depart, c.nuits,
       c.guest_count, c.payout_calcule, c.channel_type, c.run_id
FROM factures_proprietaires f
JOIN lot10_commissions c
  ON c.mois = f.mois AND c.proprietaire_id = f.proprietaire_id AND c.logement_id = f.logement_id
WHERE c.run_id = (SELECT run_id FROM lot10_runs WHERE actif = 1 ORDER BY id DESC LIMIT 1)
  AND c.reservation_id_hostaway IS NOT NULL;

-- ── Ménages : changement détecté sur un mois CLÔTURÉ ────────────────────────────────────────────
-- Un mois clôturé n'est JAMAIS recalculé, même si une source (PDF, Google Sheet, Hostaway) a
-- changé. Le silence serait pire que le recalcul : la trace est donc écrite ici, et l'économie du
-- mois reste rigoureusement intacte. Rouvrir la clôture reste une décision humaine explicite
-- (`clotures_service.rouvrir`), jamais un effet de bord d'une actualisation.
CREATE TABLE IF NOT EXISTS menages_changements_mois_clotures (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    mois           TEXT NOT NULL,
    origine        TEXT NOT NULL,                 -- PDF|GOOGLE_SHEET|HOSTAWAY
    detail         TEXT,
    statut         TEXT NOT NULL DEFAULT 'SIGNALE',   -- SIGNALE|TRAITE
    date_detection TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur         TEXT
);
CREATE INDEX IF NOT EXISTS idx_mcmc_mois ON menages_changements_mois_clotures(mois, statut);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0071');

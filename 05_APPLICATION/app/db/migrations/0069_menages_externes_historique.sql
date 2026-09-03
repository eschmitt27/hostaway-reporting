-- Migration 0069 — Historique figé des ménages externes facturés (TYPE_FLUX_014)
-- (mission « bloquer les factures A_CONTROLER + backfill SRC_MEN + TYPE_FLUX_014 SQLite »).
--
-- POURQUOI
-- Lot9 construisait TYPE_FLUX_014 en lisant le classeur `MASTER_FACT_MEN_MenagesExternes.xlsx`
-- (SRC_MEN). Le basculer vers SQLite butait exactement sur l'écart de périmètre déjà rencontré pour
-- TYPE_FLUX_018/019 : le classeur ne contient que 2026-05 (13 lignes déjà arrêtées, dont 12 VALIDE
-- pour 2381,00 €), tandis que les factures SQLite courantes ne couvrent que 2026-07 — et sont
-- toutes A_CONTROLER, donc non économiques. Basculer sans rien faire aurait fait passer
-- TYPE_FLUX_014 de 12 lignes à 0 : la suppression silencieuse d'un mois CLOTURÉ.
--
-- Même décision produit que 0068 : BACKFILL HISTORIQUE FIGÉ, jamais un recalcul. Les valeurs déjà
-- arrêtées de 2026-05 sont reprises telles quelles, sans rejouer une formule (ni lot6c, ni lot6d/e/f),
-- sans rouvrir le mois, sans toucher `ref_cloture_mensuelle`. Le mois reste CLOTURE.
--
-- AUCUNE FAUSSE FACTURE
-- Ces lignes historiques ne correspondent à aucun objet `factures` canonique en base : elles
-- viennent d'un classeur legacy antérieur au module Factures. Fabriquer des `factures` +
-- `facture_lignes_menage` de synthèse pour satisfaire les clés étrangères aurait injecté de faux
-- objets métier dans le cycle fournisseur — des factures qu'aucun humain n'a jamais reçues, mais
-- qu'un écran de validation aurait proposé de valider. L'historique vit donc dans sa propre table,
-- autonome et sans FK vers `factures`.
--
-- TABLE COMPAGNON, PAS `ALTER TABLE`
-- Les migrations sont rejouées à chaque démarrage et SQLite n'a pas d'`ADD COLUMN IF NOT EXISTS` :
-- un ALTER planterait au second passage. Même motif que 0066 et 0068.
--
-- INTERFACE UNIQUE CÔTÉ LOT9
-- Lot9 ne connaît pas cette table directement : il consomme l'historique figé et les factures
-- courantes validées par un seul lecteur, qui masque la distinction (§C). Les colonnes reprennent
-- donc exactement le contrat que le module MEN attendait du classeur — `source_pk` compris, car il
-- alimente le ROW_HASH anti-doublon et doit rester identique au centime près pour la parité.
--
-- `statut_source` est conservé tel quel (VALIDE / A_CONTROLER du classeur legacy, vocabulaire
-- distinct de l'énumération `factures.statut` VALIDEE/…). La ligne non VALIDE est importée et
-- conservée — l'historique n'est jamais amputé — mais reste hors du calcul économique, exactement
-- comme dans le comportement legacy.

CREATE TABLE IF NOT EXISTS menages_externes_historique (
    source_pk       TEXT NOT NULL,          -- clé legacy (MENEXT-…), alimente le ROW_HASH de lot9
    mois            TEXT NOT NULL,
    logement_id     TEXT,
    proprietaire_id TEXT,
    prestataire_id  TEXT,
    date_facture    TEXT,
    date_menage     TEXT,
    montant_ligne_ttc REAL,
    type_flux_id    TEXT NOT NULL DEFAULT 'TYPE_FLUX_014',
    sens            TEXT NOT NULL DEFAULT 'CHARGE',
    code_impact     TEXT NOT NULL DEFAULT 'IC',
    statut_source   TEXT NOT NULL,          -- statut_controle du classeur legacy (VALIDE / …)
    -- Provenance : mêmes champs que 0068, même finalité (audit, non-régression), jamais un calcul.
    source_type     TEXT NOT NULL DEFAULT 'LEGACY_IMPORT_FIGE',
    source_fichier  TEXT,
    source_hash     TEXT,
    statut_cloture  TEXT,
    date_import     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (source_pk)
);

CREATE INDEX IF NOT EXISTS idx_menages_externes_historique_mois
    ON menages_externes_historique(mois, statut_source);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0069');

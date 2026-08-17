-- Migration 0034 — Hostaway et réservations en SQLite.
--
-- Additive comme 0017→0033 : uniquement des CREATE TABLE / INDEX IF NOT EXISTS. Aucun
-- `ALTER TABLE ADD COLUMN` : les migrations sont rejouées à chaque démarrage et il échouerait au
-- second passage.
--
-- CE QUI EXISTAIT DÉJÀ, ET N'EST PAS DUPLIQUÉ
--   `moteur_runs` / `moteur_run_etapes` (0031)  journal des exécutions moteur
--   `ref_canaux_reservation` (0029)             vocabulaire des canaux
--   `ref_statuts_payout` (0029)                 vocabulaire des statuts de payout
--   `ref_logements`, `ref_gestion_logements_hist`, `ref_proprietaires` (0029)
--   `clotures_mensuelles` / `cloture_elements` (0021)
-- Les tables ci-dessous ne recréent aucun de ces objets : elles s'y rattachent par identifiant.
--
-- POURQUOI UN HISTORIQUE DÉDIÉ MALGRÉ `clotures_mensuelles`
-- Trois structures d'historisation existent, et aucune ne fait ce travail : `clotures_mensuelles`
-- porte le STATUT d'un mois, `cloture_elements` un instantané des CONTRÔLES, `snapshots` un
-- instantané de FICHIERS. Aucune ne fige les VALEURS ÉCONOMIQUES des réservations d'un mois clos —
-- c'est précisément ce que `reservations_historique_cloture` conserve. Réutiliser l'une des trois
-- aurait mélangé deux notions et rendu impossible de dire ce qu'un mois clos valait.

-- ═══ COUCHE RAW — ce que l'API a réellement retourné ═════════════════════════════════════════════

-- Une extraction Hostaway. Le pivot entre le journal moteur et les lignes extraites : chaque ligne
-- RAW porte l'`extraction_id`, donc on peut toujours dire de quel appel elle vient, et dans quel
-- état ce run s'est terminé.
CREATE TABLE IF NOT EXISTS hostaway_extractions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id   TEXT NOT NULL UNIQUE,   -- HAX-xxxx
    run_id          TEXT,                   -- run moteur associé (moteur_runs.run_id, migration 0031)
    mode            TEXT NOT NULL,          -- API|FIXTURE|REPRISE_EXCEL
    date_debut      TEXT NOT NULL,
    date_fin        TEXT,
    statut          TEXT NOT NULL DEFAULT 'EN_COURS',
        -- EN_COURS|SUCCES|PARTIEL|ECHEC — un run PARTIEL doit rester visible : ses données sont
        -- utilisables mais incomplètes, ce qui n'est ni un succès ni un échec.
    nb_listings     INTEGER NOT NULL DEFAULT 0,
    nb_reservations INTEGER NOT NULL DEFAULT 0,
    nb_payouts      INTEGER NOT NULL DEFAULT 0,
    message         TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_hax_statut ON hostaway_extractions(statut, date_debut);

-- Listings Hostaway. DISTINCTS du logement métier : `listingMapId` est un identifiant de la
-- plateforme, `logement_id` est interne. La relation est explicite et vit dans le référentiel
-- (`ref_logements.hostaway_listing_id`) ; on ne la déduit pas ici, et on ne les confond jamais.
CREATE TABLE IF NOT EXISTS hostaway_listings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id     TEXT NOT NULL,
    listing_map_id    TEXT NOT NULL,
    listing_id_ha     TEXT,
    nom_listing       TEXT,
    internal_name     TEXT,
    ville             TEXT,
    actif             TEXT,
    special_status    TEXT,
    airbnb_status     TEXT,
    bookingcom_status TEXT,
    sur_hostaway      TEXT,
    extrait_le        TEXT,
    row_hash          TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ha_listings_unique
    ON hostaway_listings(extraction_id, listing_map_id);

-- Réservations telles que l'API les a rendues.
--
-- `payload_json` porte la réponse brute. Ce n'est pas de la redondance : le fallback historique de
-- `guestCount` en dépend, et c'est la seule preuve de ce que la plateforme a dit un jour donné. Une
-- normalisation qui l'écarterait rendrait impossible de rejouer un calcul sur la donnée d'origine.
CREATE TABLE IF NOT EXISTS hostaway_reservations (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id             TEXT NOT NULL,
    reservation_id            TEXT NOT NULL,   -- identifiant Hostaway
    listing_map_id            TEXT,
    source                    TEXT,            -- canal brut renvoyé par l'API
    channel_type              TEXT,
    source_financiere         TEXT,
    status                    TEXT,
    payment_status            TEXT,
    check_in_date             TEXT,
    check_out_date            TEXT,
    nights                    INTEGER,
    number_of_guests          INTEGER,
    guest_count               INTEGER,
    source_guest_count        TEXT,
    controle_guest_count      TEXT,
    code_controle_guest_count TEXT,
    total_price               REAL,
    cleaning_fee_res          REAL,
    channel_commission        REAL,
    airbnb_expected_payout    REAL,
    is_owner_stay             TEXT,
    inclure_resultat          TEXT,
    updated_on                TEXT,
    created_on                TEXT,
    extrait_le                TEXT,
    row_hash                  TEXT,
    payload_json              TEXT,
    created_at                TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
-- Une réservation apparaît une seule fois par extraction. Rejouer la même extraction ne duplique
-- donc rien, et deux extractions restent comparables ligne à ligne.
CREATE UNIQUE INDEX IF NOT EXISTS idx_ha_res_unique
    ON hostaway_reservations(extraction_id, reservation_id);
CREATE INDEX IF NOT EXISTS idx_ha_res_id ON hostaway_reservations(reservation_id);
CREATE INDEX IF NOT EXISTS idx_ha_res_checkin ON hostaway_reservations(check_in_date);

-- Payouts. Table SÉPARÉE des réservations, à dessein : un payout n'est pas un attribut de la
-- réservation mais un fait financier distinct, qui peut manquer, arriver plus tard, ou être
-- incomplet. Les fondre rendrait « payout absent » indistinguable de « payout à zéro ».
--
-- La relation passe par `reservation_id`, l'identifiant que Hostaway fournit réellement. Un payout
-- n'est JAMAIS reconstruit depuis la Banque : un mouvement bancaire ne dit pas à quelle réservation
-- il correspond, et le déduire fabriquerait un lien que personne n'a établi.
CREATE TABLE IF NOT EXISTS hostaway_payouts (
    id                                 INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id                      TEXT NOT NULL,
    reservation_id                     TEXT NOT NULL,
    listing_map_id                     TEXT,
    source                             TEXT,
    channel_type                       TEXT,
    statut_calcul_payout               TEXT,   -- vocabulaire de ref_statuts_payout (0029)
    payout_calcule                     REAL,
    source_payout                      TEXT,
    menage_retenu                      REAL,
    assiette_commission                REAL,
    menage_retenu_source               TEXT,
    cout_standard_id                   TEXT,
    cout_standard_menage_snapshot      REAL,
    cout_standard_date_debut_validite  TEXT,
    cout_standard_date_fin_validite    TEXT,
    logement_id_snapshot               TEXT,
    type_logement_id_snapshot          TEXT,
    date_reference_cout_menage         TEXT,
    inclure_resultat_auto              TEXT,
    extrait_le                         TEXT,
    row_hash                           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ha_payout_unique
    ON hostaway_payouts(extraction_id, reservation_id);
CREATE INDEX IF NOT EXISTS idx_ha_payout_res ON hostaway_payouts(reservation_id);

-- Frais et champs financiers : plusieurs lignes par réservation, d'où des tables à part.
CREATE TABLE IF NOT EXISTS hostaway_reservation_fees (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id  TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    fee_id         TEXT,
    fee_name       TEXT,
    fee_type       TEXT,
    amount         REAL,
    currency       TEXT,
    row_hash       TEXT
);
CREATE INDEX IF NOT EXISTS idx_ha_fees_res ON hostaway_reservation_fees(extraction_id, reservation_id);

CREATE TABLE IF NOT EXISTS hostaway_reservation_finance_fields (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id        TEXT NOT NULL,
    reservation_id       TEXT NOT NULL,
    finance_field_name   TEXT,
    finance_field_value  TEXT,
    currency             TEXT,
    row_hash             TEXT
);
CREATE INDEX IF NOT EXISTS idx_ha_ff_res
    ON hostaway_reservation_finance_fields(extraction_id, reservation_id);

-- Anomalies détectées par l'extraction elle-même (CTR-xx de Lot1). Conservées : elles disent ce que
-- l'extraction a vu d'anormal, information perdue si seule la donnée « propre » était gardée.
CREATE TABLE IF NOT EXISTS hostaway_anomalies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    extraction_id   TEXT NOT NULL,
    reservation_id  TEXT,
    code_anomalie   TEXT NOT NULL,
    severite        TEXT,
    description     TEXT,
    statut          TEXT,
    date_detection  TEXT,
    row_hash        TEXT
);
CREATE INDEX IF NOT EXISTS idx_ha_anom ON hostaway_anomalies(extraction_id, code_anomalie);

-- ═══ COUCHE DÉRIVÉE — réservations calculées puis résolues ═══════════════════════════════════════

-- Un calcul de réservations (Lot4bis) ou une résolution (Lot4quater). Distinguer les recalculs
-- successifs est nécessaire : sans cela, une exécution en écraserait une autre et l'application ne
-- saurait pas quel jeu elle affiche.
CREATE TABLE IF NOT EXISTS reservations_datasets (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id     TEXT NOT NULL UNIQUE,   -- RDS-xxxx
    etape          TEXT NOT NULL,          -- CALCULEES|RESOLUES
    extraction_id  TEXT,                   -- extraction Hostaway d'origine, quand elle s'applique
    run_id         TEXT,                   -- run moteur associé
    date_calcul    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    nb_lignes      INTEGER NOT NULL DEFAULT 0,
    statut         TEXT NOT NULL DEFAULT 'SUCCES',   -- SUCCES|PARTIEL|ECHEC
    actif          INTEGER NOT NULL DEFAULT 1,       -- 1 = jeu courant de cette étape
    message        TEXT
);
-- Un seul jeu COURANT par étape : l'application doit pouvoir désigner « le » dataset actif sans
-- arbitrage.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rds_actif
    ON reservations_datasets(etape) WHERE actif = 1;

-- Grain : UNE LIGNE PAR RÉSERVATION ÉCONOMIQUE. `reservation_calc_id` est la clé stable, produite
-- par le moteur et conservée telle quelle — elle vaut pour une réservation Hostaway comme pour une
-- réservation hors Hostaway, ce qui est tout l'intérêt de cette table commune.
CREATE TABLE IF NOT EXISTS reservations_calculees (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id                TEXT NOT NULL,
    reservation_calc_id       TEXT NOT NULL,
    row_hash                  TEXT,
    source                    TEXT,      -- HOSTAWAY|HORS_HOSTAWAY|...
    reservation_id_hostaway   TEXT,
    reservation_hh_id         TEXT,
    mois                      TEXT,
    logement_id               TEXT,
    proprietaire_id           TEXT,
    date_arrivee              TEXT,
    date_depart               TEXT,
    nuits                     INTEGER,
    guest_count               INTEGER,
    source_guest_count        TEXT,
    montant_retenu            REAL,
    source_montant            TEXT,
    code_impact               TEXT,
    impact_resultat_reel      TEXT,
    impact_resultat_comptable TEXT,
    statut_controle           TEXT,
    niveau_anomalie           TEXT,
    code_anomalie             TEXT,
    commentaire               TEXT,
    source_module             TEXT,
    source_table              TEXT,
    source_pk                 TEXT,
    date_integration          TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_res_calc_unique
    ON reservations_calculees(dataset_id, reservation_calc_id);
CREATE INDEX IF NOT EXISTS idx_res_calc_mois ON reservations_calculees(dataset_id, mois);
CREATE INDEX IF NOT EXISTS idx_res_calc_logement ON reservations_calculees(dataset_id, logement_id);

-- Réservations résolues (Lot4quater) : mêmes colonnes plus la résolution de source. Table séparée
-- de `reservations_calculees` parce que les deux étapes doivent rester comparables — c'est ce qui
-- permet de voir ce que la résolution a changé.
CREATE TABLE IF NOT EXISTS reservations_resolues (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id                TEXT NOT NULL,
    reservation_calc_id       TEXT NOT NULL,
    row_hash                  TEXT,
    source                    TEXT,
    reservation_id_hostaway   TEXT,
    reservation_hh_id         TEXT,
    mois                      TEXT,
    logement_id               TEXT,
    proprietaire_id           TEXT,
    date_arrivee              TEXT,
    date_depart               TEXT,
    nuits                     INTEGER,
    guest_count               INTEGER,
    source_guest_count        TEXT,
    montant_retenu            REAL,
    source_montant            TEXT,
    code_impact               TEXT,
    impact_resultat_reel      TEXT,
    impact_resultat_comptable TEXT,
    statut_controle           TEXT,
    niveau_anomalie           TEXT,
    code_anomalie             TEXT,
    commentaire               TEXT,
    source_module             TEXT,
    source_table              TEXT,
    source_pk                 TEXT,
    date_integration          TEXT,
    canal                     TEXT,
    etat_mois                 TEXT,
    origine_initiale          TEXT,
    source_ligne              TEXT,
    methode                   TEXT,
    payout_calcule            REAL,
    menage_retenu             REAL,
    assiette_commission       REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_res_res_unique
    ON reservations_resolues(dataset_id, reservation_calc_id);
CREATE INDEX IF NOT EXISTS idx_res_res_mois ON reservations_resolues(dataset_id, mois);
CREATE INDEX IF NOT EXISTS idx_res_res_logement ON reservations_resolues(dataset_id, logement_id);

-- ═══ HISTORIQUE DES MOIS CLOS ═══════════════════════════════════════════════════════════════════
--
-- Valeurs FIGÉES au moment de la clôture. Aucune colonne `dataset_id` : ces lignes n'appartiennent
-- plus à un recalcul, elles appartiennent au mois. Un recalcul ultérieur produit un nouveau dataset
-- et ne touche pas cette table — c'est ce qui garantit qu'un mois clos ne change pas en silence.
--
-- LA CLÉ EST `cle_historisation`, PAS `reservation_calc_id`
-- Le moteur archive sur l'identifiant Hostaway quand il existe, et sur `reservation_calc_id`
-- seulement à défaut (réservations hors Hostaway). La raison est solide : `reservation_calc_id` est
-- positionnel (`RES-<mois>-HA-<n>`), donc il se déplace si l'ordre d'extraction change, alors qu'une
-- ligne archivée doit rester retrouvable pour toujours. Prendre la clé calc comme clé d'archive
-- aurait fait qu'un réordonnancement des sources rattache un montant figé à la mauvaise réservation.
CREATE TABLE IF NOT EXISTS reservations_historique_cloture (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id                TEXT NOT NULL,   -- ARC-xxxx : quelle opération d'archivage a écrit
    cle_historisation         TEXT NOT NULL,
    reservation_calc_id       TEXT,
    reservation_id_hostaway   TEXT,
    reservation_hh_id         TEXT,
    canal                     TEXT,
    logement_id               TEXT,
    proprietaire_id           TEXT,
    mois                      TEXT NOT NULL,
    date_arrivee              TEXT,
    date_depart               TEXT,
    nuits                     INTEGER,
    guest_count               INTEGER,
    montant_retenu            REAL,
    payout_calcule            REAL,
    menage_retenu             REAL,
    assiette_commission       REAL,
    code_impact               TEXT,
    impact_resultat_reel      TEXT,
    impact_resultat_comptable TEXT,
    statut_controle           TEXT,
    niveau_anomalie           TEXT,
    code_anomalie             TEXT,
    origine_initiale          TEXT,
    source_ligne              TEXT,
    source_montant            TEXT,
    methode                   TEXT,
    mois_cloture              TEXT,
    fige_le                   TEXT,
    row_hash                  TEXT,
    date_archivage            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
-- Une réservation est archivée UNE FOIS, pour toujours. La contrainte porte sur la seule clé
-- d'historisation, sans le mois : une réservation ne doit pas pouvoir être archivée deux fois sous
-- deux mois différents, ce qui compterait son montant deux fois dans les résultats annuels.
CREATE UNIQUE INDEX IF NOT EXISTS idx_res_hist_unique
    ON reservations_historique_cloture(cle_historisation);
CREATE INDEX IF NOT EXISTS idx_res_hist_mois ON reservations_historique_cloture(mois);
CREATE INDEX IF NOT EXISTS idx_res_hist_archive
    ON reservations_historique_cloture(archive_id);

-- Journal des archivages : qui a figé quel mois, depuis quel dataset, et combien de lignes. Sans
-- cela, un archivage supplémentaire sur un mois déjà clos serait indétectable après coup.
CREATE TABLE IF NOT EXISTS reservations_archives (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id      TEXT NOT NULL UNIQUE,
    mois_traites    TEXT,     -- mois concernés par cette opération, séparés par des virgules
    dataset_id      TEXT,
    nb_conservees   INTEGER NOT NULL DEFAULT 0,   -- lignes déjà figées, laissées intactes
    nb_ajoutees     INTEGER NOT NULL DEFAULT 0,
    motif           TEXT,
    acteur          TEXT,
    date_archivage  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_res_archives_date ON reservations_archives(date_archivage);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0034');

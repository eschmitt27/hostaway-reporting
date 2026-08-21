-- Migration 0052 — Charges (Lot3) et Réservations hors Hostaway (Lot4 HH) en SQLite.
--
-- Additive comme 0017→0051. Aucun ALTER TABLE.
--
-- POURQUOI CES DEUX-LÀ, ET MAINTENANT
-- Ce sont les deux dernières ENTRÉES ÉCONOMIQUES que l'application lisait encore dans un classeur :
--   · `charges_reader.read_charges()` → `MASTER_FACT_MAN_Charges.xlsx`, consommé par le module CHG
--     de `flux_unifie_service` (donc par Lot9 → Lot10 → Lot11 → Lot12), par l'écran Résultats, par
--     l'analytique et par les candidats bancaires ;
--   · `reservations_hh_reader` → `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx`, consommé par
--     l'écran Réservations et le pilotage mensuel.
-- Tant qu'elles restaient Excel, aucun démarrage « sans Excel » n'était possible : le moteur
-- attendait un classeur pour connaître ses charges.
--
-- ÉTAT RÉEL DES DONNÉES AU MOMENT DE LA MIGRATION (mesuré, pas supposé)
-- Charges : 0 ligne métier. La SAISIE (500 lignes de gabarit) et le MASTER (1 ligne) ne contiennent
-- que des placeholders Power Query, déjà filtrés par `_is_real_row`. La chaîne Charges n'a donc
-- jamais été alimentée — ce que Lot11 constate depuis toujours (`HC_ZERO_SOURCES_VIDES`, module
-- CHARGES). Il n'y a rien à reprendre : l'état vide devient un état vide SQLite propre, et le
-- pipeline n'a plus besoin d'un classeur vide pour démarrer.
-- Réservations hors Hostaway : 1 ligne métier réelle (RESHH-2026-05-001), reprise à l'identique.
--
-- LES COLONNES SONT CELLES DU MASTER
-- Reprises telles quelles : c'est le contrat que les consommateurs lisent déjà (`charge_id`,
-- `code_impact`, `statut_controle`…). Les renommer obligerait à réécrire des lecteurs qui, eux,
-- n'ont aucune raison de changer.

-- ═══ CHARGES (Lot3) ═════════════════════════════════════════════════════════════════════════════
--
-- Grain : UNE LIGNE PAR CHARGE. `charge_id` est la clé métier, saisie ou générée à la création —
-- jamais un rang. Contrairement aux tables de CALCUL (`flux_unifies`, `lot10_*`), cette table est
-- une SAISIE : elle n'est pas remplacée en bloc à chaque run, elle vit par créations et corrections
-- successives. D'où une clé stable et un journal d'événements plutôt qu'un `run_id`.
CREATE TABLE IF NOT EXISTS charges (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_id                   TEXT NOT NULL UNIQUE,
    date_charge                 TEXT,
    mois                        TEXT,
    montant                     REAL,
    sens_flux                   TEXT,
    sens                        TEXT,
    categorie_charge_id         TEXT,
    filtre_vue_menage           TEXT,
    type_flux_id                TEXT,
    code_impact                 TEXT,
    impact_resultat_reel        TEXT,
    impact_resultat_comptable   TEXT,
    prise_en_compta             TEXT,
    associe_id                  TEXT,
    mode_paiement_id            TEXT,
    carte_id                    TEXT,
    affectation_type            TEXT,
    logement_id                 TEXT,
    proprietaire_id             TEXT,
    reservation_id              TEXT,
    refacturable                TEXT,
    source_flux                 TEXT,
    methode_traitement          TEXT,
    paye_avec_montant_recupere  TEXT,
    lien_virement_banque        TEXT,
    statut_controle             TEXT,
    niveau_anomalie             TEXT,
    code_anomalie               TEXT,
    statut_rapprochement        TEXT,
    justificatif                TEXT,
    commentaire                 TEXT,
    row_hash                    TEXT,
    date_saisie                 TEXT,
    source_module               TEXT,
    source_table                TEXT,
    source_pk                   TEXT,
    date_integration            TEXT,
    -- Une charge n'est jamais supprimée physiquement : elle peut être référencée par une écriture,
    -- un rapprochement bancaire ou un flux déjà calculé. On l'ANNULE, et la trace demeure.
    statut                      TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE|ANNULEE
    acteur                      TEXT,
    date_creation               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification           TEXT
);
CREATE INDEX IF NOT EXISTS idx_charges_mois ON charges(mois);
CREATE INDEX IF NOT EXISTS idx_charges_logement ON charges(logement_id);
CREATE INDEX IF NOT EXISTS idx_charges_statut ON charges(statut);

-- Journal append-only : qui a créé, modifié ou annulé quoi. Une saisie corrigée doit pouvoir être
-- expliquée, pas seulement constatée.
CREATE TABLE IF NOT EXISTS charge_evenements (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_id    TEXT NOT NULL,
    evenement    TEXT NOT NULL,          -- CREATION|MODIFICATION|ANNULATION
    acteur       TEXT,
    motif        TEXT,
    avant_json   TEXT,
    apres_json   TEXT,
    horodatage   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_charge_evt ON charge_evenements(charge_id, id);

-- ═══ RÉSERVATIONS HORS HOSTAWAY (Lot4 HH) ═══════════════════════════════════════════════════════
--
-- Même nature : une SAISIE, pas un calcul. `reservation_hh_id` est la clé métier stable, celle-là
-- même dont dérive `reservation_calc_id` côté moteur (`lib_db_moteur.cle_reservation_hh`) — la
-- conserver telle quelle est ce qui garantit qu'une réservation saisie ici reste la même
-- réservation économique en aval.
CREATE TABLE IF NOT EXISTS reservations_hors_hostaway (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    reservation_hh_id         TEXT NOT NULL UNIQUE,
    row_hash                  TEXT,
    mois                      TEXT,
    canal_id                  TEXT,
    source_financiere         TEXT,
    proprietaire_id           TEXT,
    logement_id               TEXT,
    reservation_id_hostaway   TEXT,
    date_arrivee              TEXT,
    date_depart               TEXT,
    nuits                     INTEGER,
    guest_count               INTEGER,
    montant_percu             REAL,
    montant_retenu            REAL,
    mode_paiement_id          TEXT,
    code_impact               TEXT,
    impact_resultat_reel      TEXT,
    impact_resultat_comptable TEXT,
    statut_controle           TEXT,
    niveau_anomalie           TEXT,
    code_anomalie             TEXT,
    commentaire               TEXT,
    date_saisie               TEXT,
    source_module             TEXT,
    source_table              TEXT,
    source_pk                 TEXT,
    date_integration          TEXT,
    statut                    TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE|ANNULEE
    acteur                    TEXT,
    date_creation             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification         TEXT
);
CREATE INDEX IF NOT EXISTS idx_reshh_mois ON reservations_hors_hostaway(mois);
CREATE INDEX IF NOT EXISTS idx_reshh_logement ON reservations_hors_hostaway(logement_id);

CREATE TABLE IF NOT EXISTS reservation_hh_evenements (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    reservation_hh_id  TEXT NOT NULL,
    evenement          TEXT NOT NULL,   -- CREATION|MODIFICATION|ANNULATION
    acteur             TEXT,
    motif              TEXT,
    avant_json         TEXT,
    apres_json         TEXT,
    horodatage         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_reshh_evt ON reservation_hh_evenements(reservation_hh_id, id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0052');

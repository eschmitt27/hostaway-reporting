-- Migration 0044 — Lot10 : commissions, résultats, net propriétaire (SQLite).
--
-- Additive comme 0017→0043. Aucun ALTER TABLE.
--
-- CE QUI N'EST PAS CRÉÉ ICI, ET POURQUOI
--   Pas de table « facture propriétaire » ni « paiement » : elles existent déjà
--   (`factures_proprietaires` 0027, `mouvements_tresorerie_proprietaires` 0025,
--   `proprietaire_allocations` 0030). Lot10 calcule l'ÉCONOMIE d'un mois ; la facture émise et le
--   paiement reçu sont des objets distincts, avec leur propre cycle de vie (mission §26). Les
--   confondre ferait d'un calcul recalculable une pièce comptable, ou l'inverse.
--
-- GRAIN — relevé sur les classeurs legacy RÉELS, jamais supposé ni « amélioré » :
--   COMMISSIONS      : une ligne par RÉSERVATION (`reservation_calc_id`) — 1476 lignes observées.
--   A_CONTROLER      : lignes payout ÉCARTÉES du calcul, colonnes propres (vocabulaire Lot1, pas
--                      celui des commissions) — une ligne écartée n'est pas une commission à zéro.
--   RESULTATS        : (mois × logement × propriétaire × vision), les 3 visions empilées comme
--                      l'onglet PAR_MOIS_LOGEMENT. PAR_MOIS_PROPRIETAIRE et GLOBAL du classeur sont
--                      des agrégats DÉRIVABLES de cette table : ne pas les stocker évite trois
--                      copies qui divergent (le legacy les recalcule lui-même à l'écriture).
--   NET_EXPLOITATION : grain RÉSERVATION (1476 lignes, pas mois × logement) — c'est la table
--                      commissions enrichie de la charge fixe et du revenu net d'exploitation.
--   NET_REGLEMENT    : (mois × logement) — porte les 5 composants + `montant_du_conciergerie`.
--   NET_VUE_MOIS     : (mois × propriétaire) — synthèse de règlement.
--
-- DATASET VERSIONNÉ ET ATOMIQUE (mission §12-13)
-- `lot10_runs` porte le run ; chaque ligne porte son `run_id`. Un run n'est ACTIF qu'après succès
-- complet : le calcul écrit sous un run EN_COURS, puis bascule `statut` en SUCCES et désactive le
-- précédent dans la MÊME transaction. Un run interrompu laisse des lignes qu'aucun lecteur ne
-- sert (run non actif) — jamais un demi-dataset marqué frais.

CREATE TABLE IF NOT EXISTS lot10_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL UNIQUE,          -- L10-xxxx
    date_calcul       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    periode_min       TEXT,                          -- AAAA-MM le plus ancien du dataset
    periode_max       TEXT,
    statut            TEXT NOT NULL DEFAULT 'EN_COURS',  -- EN_COURS|SUCCES|ECHEC
    actif             INTEGER NOT NULL DEFAULT 0,    -- 1 = dataset servi aux lecteurs
    source_flux_run   TEXT,                          -- run_id de flux_unifies consommé (traçabilité)
    nb_commissions    INTEGER NOT NULL DEFAULT 0,
    nb_resultats      INTEGER NOT NULL DEFAULT 0,
    nb_reglements     INTEGER NOT NULL DEFAULT 0,
    message           TEXT
);
-- Un seul run actif à la fois : garantie de schéma, pas une discipline confiée au code appelant.
CREATE UNIQUE INDEX IF NOT EXISTS idx_lot10_runs_actif ON lot10_runs(actif) WHERE actif = 1;

-- ═══ COMMISSIONS — grain réservation ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS lot10_commissions (
    id                            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                        TEXT NOT NULL,
    flux_source_pk                TEXT,
    reservation_calc_id           TEXT NOT NULL,
    reservation_id_hostaway       TEXT,
    logement_id                   TEXT,
    proprietaire_id               TEXT,
    mois                          TEXT,
    date_arrivee                  TEXT,
    date_depart                   TEXT,
    nuits                         REAL,
    guest_count                   REAL,
    channel_type                  TEXT,
    source_type                   TEXT,
    statut_calcul_payout          TEXT,
    payout_calcule                REAL,
    menage_retenu                 REAL,
    menage_retenu_source          TEXT,
    cout_standard_id              TEXT,
    cout_standard_menage_snapshot REAL,
    date_reference_cout_menage    TEXT,
    assiette_commission           REAL,
    taux_commission               REAL,
    commission_conciergerie       REAL,
    taux_commission_id            TEXT,
    taux_commission_source        TEXT,
    controle_taux_commission      TEXT,
    net_proprietaire              REAL,
    inclure_resultat_auto         TEXT,
    logement_id_snapshot          TEXT,
    type_logement_id_snapshot     TEXT,
    preparation_canape_voyageurs  REAL,
    controle_preparation_canape   TEXT,
    source_preparation_canape     TEXT,
    UNIQUE (run_id, reservation_calc_id)
);
CREATE INDEX IF NOT EXISTS idx_lot10_comm_run ON lot10_commissions(run_id, mois);
CREATE INDEX IF NOT EXISTS idx_lot10_comm_logement ON lot10_commissions(run_id, logement_id);

CREATE TABLE IF NOT EXISTS lot10_commissions_a_controler (
    id                                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                            TEXT NOT NULL,
    reservation_id                    TEXT,
    listing_map_id                    TEXT,
    source                            TEXT,
    channel_type                      TEXT,
    statut_calcul_payout              TEXT,
    payout_calcule                    REAL,
    source_payout                     TEXT,
    menage_retenu                     REAL,
    assiette_commission               REAL,
    menage_retenu_source              TEXT,
    cout_standard_id                  TEXT,
    cout_standard_menage_snapshot     REAL,
    cout_standard_date_debut_validite TEXT,
    cout_standard_date_fin_validite   TEXT,
    logement_id_snapshot              TEXT,
    type_logement_id_snapshot         TEXT,
    date_reference_cout_menage        TEXT,
    inclure_resultat_auto             TEXT,
    extrait_le                        TEXT,
    row_hash                          TEXT,
    code_anomalie_lot10               TEXT
);
CREATE INDEX IF NOT EXISTS idx_lot10_comm_ac_run ON lot10_commissions_a_controler(run_id);

-- ═══ RESULTATS — (mois × logement × propriétaire × vision) ══════════════════════════════════════
CREATE TABLE IF NOT EXISTS lot10_resultats (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT NOT NULL,
    mois             TEXT,
    logement_id      TEXT,
    proprietaire_id  TEXT,
    vision           TEXT NOT NULL,     -- REEL|COMPTABLE|HORS_COMPTA
    total_produits   REAL,
    total_charges    REAL,
    resultat         REAL,
    nb_flux          REAL,
    commentaire      TEXT,
    UNIQUE (run_id, mois, logement_id, proprietaire_id, vision)
);
CREATE INDEX IF NOT EXISTS idx_lot10_res_run ON lot10_resultats(run_id, vision, mois);

-- ═══ NET PROPRIÉTAIRE — 3 grains distincts, comme les 3 onglets legacy ══════════════════════════
-- EXPLOITATION : grain RÉSERVATION (vérifié sur le classeur réel : 1476 lignes, pas un agrégat).
CREATE TABLE IF NOT EXISTS lot10_net_exploitation (
    id                            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                        TEXT NOT NULL,
    flux_source_pk                TEXT,
    reservation_calc_id           TEXT NOT NULL,
    reservation_id_hostaway       TEXT,
    logement_id                   TEXT,
    proprietaire_id               TEXT,
    mois                          TEXT,
    date_arrivee                  TEXT,
    date_depart                   TEXT,
    nuits                         REAL,
    guest_count                   REAL,
    channel_type                  TEXT,
    source_type                   TEXT,
    statut_calcul_payout          TEXT,
    payout_calcule                REAL,
    menage_retenu                 REAL,
    menage_retenu_source          TEXT,
    cout_standard_id              TEXT,
    cout_standard_menage_snapshot REAL,
    date_reference_cout_menage    TEXT,
    assiette_commission           REAL,
    taux_commission               REAL,
    commission_conciergerie       REAL,
    taux_commission_id            TEXT,
    taux_commission_source        TEXT,
    controle_taux_commission      TEXT,
    net_proprietaire              REAL,
    inclure_resultat_auto         TEXT,
    logement_id_snapshot          TEXT,
    type_logement_id_snapshot     TEXT,
    preparation_canape_voyageurs  REAL,
    controle_preparation_canape   TEXT,
    source_preparation_canape     TEXT,
    charge_fixe_mensuelle         REAL,
    commentaire_charge_fixe       TEXT,
    revenu_net_exploitation       REAL,
    UNIQUE (run_id, reservation_calc_id)
);
CREATE INDEX IF NOT EXISTS idx_lot10_expl_run ON lot10_net_exploitation(run_id, mois);

-- Les 5 composants du montant dû à la conciergerie vivent ici (mission §7) :
-- commission + ménage + préparation canapé + charge fixe + refacturations.
CREATE TABLE IF NOT EXISTS lot10_net_reglement (
    id                                   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                               TEXT NOT NULL,
    mois                                 TEXT,
    logement_id                          TEXT,
    proprietaire_id                      TEXT,
    charge_fixe_mensuelle                REAL,
    charge_fixe_source                   TEXT,
    total_payout_mois                    REAL,
    total_menage_mois                    REAL,
    total_commission_mois                REAL,
    total_preparation_canape_mois        REAL,
    net_proprietaire_avant_charge_mois   REAL,
    nb_reservations                      REAL,
    charges_exceptionnelles_refacturees  REAL,
    montant_du_conciergerie              REAL,
    acompte_conciergerie_recu_via_airbnb REAL,
    autres_acomptes_recus                REAL,
    paiement_deja_recu                   REAL,
    reste_a_payer_conciergerie           REAL,
    credit_a_traiter                     REAL,
    statut_credit                        TEXT,
    net_proprietaire_apres_charge_mois   REAL,
    statut_reglement                     TEXT,
    UNIQUE (run_id, mois, logement_id)
);
CREATE INDEX IF NOT EXISTS idx_lot10_regl_run ON lot10_net_reglement(run_id, mois);
CREATE INDEX IF NOT EXISTS idx_lot10_regl_prop ON lot10_net_reglement(run_id, proprietaire_id);

CREATE TABLE IF NOT EXISTS lot10_net_vue_mois (
    id                                   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                               TEXT NOT NULL,
    mois                                 TEXT,
    proprietaire_id                      TEXT,
    total_payout_mois                    REAL,
    total_menage_mois                    REAL,
    total_commission_mois                REAL,
    total_preparation_canape_mois        REAL,
    charge_fixe_mensuelle                REAL,
    charges_exceptionnelles_refacturees  REAL,
    montant_du_conciergerie              REAL,
    acompte_conciergerie_recu_via_airbnb REAL,
    autres_acomptes_recus                REAL,
    paiement_deja_recu                   REAL,
    reste_a_payer_conciergerie           REAL,
    credit_a_traiter                     REAL,
    net_proprietaire_avant_charge_mois   REAL,
    net_proprietaire_apres_charge_mois   REAL,
    nb_reservations                      REAL,
    UNIQUE (run_id, mois, proprietaire_id)
);
CREATE INDEX IF NOT EXISTS idx_lot10_vue_run ON lot10_net_vue_mois(run_id, mois);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0044');

-- Migration 0060 — Dernier durcissement SQLite ciblé (Mission 12).
--
-- Contrairement à 0055 (banque_mouvements.sens, factures_proprietaires.statut/type_document,
-- factures_proprietaires_lignes.type_ligne), qui a fermé des domaines RAW/importés dont l'un a dû
-- être partiellement retiré (0056 : banque_mouvements.sens devait rester ouvert — les contrôles ont
-- besoin de voir une valeur anormale), les 4 CHECK ci-dessous ne portent QUE sur des tables
-- entièrement APPLICATION-GÉNÉRÉES : aucune n'est alimentée par un import externe brut (PDF/CSV/
-- Hostaway). Chaque valeur est choisie par le code applicatif lui-même (formulaire, service),
-- jamais copiée telle quelle depuis une source tierce — donc aucun risque de bloquer une anomalie
-- RAW utile aux contrôles (leçon de la migration 0056, non répétée ici).
--
-- Domaines vérifiés EXHAUSTIFS par lecture du code avant fermeture (mission §5/§9/§11) :
--   charges.statut                              : STATUT_ACTIVE/STATUT_ANNULEE (charges_saisie_service.py:32-33)
--   reservations_hors_hostaway.statut            : STATUT_ACTIVE/STATUT_ANNULEE (reservations_hh_saisie_service.py:29-30)
--   mouvements_tresorerie_proprietaires.sens      : SENS = 2 valeurs (proprietaires_tresorerie_service.py:24)
--   mouvements_tresorerie_proprietaires.nature    : NATURES = 7 valeurs incl. AUTRE_A_CONTROLER
--                                                    (proprietaires_tresorerie_service.py:25-29 — catch-all
--                                                    DÉCLARÉ, pas une porte ouverte à l'imprévu)
--   mouvements_tresorerie_proprietaires.statut    : STATUTS = 4 valeurs (proprietaires_tresorerie_service.py:36)
--   factures.statut                              : 7 valeurs, TRANSITIONS n'en utilise aucune autre
--                                                    (factures_service.py:23-29, 72-80)
--
-- FK, UNIQUE, index : AUCUN ajouté ici. Les décisions historiques de ne pas mettre de FK SQL sur
-- charges.proprietaire_id/logement_id, reservations_hors_hostaway.proprietaire_id/logement_id et
-- mouvements_tresorerie_proprietaires.proprietaire_id (référentiel Excel historique, migrations
-- 0025/0027/0052) sont maintenues telles quelles — aucun élément nouveau ne les remet en cause
-- (mission §8). Une charge commune sans logement_id direct reste un état métier valide (périmètre
-- défini autrement, cf. `charges_impact_service`/Mission 8) : PAS de NOT NULL ajouté sur
-- logement_id. `reservations_hors_hostaway.montant_retenu` reste nullable (placeholder réel,
-- Mission 11). Les UNIQUE/index déjà en place (0017/0025/0052/0055) couvrent déjà les
-- duplications réellement impossibles ; aucun n'a été jugé manquant après audit.
--
-- Idempotent par construction du projet (apply_migrations() court-circuite dès que
-- schema_migrations porte la dernière version). Vraie app.db reste en 0016 : jamais migrée ici.

PRAGMA foreign_keys=OFF;

-- ── 1. charges : CHECK(statut) ───────────────────────────────────────────────

CREATE TABLE charges_new (
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
    statut                      TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (statut IN ('ACTIVE', 'ANNULEE')),
    acteur                      TEXT,
    date_creation               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification           TEXT
);
INSERT INTO charges_new SELECT * FROM charges;
DROP TABLE charges;
ALTER TABLE charges_new RENAME TO charges;

CREATE INDEX IF NOT EXISTS idx_charges_mois ON charges(mois);
CREATE INDEX IF NOT EXISTS idx_charges_logement ON charges(logement_id);
CREATE INDEX IF NOT EXISTS idx_charges_statut ON charges(statut);
CREATE INDEX IF NOT EXISTS idx_charges_proprietaire ON charges(proprietaire_id);

-- ── 2. reservations_hors_hostaway : CHECK(statut) ────────────────────────────

CREATE TABLE reservations_hors_hostaway_new (
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
    statut                    TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (statut IN ('ACTIVE', 'ANNULEE')),
    acteur                    TEXT,
    date_creation             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification         TEXT
);
INSERT INTO reservations_hors_hostaway_new SELECT * FROM reservations_hors_hostaway;
DROP TABLE reservations_hors_hostaway;
ALTER TABLE reservations_hors_hostaway_new RENAME TO reservations_hors_hostaway;

CREATE INDEX IF NOT EXISTS idx_reshh_mois ON reservations_hors_hostaway(mois);
CREATE INDEX IF NOT EXISTS idx_reshh_logement ON reservations_hors_hostaway(logement_id);
CREATE INDEX IF NOT EXISTS idx_reshh_proprietaire ON reservations_hors_hostaway(proprietaire_id);

-- ── 3. mouvements_tresorerie_proprietaires : CHECK(sens), CHECK(nature), CHECK(statut) ──

CREATE TABLE mouvements_tresorerie_proprietaires_new (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_opaque         TEXT NOT NULL UNIQUE,
    proprietaire_id          TEXT NOT NULL,
    logement_id              TEXT,
    date_mouvement           TEXT NOT NULL,
    montant                  REAL NOT NULL,
    sens                     TEXT NOT NULL
        CHECK (sens IN ('PROPRIETAIRE_VERS_SOCIETE', 'SOCIETE_VERS_PROPRIETAIRE')),
    nature                   TEXT NOT NULL
        CHECK (nature IN ('ACOMPTE_PROPRIETAIRE', 'REMBOURSEMENT_PROPRIETAIRE',
                          'REGULARISATION_PROPRIETAIRE', 'COMPENSATION_PROPRIETAIRE',
                          'AVANCE_PROPRIETAIRE', 'RESTITUTION_PROPRIETAIRE', 'AUTRE_A_CONTROLER')),
    statut                   TEXT NOT NULL DEFAULT 'BROUILLON'
        CHECK (statut IN ('BROUILLON', 'A_CONTROLER', 'VALIDE', 'ANNULE')),
    mode_reglement            TEXT,
    reference_metier          TEXT,
    justification             TEXT,
    justificatif_present      INTEGER NOT NULL DEFAULT 0,
    source_type               TEXT NOT NULL DEFAULT 'MANUEL',
    source_id                 TEXT,
    source_hash               TEXT,
    cree_le                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    valide_le                 TEXT,
    annule_le                 TEXT,
    cree_par                  TEXT,
    valide_par                TEXT,
    version                   INTEGER NOT NULL DEFAULT 1,
    actif                     INTEGER NOT NULL DEFAULT 1
);
INSERT INTO mouvements_tresorerie_proprietaires_new SELECT * FROM mouvements_tresorerie_proprietaires;
DROP TABLE mouvements_tresorerie_proprietaires;
ALTER TABLE mouvements_tresorerie_proprietaires_new RENAME TO mouvements_tresorerie_proprietaires;

CREATE INDEX IF NOT EXISTS idx_mtp_proprietaire ON mouvements_tresorerie_proprietaires(proprietaire_id, statut);
CREATE INDEX IF NOT EXISTS idx_mtp_statut ON mouvements_tresorerie_proprietaires(statut);

-- ── 4. factures (fournisseurs) : CHECK(statut) ───────────────────────────────

CREATE TABLE factures_new (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    facture_id_opaque     TEXT NOT NULL UNIQUE,
    fournisseur_id_opaque TEXT NOT NULL,
    facture_ref           TEXT NOT NULL,
    date_facture          TEXT,
    date_echeance         TEXT,
    montant_ht            REAL,
    montant_tva           REAL,
    montant_ttc           REAL NOT NULL,
    devise                TEXT NOT NULL DEFAULT 'EUR',
    statut                TEXT NOT NULL DEFAULT 'BROUILLON'
        CHECK (statut IN ('BROUILLON', 'A_CONTROLER', 'VALIDEE', 'PARTIELLEMENT_REGLEE',
                          'REGLEE', 'ANNULEE', 'LITIGE')),
    charge_id             TEXT,
    justificatif          TEXT,
    source                TEXT NOT NULL DEFAULT 'SAISIE',
    empreinte             TEXT,
    commentaire           TEXT,
    date_import           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    date_modification     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);
INSERT INTO factures_new SELECT * FROM factures;
DROP TABLE factures;
ALTER TABLE factures_new RENAME TO factures;

CREATE UNIQUE INDEX IF NOT EXISTS idx_factures_fournisseur_ref
    ON factures(fournisseur_id_opaque, facture_ref) WHERE statut <> 'ANNULEE';
CREATE INDEX IF NOT EXISTS idx_factures_statut ON factures(statut);
CREATE INDEX IF NOT EXISTS idx_factures_echeance ON factures(date_echeance);
CREATE UNIQUE INDEX IF NOT EXISTS idx_factures_charge
    ON factures(charge_id) WHERE charge_id IS NOT NULL AND statut <> 'ANNULEE';

-- DROP TABLE factures ci-dessus supprime aussi ses triggers (SQLite ne les rattache jamais à la
-- table recréée) : `trg_facture_classification_defaut` (migration 0020) est donc recréé ici à
-- l'identique — sans ce recréation, une facture créée après cette migration ne recevrait plus sa
-- classification par défaut (régression trouvée par la campagne complète, corrigée avant tout
-- commit).
CREATE TRIGGER IF NOT EXISTS trg_facture_classification_defaut
AFTER INSERT ON factures
BEGIN
    INSERT OR IGNORE INTO facture_classification (facture_id_opaque)
    VALUES (NEW.facture_id_opaque);
END;

PRAGMA foreign_keys=ON;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0060');

-- Migration 0025 — trésorerie propriétaires (objet métier distinct de Lot5).
-- MOUVEMENT_TRESORERIE_PROPRIETAIRE représente un mouvement financier société <-> propriétaire
-- (acompte, remboursement, régularisation, compensation, avance, restitution). Ce n'est ni une
-- réservation, ni un acompte de réservation hors Hostaway (Lot5 conserve son rôle historique,
-- non modifié, non renommé), ni une charge, ni une facture, ni une écriture bancaire ou comptable.
-- Un mouvement ne crée jamais automatiquement une écriture comptable — il devient seulement un
-- candidat au rapprochement bancaire générique existant (banque_rapprochements, migration 0015),
-- via le type_objet REVERSEMENT_PROPRIETAIRE déjà déclaré là-bas.
-- Historique append-only, aucune suppression physique d'un mouvement validé.

CREATE TABLE IF NOT EXISTS mouvements_tresorerie_proprietaires (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_opaque         TEXT NOT NULL UNIQUE,   -- MTP-xxxx
    proprietaire_id          TEXT NOT NULL,          -- id métier propriétaire (référentiel Excel, non FK SQL)
    logement_id              TEXT,                   -- facultatif
    date_mouvement           TEXT NOT NULL,
    montant                  REAL NOT NULL,
    sens                     TEXT NOT NULL,
        -- PROPRIETAIRE_VERS_SOCIETE|SOCIETE_VERS_PROPRIETAIRE
    nature                   TEXT NOT NULL,
        -- ACOMPTE_PROPRIETAIRE|REMBOURSEMENT_PROPRIETAIRE|REGULARISATION_PROPRIETAIRE|
        -- COMPENSATION_PROPRIETAIRE|AVANCE_PROPRIETAIRE|RESTITUTION_PROPRIETAIRE|AUTRE_A_CONTROLER
    statut                   TEXT NOT NULL DEFAULT 'BROUILLON',
        -- BROUILLON|A_CONTROLER|VALIDE|ANNULE
    mode_reglement            TEXT,
    reference_metier          TEXT,
    justification             TEXT,
    justificatif_present      INTEGER NOT NULL DEFAULT 0,
    source_type               TEXT NOT NULL DEFAULT 'MANUEL',   -- MANUEL|OBJET_VALIDE
    source_id                 TEXT,                              -- id de l'objet d'origine si source_type=OBJET_VALIDE
    source_hash               TEXT,
    cree_le                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    valide_le                 TEXT,
    annule_le                 TEXT,
    cree_par                  TEXT,
    valide_par                TEXT,
    version                   INTEGER NOT NULL DEFAULT 1,
    actif                     INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_mtp_proprietaire ON mouvements_tresorerie_proprietaires(proprietaire_id, statut);
CREATE INDEX IF NOT EXISTS idx_mtp_statut ON mouvements_tresorerie_proprietaires(statut);

-- Historique append-only des événements du mouvement.
CREATE TABLE IF NOT EXISTS mouvements_tresorerie_proprietaires_evenements (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    mouvement_id             TEXT NOT NULL,          -- mouvement_opaque (MTP-xxxx)
    evenement_opaque         TEXT NOT NULL UNIQUE,   -- MTE-xxxx
    type_evenement           TEXT NOT NULL,          -- CREATION|VALIDATION|ANNULATION|MODIFICATION
    ancien_statut             TEXT,
    nouveau_statut            TEXT,
    commentaire               TEXT,
    date_evenement            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur                    TEXT,
    correlation_id            TEXT
);
CREATE INDEX IF NOT EXISTS idx_mtp_evt_mouvement
    ON mouvements_tresorerie_proprietaires_evenements(mouvement_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0025');

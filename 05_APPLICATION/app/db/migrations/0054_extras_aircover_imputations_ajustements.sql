-- Migration 0054 — AirCover / Imputations Airbnb / Ajustements post-clôture (APP-3D, dernières
-- familles extras Excel restantes).
--
-- Colonnes reprises verbatim des en-têtes réels observés dans SAISIE_AirCover.xlsx,
-- SAISIE_ImputationsAirbnb.xlsx, SAISIE_Ajustements_PostCloture.xlsx (onglet MASTER) — ces trois
-- fichiers ne contiennent aucune ligne de donnée réelle (header seul), donc aucune reprise
-- historique n'est requise (mission §19 : reprise seulement « si donnée historique nécessaire »).
--
-- Additif uniquement (CREATE TABLE IF NOT EXISTS), rejouable sans effet — cohérent avec le
-- court-circuit de version ajouté à apply_migrations().

CREATE TABLE IF NOT EXISTS aircover (
    aircover_id TEXT PRIMARY KEY,
    date_aircover TEXT,
    montant REAL,
    beneficiaire_reel TEXT,
    proprietaire_id TEXT,
    logement_id TEXT,
    reservation_id TEXT,
    mois TEXT,
    justificatif TEXT,
    traitement TEXT,
    statut_controle TEXT,
    commentaire TEXT
);

CREATE INDEX IF NOT EXISTS idx_aircover_prop_mois ON aircover(proprietaire_id, mois);

CREATE TABLE IF NOT EXISTS imputations_airbnb (
    imputation_airbnb_id TEXT PRIMARY KEY,
    transaction_banque_id TEXT,
    reference_airbnb TEXT,
    proprietaire_id TEXT,
    logement_id TEXT,
    mois TEXT,
    document_id TEXT,
    montant_impute REAL,
    date_imputation TEXT,
    justificatif TEXT,
    statut TEXT,
    commentaire TEXT
);

CREATE INDEX IF NOT EXISTS idx_imputations_airbnb_prop_mois ON imputations_airbnb(proprietaire_id, mois);

CREATE TABLE IF NOT EXISTS ajustements_post_cloture (
    ajustement_id TEXT PRIMARY KEY,
    mois_origine TEXT,
    mois_effet TEXT,
    source_module TEXT,
    source_pk TEXT,
    logement_id TEXT,
    proprietaire_id TEXT,
    type_ajustement TEXT,
    montant REAL,
    sens TEXT,
    impact_reel TEXT,
    impact_comptable TEXT,
    motif TEXT,
    justificatif TEXT,
    auteur TEXT,
    date_saisie TEXT,
    statut_validation TEXT
);

CREATE INDEX IF NOT EXISTS idx_ajustements_prop_mois_effet ON ajustements_post_cloture(proprietaire_id, mois_effet);

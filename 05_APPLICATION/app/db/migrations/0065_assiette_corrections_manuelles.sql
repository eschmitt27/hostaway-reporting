-- Migration 0065 — Correction manuelle de l'assiette de commission (mission « contrôles/facturation »).

-- POURQUOI CETTE TABLE
-- `ASSIETTE_NEGATIVE_RAMENEE_ZERO` (Lot11) signale une assiette brute négative dont la commission a
-- été plafonnée à 0 automatiquement (`plafonner_assiette_pour_commission`). L'utilisateur doit
-- pouvoir remplacer cette assiette AUTOMATIQUE par une valeur choisie, MAIS sans jamais réécrire
-- l'assiette brute (preuve/audit) ni la valeur automatique elle-même : les trois valeurs coexistent,
-- avec une justification et un auteur, un peu comme `reservation_hh_overrides` (0053) sépare
-- standard/override/motif pour ménage et taux de commission.

CREATE TABLE IF NOT EXISTS assiette_corrections_manuelles (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    reservation_calc_id     TEXT NOT NULL,
    assiette_brute          REAL,
    assiette_automatique    REAL,
    assiette_manuelle       REAL NOT NULL,
    justification           TEXT NOT NULL,
    acteur                  TEXT,
    date_correction         TEXT NOT NULL,
    ancienne_commission     REAL,
    nouvelle_commission     REAL,
    taux_commission         REAL,
    -- Une correction précédente n'est jamais supprimée ni écrasée (audit) ; seule `actif` bascule
    -- à 0 quand une nouvelle correction la remplace pour la même réservation.
    actif                   INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_assiette_corr_resa
    ON assiette_corrections_manuelles(reservation_calc_id, actif);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0065');

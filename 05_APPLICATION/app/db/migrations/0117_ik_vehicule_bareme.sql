-- IK : véhicule de la fiche + barème kilométrique de référence (contrôle indicatif, jamais bloquant).
--
-- ADDITIVE. Aucune donnée existante n'est modifiée ; les IK existantes gardent ces champs vides
-- (le contrôle dit alors simplement « véhicule à renseigner »).

ALTER TABLE ik ADD COLUMN vehicule_libelle TEXT;
ALTER TABLE ik ADD COLUMN type_vehicule TEXT;          -- AUTO | MOTO | CYCLO
ALTER TABLE ik ADD COLUMN puissance_fiscale INTEGER;   -- en CV
ALTER TABLE ik ADD COLUMN motorisation TEXT;           -- THERMIQUE | ELECTRIQUE

-- BARÈME KILOMÉTRIQUE — un référentiel, pas des montants dispersés dans le code.
-- Une année de barème = une ligne dans `ref_bareme_ik_annees` + ses tranches dans `ref_bareme_ik`.
-- Mettre à jour le barème = ajouter une année ; rien d'autre à modifier.
-- Formule d'une tranche : montant = kilomètres × coefficient + forfait.
-- Tranche retenue : km_min < km <= km_max (km_max vide = au-delà) ; puissance cv_min..cv_max
-- (cv_max vide = et plus).
CREATE TABLE IF NOT EXISTS ref_bareme_ik_annees (
    annee                   INTEGER PRIMARY KEY,
    majoration_electrique   REAL NOT NULL DEFAULT 0,     -- 0.20 = +20 % pour un véhicule électrique
    source                  TEXT
);

CREATE TABLE IF NOT EXISTS ref_bareme_ik (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    annee           INTEGER NOT NULL REFERENCES ref_bareme_ik_annees(annee),
    type_vehicule   TEXT NOT NULL CHECK (type_vehicule IN ('AUTO', 'MOTO', 'CYCLO')),
    cv_min          INTEGER NOT NULL,
    cv_max          INTEGER,
    km_min          REAL NOT NULL,
    km_max          REAL,
    coefficient     REAL NOT NULL,
    forfait         REAL NOT NULL DEFAULT 0,
    UNIQUE (annee, type_vehicule, cv_min, km_min)
);

-- Barème kilométrique fiscal publié pour les revenus 2024 (identique au barème 2023).
-- À compléter par une nouvelle année dès la publication du barème suivant.
INSERT OR IGNORE INTO ref_bareme_ik_annees (annee, majoration_electrique, source) VALUES
    (2024, 0.20, 'Barème kilométrique fiscal, revenus 2024 (identique à 2023) — majoration de 20 % pour les véhicules électriques');

INSERT OR IGNORE INTO ref_bareme_ik (annee, type_vehicule, cv_min, cv_max, km_min, km_max, coefficient, forfait) VALUES
    -- Voitures
    (2024, 'AUTO', 0, 3, 0, 5000, 0.529, 0),
    (2024, 'AUTO', 0, 3, 5000, 20000, 0.316, 1065),
    (2024, 'AUTO', 0, 3, 20000, NULL, 0.370, 0),
    (2024, 'AUTO', 4, 4, 0, 5000, 0.606, 0),
    (2024, 'AUTO', 4, 4, 5000, 20000, 0.340, 1330),
    (2024, 'AUTO', 4, 4, 20000, NULL, 0.407, 0),
    (2024, 'AUTO', 5, 5, 0, 5000, 0.636, 0),
    (2024, 'AUTO', 5, 5, 5000, 20000, 0.357, 1395),
    (2024, 'AUTO', 5, 5, 20000, NULL, 0.427, 0),
    (2024, 'AUTO', 6, 6, 0, 5000, 0.665, 0),
    (2024, 'AUTO', 6, 6, 5000, 20000, 0.374, 1457),
    (2024, 'AUTO', 6, 6, 20000, NULL, 0.447, 0),
    (2024, 'AUTO', 7, NULL, 0, 5000, 0.697, 0),
    (2024, 'AUTO', 7, NULL, 5000, 20000, 0.394, 1515),
    (2024, 'AUTO', 7, NULL, 20000, NULL, 0.470, 0),
    -- Motocyclettes (plus de 50 cm³)
    (2024, 'MOTO', 0, 2, 0, 3000, 0.395, 0),
    (2024, 'MOTO', 0, 2, 3000, 6000, 0.099, 891),
    (2024, 'MOTO', 0, 2, 6000, NULL, 0.248, 0),
    (2024, 'MOTO', 3, 5, 0, 3000, 0.468, 0),
    (2024, 'MOTO', 3, 5, 3000, 6000, 0.082, 1158),
    (2024, 'MOTO', 3, 5, 6000, NULL, 0.275, 0),
    (2024, 'MOTO', 6, NULL, 0, 3000, 0.606, 0),
    (2024, 'MOTO', 6, NULL, 3000, 6000, 0.079, 1583),
    (2024, 'MOTO', 6, NULL, 6000, NULL, 0.343, 0),
    -- Cyclomoteurs (50 cm³ et moins)
    (2024, 'CYCLO', 0, NULL, 0, 3000, 0.315, 0),
    (2024, 'CYCLO', 0, NULL, 3000, 6000, 0.079, 711),
    (2024, 'CYCLO', 0, NULL, 6000, NULL, 0.198, 0);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0117');

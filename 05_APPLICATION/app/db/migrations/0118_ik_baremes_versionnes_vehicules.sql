-- Barème IK administrable et versionné ; identité stable des véhicules ; cumul annuel par véhicule.
--
-- ADDITIVE. Les tables `ref_bareme_ik` / `ref_bareme_ik_annees` (0117) ne sont ni modifiées ni
-- supprimées : elles ne sont simplement plus lues. Leur contenu (barème 2024 saisi de mémoire) est
-- repris ci-dessous en barème ARCHIVÉ, pour trace — jamais utilisé comme référence.

-- Un barème = une année + une version. Statuts : BROUILLON (en préparation, modifiable), ACTIF
-- (utilisé par le moteur ; un seul par année), ARCHIVE (conservé, plus utilisé). Jamais supprimé.
CREATE TABLE IF NOT EXISTS ik_baremes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    bareme_id_opaque        TEXT NOT NULL UNIQUE,
    annee                   INTEGER NOT NULL CHECK (annee BETWEEN 2000 AND 2100),
    version                 INTEGER NOT NULL DEFAULT 1,
    statut                  TEXT NOT NULL DEFAULT 'BROUILLON'
                            CHECK (statut IN ('BROUILLON', 'ACTIF', 'ARCHIVE')),
    majoration_electrique   REAL NOT NULL DEFAULT 0 CHECK (majoration_electrique BETWEEN 0 AND 1),
    source                  TEXT,
    cree_le                 TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    modifie_le              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    cree_par                TEXT,
    UNIQUE (annee, version)
);
-- Une seule version active pour une même année.
CREATE UNIQUE INDEX IF NOT EXISTS ux_ik_baremes_actif ON ik_baremes(annee) WHERE statut = 'ACTIF';

-- Tranches STRUCTURÉES (jamais une formule en texte) : montant = km × coefficient + constante.
-- Tranche retenue : km_min < km <= km_max (km_max vide = au-delà) ; puissance cv_min..cv_max
-- (cv_max vide = et plus).
CREATE TABLE IF NOT EXISTS ik_bareme_tranches (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    bareme_id_opaque    TEXT NOT NULL REFERENCES ik_baremes(bareme_id_opaque),
    type_vehicule       TEXT NOT NULL CHECK (type_vehicule IN ('AUTO', 'MOTO', 'CYCLO')),
    cv_min              INTEGER NOT NULL CHECK (cv_min >= 0),
    cv_max              INTEGER,
    km_min              REAL NOT NULL CHECK (km_min >= 0),
    km_max              REAL,
    coefficient         REAL NOT NULL CHECK (coefficient >= 0),
    constante           REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ik_bareme_tranches ON ik_bareme_tranches(bareme_id_opaque);

-- Véhicule : identité stable, pour un cumul kilométrique annuel PAR véhicule.
CREATE TABLE IF NOT EXISTS ik_vehicules (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicule_id_opaque  TEXT NOT NULL UNIQUE,
    associe_id          TEXT NOT NULL,
    libelle             TEXT,
    type_vehicule       TEXT NOT NULL CHECK (type_vehicule IN ('AUTO', 'MOTO', 'CYCLO')),
    puissance_fiscale   INTEGER,
    motorisation        TEXT NOT NULL DEFAULT 'THERMIQUE'
                        CHECK (motorisation IN ('THERMIQUE', 'ELECTRIQUE')),
    actif               INTEGER NOT NULL DEFAULT 1,
    cree_le             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

ALTER TABLE ik ADD COLUMN vehicule_id TEXT;          -- ik_vehicules.vehicule_id_opaque
ALTER TABLE ik ADD COLUMN bareme_id_opaque TEXT;     -- barème figé à la validation de l'IK

-- Reprise : un véhicule par combinaison déjà saisie sur une IK (0117). Aucune perte.
INSERT OR IGNORE INTO ik_vehicules (vehicule_id_opaque, associe_id, libelle, type_vehicule,
                                    puissance_fiscale, motorisation)
SELECT DISTINCT 'VEH-' || lower(hex(associe_id || '|' || COALESCE(vehicule_libelle, '') || '|'
                                 || type_vehicule || '|' || COALESCE(puissance_fiscale, '') || '|'
                                 || COALESCE(motorisation, 'THERMIQUE'))),
       associe_id, vehicule_libelle, type_vehicule, puissance_fiscale,
       COALESCE(motorisation, 'THERMIQUE')
  FROM ik WHERE type_vehicule IS NOT NULL;
UPDATE ik SET vehicule_id = 'VEH-' || lower(hex(associe_id || '|' || COALESCE(vehicule_libelle, '')
                                              || '|' || type_vehicule || '|'
                                              || COALESCE(puissance_fiscale, '') || '|'
                                              || COALESCE(motorisation, 'THERMIQUE')))
 WHERE type_vehicule IS NOT NULL AND vehicule_id IS NULL;

-- Barème automobile 2025 — valeurs officielles (thermique, hybride, hydrogène ; électrique +20 %).
INSERT OR IGNORE INTO ik_baremes (bareme_id_opaque, annee, version, statut, majoration_electrique,
                                  source, cree_par)
VALUES ('IKB-2025-V1', 2025, 1, 'ACTIF', 0.20,
        'Barème kilométrique automobile 2025 — valeurs officielles ; véhicules 100 % électriques +20 %',
        'migration 0118');
INSERT INTO ik_bareme_tranches (bareme_id_opaque, type_vehicule, cv_min, cv_max, km_min, km_max,
                                coefficient, constante)
SELECT 'IKB-2025-V1', t.* FROM (
    SELECT 'AUTO' AS type_vehicule, 0 AS cv_min, 3 AS cv_max, 0 AS km_min, 5000 AS km_max, 0.529 AS coefficient, 0 AS constante
    UNION ALL SELECT 'AUTO', 0, 3, 5000, 20000, 0.316, 1065
    UNION ALL SELECT 'AUTO', 0, 3, 20000, NULL, 0.370, 0
    UNION ALL SELECT 'AUTO', 4, 4, 0, 5000, 0.606, 0
    UNION ALL SELECT 'AUTO', 4, 4, 5000, 20000, 0.340, 1330
    UNION ALL SELECT 'AUTO', 4, 4, 20000, NULL, 0.407, 0
    UNION ALL SELECT 'AUTO', 5, 5, 0, 5000, 0.636, 0
    UNION ALL SELECT 'AUTO', 5, 5, 5000, 20000, 0.357, 1395
    UNION ALL SELECT 'AUTO', 5, 5, 20000, NULL, 0.427, 0
    UNION ALL SELECT 'AUTO', 6, 6, 0, 5000, 0.665, 0
    UNION ALL SELECT 'AUTO', 6, 6, 5000, 20000, 0.374, 1457
    UNION ALL SELECT 'AUTO', 6, 6, 20000, NULL, 0.447, 0
    UNION ALL SELECT 'AUTO', 7, NULL, 0, 5000, 0.697, 0
    UNION ALL SELECT 'AUTO', 7, NULL, 5000, 20000, 0.394, 1515
    UNION ALL SELECT 'AUTO', 7, NULL, 20000, NULL, 0.470, 0
) AS t
WHERE NOT EXISTS (SELECT 1 FROM ik_bareme_tranches WHERE bareme_id_opaque = 'IKB-2025-V1');

-- Barème 2024 de 0117 (saisi de mémoire) : ARCHIVÉ pour trace, jamais utilisé comme référence.
INSERT OR IGNORE INTO ik_baremes (bareme_id_opaque, annee, version, statut, majoration_electrique,
                                  source, cree_par)
SELECT 'IKB-2024-V1', 2024, 1, 'ARCHIVE', majoration_electrique,
       'Saisi de mémoire (migration 0117), remplacé par le barème officiel 2025 — conservé pour trace',
       'migration 0118'
  FROM ref_bareme_ik_annees WHERE annee = 2024;
INSERT INTO ik_bareme_tranches (bareme_id_opaque, type_vehicule, cv_min, cv_max, km_min, km_max,
                                coefficient, constante)
SELECT 'IKB-2024-V1', type_vehicule, cv_min, cv_max, km_min, km_max, coefficient, forfait
  FROM ref_bareme_ik WHERE annee = 2024
   AND EXISTS (SELECT 1 FROM ik_baremes WHERE bareme_id_opaque = 'IKB-2024-V1')
   AND NOT EXISTS (SELECT 1 FROM ik_bareme_tranches WHERE bareme_id_opaque = 'IKB-2024-V1');

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0118');

-- Migration 0053 — Réservations hors Hostaway : champs override/dérogation (parité legacy).

-- POURQUOI CETTE MIGRATION
-- `reservations_hors_hostaway` (0052) porte déjà l'identité et les champs économiques de base,
-- mais pas les champs de dérogation métier de l'ancien circuit SAISIE_ReservationsHorsHostaway.xlsx
-- (colonnes M/P/R/S/T + les 7 colonnes ajoutées ensuite dans le classeur, `NEW_SAISIE_FIELDS` de
-- `saisie_hh_schema_migration.py`). Sans ces colonnes, une saisie ne peut pas représenter une
-- dérogation de taux de commission ou de coût ménage — la seule vraie complexité du circuit HH,
-- identifiée par l'audit avant migration (mission "RESERVATIONS HORS HOSTAWAY").
--
-- POURQUOI UNE TABLE COMPAGNE PLUTÔT QUE DES COLONNES EN PLUS
-- Les migrations sont rejouées à chaque démarrage (`apply_migrations` exécute tous les fichiers à
-- chaque fois) : un `ALTER TABLE ADD COLUMN` échouerait au second passage. `reservation_hh_overrides`
-- complète donc `reservations_hors_hostaway` par une relation 1-1 (même clé, `reservation_hh_id`),
-- comme `banque_classification_signaux` complète `banque_classifications` (0033).
--
-- SÉMANTIQUE (prouvée dans `saisie_hh_service.valider`, pas déduite du nom) :
--   - `*_standard`  : valeur résolue depuis le référentiel (REF_Taux_Commission / REF_Couts_Standards_Menage).
--   - `*_override`  : NULL si la valeur saisie est absente OU égale au standard résolu (le legacy
--                     lui-même retombe alors à NULL — ce n'est pas une invention de cette migration).
--                     Non NULL uniquement si elle DÉVIE du standard, et alors motif + confirmation
--                     sont obligatoires (contrôlé par le service, pas par une contrainte SQL : une
--                     dérogation existante ne doit jamais devenir invalide après coup si la règle
--                     de confirmation change).
--   - `montant_recupere` / `associe_id_recuperateur` : uniquement pour un paiement par
--                     compte perso associé ou carte associée (montant à récupérer par l'associé).
--   - `montant_reverse_proprietaire` : uniquement pour un paiement en espèces (montant reversé
--                     au propriétaire).
--   - `comptabilisation` déjà couverte par `impact_resultat_comptable` (0052) : pas de doublon.

CREATE TABLE IF NOT EXISTS reservation_hh_overrides (
    reservation_hh_id                      TEXT PRIMARY KEY REFERENCES reservations_hors_hostaway(reservation_hh_id),
    -- Colonne M du classeur legacy (MANUAL_COL_MAP["M"] = "menage") : le coût ménage EFFECTIF
    -- (= menage_override si dérogation, sinon menage_standard). C'est la seule des deux valeurs
    -- que le legacy persistait réellement ; menage_standard/taux_commission_standard ci-dessous
    -- n'étaient jamais écrits dans le classeur (calculés à l'écran seulement) — ajoutés ici en
    -- plus, pour la traçabilité, sans que cela ne soit une exigence de parité.
    menage                                 REAL,
    menage_standard                        REAL,
    menage_standard_source                 TEXT,
    menage_override                        REAL,
    motif_override_menage                  TEXT,
    confirmation_override_menage           TEXT,
    taux_commission_standard                REAL,
    taux_commission_standard_source        TEXT,
    taux_commission_override               REAL,
    motif_override_taux_commission         TEXT,
    confirmation_override_taux_commission  TEXT,
    commentaire_taux_commission            TEXT,
    montant_recupere                       REAL,
    associe_id_recuperateur                TEXT,
    montant_reverse_proprietaire           REAL,
    -- Présente dans NEW_SAISIE_FIELDS (schéma legacy) mais sans consommateur identifié dans le
    -- circuit HH audité (probablement une liaison future avec les Acomptes propriétaires, hors
    -- périmètre de cette mission). Colonne ajoutée pour fidélité au schéma legacy, jamais peuplée
    -- par ce service.
    source_acompte_facture                 TEXT
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0053');

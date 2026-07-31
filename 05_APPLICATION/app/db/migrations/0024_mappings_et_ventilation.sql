-- Migration 0024 — Mappings comptables historisés et ventilation analytique.
--
-- Additive uniquement : ne touche aucune table de 0021/0022/0023. `mapping_categorie_compte`
-- (0023) reste en place tel quel (utilisé par le contrôle CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE) ;
-- la résolution RÉELLE du compte au moment de générer une écriture passe désormais par
-- `mapping_comptable_regles`, qui porte l'historisation (date de validité) que la table 0023 ne
-- portait pas — même raison que REF_Gestion_Logements_Hist à côté de REF_Logements dans les sources
-- réelles : une table simple pour l'affichage/le signalement, une table historisée pour la règle
-- réellement appliquée.

CREATE TABLE IF NOT EXISTS mapping_comptable_regles (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    regle_id_opaque       TEXT NOT NULL UNIQUE,      -- MAP-xxxx
    portee                TEXT NOT NULL,
        -- CATEGORIE | TYPE_FLUX | PROVISOIRE_GENERIQUE
    cle                   TEXT,                       -- categorie_charge_id ou type_flux_id ; NULL si PROVISOIRE_GENERIQUE
    compte                TEXT NOT NULL,
    statut                TEXT NOT NULL DEFAULT 'PROVISOIRE',   -- PROVISOIRE | VALIDE
    date_debut_validite   TEXT,                       -- NULL = valide depuis toujours
    date_fin_validite     TEXT,                       -- NULL = toujours valide
    source                TEXT,                       -- commentaire libre : qui a arbitré, pourquoi
    acteur                TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version               INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_mapping_regles_portee_cle ON mapping_comptable_regles(portee, cle);

-- Seed : une règle PROVISOIRE_GENERIQUE couvrant tout, explicitement autorisée (le filet actuel :
-- 606000 générique). Sans cette ligne, une catégorie inconnue serait bloquante plutôt que
-- A_CONTROLER — le brief demande explicitement un provisoire "autorisé", pas un blocage.
INSERT OR IGNORE INTO mapping_comptable_regles
    (regle_id_opaque, portee, cle, compte, statut, source)
VALUES
    ('MAP-GENERIQUE-606000', 'PROVISOIRE_GENERIQUE', NULL, '606000', 'PROVISOIRE',
     'Filet générique hérité du cadrage 43/46 — aucune catégorie arbitrée à ce jour.');

-- ── Ventilation analytique par ligne d'écriture ──────────────────────────────────────────────────
-- Satellite de `ecriture_lignes` (0021), jamais une modification de cette table. Une ligne
-- d'écriture existe indépendamment de sa ventilation ; celle-ci n'est qu'une explication de comment
-- logement_id/proprietaire_id (déjà des colonnes de ecriture_lignes) ont été déterminés.
CREATE TABLE IF NOT EXISTS ecriture_ligne_ventilation (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ecriture_id_opaque    TEXT NOT NULL,
    ligne_num             INTEGER NOT NULL,
    methode               TEXT NOT NULL,
        -- AFFECTATION_DIRECTE_LOGEMENT | AFFECTATION_DIRECTE_PROPRIETAIRE |
        -- FACTURE_MULTI_LIGNES | REPARTITION_MENAGE | SANS_DIMENSION
    pourcentage           REAL,                        -- part de la ligne source, 0-100
    montant_non_arrondi   REAL,
    montant_affiche       REAL,
    statut_ventilation    TEXT NOT NULL DEFAULT 'VALIDE',   -- VALIDE | A_CONTROLER
    origine_type          TEXT,                        -- FACTURE_LIGNE | CHARGE | MENAGE
    origine_id            TEXT,
    mapping_regle_id_opaque TEXT,
    date_creation         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_ecriture_ligne_ventilation_ecriture
    ON ecriture_ligne_ventilation(ecriture_id_opaque);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0024');

-- Migration 0114 — Circuit Banque → Charges → Comptabilité → Facturation (Mission 36).
--
-- ADDITIVE. Aucune ligne existante n'est supprimée ; aucune charge n'est reclassée ; aucune
-- écriture n'est créée ni validée ; aucune pièce n'est créée. Ce qui est ajouté :
--   1. le mode d'auxiliaire de chaque compte (NONE / OPTIONAL / REQUIRED + type de tiers) ;
--   2. le plan interne proposé par l'utilisateur (charges, produits par nature, acomptes clients) ;
--   3. les catégories du catalogue fonctionnel qui manquaient ;
--   4. les règles de mapping catégorie → compte, par DÉFAUT ou simplement AUTORISÉES ;
--   5. le compte de chaque type économique de ligne de facture propriétaire ;
--   6. le compte et la justification portés par une charge, la justification d'un écart de montant ;
--   7. les justificatifs (référence documentaire, dossier canonique, présence confirmée ou absence
--      justifiée) et leur historique.

-- ── 1. Auxiliaires portés par le plan comptable ───────────────────────────────────────────────
-- NULL = compte antérieur non paramétré : le mode se déduit alors du numéro (401, 411, 419, 455 :
-- tiers obligatoire) et de `auxiliaire_autorise` (voir comptabilite_plan_service.mode_auxiliaire).
ALTER TABLE plan_comptable ADD COLUMN auxiliaire_mode TEXT
    CHECK (auxiliaire_mode IS NULL OR auxiliaire_mode IN ('NONE', 'OPTIONAL', 'REQUIRED'));
ALTER TABLE plan_comptable ADD COLUMN auxiliaire_type TEXT
    CHECK (auxiliaire_type IS NULL OR auxiliaire_type IN ('FOURNISSEUR', 'CLIENT', 'ASSOCIE'));

UPDATE plan_comptable SET auxiliaire_mode = 'REQUIRED', auxiliaire_type = 'FOURNISSEUR'
    WHERE compte LIKE '401%';
UPDATE plan_comptable SET auxiliaire_mode = 'REQUIRED', auxiliaire_type = 'CLIENT'
    WHERE compte LIKE '411%' OR compte LIKE '4191%';
UPDATE plan_comptable SET auxiliaire_mode = 'REQUIRED', auxiliaire_type = 'ASSOCIE'
    WHERE compte LIKE '455%' OR compte LIKE '467%';
UPDATE plan_comptable SET auxiliaire_mode = 'NONE'
    WHERE auxiliaire_mode IS NULL AND (compte LIKE '5%' OR compte LIKE '6%' OR compte LIKE '7%');

-- ── 2. Plan interne proposé (catalogue fonctionnel de l'utilisateur) ─────────────────────────
-- Une nature économique → un compte. Aucun compte « par dépense ». `INSERT OR IGNORE` : un compte
-- déjà présent n'est ni renommé ni réactivé.
INSERT OR IGNORE INTO plan_comptable
    (compte, libelle, type_compte, actif, auxiliaire_autorise, auxiliaire_mode, auxiliaire_type,
     commentaire, date_creation)
VALUES
    ('611100', 'Ménage sous-traité', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('611200', 'Blanchisserie / linge sous-traité', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('606310', 'Consommables logement', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('606320', 'Petit équipement logement', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('606400', 'Fournitures administratives', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('606800', 'Autres fournitures', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('615200', 'Réparation / entretien immobilier', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('615500', 'Réparation mobilier / équipement', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('615600', 'Maintenance technique', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('627800', 'Frais bancaires', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('622200', 'Commissions plateformes / intermédiaires', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('622600', 'Honoraires comptables / juridiques / conseil', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('622700', 'Frais d''actes / contentieux', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('623100', 'Publicité / annonces / acquisition', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('623400', 'Cadeaux clientèle', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('624100', 'Livraison / transport d''achats', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('625100', 'Déplacements', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('625700', 'Réceptions', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('626000', 'Téléphone / télécommunications', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('616000', 'Assurance', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('613200', 'Loyer', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('614000', 'Charges locatives / copropriété', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('628100', 'Cotisations / adhésions', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('651100', 'Logiciels / licences / SaaS', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    -- Impôts et taxes : aucun compte générique. Deux subdivisions de nature connue, AUTORISÉES
    -- (jamais proposées par défaut) ; une autre taxe se crée dans le plan le jour où elle existe.
    ('635110', 'Cotisation foncière des entreprises (CFE)', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36) — impôts : subdivision de nature connue', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('635400', 'Droits d''enregistrement et de timbre', 'CHARGE', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36) — impôts : subdivision de nature connue', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    -- Produits : une nature de prestation → un compte (fin du « tout en 706000 »).
    ('706100', 'Gestion / commission de conciergerie', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('706200', 'Prestations de ménage facturées', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('706300', 'Forfait logiciel / consommables', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('706400', 'Gestion de sinistre / intervention', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('706500', 'Services additionnels / préparation canapé / extras', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('706900', 'Autres prestations de conciergerie', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('708800', 'Refacturations de frais / produits accessoires', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    ('709600', 'Rabais, remises et ristournes accordés sur prestations de services', 'PRODUIT', 1, 0, 'NONE', NULL, 'Catalogue fonctionnel (Mission 36)', strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    -- Acomptes clients (4191) : un acompte n'est jamais un produit. Le compte client reste 411000.
    ('419100', 'Clients - avances et acomptes reçus', 'PASSIF', 1, 1, 'REQUIRED', 'CLIENT', 'Catalogue fonctionnel (Mission 36) — acomptes et reversements Airbnb (famille acompte)', strftime('%Y-%m-%dT%H:%M:%SZ','now'));

INSERT INTO plan_comptable_evenements (compte, type_evenement, apres_json, motif, acteur)
SELECT compte, 'CREATION', json_object('compte', compte, 'libelle', libelle, 'type_compte', type_compte,
                                       'auxiliaire_mode', auxiliaire_mode, 'auxiliaire_type', auxiliaire_type),
       'Catalogue fonctionnel fourni par l''utilisateur (Mission 36)', 'Migration 0114'
FROM plan_comptable
WHERE commentaire LIKE 'Catalogue fonctionnel (Mission 36)%'
  AND compte NOT IN (SELECT compte FROM plan_comptable_evenements WHERE motif LIKE '%(Mission 36)');

-- ── 3. Catégories du catalogue qui manquaient ────────────────────────────────────────────────
-- `import_id = 'SAISIE_APPLICATION'` : un réimport du classeur REF_Setup ne supprime jamais une
-- ligne administrée dans l'application (ref_setup_import_service). Les 27 catégories existantes
-- ne sont ni renommées ni supprimées.
INSERT OR IGNORE INTO ref_categories_charges
    (categorie_charge_id, categorie_niveau_1, categorie_niveau_2, description, impact_resultat,
     refacturable_defaut, hors_compta_defaut, actif, filtre_vue_menage, famille_impact_categorie,
     profils_impact_autorises, import_id)
VALUES
    ('CHG_028', 'Fournitures', 'Fournitures administratives', 'Papeterie, impressions, petites fournitures de bureau', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_029', 'Fournitures', 'Autres fournitures', 'Fournitures non stockées sans autre nature', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_030', 'Maintenance', 'Réparation mobilier / équipement', 'Réparation de meubles et d''équipements', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_031', 'Maintenance', 'Maintenance technique', 'Contrats et interventions de maintenance technique', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_032', 'Plateformes', 'Commissions plateformes / intermédiaires', 'Commissions prélevées par une plateforme ou un intermédiaire', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_033', 'Honoraires', 'Honoraires comptables / juridiques / conseil', 'Expert-comptable, avocat, conseil', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_034', 'Honoraires', 'Frais d''actes / contentieux', 'Frais d''actes et de contentieux', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_035', 'Commercial', 'Publicité / annonces / acquisition', 'Annonces, publicité, acquisition de clients', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_036', 'Commercial', 'Cadeaux clientèle', 'Cadeaux offerts aux clients', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_037', 'Achat', 'Livraison / transport d''achats', 'Frais de livraison et de transport sur achats', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_038', 'Commercial', 'Réceptions', 'Réceptions et repas d''affaires', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_039', 'Télécom', 'Téléphone / télécommunications', 'Abonnements téléphone et internet', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_040', 'Locaux', 'Loyer', 'Loyer des locaux professionnels', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_041', 'Locaux', 'Charges locatives / copropriété', 'Charges locatives et de copropriété', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_042', 'Adhésions', 'Cotisations / adhésions', 'Cotisations professionnelles et adhésions', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION'),
    ('CHG_043', 'Impôts', 'Impôts / taxes', 'Compte à choisir selon la taxe (jamais de compte générique)', 'OUI', 'NON', 'NON', 'OUI', 'NON', 'GLOBAL', 'GLOBAL', 'SAISIE_APPLICATION');
-- « Autre charge » (imputation manuelle justifiée) n'est PAS recréée : c'est CHG_024 « Autre
-- (catégorie personnalisée) », sans compte par défaut — un doublon évident aurait été créé.

-- ── 4. Mapping catégorie → compte : DÉFAUT ou AUTORISÉ ────────────────────────────────────────
-- DEFAUT   : le compte présélectionné (au plus un par catégorie et par période).
-- AUTORISE : un autre compte que l'utilisateur peut choisir sans justification.
ALTER TABLE mapping_comptable_regles ADD COLUMN role TEXT NOT NULL DEFAULT 'DEFAUT'
    CHECK (role IN ('DEFAUT', 'AUTORISE'));

INSERT OR IGNORE INTO mapping_comptable_regles
    (regle_id_opaque, portee, cle, compte, statut, source, acteur, role)
VALUES
    ('MAP-CAT-CHG_001-611100', 'CATEGORIE', 'CHG_001', '611100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Ménage sous-traité', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_003-611200', 'CATEGORIE', 'CHG_003', '611200', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Blanchisserie / linge sous-traité', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_004-606310', 'CATEGORIE', 'CHG_004', '606310', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Consommables logement', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_005-651100', 'CATEGORIE', 'CHG_005', '651100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Logiciels / licences / SaaS', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_006-651100', 'CATEGORIE', 'CHG_006', '651100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Logiciels / licences / SaaS', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_007-651100', 'CATEGORIE', 'CHG_007', '651100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Logiciels / licences / SaaS', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_008-615200', 'CATEGORIE', 'CHG_008', '615200', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Réparation / entretien immobilier', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_009-625100', 'CATEGORIE', 'CHG_009', '625100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Déplacements', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_010-627800', 'CATEGORIE', 'CHG_010', '627800', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Frais bancaires', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_011-616000', 'CATEGORIE', 'CHG_011', '616000', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Assurance', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_018-606320', 'CATEGORIE', 'CHG_018', '606320', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Petit équipement logement', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_028-606400', 'CATEGORIE', 'CHG_028', '606400', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Fournitures administratives', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_029-606800', 'CATEGORIE', 'CHG_029', '606800', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Autres fournitures', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_030-615500', 'CATEGORIE', 'CHG_030', '615500', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Réparation mobilier / équipement', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_031-615600', 'CATEGORIE', 'CHG_031', '615600', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Maintenance technique', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_032-622200', 'CATEGORIE', 'CHG_032', '622200', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Commissions plateformes / intermédiaires', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_033-622600', 'CATEGORIE', 'CHG_033', '622600', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Honoraires comptables / juridiques / conseil', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_034-622700', 'CATEGORIE', 'CHG_034', '622700', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Frais d''actes / contentieux', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_035-623100', 'CATEGORIE', 'CHG_035', '623100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Publicité / annonces / acquisition', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_036-623400', 'CATEGORIE', 'CHG_036', '623400', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Cadeaux clientèle', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_037-624100', 'CATEGORIE', 'CHG_037', '624100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Livraison / transport d''achats', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_038-625700', 'CATEGORIE', 'CHG_038', '625700', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Réceptions', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_039-626000', 'CATEGORIE', 'CHG_039', '626000', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Téléphone / télécommunications', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_040-613200', 'CATEGORIE', 'CHG_040', '613200', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Loyer', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_041-614000', 'CATEGORIE', 'CHG_041', '614000', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Charges locatives / copropriété', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_042-628100', 'CATEGORIE', 'CHG_042', '628100', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Cotisations / adhésions', 'Migration 0114', 'DEFAUT'),
    ('MAP-CAT-CHG_043-635110', 'CATEGORIE', 'CHG_043', '635110', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Impôts — CFE (compte autorisé, à choisir selon la taxe)', 'Migration 0114', 'AUTORISE'),
    ('MAP-CAT-CHG_043-635400', 'CATEGORIE', 'CHG_043', '635400', 'VALIDE', 'Catalogue fonctionnel (Mission 36) : Impôts — droits d''enregistrement (compte autorisé, à choisir selon la taxe)', 'Migration 0114', 'AUTORISE');

INSERT INTO mapping_regle_evenements (regle_id_opaque, type_evenement, apres_json, motif, acteur)
SELECT regle_id_opaque, 'CREATION',
       json_object('portee', portee, 'cle', cle, 'compte', compte, 'statut', statut, 'role', role),
       source, 'Migration 0114'
FROM mapping_comptable_regles
WHERE acteur = 'Migration 0114'
  AND regle_id_opaque NOT IN (SELECT regle_id_opaque FROM mapping_regle_evenements);

-- ── 5. Type économique des lignes de facture propriétaire → compte ───────────────────────────
-- Le compte découle du TYPE de la ligne, jamais de son libellé. `famille` dit comment la ligne se
-- comptabilise : PRODUIT (crédit), REDUCTION (7096, débit), ACOMPTE (4191 → 411, hors produit).
CREATE TABLE IF NOT EXISTS mapping_produits_facture (
    type_economique   TEXT PRIMARY KEY,
    libelle           TEXT NOT NULL,
    famille           TEXT NOT NULL CHECK (famille IN ('PRODUIT', 'REDUCTION', 'ACOMPTE')),
    compte            TEXT NOT NULL,
    actif             INTEGER NOT NULL DEFAULT 1,
    source            TEXT,
    date_modification TEXT
);
INSERT OR IGNORE INTO mapping_produits_facture (type_economique, libelle, famille, compte, source) VALUES
    ('GESTION', 'Gestion / commission de conciergerie', 'PRODUIT', '706100', 'Mission 36'),
    ('MENAGE', 'Prestations de ménage facturées', 'PRODUIT', '706200', 'Mission 36'),
    ('FORFAIT', 'Forfait logiciel / consommables', 'PRODUIT', '706300', 'Mission 36'),
    ('SINISTRE', 'Gestion de sinistre / intervention', 'PRODUIT', '706400', 'Mission 36'),
    ('SERVICE_ADDITIONNEL', 'Services additionnels / préparation canapé / extras', 'PRODUIT', '706500', 'Mission 36'),
    ('AUTRE_PRESTATION', 'Autres prestations de conciergerie', 'PRODUIT', '706900', 'Mission 36'),
    ('REFACTURATION', 'Refacturations de frais', 'PRODUIT', '708800', 'Mission 36'),
    ('REDUCTION', 'Rabais, remises et ristournes accordés', 'REDUCTION', '709600', 'Mission 36'),
    ('ACOMPTE_APPLIQUE', 'Acompte client imputé', 'ACOMPTE', '419100', 'Mission 36'),
    ('REVERSEMENT_AIRBNB', 'Reversement Airbnb imputé (famille acompte)', 'ACOMPTE', '419100', 'Mission 36');

ALTER TABLE factures_proprietaires_lignes ADD COLUMN type_economique TEXT;
-- Dérivation CERTAINE depuis le type technique existant (aucune ligne n'est reclassée au jugé) :
UPDATE factures_proprietaires_lignes SET type_economique = CASE type_ligne
        WHEN 'COMMISSION_CONCIERGERIE' THEN 'GESTION'
        WHEN 'MENAGE_FACTURE' THEN 'MENAGE'
        WHEN 'CHARGE_FIXE' THEN 'FORFAIT'
        WHEN 'PREPARATION_CANAPE' THEN 'SERVICE_ADDITIONNEL'
        WHEN 'EXTRA' THEN 'SERVICE_ADDITIONNEL'
        WHEN 'CHARGES_EXCEPT_REFAC' THEN 'REFACTURATION'
        WHEN 'CHARGE_REFACTUREE' THEN 'REFACTURATION'
        WHEN 'REDUCTION' THEN 'REDUCTION'
    END
WHERE type_economique IS NULL;
-- Toute ligne créée ensuite reçoit la même dérivation si l'appelant n'a pas précisé son type (un
-- EXTRA peut être déclaré SINISTRE ou AUTRE_PRESTATION par la composition de la facture).
CREATE TRIGGER IF NOT EXISTS trg_fprl_type_economique
AFTER INSERT ON factures_proprietaires_lignes
WHEN NEW.type_economique IS NULL
BEGIN
    UPDATE factures_proprietaires_lignes SET type_economique = CASE NEW.type_ligne
            WHEN 'COMMISSION_CONCIERGERIE' THEN 'GESTION'
            WHEN 'MENAGE_FACTURE' THEN 'MENAGE'
            WHEN 'CHARGE_FIXE' THEN 'FORFAIT'
            WHEN 'PREPARATION_CANAPE' THEN 'SERVICE_ADDITIONNEL'
            WHEN 'EXTRA' THEN 'SERVICE_ADDITIONNEL'
            WHEN 'CHARGES_EXCEPT_REFAC' THEN 'REFACTURATION'
            WHEN 'CHARGE_REFACTUREE' THEN 'REFACTURATION'
            WHEN 'REDUCTION' THEN 'REDUCTION'
        END
    WHERE id = NEW.id;
END;

-- ── 6. Charges : compte porté, justifications ─────────────────────────────────────────────────
-- compte_origine : MAPPING (défaut de la catégorie), AUTORISE (autre compte autorisé),
-- LIBRE (imputation libre : justification obligatoire). NULL : charge antérieure, le compte reste
-- résolu par le mapping au moment du rapprochement, comme avant.
ALTER TABLE charges ADD COLUMN compte_comptable TEXT;
ALTER TABLE charges ADD COLUMN compte_origine TEXT
    CHECK (compte_origine IS NULL OR compte_origine IN ('MAPPING', 'AUTORISE', 'LIBRE'));
ALTER TABLE charges ADD COLUMN justification_imputation TEXT;
-- Charge créée depuis un mouvement d'un autre montant (acompte puis solde, paiement groupé…).
ALTER TABLE charges ADD COLUMN justification_ecart_montant TEXT;

-- ── 7. Justificatifs : référence documentaire, présence confirmée ou absence justifiée ─────────
CREATE TABLE IF NOT EXISTS justificatifs_sequence (
    serie             TEXT PRIMARY KEY,                 -- CHG-2026-09, FAF-2026-09…
    dernier_numero    INTEGER NOT NULL DEFAULT 0,
    date_modification TEXT
);
CREATE TABLE IF NOT EXISTS justificatifs (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    reference              TEXT NOT NULL UNIQUE,        -- CHG-2026-09-001
    objet_type             TEXT NOT NULL CHECK (objet_type IN ('CHARGE', 'FACTURE_FOURNISSEUR')),
    objet_id               TEXT NOT NULL,
    dossier                TEXT NOT NULL,               -- dossier canonique, relatif au projet
    statut                 TEXT NOT NULL DEFAULT 'A_CONFIRMER'
        CHECK (statut IN ('A_CONFIRMER', 'JUSTIFICATIF_ARCHIVE', 'JUSTIFICATIF_ABSENT_JUSTIFIE')),
    justification_absence  TEXT,
    fichier_constate       TEXT,                        -- nom du fichier trouvé, si vérifiable
    confirme_par           TEXT,
    confirme_le            TEXT,
    date_creation          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (objet_type, objet_id),
    CHECK (statut <> 'JUSTIFICATIF_ABSENT_JUSTIFIE' OR length(trim(COALESCE(justification_absence, ''))) > 0)
);
CREATE TABLE IF NOT EXISTS justificatif_evenements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reference       TEXT NOT NULL,
    type_evenement  TEXT NOT NULL CHECK (type_evenement IN ('ATTRIBUTION', 'CONFIRMATION_ARCHIVE',
                                                            'ABSENCE_JUSTIFIEE')),
    statut          TEXT,
    justification   TEXT,
    fichier         TEXT,
    acteur          TEXT NOT NULL,
    horodatage      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_justificatif_evenements_reference ON justificatif_evenements(reference);
CREATE TRIGGER IF NOT EXISTS trg_justificatifs_sans_suppression
BEFORE DELETE ON justificatifs
BEGIN
    SELECT RAISE(ABORT, 'Suppression interdite : une référence de justificatif ne disparaît pas.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0114');

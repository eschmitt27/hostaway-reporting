-- Migration 0029 — Référentiel Setup canonique en SQLite.
--
-- Additive comme 0017→0028 : uniquement des CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT
-- EXISTS, aucun ALTER. Les migrations sont rejouées intégralement à chaque démarrage.
--
-- OBJET
-- Les 28 onglets de REF_Setup.xlsm deviennent 28 tables. Aucune de ces tables n'existait : l'audit
-- des 73 tables présentes en 0028 n'a trouvé que des objets transactionnels et des journaux, aucun
-- référentiel. Rien n'est donc dupliqué ici.
--
-- FIDÉLITÉ DU SCHÉMA
-- Les noms de table dérivent mécaniquement de l'onglet (REF_Taux_Commission → ref_taux_commission)
-- et les colonnes reprennent EXACTEMENT les en-têtes du classeur, dans leur ordre. Le schéma a été
-- généré depuis le classeur réel, pas recopié à la main : une colonne renommée dans Excel se voit
-- immédiatement à l'import, elle ne se perd pas dans une correspondance implicite.
--
-- POURQUOI TOUT EN TEXT
-- Chaque colonne est TEXT, y compris les montants, taux et dates. Ce n'est pas un raccourci :
--   - c'est LOSSLESS — une cellule Excel hétérogène est conservée telle qu'elle est saisie, et
--     l'import reste comparable octet à octet d'une exécution à l'autre (idempotence, empreintes) ;
--   - c'est COHÉRENT avec le chemin existant — `readers/csv_reader.py` rend déjà des chaînes, et
--     les services (`logements_service`, etc.) convertissent explicitement au moment de l'usage.
--     Typer ici créerait DEUX conventions selon la source d'une même donnée ;
--   - typer imposerait de DÉCIDER, colonne par colonne, ce qu'est un nombre ou une date — c'est
--     une interprétation métier, et elle n'a pas à être prise par une migration.
-- Les contrôles de type vivent dans l'importateur, qui refuse plutôt que de convertir en silence.
--
-- TRAÇABILITÉ
-- Chaque ligne porte `import_id` : on sait de quel import elle vient. `ref_setup_imports` journalise
-- les imports (empreinte du classeur, compteurs, statut) et `ref_setup_import_feuilles` le détail
-- par onglet. Un import n'écrase jamais un journal : il ajoute une ligne.

-- REF_Logements (15 colonnes)
CREATE TABLE IF NOT EXISTS ref_logements (
    logement_id TEXT NOT NULL,
    hostaway_listing_id TEXT,
    nom_logement_officiel TEXT,
    nom_court TEXT,
    adresse TEXT,
    ville TEXT,
    type_logement_id TEXT,
    sur_hostaway TEXT,
    dynamic_pricing TEXT,
    actif TEXT,
    statut_parc TEXT,
    commentaire TEXT,
    forfait_logiciel_consommables_mensuel TEXT,
    seuil_voyageurs_preparation_canape TEXT,
    montant_preparation_canape TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (logement_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_logements_import ON ref_logements(import_id);

-- REF_Proprietaires (9 colonnes)
CREATE TABLE IF NOT EXISTS ref_proprietaires (
    proprietaire_id TEXT NOT NULL,
    nom_proprietaire TEXT,
    prenom_proprietaire TEXT,
    email TEXT,
    telephone TEXT,
    adresse_facturation TEXT,
    mode_facturation TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (proprietaire_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_proprietaires_import ON ref_proprietaires(import_id);

-- REF_Types_Logements (3 colonnes)
CREATE TABLE IF NOT EXISTS ref_types_logements (
    type_logement_id TEXT NOT NULL,
    type_logement TEXT,
    description TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (type_logement_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_types_logements_import ON ref_types_logements(import_id);

-- REF_Couts_Standards_Menage (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_couts_standards_menage (
    cout_standard_id TEXT NOT NULL,
    type_logement_id TEXT,
    cout_standard_menage TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (cout_standard_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_couts_standards_menage_import ON ref_couts_standards_menage(import_id);

-- REF_Categories_Charges (11 colonnes)
CREATE TABLE IF NOT EXISTS ref_categories_charges (
    categorie_charge_id TEXT NOT NULL,
    categorie_niveau_1 TEXT,
    categorie_niveau_2 TEXT,
    description TEXT,
    impact_resultat TEXT,
    refacturable_defaut TEXT,
    hors_compta_defaut TEXT,
    actif TEXT,
    filtre_vue_menage TEXT,
    famille_impact_categorie TEXT,
    profils_impact_autorises TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (categorie_charge_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_categories_charges_import ON ref_categories_charges(import_id);

-- REF_Mapping_Logements (8 colonnes)
CREATE TABLE IF NOT EXISTS ref_mapping_logements (
    mapping_logement_id TEXT NOT NULL,
    source TEXT,
    champ_source TEXT,
    valeur_source TEXT,
    logement_id TEXT,
    niveau_confiance TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (mapping_logement_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_mapping_logements_import ON ref_mapping_logements(import_id);

-- REF_Intervenants (20 colonnes)
CREATE TABLE IF NOT EXISTS ref_intervenants (
    intervenant_id TEXT NOT NULL,
    nom_intervenant TEXT,
    type_intervenant TEXT,
    societe TEXT,
    email TEXT,
    telephone TEXT,
    actif TEXT,
    commentaire TEXT,
    nom_normalise TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    nom_legal TEXT,
    siret_rcs TEXT,
    email_facturation TEXT,
    hostaway_assigneeUserId TEXT,
    hostaway_nom_affiche TEXT,
    hostaway_mapping_actif TEXT,
    hostaway_mapping_date_debut TEXT,
    hostaway_mapping_date_fin TEXT,
    commentaire_mapping_hostaway TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (intervenant_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_intervenants_import ON ref_intervenants(import_id);

-- REF_Parametres_Generaux (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_parametres_generaux (
    parametre_id TEXT NOT NULL,
    nom_parametre TEXT,
    valeur TEXT,
    description TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    actif TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (parametre_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_parametres_generaux_import ON ref_parametres_generaux(import_id);

-- REF_Sources_Systeme (8 colonnes)
CREATE TABLE IF NOT EXISTS ref_sources_systeme (
    source_id TEXT NOT NULL,
    module TEXT,
    nom_source TEXT,
    type_source TEXT,
    alimentation TEXT,
    dossier_source TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (source_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_sources_systeme_import ON ref_sources_systeme(import_id);

-- REF_Statuts (6 colonnes)
CREATE TABLE IF NOT EXISTS ref_statuts (
    statut_id TEXT NOT NULL,
    famille_statut TEXT,
    statut TEXT,
    ordre_affichage TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (statut_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_statuts_import ON ref_statuts(import_id);

-- REF_Modes_Paiement (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_modes_paiement (
    mode_paiement_id TEXT NOT NULL,
    mode_paiement TEXT,
    impact_banque TEXT,
    impact_caisse TEXT,
    impact_associee TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (mode_paiement_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_modes_paiement_import ON ref_modes_paiement(import_id);

-- REF_Types_Lignes_Menage (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_types_lignes_menage (
    type_ligne_menage_id TEXT NOT NULL,
    type_ligne_menage TEXT,
    compte_comme_menage TEXT,
    repartissable_sur_menages TEXT,
    impact_cout_menage TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (type_ligne_menage_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_types_lignes_menage_import ON ref_types_lignes_menage(import_id);

-- REF_Canaux_Reservation (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_canaux_reservation (
    canal_id TEXT NOT NULL,
    canal TEXT,
    source_principale TEXT,
    dans_hostaway TEXT,
    hors_compta_possible TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (canal_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_canaux_reservation_import ON ref_canaux_reservation(import_id);

-- REF_Abonnements_Logiciels (11 colonnes)
CREATE TABLE IF NOT EXISTS ref_abonnements_logiciels (
    abonnement_logiciel_id TEXT NOT NULL,
    logiciel TEXT,
    type_abonnement TEXT,
    condition_ref_logements TEXT,
    valeur_declencheur TEXT,
    montant_mensuel_par_logement TEXT,
    devise TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (abonnement_logiciel_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_abonnements_logiciels_import ON ref_abonnements_logiciels(import_id);

-- REF_Cartes_Paiement (10 colonnes)
CREATE TABLE IF NOT EXISTS ref_cartes_paiement (
    carte_id TEXT NOT NULL,
    suffixe_carte TEXT,
    personne_id TEXT,
    nom_personne TEXT,
    type_personne TEXT,
    mode_paiement_id TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (carte_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_cartes_paiement_import ON ref_cartes_paiement(import_id);

-- REF_Associes (5 colonnes)
CREATE TABLE IF NOT EXISTS ref_associes (
    personne_id TEXT NOT NULL,
    nom_personne TEXT,
    type_personne TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (personne_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_associes_import ON ref_associes(import_id);

-- REF_Codes_Impact (8 colonnes)
CREATE TABLE IF NOT EXISTS ref_codes_impact (
    code_impact TEXT NOT NULL,
    libelle TEXT,
    impact_resultat_comptable TEXT,
    impact_resultat_extra TEXT,
    impact_resultat_reel TEXT,
    impact_avantages TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (code_impact)
);
CREATE INDEX IF NOT EXISTS idx_ref_codes_impact_import ON ref_codes_impact(import_id);

-- REF_Types_Flux (9 colonnes)
CREATE TABLE IF NOT EXISTS ref_types_flux (
    type_flux_id TEXT NOT NULL,
    type_flux TEXT,
    description TEXT,
    code_impact_defaut TEXT,
    avantage_brut_defaut TEXT,
    deduit_avantage_defaut TEXT,
    comptabilisable_defaut TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (type_flux_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_types_flux_import ON ref_types_flux(import_id);

-- REF_Types_Affectation (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_types_affectation (
    affectation_id TEXT NOT NULL,
    type_affectation TEXT,
    description TEXT,
    logement_obligatoire TEXT,
    proprietaire_obligatoire TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (affectation_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_types_affectation_import ON ref_types_affectation(import_id);

-- REF_Statuts_Payout (5 colonnes)
CREATE TABLE IF NOT EXISTS ref_statuts_payout (
    statut_payout_id TEXT NOT NULL,
    statut_calcul_payout TEXT,
    description TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (statut_payout_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_statuts_payout_import ON ref_statuts_payout(import_id);

-- REF_Cloture_Mensuelle (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_cloture_mensuelle (
    mois TEXT NOT NULL,
    statut_mois TEXT,
    date_passage_controle TEXT,
    date_cloture TEXT,
    nb_lignes_bancaires_non_classees TEXT,
    nb_controles_bloquants_ouverts TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (mois)
);
CREATE INDEX IF NOT EXISTS idx_ref_cloture_mensuelle_import ON ref_cloture_mensuelle(import_id);

-- REF_Charges_Recurrentes (19 colonnes)
CREATE TABLE IF NOT EXISTS ref_charges_recurrentes (
    charge_recurrente_id TEXT NOT NULL,
    libelle_charge_recurrente TEXT,
    categorie_charge_id TEXT,
    type_flux_id TEXT,
    montant_ttc TEXT,
    devise TEXT,
    periodicite TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    affectation_type TEXT,
    logement_id TEXT,
    proprietaire_id TEXT,
    cle_repartition TEXT,
    code_impact_defaut TEXT,
    prise_en_compta_defaut TEXT,
    refacturable_defaut TEXT,
    filtre_vue_menage TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (charge_recurrente_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_charges_recurrentes_import ON ref_charges_recurrentes(import_id);

-- REF_Banque_Regles (20 colonnes)
CREATE TABLE IF NOT EXISTS ref_banque_regles (
    regle_id TEXT NOT NULL,
    priorite TEXT,
    actif TEXT,
    compte_id TEXT,
    type_match TEXT,
    champ_cible TEXT,
    motif TEXT,
    tiers_detecte TEXT,
    categorie TEXT,
    type_flux_id TEXT,
    code_impact TEXT,
    source_economique TEXT,
    rapprochement_requis TEXT,
    validation_automatique TEXT,
    niveau_risque TEXT,
    statut_controle_defaut TEXT,
    statut_classification_defaut TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (regle_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_banque_regles_import ON ref_banque_regles(import_id);

-- REF_Couts_Menage_Interne (12 colonnes)
CREATE TABLE IF NOT EXISTS ref_couts_menage_interne (
    cout_menage_interne_id TEXT NOT NULL,
    type_logement_id TEXT,
    type_logement_libelle TEXT,
    montant_interne_standard TEXT,
    date_debut_validite TEXT,
    date_fin_validite TEXT,
    actif TEXT,
    commentaire TEXT,
    intervenant_id TEXT,
    logement_id TEXT,
    cout_fixe_par_menage TEXT,
    priorite TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (cout_menage_interne_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_couts_menage_interne_import ON ref_couts_menage_interne(import_id);

-- REF_Taux_Commission (9 colonnes)
CREATE TABLE IF NOT EXISTS ref_taux_commission (
    taux_commission_id TEXT NOT NULL,
    proprietaire_id TEXT,
    logement_id TEXT,
    taux_commission TEXT,
    date_debut TEXT,
    date_fin TEXT,
    actif TEXT,
    justification TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (taux_commission_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_taux_commission_import ON ref_taux_commission(import_id);

-- REF_Gestion_Logements_Hist (8 colonnes)
CREATE TABLE IF NOT EXISTS ref_gestion_logements_hist (
    gestion_id TEXT NOT NULL,
    logement_id TEXT,
    proprietaire_id TEXT,
    date_debut TEXT,
    date_fin TEXT,
    statut_gestion TEXT,
    source TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (gestion_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_gestion_logements_hist_import ON ref_gestion_logements_hist(import_id);

-- REF_Taux_Heures_Menage (7 colonnes)
CREATE TABLE IF NOT EXISTS ref_taux_heures_menage (
    taux_horaire_id TEXT NOT NULL,
    intervenant_id TEXT,
    taux_horaire TEXT,
    date_debut TEXT,
    date_fin TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (taux_horaire_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_taux_heures_menage_import ON ref_taux_heures_menage(import_id);

-- REF_Assoc_Mode (6 colonnes)
CREATE TABLE IF NOT EXISTS ref_assoc_mode (
    assoc_mode_id TEXT NOT NULL,
    mode_paiement_id TEXT,
    associe_id TEXT,
    assoc_mode TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (assoc_mode_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_assoc_mode_import ON ref_assoc_mode(import_id);

-- ── Journal des imports ─────────────────────────────────────────────────────────────────────────
-- Un import est une TENTATIVE : il est journalisé même quand il échoue, et surtout quand il échoue.
-- `empreinte_source` = SHA256 du classeur lu. Deux imports de la même empreinte doivent produire le
-- même contenu : c'est le test d'idempotence.
CREATE TABLE IF NOT EXISTS ref_setup_imports (
    import_id         TEXT PRIMARY KEY,
    horodatage        TEXT NOT NULL,
    chemin_source     TEXT NOT NULL,
    empreinte_source  TEXT NOT NULL,
    statut            TEXT NOT NULL,          -- PREVISUALISE | IMPORTE | REFUSE
    nb_feuilles       INTEGER NOT NULL DEFAULT 0,
    nb_lignes         INTEGER NOT NULL DEFAULT 0,
    code_refus        TEXT,
    message           TEXT
);
CREATE INDEX IF NOT EXISTS idx_ref_setup_imports_horodatage ON ref_setup_imports(horodatage);

-- Détail par onglet : ce qui permet de dire « REF_Taux_Commission : 19 lignes, empreinte X » sans
-- relire le classeur.
CREATE TABLE IF NOT EXISTS ref_setup_import_feuilles (
    import_id         TEXT NOT NULL,
    onglet            TEXT NOT NULL,
    table_cible       TEXT NOT NULL,
    nb_lignes_source  INTEGER NOT NULL DEFAULT 0,
    nb_lignes_ecrites INTEGER NOT NULL DEFAULT 0,
    empreinte_contenu TEXT NOT NULL,
    PRIMARY KEY (import_id, onglet)
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0029');

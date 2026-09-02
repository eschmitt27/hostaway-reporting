-- Historisation des paramètres canapé (seuil de voyageurs / montant) par logement — Mission 6.
--
-- Jusqu'ici, `ref_logements.seuil_voyageurs_preparation_canape`/`montant_preparation_canape`
-- étaient deux colonnes COURANTES sans période : un recalcul futur d'une réservation passée
-- aurait utilisé le paramètre ACTUEL, jamais celui en vigueur à la date de la réservation. Ces
-- deux colonnes restent en place sur `ref_logements` (jamais supprimées, parité avec le classeur),
-- mais deviennent vestigiales pour le calcul : c'est cette nouvelle table, historisée, qui fait
-- foi désormais (résolution par date via `lib_ref_history.resolve_canape_parametres`).
--
-- Backfill : une période OUVERTE depuis l'origine (date_debut/date_fin vides = valide depuis
-- toujours, même convention que les lignes "non datées" de `ref_gestion_logements_hist`) par
-- logement ayant une valeur actuellement configurée — pour préserver exactement le comportement
-- actuel (aucune règle métier changée, écart économique 0,00€ garanti par construction).

CREATE TABLE IF NOT EXISTS ref_canape_parametres (
    canape_parametre_id TEXT NOT NULL,
    logement_id TEXT,
    seuil_voyageurs_preparation_canape TEXT,
    montant_preparation_canape TEXT,
    date_debut TEXT,
    date_fin TEXT,
    actif TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (canape_parametre_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_canape_parametres_import ON ref_canape_parametres(import_id);
CREATE INDEX IF NOT EXISTS idx_ref_canape_parametres_logement ON ref_canape_parametres(logement_id);

-- OR IGNORE : un rejeu complet de apply_migrations() réexécute ce fichier même après application ;
-- sans OR IGNORE, la contrainte PRIMARY KEY casse le redémarrage dès qu'une migration postérieure est ajoutée.
INSERT OR IGNORE INTO ref_canape_parametres
    (canape_parametre_id, logement_id, seuil_voyageurs_preparation_canape,
     montant_preparation_canape, date_debut, date_fin, actif, commentaire, import_id)
SELECT
    'CNP_' || logement_id,
    logement_id,
    seuil_voyageurs_preparation_canape,
    montant_preparation_canape,
    '', '', 'OUI', 'Backfill migration 0058 (valeur courante au moment de la migration)',
    'MIGRATION_0058'
FROM ref_logements
WHERE seuil_voyageurs_preparation_canape IS NOT NULL
  AND TRIM(seuil_voyageurs_preparation_canape) != ''
  AND montant_preparation_canape IS NOT NULL
  AND TRIM(montant_preparation_canape) != ''
  AND CAST(montant_preparation_canape AS REAL) > 0;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0058');

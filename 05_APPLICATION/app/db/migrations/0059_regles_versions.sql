-- Versionnement des règles ALGORITHMIQUES (par opposition aux variables simples déjà historisées
-- : taux commission, coût ménage, canapé) — Mission 6 bis.
--
-- Une variable (taux, montant) se stocke directement avec sa période de validité. Une règle de
-- CALCUL (assiette de commission, répartition d'une charge commune de facture) doit être
-- historisée par VERSION : RULE_CODE stable + VERSION stable + période de validité. L'implémen-
-- tation reste dans le code (jamais de Python/formule libre en base) ; cette table dit seulement
-- QUELLE version est applicable à quelle date, pour un RULE_CODE donné.
--
-- Backfill : une ligne "V1" par règle actuellement connue, période OUVERTE DEPUIS L'ORIGINE
-- (date_debut/date_fin vides) — la formule actuelle devient V1 sans qu'aucun comportement ne
-- change (écart économique 0,00€ garanti par construction, comme pour ref_canape_parametres).
-- Aucune V2 n'existe : cette mission construit la CAPACITÉ de versionner, pas une nouvelle formule.

CREATE TABLE IF NOT EXISTS ref_regles_versions (
    regle_version_id TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    version TEXT NOT NULL,
    date_debut TEXT,
    date_fin TEXT,
    actif TEXT,
    parametres TEXT,
    commentaire TEXT,
    import_id TEXT NOT NULL,
    PRIMARY KEY (regle_version_id)
);
CREATE INDEX IF NOT EXISTS idx_ref_regles_versions_import ON ref_regles_versions(import_id);
CREATE INDEX IF NOT EXISTS idx_ref_regles_versions_code ON ref_regles_versions(rule_code);

INSERT OR IGNORE INTO ref_regles_versions
    (regle_version_id, rule_code, version, date_debut, date_fin, actif, parametres, commentaire,
     import_id)
VALUES
    ('RGV_ASSIETTE_COMMISSION_V1', 'ASSIETTE_COMMISSION', 'V1', '', '', 'OUI', '',
     'Backfill migration 0059 : formule actuelle (par canal HA/VRBO/HH, lot10_calculer_resultats.py), aucune modification.',
     'MIGRATION_0059'),
    ('RGV_REGLE_REPARTITION_CHARGE_COMMUNE_V1', 'REGLE_REPARTITION_CHARGE_COMMUNE', 'V1', '', '', 'OUI', '',
     'Backfill migration 0059 : répartition égale déterministe (charges_impact_service.repartir_egal), aucune modification.',
     'MIGRATION_0059'),
    ('RGV_CANAPE_FORMULE_V1', 'CANAPE_FORMULE', 'V1', '', '', 'OUI', '',
     'Backfill migration 0059 : seuil de voyageurs -> montant fixe si atteint, sinon 0 (lib_canape.py::calculate_canape_amount), aucune modification. Le SEUIL/MONTANT restent des variables (ref_canape_parametres, migration 0058) ; ceci verse la FORMULE elle-meme (encore jamais differente) au meme mecanisme de versionnement.',
     'MIGRATION_0059');

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0059');

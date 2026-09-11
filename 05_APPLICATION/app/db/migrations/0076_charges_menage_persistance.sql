-- Migration 0076 — la branche MÉNAGE d'une charge cesse d'être calculée puis jetée.
--
-- DEUX VALEURS ÉTAIENT PERDUES À L'ÉCRITURE
--
-- 1. `affectable_menage`. `charges_preview_service._build_row_data()` la calcule explicitement
--    (« Charge ménage : affectable_menage=OUI, refacturable=NON », commentaire d'origine) et la
--    renvoie dans `row_data`. Mais la colonne n'existait pas, et `CHAMPS_SAISIE` ne la listait pas :
--    la valeur disparaissait en silence à l'INSERT. Or `lot6f_cout_complet_menages.py` FILTRE
--    précisément sur ce drapeau (`if str(d.get("affectable_menage")) != "OUI": continue`) pour
--    constituer les pools de coût ménage. Une charge ménage saisie dans l'application ne pouvait
--    donc structurellement jamais rejoindre le coût complet ménage.
--
-- 2. La VENTILATION MÉNAGE. `charges_impact_service.menage_perimetre()` résout le périmètre
--    (intervenants OU logements) au moment de la prévisualisation ; `confirmer()` ne conservait
--    que `row_data`, et ce périmètre mourait avec le manifest. C'est EXACTEMENT le défaut corrigé
--    en migration 0074 pour les charges non ménage — il survivait sur la branche ménage.
--
-- POURQUOI UNE TABLE DISTINCTE DE 0074
-- `charges_perimetre_analytique` porte des LOGEMENTS et une quote-part monétaire. Le parcours
-- ménage ventile, lui, soit sur des logements, soit sur des INTERVENANTS — une dimension que la
-- table 0074 ne peut pas représenter sans y rendre `logement_id` facultatif, ce qui affaiblirait
-- sa contrainte pour tous les autres cas. Deux périmètres, deux tables, chacune contrainte.
--
-- CE QUE CETTE MIGRATION NE FAIT PAS
-- Elle ne branche PAS `lot6f` sur SQLite : ce moteur lit encore le classeur `SAISIE_Charges_Flux`.
-- Persister le drapeau rend la bascule possible ; la décider reste un arbitrage métier, documenté
-- dans `ARBRE_CHARGES_CONSEQUENCES.md`.

ALTER TABLE charges ADD COLUMN affectable_menage TEXT;

CREATE TABLE IF NOT EXISTS charges_perimetre_menage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_id       TEXT    NOT NULL,
    mode            TEXT    NOT NULL CHECK (mode IN ('INTERVENANT', 'LOGEMENT')),
    intervenant_id  TEXT,                 -- renseigné si mode = INTERVENANT
    logement_id     TEXT,                 -- renseigné si mode = LOGEMENT
    proprietaire_id TEXT,                 -- résolu à la création pour le mode LOGEMENT
    mois            TEXT,
    quote_part_montant REAL NOT NULL DEFAULT 0,
    date_creation   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    acteur          TEXT,
    -- Exactement une dimension renseignée : un périmètre ménage ne peut pas désigner à la fois un
    -- intervenant et un logement, et n'a aucun sens s'il ne désigne ni l'un ni l'autre.
    CHECK ((intervenant_id IS NOT NULL AND logement_id IS NULL)
        OR (intervenant_id IS NULL AND logement_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_cpm_charge ON charges_perimetre_menage(charge_id);
CREATE INDEX IF NOT EXISTS idx_cpm_intervenant ON charges_perimetre_menage(intervenant_id, mois);
CREATE INDEX IF NOT EXISTS idx_cpm_logement ON charges_perimetre_menage(logement_id, mois);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cpm_unique_intervenant
    ON charges_perimetre_menage(charge_id, intervenant_id) WHERE intervenant_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_cpm_unique_logement
    ON charges_perimetre_menage(charge_id, logement_id) WHERE logement_id IS NOT NULL;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0076');

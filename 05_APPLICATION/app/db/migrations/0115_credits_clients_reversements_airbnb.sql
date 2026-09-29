-- Migration 0115 — Crédits clients : origine comptable des reversements Airbnb (Mission 37).
--
-- ADDITIVE. Aucune ligne existante n'est modifiée ; aucune écriture n'est créée.
--
-- UN REVERSEMENT AIRBNB (D032) est un versement d'Airbnb reçu par la conciergerie pour le compte
-- d'un propriétaire ; il réduit ce que ce propriétaire doit. Il n'a jamais de lien avec une
-- réservation (la règle Banque reste inchangée). Jusqu'ici il n'existait que sous forme
-- d'IMPUTATION sur une facture (`imputations_airbnb`), sans objet représentant l'argent reçu :
-- impossible d'en connaître l'origine comptable, le reste disponible ni l'historique.
--
-- `credits_clients` est cet objet : le CRÉDIT, avec son montant initial et son origine.
--   mode_origine BANQUE   : l'encaissement réel est rapproché dans Flux → écriture 512 / 419100 ;
--                           en attendant, le crédit est EN_ATTENTE_ORIGINE et ne s'impute pas.
--   mode_origine JUSTIFIE : aucune trace bancaire exploitable (encaissement antérieur à
--                           l'historique Qonto…) ; l'utilisateur nomme le compte source et
--                           justifie → écriture <compte source> / 419100. Jamais 512 ni 530 :
--                           une trésorerie réelle se prouve par le rapprochement.
-- Chaque IMPUTATION reste une ligne `imputations_airbnb` (contrat Lot10 inchangé), désormais
-- reliée à son crédit ; l'écriture 419100 → 411000 est tracée sur elle.
-- Les ACOMPTES ne sont pas recopiés ici : ils restent des mouvements de trésorerie propriétaire
-- imputés par le FIFO (règle métier du compte propriétaire) ; la vue « crédits » les présente.

CREATE TABLE IF NOT EXISTS credits_clients (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    credit_id_opaque    TEXT NOT NULL UNIQUE,                    -- CRD-xxxx
    proprietaire_id     TEXT NOT NULL,
    origine             TEXT NOT NULL CHECK (origine IN ('REVERSEMENT_AIRBNB')),
    mode_origine        TEXT NOT NULL CHECK (mode_origine IN ('BANQUE', 'JUSTIFIE')),
    date_origine        TEXT NOT NULL,
    mois                TEXT,
    montant_initial     REAL NOT NULL CHECK (montant_initial > 0),
    reference           TEXT,                                    -- référence Airbnb, libellé
    justification       TEXT,
    compte_source       TEXT,                                    -- mode JUSTIFIE seulement
    statut              TEXT NOT NULL DEFAULT 'EN_ATTENTE_ORIGINE'
        CHECK (statut IN ('EN_ATTENTE_ORIGINE', 'DISPONIBLE', 'ANNULE')),
    mouvement_origine   TEXT,                                    -- mouvement bancaire rapproché
    ecriture_origine    TEXT,                                    -- écriture … / 419100
    lettrage_origine    TEXT,
    cree_par            TEXT NOT NULL,
    cree_le             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    version             INTEGER NOT NULL DEFAULT 1,
    CHECK (mode_origine <> 'JUSTIFIE'
           OR (compte_source IS NOT NULL AND length(trim(COALESCE(justification, ''))) > 0))
);
CREATE INDEX IF NOT EXISTS idx_credits_clients_proprietaire ON credits_clients(proprietaire_id);

CREATE TABLE IF NOT EXISTS credit_client_evenements (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    credit_id_opaque  TEXT NOT NULL,
    type_evenement    TEXT NOT NULL CHECK (type_evenement IN (
        'CREATION', 'ORIGINE_CONSTATEE', 'ORIGINE_RETIREE', 'IMPUTATION', 'REGULARISATION',
        'ECRITURE_IMPUTATION', 'ANNULATION')),
    montant           REAL,
    facture_id        TEXT,
    ecriture_id       TEXT,
    detail            TEXT,
    acteur            TEXT NOT NULL,
    horodatage        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_credit_client_evenements ON credit_client_evenements(credit_id_opaque);

CREATE TRIGGER IF NOT EXISTS trg_credits_clients_sans_suppression
BEFORE DELETE ON credits_clients
BEGIN
    SELECT RAISE(ABORT, 'Suppression interdite : un crédit s''annule, il ne se supprime pas.');
END;

-- Imputation ↔ crédit, et écriture 419100 → 411000 qui la constate. NULL pour les imputations
-- antérieures : elles restent « à régulariser » tant qu'aucun crédit d'origine n'y est rattaché.
ALTER TABLE imputations_airbnb ADD COLUMN credit_id_opaque TEXT;
ALTER TABLE imputations_airbnb ADD COLUMN ecriture_imputation TEXT;

-- Catégories ambiguës (audit Mission 37) : deux natures possibles, aucune par défaut — le compte
-- se CHOISIT à la saisie, rien n'est présélectionné.
INSERT OR IGNORE INTO mapping_comptable_regles
    (regle_id_opaque, portee, cle, compte, statut, source, acteur, role)
VALUES
    ('MAP-CAT-CHG_019-615200', 'CATEGORIE', 'CHG_019', '615200', 'VALIDE', 'Audit Mission 37 : sinistre sur le logement (immobilier)', 'Migration 0115', 'AUTORISE'),
    ('MAP-CAT-CHG_019-615500', 'CATEGORIE', 'CHG_019', '615500', 'VALIDE', 'Audit Mission 37 : sinistre sur le mobilier / équipement', 'Migration 0115', 'AUTORISE'),
    ('MAP-CAT-CHG_025-625700', 'CATEGORIE', 'CHG_025', '625700', 'VALIDE', 'Audit Mission 37 : repas d''affaires (réception)', 'Migration 0115', 'AUTORISE'),
    ('MAP-CAT-CHG_025-625100', 'CATEGORIE', 'CHG_025', '625100', 'VALIDE', 'Audit Mission 37 : repas en déplacement', 'Migration 0115', 'AUTORISE');

INSERT INTO mapping_regle_evenements (regle_id_opaque, type_evenement, apres_json, motif, acteur)
SELECT regle_id_opaque, 'CREATION',
       json_object('portee', portee, 'cle', cle, 'compte', compte, 'statut', statut, 'role', role),
       source, 'Migration 0115'
FROM mapping_comptable_regles
WHERE acteur = 'Migration 0115'
  AND regle_id_opaque NOT IN (SELECT regle_id_opaque FROM mapping_regle_evenements);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0115');

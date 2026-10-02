-- 0120 — Garde-fous de la comptabilité applicative V1 (cutover du 2026-09-01).
--
-- La date de début V1 vit dans `parametres_societe_facturation`, clé `V1_ACCOUNTING_START_DATE`,
-- posée UNE fois par la transaction de cutover (`cutover_v1_service`). Ces déclencheurs sont la
-- DERNIÈRE ligne de défense : les services refusent déjà avec un message lisible ; ici, même un
-- accès direct à la base (script, appel bas niveau, repository) ne peut plus créer une comptabilité
-- antérieure au cutover.
--
-- Tant que le paramètre est absent (installation neuve, bases de test), la sous-requête vaut NULL,
-- la comparaison aussi, et AUCUN déclencheur ne se déclenche : la règle naît avec le cutover.
--
-- Additive : aucune table ni donnée modifiée.

-- Facture propriétaire : ni création, ni déplacement sur un mois ou une période antérieurs.
CREATE TRIGGER IF NOT EXISTS trg_fpr_periode_v1_insert
BEFORE INSERT ON factures_proprietaires
WHEN NEW.mois < (SELECT substr(valeur, 1, 7) FROM parametres_societe_facturation
                  WHERE cle = 'V1_ACCOUNTING_START_DATE')
  OR (COALESCE(NEW.periode_debut, '') <> ''
      AND NEW.periode_debut < (SELECT valeur FROM parametres_societe_facturation
                                WHERE cle = 'V1_ACCOUNTING_START_DATE'))
BEGIN
    SELECT RAISE(ABORT, 'FACTURATION_AVANT_V1 : la facturation V1 ne porte sur aucune période antérieure à son début.');
END;

CREATE TRIGGER IF NOT EXISTS trg_fpr_periode_v1_update
BEFORE UPDATE OF mois, periode_debut ON factures_proprietaires
WHEN NEW.mois < (SELECT substr(valeur, 1, 7) FROM parametres_societe_facturation
                  WHERE cle = 'V1_ACCOUNTING_START_DATE')
  OR (COALESCE(NEW.periode_debut, '') <> ''
      AND NEW.periode_debut < (SELECT valeur FROM parametres_societe_facturation
                                WHERE cle = 'V1_ACCOUNTING_START_DATE'))
BEGIN
    SELECT RAISE(ABORT, 'FACTURATION_AVANT_V1 : une facture ne se déplace pas vers une période antérieure au début de la facturation V1.');
END;

-- Écriture comptable : aucune période antérieure au début V1.
CREATE TRIGGER IF NOT EXISTS trg_ecritures_periode_v1
BEFORE INSERT ON ecritures
WHEN NEW.periode < (SELECT substr(valeur, 1, 7) FROM parametres_societe_facturation
                     WHERE cle = 'V1_ACCOUNTING_START_DATE')
BEGIN
    SELECT RAISE(ABORT, 'COMPTABILITE_AVANT_V1 : aucune écriture ne porte sur une période antérieure au début de la comptabilité V1.');
END;

-- Clôture mensuelle : aucun mois antérieur au début V1 n'est clôturable.
CREATE TRIGGER IF NOT EXISTS trg_clotures_mois_v1
BEFORE INSERT ON clotures_mensuelles
WHEN NEW.mois < (SELECT substr(valeur, 1, 7) FROM parametres_societe_facturation
                  WHERE cle = 'V1_ACCOUNTING_START_DATE')
BEGIN
    SELECT RAISE(ABORT, 'CLOTURE_AVANT_V1 : un mois antérieur au début de la comptabilité V1 ne se clôture pas.');
END;

-- Le paramètre de cutover, une fois posé, ne se modifie ni ne s'efface : c'est lui qui garantit que
-- le cutover reste appliqué après un redémarrage, une reprise de paramètres ou une erreur de saisie.
CREATE TRIGGER IF NOT EXISTS trg_parametre_v1_immuable_update
BEFORE UPDATE ON parametres_societe_facturation
WHEN OLD.cle = 'V1_ACCOUNTING_START_DATE'
BEGIN
    SELECT RAISE(ABORT, 'PARAMETRE_V1_IMMUABLE : la date de début de la comptabilité V1 ne se modifie pas.');
END;

CREATE TRIGGER IF NOT EXISTS trg_parametre_v1_immuable_delete
BEFORE DELETE ON parametres_societe_facturation
WHEN OLD.cle = 'V1_ACCOUNTING_START_DATE'
BEGIN
    SELECT RAISE(ABORT, 'PARAMETRE_V1_IMMUABLE : la date de début de la comptabilité V1 ne s''efface pas.');
END;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0120');

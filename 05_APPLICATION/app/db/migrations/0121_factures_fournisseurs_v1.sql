-- 0121 — Finition du cutover V1 (D-V1-FIN-2) : aucune facture fournisseur antérieure à la V1.
--
-- Une facture fournisseur datée d'avant `V1_ACCOUNTING_START_DATE` ne peut plus entrer dans la base,
-- quel que soit le chemin : import PDF, saisie, remplacement de version, INSERT direct. C'est le
-- second verrou ; le premier est `factures_service.creer` (refus métier avec un message lisible).
--
-- Même construction que 0120 : la borne est relue dans `parametres_societe_facturation`, jamais
-- recopiée. Tant que le paramètre n'existe pas (installation neuve, base de test), la sous-requête
-- rend NULL, la comparaison aussi, et rien n'est restreint.

CREATE TRIGGER IF NOT EXISTS trg_factures_fournisseurs_v1_insert
BEFORE INSERT ON factures
WHEN NEW.date_facture IS NOT NULL AND TRIM(NEW.date_facture) <> ''
 AND substr(NEW.date_facture, 1, 10) < (SELECT valeur FROM parametres_societe_facturation
                                        WHERE cle = 'V1_ACCOUNTING_START_DATE')
BEGIN
    SELECT RAISE(ABORT, 'FACTURE_FOURNISSEUR_AVANT_V1 : aucune facture fournisseur antérieure à la comptabilité V1');
END;

CREATE TRIGGER IF NOT EXISTS trg_factures_fournisseurs_v1_update
BEFORE UPDATE OF date_facture ON factures
WHEN NEW.date_facture IS NOT NULL AND TRIM(NEW.date_facture) <> ''
 AND substr(NEW.date_facture, 1, 10) < (SELECT valeur FROM parametres_societe_facturation
                                        WHERE cle = 'V1_ACCOUNTING_START_DATE')
BEGIN
    SELECT RAISE(ABORT, 'FACTURE_FOURNISSEUR_AVANT_V1 : aucune facture fournisseur antérieure à la comptabilité V1');
END;

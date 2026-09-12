-- 0082 — ce que le document DISAIT reste lisible même après qu'on l'ait corrigé (§26/§27).
--
-- LE BESOIN
-- §27 pose que « une quantité extraite du PDF est une donnée source ; elle ne doit pas être
-- librement modifiée ». Jusqu'ici, corriger une quantité l'écrasait : la valeur lue sur la pièce
-- disparaissait, et plus rien ne permettait de dire si « 4 » venait du prestataire ou d'un
-- utilisateur. Une facture n'est plus opposable quand on ne sait plus distinguer ce qu'elle
-- portait de ce qu'on en a fait.
--
-- CE QUE CETTE MIGRATION AJOUTE — quatre photographies de l'extraction, jamais réécrites ensuite :
--   · detail.quantite_source / prix_unitaire_source
--   · lignes.montant_ttc_source
--   · lignes.logement_id_source       (le logement que le rapprochement avait proposé)
-- et deux colonnes de traçabilité du rapprochement lui-même :
--   · lignes.logement_confiance       CERTAIN | PROBABLE | AUCUN | CONFIRME_MANUELLEMENT
--   · lignes.logement_methode         MAPPING_REFERENTIEL | NOM_OFFICIEL | ADRESSE | TOKENS | …
-- plus, pour §27 (répartition d'une ligne sur plusieurs logements) :
--   · lignes.ligne_parente_id_opaque  la ligne du document dont celle-ci est une part
--
-- LE BACKFILL N'INVENTE RIEN
-- Les 11 lignes existantes n'ont jamais été corrigées : leur valeur actuelle EST la valeur
-- extraite. La recopier dans les colonnes `_source` est donc une constatation, pas une hypothèse.
-- `logement_confiance` reste NULL pour elles : on ne sait pas par quelle voie le dictionnaire codé
-- en dur les avait rapprochées, et prétendre le contraire serait une invention.

ALTER TABLE facture_lignes_menage_detail ADD COLUMN quantite_source INTEGER;
ALTER TABLE facture_lignes_menage_detail ADD COLUMN prix_unitaire_source REAL;
ALTER TABLE facture_lignes_menage ADD COLUMN montant_ttc_source REAL;
ALTER TABLE facture_lignes_menage ADD COLUMN logement_id_source TEXT;
ALTER TABLE facture_lignes_menage ADD COLUMN logement_confiance TEXT;
ALTER TABLE facture_lignes_menage ADD COLUMN logement_methode TEXT;
ALTER TABLE facture_lignes_menage ADD COLUMN ligne_parente_id_opaque TEXT;

UPDATE facture_lignes_menage_detail
   SET quantite_source = quantite, prix_unitaire_source = prix_unitaire
 WHERE quantite_source IS NULL AND prix_unitaire_source IS NULL;

UPDATE facture_lignes_menage
   SET montant_ttc_source = montant_ttc, logement_id_source = logement_id
 WHERE montant_ttc_source IS NULL AND source = 'PDF_EXTRACTION';

CREATE INDEX IF NOT EXISTS idx_flm_ligne_parente
    ON facture_lignes_menage (ligne_parente_id_opaque);

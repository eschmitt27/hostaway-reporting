-- 0081 — corriger une ligne de facture fournisseur SANS jamais effacer la donnée brute.
--
-- LE BESOIN (recette utilisateur n°3, §29/§30)
-- Les deux factures PDF réelles ne sont pas validables : la somme de leurs lignes ne reconstitue
-- pas le total du document (-89,00 € et +36,00 €). C'est le comportement attendu — une facture
-- dont les lignes ne bouclent pas n'est pas contrôlée. Mais l'utilisateur n'avait AUCUN moyen
-- tracé de résoudre l'écart : ni déclarer la ligne que le parseur a omise, ni signaler qu'une
-- ligne a été mal extraite. Le seul geste disponible aurait été de modifier le total du document
-- ou d'inventer une ligne — c'est-à-dire de mentir sur la pièce reçue.
--
-- CE QUE CETTE MIGRATION AJOUTE
--   · `statut_ligne`     : ACTIVE (défaut) ou EXTRACTION_INCORRECTE. Une ligne mal extraite est
--                          NEUTRALISÉE, jamais supprimée : elle reste visible, avec son libellé
--                          d'origine, et cesse simplement de compter dans le total des lignes.
--   · `motif_correction` : motif OBLIGATOIRE de l'ajout manuel ou de la neutralisation. Sans
--                          motif, aucune correction n'est enregistrée.
--
-- Le couple (`source`, `statut_ligne`) distingue les trois natures de ligne :
--   PDF_EXTRACTION + ACTIVE                → ligne du document, libellé et quantité IMMUABLES
--   PDF_EXTRACTION + EXTRACTION_INCORRECTE → ligne du document écartée, motif obligatoire
--   SAISIE_MANUELLE_CORRECTIVE + ACTIVE    → ligne manquante ajoutée à la main, motif obligatoire
--
-- Aucune donnée existante n'est modifiée : les 11 lignes déjà extraites restent ACTIVE, sans
-- motif, ce qui est exactement leur état — elles viennent du document, personne n'y a touché.

ALTER TABLE facture_lignes_menage ADD COLUMN statut_ligne TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE facture_lignes_menage ADD COLUMN motif_correction TEXT;

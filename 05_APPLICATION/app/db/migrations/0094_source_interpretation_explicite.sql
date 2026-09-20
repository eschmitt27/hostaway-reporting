-- Toute facture issue d'un PDF dit explicitement d'où viennent ses lignes.
--
-- Les factures importées AVANT la migration 0093 n'avaient pas de `source_interpretation` : elles
-- s'affichaient « non posée » alors que leurs lignes viennent bel et bien du parseur PDF. Le
-- champ est donc normalisé à `PDF` pour elles — c'est une mise au net de l'existant, pas un
-- changement de lecture : ni les lignes, ni les montants, ni les statuts, ni les rapprochements
-- ne sont touchés.
--
-- IDEMPOTENT par construction : seules les valeurs ABSENTES sont posées. Rejouer cette migration
-- (ou la relancer sur une base déjà à jour) ne modifie plus rien.
--
-- Une facture SAISIE À LA MAIN n'est volontairement pas concernée : elle n'a ni PDF ni MD, donc
-- aucune « interprétation de document » à déclarer. Lui coller `PDF` affirmerait une provenance
-- qu'elle n'a pas.
UPDATE factures
   SET source_interpretation = 'PDF'
 WHERE source_interpretation IS NULL
   AND source = 'PDF_EXTRACTION';

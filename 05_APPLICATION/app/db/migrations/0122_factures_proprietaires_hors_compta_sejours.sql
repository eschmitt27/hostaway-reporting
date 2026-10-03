-- Migration 0122 — Factures propriétaires : émission HORS COMPTA, séjours éditables (2026-10-03).
--
-- HORS COMPTA
--   Une facture peut être émise (numéro, PDF, envoi par l'utilisateur) SANS constater de vente :
--   aucune écriture VENTES, aucune créance, aucun règlement attendu. Elle reste conservée, avec
--   la mention dans son statut. Par défaut : en comptabilité (0).
--
-- SÉJOURS ÉDITABLES
--   L'instantané des séjours d'un BROUILLON porte désormais, par séjour, l'assiette, le taux, le
--   ménage facturé et la commission. Ménage et commission sont modifiables (ex. ménage offert) ;
--   les lignes « Ménage » et « Commission » de la facture en sont la somme. Les valeurs d'origine
--   (calcul Lot10) sont conservées à côté, pour que toute modification reste visible.
--
-- Fichier appliqué une seule fois (suivi par version dans `schema_migrations`).

ALTER TABLE factures_proprietaires ADD COLUMN hors_compta INTEGER NOT NULL DEFAULT 0;
ALTER TABLE factures_proprietaires ADD COLUMN motif_hors_compta TEXT;

ALTER TABLE factures_proprietaires_reservations ADD COLUMN assiette REAL;
ALTER TABLE factures_proprietaires_reservations ADD COLUMN taux REAL;
ALTER TABLE factures_proprietaires_reservations ADD COLUMN menage REAL;
ALTER TABLE factures_proprietaires_reservations ADD COLUMN commission REAL;
ALTER TABLE factures_proprietaires_reservations ADD COLUMN menage_initial REAL;
ALTER TABLE factures_proprietaires_reservations ADD COLUMN commission_initiale REAL;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0122');

-- Migration 0039 — quantité par ligne de facture ménage (extension 1-1 de facture_lignes_menage).

-- Additive comme 0017→0038. Aucun ALTER TABLE : `facture_lignes_menage` (0037) est déjà en usage,
-- et les migrations sont rejouées à chaque démarrage — une table compagnon plutôt qu'une colonne
-- ajoutée après coup (même raison que `fournisseur_menage_qualification`, 0019).
--
-- POURQUOI CETTE DONNÉE MANQUE
-- Lot6d compte des MÉNAGES (nb visites), pas des euros. `facture_lignes_menage.montant_ttc` porte
-- le montant, jamais la quantité — l'extracteur PDF (`lib_menages_externes_pdf.LigneFacture`) la
-- connaît (`quantite`), mais 0037 ne la persistait pas. Sans elle, un comptage Lot6d perdrait la
-- distinction entre « 1 ménage à 90€ » et « 3 ménages à 30€ ».
CREATE TABLE IF NOT EXISTS facture_lignes_menage_detail (
    ligne_id_opaque TEXT PRIMARY KEY,     -- facture_lignes_menage.ligne_id_opaque
    quantite        INTEGER,
    prix_unitaire   REAL
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0039');

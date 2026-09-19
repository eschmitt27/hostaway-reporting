-- Le numéro IMPRIMÉ par le fournisseur n'est pas une clé : il lui arrive de le réutiliser.
--
-- Cas réel : « 2026-37 » a d'abord servi à une petite facture (30 avril, 15 €), puis le
-- prestataire a repris sa numérotation le mois suivant sans en tenir compte (31 mai, 1 439 €).
-- Deux documents réellement distincts portent donc le même numéro.
--
-- Deux références, deux rôles :
--   · `facture_ref_source` = le numéro RÉELLEMENT IMPRIMÉ sur la pièce. Jamais modifié, jamais
--     suffixé, jamais unique : c'est ce que dit le document, et le fournisseur en est seul auteur.
--   · `facture_ref`        = la référence INTERNE, celle que l'application manipule. Elle vaut le
--     numéro imprimé tant qu'il ne désigne qu'une facture ; en cas de collision réelle, elle est
--     désambiguïsée (« 2026-37-A », « 2026-37-B »). Elle, reste unique par fournisseur.
--
-- L'index d'unicité redevient donc (fournisseur, référence INTERNE) : plus aucune contrainte ne
-- porte sur le numéro imprimé. `sha256_pdf` au diagnostic permet de reconnaître un doublon EXACT
-- (même fichier) et de ne créer alors qu'une seule facture.
ALTER TABLE factures ADD COLUMN facture_ref_source TEXT;
UPDATE factures SET facture_ref_source = facture_ref WHERE facture_ref_source IS NULL;

DROP INDEX IF EXISTS idx_factures_fournisseur_ref_mois;
CREATE UNIQUE INDEX IF NOT EXISTS idx_factures_fournisseur_ref
    ON factures(fournisseur_id_opaque, facture_ref) WHERE statut <> 'ANNULEE';
CREATE INDEX IF NOT EXISTS idx_factures_ref_source
    ON factures(fournisseur_id_opaque, facture_ref_source);

ALTER TABLE facture_pdf_diagnostics ADD COLUMN sha256_pdf TEXT;

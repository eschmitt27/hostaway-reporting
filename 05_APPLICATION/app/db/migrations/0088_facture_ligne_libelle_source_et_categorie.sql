-- 0088 — ce que le document ÉCRIT, et ce que la prestation EST (recette utilisateur n°4, §8-§13).
--
-- LE BESOIN
-- 1. LIBELLÉ SOURCE. `description` porte aujourd'hui une forme déjà nettoyée du libellé : le
--    montant, les « x N passages », l'adresse entre parenthèses et les dates en ont été retirés
--    pour servir le rapprochement du logement. C'est utile, mais ce n'est PAS ce que dit la pièce.
--    En base, une ligne portait « studio 76 (Dureuil) » là où la facture écrit « 1.service de
--    nettoyage studio 76 (Dureuil) (76 allée de Barcelone) le 10 mai 2026 ». L'utilisateur qui
--    contrôle une facture doit pouvoir lire la ligne telle qu'elle a été reçue.
--    `libelle_source` est donc la reproduction du document, immuable ; `description` devient le
--    libellé MÉTIER, corrigeable au contrôle humain — sans jamais altérer la source.
-- 2. NATURE DE LA PRESTATION. `type_ligne` (MENAGE_INTERNE | MENAGE_EXTERNE | FRAIS_NON_AFFECTE |
--    AUTRE) ne dit pas ce qui a été fait : il dit si un logement a été reconnu. Rien ne
--    distinguait donc un ménage d'une remise en état ou d'un achat de consommables.
--    Le vocabulaire existait déjà, inutilisé : `ref_types_lignes_menage` (TLM_001..TLM_006) porte
--    MENAGE_STANDARD, REMISE_EN_ETAT, FRAIS_DEPLACEMENT, LINGE, ACHAT_PRODUIT, AUTRE, avec les
--    drapeaux compte_comme_menage / repartissable_sur_menages / impact_cout_menage. On le
--    RATTACHE plutôt que d'inventer une seconde nomenclature qui en aurait divergé au premier
--    changement de référentiel.
--    C'est `compte_comme_menage` qui tranche ensuite la question métier : une REMISE EN ÉTAT
--    compte pour un ménage (au coût réel de la ligne), un FRAIS DE DÉPLACEMENT non.
--
-- CE QUE CETTE MIGRATION N'AJOUTE PAS
-- Aucun backfill des lignes déjà importées : leur libellé source n'a pas été conservé à l'époque,
-- et le reconstituer serait l'inventer. Elles gardent `libelle_source` NULL, ce qui se lit
-- exactement comme ce que c'est — une donnée que nous n'avons pas — et leur catégorie reste à
-- classer par un humain ou par un réimport du PDF.
--
-- CONFIANCE
-- `type_ligne_menage_confiance` vaut CERTAIN | PROBABLE | AUCUN | CONFIRME_MANUELLEMENT, dans le
-- même esprit que `logement_confiance` (migration 0082) : AUCUN signifie « le logiciel n'a pas
-- compris ce libellé », pas « c'est une autre prestation ». La différence est ce qui empêche une
-- facture d'être validée sur un classement que personne n'a fait.

ALTER TABLE facture_lignes_menage ADD COLUMN libelle_source TEXT;
ALTER TABLE facture_lignes_menage ADD COLUMN type_ligne_menage_id TEXT
    REFERENCES ref_types_lignes_menage(type_ligne_menage_id);
ALTER TABLE facture_lignes_menage ADD COLUMN type_ligne_menage_confiance TEXT;

CREATE INDEX IF NOT EXISTS idx_flm_type_ligne_menage
    ON facture_lignes_menage(type_ligne_menage_id);

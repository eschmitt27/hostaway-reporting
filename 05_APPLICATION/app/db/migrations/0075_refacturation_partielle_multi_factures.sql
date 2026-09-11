-- Migration 0075 — autoriser la refacturation PARTIELLE d'une charge sur PLUSIEURS factures.
--
-- CE QUI ÉTAIT TROP STRICT
-- La migration 0072 posait `idx_fprlc_charge_unique UNIQUE (charge_id)` pour empêcher qu'une même
-- charge soit refacturée deux fois. L'intention était juste, la contrainte trop large : elle
-- interdisait aussi le cas métier légitime d'une dépense de 700 € récupérée 350 € sur la facture
-- d'un propriétaire et 350 € sur celle d'un autre — exactement ce que permet une charge commune à
-- deux logements. Une garde ne doit pas interdire le cas normal pour empêcher l'anormal.
--
-- CE QUI GARDE L'INVARIANT À LA PLACE
-- L'anti-double-facturation ne repose plus sur « une charge = une ligne » mais sur le MONTANT :
--   somme des imputations validées + somme des lignes de BROUILLON  <=  montant éligible
-- La première moitié vit dans `charges_refacturation_positions.montant_impute_total` (écrite par
-- `imputer()`, à la validation) ; la seconde est dérivée des lignes de brouillon en cours, ce qui
-- « réserve » un montant sans l'imputer et le libère dès qu'on détache la ligne ou qu'on annule la
-- facture. Voir `charges_refacturation_service.montant_disponible()`.
--
-- L'index non unique sur `charge_id` (0071) est conservé : la recherche par charge reste utile.

DROP INDEX IF EXISTS idx_fprlc_charge_unique;

-- Une même position ne peut pas être portée DEUX FOIS par la MÊME facture : ce serait un
-- double-clic, jamais une intention. Deux factures différentes restent libres de s'en partager le
-- montant. L'index porte sur la ligne de facture, qui référence la position (contrat 0072).
CREATE UNIQUE INDEX IF NOT EXISTS idx_fprl_position_par_facture
    ON factures_proprietaires_lignes(facture_id_opaque, objet_source_ref)
    WHERE type_ligne = 'CHARGE_REFACTUREE' AND objet_source_ref IS NOT NULL;

INSERT OR IGNORE INTO schema_migrations (version) VALUES ('0075');

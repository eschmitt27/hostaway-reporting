-- 0085 — une prestation ponctuelle doit pouvoir être facturée (§79).
--
-- LE MANQUE. Toute facture propriétaire naissait d'une source Lot12 : un calcul mensuel, par
-- propriétaire et par logement. Une intervention d'urgence, un service rendu une fois, n'ont
-- aucune source de ce genre — et n'avaient donc AUCUN chemin. Il fallait attendre le cycle suivant
-- et l'y glisser, ou renoncer à facturer.
--
-- CE QUE CETTE MIGRATION CHANGE, ET CE QU'ELLE NE CHANGE PAS
-- Elle ne touche ni au schéma des colonnes, ni aux données : elle redéfinit UN INDEX.
--
-- `idx_fpr_grain` interdisait deux documents de même type pour un même (mois, propriétaire,
-- logement). Son rôle est d'empêcher qu'un second passage du cycle mensuel fabrique un doublon.
-- Mais il interdisait aussi, par effet de bord, toute facture ponctuelle sur un mois déjà facturé
-- — et même DEUX interventions d'urgence dans le même mois, ce qui est pourtant le cas normal.
--
-- L'index ne garde donc plus que les factures ISSUES DU CYCLE (`source_calcul` différent de
-- `SAISIE_EXCEPTIONNELLE`). L'anti-doublon mensuel est intact ; la saisie ponctuelle est libre.
--
-- Une facture exceptionnelle reste `type_document = 'FACTURE'` : fiscalement rien ne l'en
-- distingue. Ce qui la distingue tient dans `source_calcul`, qui dit d'où elle vient.

DROP INDEX IF EXISTS idx_fpr_grain;

CREATE UNIQUE INDEX idx_fpr_grain
    ON factures_proprietaires(mois, proprietaire_id, logement_id, type_document)
 WHERE statut <> 'ANNULE'
   AND COALESCE(source_calcul, '') <> 'SAISIE_EXCEPTIONNELLE';

CREATE INDEX IF NOT EXISTS idx_fpr_source_calcul
    ON factures_proprietaires(source_calcul);

-- 0079 — l'exclusion d'une réservation devient explicite ; le code `HR` disparaît.
--
-- POURQUOI
-- `code_impact = 'HR'` disait « ni résultat réel, ni comptabilité ». Mais un code d'impact décrit
-- COMMENT une somme pèse sur l'économie ; il ne sait pas dire qu'une ligne est HORS de cette
-- économie. Les 125 réservations concernées ne sont pas « neutres » : ce sont des séjours du
-- propriétaire dans son propre logement — une occupation réelle, sans voyageur, sans encaissement
-- et sans commission. Elles n'ont pas un impact neutre, elles n'ont pas d'impact.
--
-- L'EXCLUSION EXISTAIT DÉJÀ, `HR` LA DOUBLAIT
-- `statut_controle` porte `EXCLU_RESULTAT` depuis l'origine (vocabulaire partagé par lot4bis,
-- lot5, lot7 et la saisie HH), et `lot4bis` forçait déjà l'impact à NON/NON dès que ce statut était
-- posé — quel que soit le code. Le filtre économique est d'ailleurs TRIPLE et redondant :
--     statut_controle = 'VALIDE' AND impact_resultat_reel = 'OUI' AND montant_retenu <> 0
-- Les 125 lignes échouent aux trois. Retirer `HR` ne peut donc RIEN changer au résultat : c'est
-- l'objet de la comparaison avant/après exigée par la mission.
--
-- CE QUI MANQUAIT : LE MOTIF
-- La raison de l'exclusion vivait dans `commentaire`, en texte libre
-- (« Hostaway ownerStay — exclu résultat »). Non requêtable, donc invérifiable. `motif_exclusion`
-- la rend interrogeable : on peut désormais compter les séjours propriétaire d'un mois sans
-- chercher une sous-chaîne.
--
-- CE QUI N'EST PAS TOUCHÉ
-- Aucun montant, aucun statut, aucune date. `impact_resultat_reel` / `impact_resultat_comptable`
-- restent à 'NON' : ils étaient déjà justes, et les filtres aval les lisent.

ALTER TABLE reservations_calculees ADD COLUMN motif_exclusion TEXT;
ALTER TABLE reservations_resolues  ADD COLUMN motif_exclusion TEXT;

-- Motif déduit de ce que les lignes portent DÉJÀ (`source`, `code_anomalie`) — jamais inventé.
-- L'ordre des UPDATE compte : le plus spécifique d'abord, `OWNERSTAY` couvre le reste.
UPDATE reservations_calculees SET motif_exclusion = 'HORS_PARC_TECHNIQUE'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL
   AND (source = 'HORS_PARC_TECHNIQUE' OR code_anomalie = 'HORS_PARC_TECHNIQUE');
UPDATE reservations_calculees SET motif_exclusion = 'STATUT_HOSTAWAY_HORS_PERIMETRE'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL
   AND (source = 'STATUT_HOSTAWAY_HORS_PERIMETRE'
        OR code_anomalie = 'STATUT_HOSTAWAY_HORS_PERIMETRE');
UPDATE reservations_calculees SET motif_exclusion = 'OWNERSTAY'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL
   AND source LIKE 'OWNERSTAY%';
UPDATE reservations_calculees SET motif_exclusion = 'LEGACY_SANS_ARCHIVE_ORIGINE'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL;

UPDATE reservations_resolues SET motif_exclusion = 'HORS_PARC_TECHNIQUE'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL
   AND (source = 'HORS_PARC_TECHNIQUE' OR code_anomalie = 'HORS_PARC_TECHNIQUE');
UPDATE reservations_resolues SET motif_exclusion = 'STATUT_HOSTAWAY_HORS_PERIMETRE'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL
   AND (source = 'STATUT_HOSTAWAY_HORS_PERIMETRE'
        OR code_anomalie = 'STATUT_HOSTAWAY_HORS_PERIMETRE');
UPDATE reservations_resolues SET motif_exclusion = 'OWNERSTAY'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL
   AND source LIKE 'OWNERSTAY%';
UPDATE reservations_resolues SET motif_exclusion = 'LEGACY_SANS_ARCHIVE_ORIGINE'
 WHERE statut_controle IN ('EXCLU_RESULTAT','EXCLU_LEGACY') AND motif_exclusion IS NULL;

-- `HR` n'est plus une valeur. NULL se lit « aucun impact », et non « impact neutre ».
UPDATE reservations_calculees SET code_impact = NULL WHERE code_impact = 'HR';
UPDATE reservations_resolues  SET code_impact = NULL WHERE code_impact = 'HR';

-- TYPE_FLUX_005 (« remboursement d'une dépense personnelle payée par le compte pro ») portait
-- `HR` comme impact par défaut. Ce type n'est utilisé par AUCUNE ligne (0 charge, 0 flux) : c'est
-- un défaut dormant. Sa neutralisation n'a jamais dépendu de ce code — elle vient de
-- `sens_flux = REMBOURSEMENT`, que lot3 traduit en `sens = NEUTRALISATION`. Le défaut passe donc à
-- NULL : retirer une valeur devenue invalide sans lui en inventer une autre (`IC` ou `HC`
-- changerait ce que fait un futur remboursement — un arbitrage métier, pas une migration).
UPDATE ref_types_flux SET code_impact_defaut = NULL
 WHERE type_flux_id = 'TYPE_FLUX_005' AND code_impact_defaut = 'HR';

-- Plus aucune donnée ne référence `HR` : la ligne de référentiel peut partir. C'est la dernière
-- trace de ce code comme valeur métier active.
DELETE FROM ref_codes_impact WHERE code_impact = 'HR';

CREATE INDEX IF NOT EXISTS idx_reservations_resolues_motif_exclusion
    ON reservations_resolues (motif_exclusion);

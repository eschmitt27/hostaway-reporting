-- 0080 — un seul mot pour « charge validée » : VALIDE.
--
-- LE DÉFAUT
-- Deux parcours écrivaient la MÊME colonne `charges.statut_controle` avec DEUX mots différents :
--
--   · « Charges à contrôler » (`charges_validation_service.valider`)  → `VALIDE`
--   · « Valider la charge » sur la fiche (`charges_saisie_service.valider_controle`) → `CONFORME`
--
-- Or la chaîne économique ne connaît que `VALIDE` : lot9 n'ingère que celui-là, et les 336 lignes
-- de `flux_unifies` le portent. Une charge validée depuis la FICHE devenait donc `CONFORME`, et
-- restait invisible au résultat — l'écran affichait « conforme » pendant que la dépense ne pesait
-- nulle part. Aucun message, aucune anomalie : la charge était simplement absente.
--
-- CE QUI EST FAIT
-- `CONFORME` devient `VALIDE`. Ce n'est pas une conversion de sens : les deux mots désignaient
-- exactement le même acte — un humain a examiné la charge et l'a acceptée. Le mot retenu est celui
-- que la chaîne lit déjà, pour que le geste de validation ait enfin l'effet qu'il annonce.
--
-- PORTÉE RÉELLE SUR LA BASE DE RECETTE : 0 ligne (les 5 charges sont `A_CONTROLER` ou NULL). La
-- migration existe pour les bases qui en porteraient, et pour que le vocabulaire soit fixé une
-- fois pour toutes.
--
-- `ANOMALIE` n'est PAS touché : c'est un état distinct et légitime — contrôlée, et refusée. Il
-- reste exclu des calculs, au même titre que `A_CONTROLER`.

UPDATE charges SET statut_controle = 'VALIDE' WHERE statut_controle = 'CONFORME';

UPDATE charge_evenements
   SET apres_json = REPLACE(apres_json, '"statut_controle": "CONFORME"',
                                        '"statut_controle": "VALIDE"')
 WHERE apres_json LIKE '%"statut_controle": "CONFORME"%';

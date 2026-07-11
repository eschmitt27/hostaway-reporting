# Plan nettoyage par lots

| lot | action | fichiers touch?s | contr?le avant action | backup n?cessaire | risque | test apr?s action | condition de validation humaine |
|---|---|---|---|---|---|---|---|
| LOT A | Identifier backups, PDF simulations, logs anciens et sorties g?n?r?es suivies | 99_ARCHIVES, 04_LOGS, 02_TRAVAIL/Lot*_*.xlsx | rg r?f?rences + validation humaine | Oui | Perte de preuve audit ou sortie non reproductible | git status + comparaison fichiers + test lot concern? | Validation explicite op?rateur |
| LOT A | D?placer/archiver les PDF de simulation hors p?rim?tre Git si confirm? | 04_LOGS/TEST_FACTURES_*/*.pdf | V?rifier aucune facture finale l?gale | Oui | Confusion simulation/facture | liste PDF avant/apr?s | Validation humaine |
| LOT B | Clarifier source officielle cl?ture mensuelle | REF_Setup.xlsm vs BANQUE_LOT8_IMPORT.xlsx | Rechercher lecteurs/?crivains | Oui | Deux statuts de cl?ture concurrents | test Lot11/Lot12 cibl? | D?cision m?tier |
| LOT B | D?classer champs legacy propri?taires/taux si historique complet | REF_Proprietaires.taux_commission, REF_Logements.proprietaire_id | V?rifier tous scripts migr?s | Oui | Recalcul r?troactif faux | tests ref_history + Lot10/11 | Validation m?tier |
| LOT C | Qualifier colonnes non consomm?es par scan | classeurs actifs | rg scripts/tests/docs/PowerBI + formules Excel | Oui | Suppression colonne utilis?e par Excel/PowerQuery | ouvrir/inspecter classeur + test cibl? | Validation fichier par fichier |
| LOT C | S?parer saisies vs colonnes calcul?es | SAISIE_* et MASTER_* | V?rifier producteurs/consommateurs | Oui | Double saisie ou ?crasement calcul | test lot concern? | Validation op?rateur |
| LOT D | Harmoniser mise en forme des saisies actives | 01_SOURCES_BRUTES/*.xlsx | Audit formats/validations/tables | Oui | Perte validation Excel | comparaison workbook structure | Validation visuelle |
| LOT E | Contr?le global apr?s nettoyage | tout projet | git diff, rg, tests cibl?s, rejeu isol? si autoris? | Oui | R?gression invisible | tests + manifeste sorties | Validation finale |

## Contr?les obligatoires avant toute suppression/fusion/renommage

- recherche de r?f?rences dans les scripts;
- recherche dans les tests;
- recherche dans la documentation;
- recherche dans les exports Power BI;
- backup;
- comparaison avant/apr?s;
- test cibl?;
- validation humaine avant suppression d?finitive.
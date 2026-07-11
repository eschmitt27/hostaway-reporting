# 12 - Codes techniques hors parc

Date audit : 2026-06-28 22:55:39

Aucune modification, suppression, correction de donn?es, commit, merge ou push. Rapport uniquement.

## R?gle m?tier valid?e

`APPARTEMENT_DIVERS` et `LOGEMENT_DIVERS` ne sont pas des logements du parc g?r?. Ils ne doivent jamais recevoir de propri?taire, ni produire commission, net propri?taire, facture/pr?facture, m?nage attendu, co?t m?nage, flux propri?taire ou r?sultat par logement.

## APPARTEMENT_DIVERS

### Ligne REF_Logements

- nom : Appartement divers (hors parc)
- actif : OUI
- sur_hostaway : NON
- type_logement_id : None
- dates gestion : entr?e=(vide), sortie=(vide)
- commentaire : Code technique hors parc - jamais utilise pour masquer un mauvais mapping

### Usages texte

- `02_TRAVAIL/lot10_calculer_resultats.py` lignes 555
- `00_CADRAGE/CLAUDE.md` lignes 115
- `00_CADRAGE/DECISIONS_METIER.md` lignes 697
- `00_CADRAGE/ETAT_AVANCEMENT.md` lignes 60
- `00_CADRAGE/JOURNAL_ANOMALIES.md` lignes 188, 191
- `00_CADRAGE/JOURNAL_CONTROLES.md` lignes 570, 597, 619
- `00_CADRAGE/PLAN_CONSTRUCTION.md` lignes 54, 107
- `00_CADRAGE/REGLES_METIER.md` lignes 56, 191

### Usages Excel / Power Query / Power BI

- `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` / `REF_LOCALE` / `I18` / donn?e : `APPARTEMENT_DIVERS`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` / `REF_Logements` / `A19` / r?f?rentiel logement : `APPARTEMENT_DIVERS`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` / `REF_Logements` / `D19` / r?f?rentiel logement : `APPARTEMENT_DIVERS`
- `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx` / `REF_LOCALE` / `C18` / donn?e : `APPARTEMENT_DIVERS`

### Flux historiques r?els d?tect?s

- Aucun flux/r?servation/r?sultat/pr?facture/m?nage local d?tect? avec ce code dans les sorties analys?es.

### N?cessit? technique apparente

- R?f?rence directe dans script actif : oui, conserver tant que la logique existe.

## LOGEMENT_DIVERS

### Ligne REF_Logements

- nom : Logement divers (hors parc)
- actif : OUI
- sur_hostaway : NON
- type_logement_id : None
- dates gestion : entr?e=(vide), sortie=(vide)
- commentaire : Code technique hors parc - jamais utilise pour masquer un mauvais mapping

### Usages texte

- `02_TRAVAIL/lot10_calculer_resultats.py` lignes 555
- `00_CADRAGE/CLAUDE.md` lignes 115
- `00_CADRAGE/ETAT_AVANCEMENT.md` lignes 60
- `00_CADRAGE/JOURNAL_ANOMALIES.md` lignes 188
- `00_CADRAGE/JOURNAL_CONTROLES.md` lignes 570, 597, 619
- `00_CADRAGE/PLAN_CONSTRUCTION.md` lignes 54, 107
- `00_CADRAGE/REGLES_METIER.md` lignes 56, 191

### Usages Excel / Power Query / Power BI

- `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` / `REF_LOCALE` / `I19` / donn?e : `LOGEMENT_DIVERS`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` / `REF_Logements` / `A20` / r?f?rentiel logement : `LOGEMENT_DIVERS`
- `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` / `REF_Logements` / `D20` / r?f?rentiel logement : `LOGEMENT_DIVERS`
- `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx` / `REF_LOCALE` / `C19` / donn?e : `LOGEMENT_DIVERS`

### Flux historiques r?els d?tect?s

- Aucun flux/r?servation/r?sultat/pr?facture/m?nage local d?tect? avec ce code dans les sorties analys?es.

### N?cessit? technique apparente

- R?f?rence directe dans script actif : oui, conserver tant que la logique existe.

## Recommandation de mod?lisation

Recommandation : conserver provisoirement dans `REF_Logements` avec un statut explicite `HORS_PARC_TECHNIQUE`, puis d?placer plus tard vers un r?f?rentiel technique distinct si le mod?le le permet. ? court terme, passer simplement `actif=NON` est coh?rent avec la r?gle m?tier mais n?cessite de v?rifier que les contr?les de mauvais mapping ne s?appuient pas sur `actif=OUI`.

Comparaison options :

- Conserver avec `actif=NON` : r?duit le risque d?int?gration op?rationnelle ; impact probable faible car aucun flux r?el d?tect?, mais contr?ler les r?gles de validation qui filtrent seulement les logements actifs.
- Conserver avec statut explicite `HORS_PARC_TECHNIQUE` : meilleure lisibilit? m?tier, mais n?cessite une colonne/statut d?di? ou une convention document?e.
- D?placer vers r?f?rentiel technique distinct : mod?le le plus propre ? terme, mais n?cessite migration code/Excel/tests et ne doit pas ?tre fait dans ce lot.

## Impact si actif=NON

- Aucun flux historique local identifi? avec ces codes.
- Risque principal : une saisie ou un contr?le de mauvais mapping pourrait ne plus retrouver ces codes si elle exige `actif=OUI`.
- Effet attendu souhaitable : emp?cher qu?ils soient trait?s comme logements op?rationnels.

## Pr?requis exacts avant suppression de date_entree_gestion / date_sortie_gestion

1. Arbitrer la mod?lisation des deux codes hors parc (`actif=NON`, statut technique ou r?f?rentiel s?par?).
2. Confirmer qu?aucun script ne peut les utiliser comme fallback logement g?r?.
3. Adapter les scripts consommateurs des dates REF_Logements vers `REF_Gestion_Logements_Hist`.
4. Adapter tables/formules/Power Query/validations/noms d?finis Excel qui lisent les dates.
5. Ajouter tests : exclusion des codes hors parc, absence de flux propri?taire/facture/m?nage, et r?solution historique uniquement.
6. Sauvegarder `REF_Setup.xlsm`, v?rifier `keep_vba=True`, `xl/vbaProject.bin`, absence `#REF!` et scan r?f?rences.

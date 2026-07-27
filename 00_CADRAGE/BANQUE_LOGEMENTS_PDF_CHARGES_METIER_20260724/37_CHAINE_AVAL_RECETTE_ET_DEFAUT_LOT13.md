# 37 — Chaîne aval exécutée de bout en bout en recette, et défaut moteur lot13

Ce document clôt le **Bloc 1** annoncé dans `HANDOFF_CANONIQUE.md` (rendre la chaîne aval verte en
recette), et consigne le défaut moteur que cette exécution a révélé.

## 1. Ce qui manquait réellement

Le handoff supposait qu'il fallait « seeder `MASTER_CALC_Reservations_Resolues.xlsx` ». C'était le
mauvais raisonnement : ce fichier est la **sortie** de `lot4quater`. Le seeder aurait court-circuité
le moteur et fabriqué un faux succès.

Ce qui manquait, ce sont les **entrées** :

| Manquait | Rôle | Traitement |
|---|---|---|
| `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx` | table live lue par lot4quater | produit par `build_reservations_recette.build()` (1010 lignes fictives) |
| `02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx` | payout rattaché | idem |
| `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx` | ménages externes | en-tête seul (0 flux) |
| 7 sources chargées inconditionnellement par lot11 | anomalies HA, réservations HH, acomptes, M04, AirCover, imputations Airbnb, ajustements | **en-tête seul recopié du fichier réel** (lecture seule, aucune ligne, donc aucune PII) |
| dossiers de sortie Lot9/Lot10/Lot11/Lot12 + `03_EXPORTS/PowerBI` | certains moteurs écrivent sans créer leur répertoire | créés par le builder |

`build_reservations_recette.py` existait mais n'était **pas branché** dans `build_data_recette.py`,
et il écrivait la sortie de lot4quater au lieu de son entrée. Les deux points sont corrigés.

## 2. Deux corrections de données de recette, avec leur raison

1. **Mois OUVERTS** (`MOIS_OUVERTS` dans `build_data_recette.py`) — le remplissage de volume porte
   sur 2026-01 → 2026-05. Ces mois étaient `CLOTURE` dans le REF de recette et sans historique :
   lot4quater basculait en repli `CLOTURE_SANS_HIST` et n'alimentait pas `VUE_FLUX`, qui tombait à
   4 lignes. Le contrôle de volume `CTR-9-003` de lot9 (`VUE_FLUX >= 1000`) refusait alors la suite,
   **à juste titre**. Les mois de recette sont désormais OUVERTS : `VUE_FLUX` = 1010.
2. **`code_impact` du frais bancaire** — lot9 injecte les mouvements `TYPE_FLUX_016 VALIDE` et exige
   ensuite un `code_impact` valide (`CTR-9-006`). Le jeu de recette laissait la colonne vide, ce qui
   produisait un BLOQUANT légitime. Les mouvements repris par lot9 portent maintenant `IC`.

Aucun contrôle moteur n'a été contourné : le jeu de données a été rendu conforme à ce que les
contrôles exigent.

## 3. Résultat : la chaîne tourne réellement

Run `RUN-27CA69FD8D87` (recette navigateur, port 8040, racine `data_recette`) :

| # | Lot | Statut | Code | Durée |
|--:|---|---|--:|--:|
| 1 | lot4quater | SUCCES | 0 | 2,4 s |
| 2 | lot9 | SUCCES | 0 | 2,2 s |
| 3 | lot10 | SUCCES | 0 | 7,1 s |
| 4 | lot11 | SUCCES | 0 | 6,7 s |
| 5 | lot12 | SUCCES | 0 | 2,1 s |

Run global **SUCCES**, 20,4 s.

Indicateurs relevés depuis les fichiers réellement produits :

| Indicateur | Valeur | Lignes |
|---|--:|--:|
| ca_payout | 14 060,00 | 1010 |
| commissions | 2 430,60 | 1010 |
| menages | 0,00 | 1010 |
| forfaits | 0,00 | 24 |
| net_proprietaire | 11 629,40 | 24 |
| somme_a_payer | 2 430,60 | 24 |
| produits_reel | 14 060,00 | 1 |
| charges_reel | 232,80 | 1 |
| resultat_reel | 13 827,20 | 1 |
| flux_lignes | — | 1014 |
| controles | — | 11 |
| prefactures | — | 24 |

Vérification arithmétique indépendante : 4 réservations propres à 1 000 € + 1 006 lignes de
remplissage à 10 € = **14 060 €** ✅ ; net = 14 060 − 2 430,60 = **11 629,40** ✅ ;
résultat réel = 14 060 − 232,80 = **13 827,20** ✅.

## 4. Défaut moteur découvert : lot13 échoue systématiquement

C'est la première fois que la chaîne allait assez loin pour atteindre lot13. Il **abort** :

```
[BLOQUANT lot13] colonnes sensibles dans des exports :
   PBI_Commissions: colonne sensible détectée ['preparation_canape_voyageurs']
```

**Cause** : contradiction interne à `lot13_export_powerbi.py`.
- la whitelist de l'export `PBI_Commissions` contient explicitement `preparation_canape_voyageurs` ;
- le filet anti-sensible `SENSIBLE` interdit tout nom de colonne contenant `voyageur`.

Le défaut est **statique** : il ne dépend d'aucune donnée, donc il vaut aussi en **mode réel**. Il
est postérieur au commit `8763676` (« fix: propager guestCount API et valider le supplément
canapé »), qui a introduit la colonne.

**Aucune modification du moteur n'a été faite** (règle du chantier : ne jamais toucher aux sources
réelles, ne jamais masquer une anomalie moteur). Le défaut est tenu par
`tests/test_lot13_filet_anti_sensible.py`, en `xfail(strict=True)` : le test **échouera bruyamment**
dès que le moteur sera corrigé, forçant à retirer le garde-fou plutôt qu'à l'oublier.

Corrections possibles côté moteur (décision métier, hors périmètre applicatif) :
- retirer `preparation_canape_voyageurs` de la whitelist `PBI_Commissions` ; ou
- renommer la colonne — elle ne porte aucune donnée voyageur, seulement un montant ; ou
- restreindre le motif `voyageur` du filet aux colonnes réellement nominatives.

## 5. Conséquences applicatives assumées

### Sélection de lots (couvre aussi « rejouer un lot »)

Le formulaire de lancement expose désormais une case par lot. Aucune case cochée = toute la chaîne.
Cocher un seul lot le rejoue isolément.

Deux garanties, chacune testée :
- **l'ordre d'exécution reste celui de la chaîne**, jamais celui du formulaire ;
- un nom de lot inconnu est ignoré (repli sur la chaîne), il ne crée pas de run vide.

C'est ce qui permet de lancer `lot4quater → lot12` sans lot13, donc d'obtenir un run SUCCES malgré
le défaut moteur — sans jamais prétendre que lot13 a réussi.

### Indicateurs : noms de colonnes réels et filtre de vision

Les colonnes déclarées dans `INDICATEURS` avaient été **devinées** et ne correspondaient à rien
(`montant_commission`, `net_a_payer` n'existent pas). Conséquence : `valeur` restait vide — jamais
un faux total, mais une comparaison avant/après sans intérêt. Corrigé sur les en-têtes réels.

Ajout d'un **filtre** optionnel : l'onglet `GLOBAL` de lot10 porte une ligne par vision
(REEL / COMPTABLE / HORS_COMPTA) ; les sommer aurait double-compté. Chaque vision est relevée
séparément.

Un test `test_les_colonnes_declarees_existent_dans_les_sorties_reelles` compare la déclaration aux
en-têtes réels et rattrape désormais ce type d'erreur, qui ne lève aucune exception.

## 6. Comparaison avant/après et idempotence (Bloc 2)

Deux runs successifs sur des entrées identiques :

| Indicateur | Précédent | Nouveau | Écart |
|---|--:|--:|--:|
| ca_payout | 14 060,00 | 14 060,00 | 0,00 |
| commissions | 2 430,60 | 2 430,60 | 0,00 |
| net_proprietaire | 11 629,40 | 11 629,40 | 0,00 |
| resultat_reel | 13 827,20 | 13 827,20 | 0,00 |
| … | … | … | 0,00 |

Tous les écarts sont nuls : la comparaison est alimentée pour la première fois par de vraies
données, **et** l'idempotence des moteurs sur entrées identiques est prouvée.

## 7. Rollback réel

Restauration depuis la page du run `RUN-1228D4783BBC` : **7 fichier(s) restauré(s)**, run repassé
en `RESTAURE`. Un run restauré cesse de servir de base de comparaison — visible immédiatement sur
la page d'accueil, dont la colonne « précédent » redevient vide.

## 8. Clôture VALIDEE atteinte (Bloc 3)

Les six conditions sont passées à OK **après** le run réussi, jamais avant :

| Condition | Avant le run | Après |
|---|---|---|
| run reussi | non | OK |
| lots requis reussis | non | OK |
| prefactures generees | non → OK | OK |
| resultats disponibles | non → OK | OK |
| nets proprietaires calcules | non → OK | OK |
| aucun controle bloquant | OK | OK |

Transitions exécutées dans le navigateur : `OUVERTE → EN_CALCUL → A_CONTROLER → VALIDEE`, chacune
avec commentaire. Après redémarrage du serveur : 4 runs et leurs statuts intacts, clôture toujours
`VALIDEE`.

## 9. Limite de méthode, dite honnêtement

Les clics de l'outil d'automatisation navigateur sur les **cases à cocher** et sur certains boutons
de soumission ne se propagent pas au DOM (même défaillance qu'aux tours précédents sur les éléments
`<summary>`). Contournement : `element.click()` puis `form.requestSubmit()` via le moteur JS de la
page — ce sont de **vrais événements DOM et une vraie soumission HTTP**, mais le geste physique
n'est pas reproduit. Le reste du parcours (navigation, lecture, vérification) est réel.

## 10. Ce qui reste ouvert

| Sujet | État | Raison |
|---|---|---|
| lot13 / export PowerBI | ⛔ bloqué | défaut moteur ci-dessus, décision métier requise |
| Chaînes `charges` et `menages` | ⚠️ | déclarées et lançables, non exercées en recette |
| Mode réel du pilotage | ⛔ | garde-fous en place, `CALCULS_REAL_RUN_ENABLED` jamais activé |
| Forfait logiciel/consommables historisé | ⛔ | nécessite `build_charge_fixe()` de lot10 — jamais commencé |
| Durées de référence *a priori* | ⛔ | volontaire : pas d'estimation inventée |

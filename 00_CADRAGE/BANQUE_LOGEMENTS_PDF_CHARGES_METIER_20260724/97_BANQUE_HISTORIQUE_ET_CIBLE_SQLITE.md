# 97 — Banque : source historique déclarée et cible SQLite

## 1. Le fichier portait un nom faux

| | |
|---|---|
| Ancien nom | `2026_03_BRUT_Banque_CreditMutuel.xlsx` |
| Nouveau nom | `BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx` |
| SHA256 | `a84c9b51b1c0eb50d17216272bd3c6cf2669d159bf7e1299c2b762face0ca4a8` — **identique avant et après** |

Le nom annonçait mars 2026. Le contenu, mesuré : **541 mouvements du 2025-11-03 au 2026-08-01**,
soit **dix mois**, dont seulement 74 en mars.

| Mois | Mouvements | | Mois | Mouvements |
|---|---|---|---|---|
| 2025-11 | 58 | | 2026-04 | 59 |
| 2025-12 | 55 | | 2026-05 | 53 |
| 2026-01 | 68 | | 2026-06 | 45 |
| 2026-02 | 65 | | 2026-07 | 63 |
| 2026-03 | 74 | | 2026-08 | 1 |

Ce fichier n'est pas un relevé mensuel : c'est **l'historique consolidé de la banque actuelle**.

## 2. Le contrôle de période était faux, pas la donnée

`lot8a` comparait la période réelle à un `NOM_ANNEE = 2026 / NOM_MOIS = 3` **codé en dur**.
`BANQUE_FICHIER_PERIODE_INCOHERENTE` se déclenchait donc à chaque import, alors qu'aucune anomalie
n'existait. Un contrôle qui crie toujours n'est plus un contrôle.

Le **nom déclare** désormais la nature de la source, et le contrôle ne vérifie que ce que cette
déclaration promet :

| Forme du nom | Type déclaré | Ce qui est vérifié |
|---|---|---|
| `BANQUE_ACTUELLE_HISTORIQUE_<début>_<fin>.xlsx` | `HISTORIQUE` | plusieurs mois sont **normaux** ; les bornes annoncées doivent correspondre au contenu |
| `<AAAA>_<MM>_BRUT_….xlsx` | `MENSUEL` | tout mouvement hors du mois annoncé est une anomalie |
| tout autre nom | `INDETERMINE` | aucune promesse, donc **aucun** contrôle de période |

Le contrôle n'est pas désactivé : il reste entier pour un fichier déclaré mensuel. Il dit
maintenant `[OK] Période conforme à la déclaration HISTORIQUE : 2025-11-03 -> 2026-08-01`.

## 3. Le renommage n'a rien changé d'économique

Chaîne relancée après renommage — comparaison stricte :

| Indicateur | Avant | Après |
|---|---|---|
| Mouvements | 541 | 541 |
| `RAPPROCHEMENT_REQUIS` | 222 | 222 |
| `CLASSE` | 236 | 236 |
| `A_ENVOYER_IA` | 83 | 83 |
| Airbnb en attente | 166 lignes / 14 467,27 € | 166 lignes / 14 467,27 € |
| Virements propriétaires | 56 lignes / 27 069,18 € | 56 lignes / 27 069,18 € |

**Écart économique : 0.**

## 4. Apporter de nouveaux exports avant le cut-over

La cible **n'est pas** de concaténer des classeurs. Chaque export externe s'importe séparément et
alimente la même table :

    export 1 (xlsx)  ─┐
    export 2 (xlsx)  ─┼─► import ─► table bancaire SQLite ─► classification ─► rapprochement
    API (demain)     ─┘

Le classeur actuel reste **transitoire**. Il n'est pas la base de données.

## 5. Contrat cible du mouvement bancaire en SQLite

À définir en migration lors de la bascule Banque. Champs minimaux :

| Champ | Rôle |
|---|---|
| `mouvement_id_opaque` | identité applicative stable |
| `bank_account_id` | **permet de créer la future banque comme contexte séparé** |
| `source_type` | `HISTORIQUE` / `MENSUEL` / `INCREMENTAL` / `API` |
| `source_import_id` | quel import a produit la ligne |
| `external_transaction_id` | identifiant banque, **quand il est fourni** |
| `date_operation`, `date_valeur` | dates, la seconde si disponible |
| `sens`, `montant`, `devise` | montant signé et devise |
| `libelle_brut`, `contrepartie_brute` | tels que reçus, jamais réinterprétés |
| `fingerprint` | empreinte de déduplication |
| `date_import`, `run_id` | traçabilité |
| `statut_classification` | résultat de la classification |

Réutiliser les tables existantes plutôt que d'en créer : `banque_imports`,
`banque_rapprochements`, `banque_classement_decisions` couvrent déjà une partie du besoin.

## 6. Déduplication — la règle, pas encore le code

Deux exports qui se chevauchent ne doivent pas produire deux fois le même mouvement.

1. **Si la banque fournit un identifiant de transaction stable** : c'est lui qui fait foi. Point.
2. **Sinon** : empreinte déterministe sur les champs réellement fiables — compte, date d'opération,
   montant signé, libellé brut normalisé.
3. **En cas d'ambiguïté** : `A_CONTROLER`, jamais une fusion silencieuse.

Un même montant, à la même date, avec le même libellé peut être **deux vrais mouvements** — un
double prélèvement existe. Déduire une identité économique de ces seuls champs produirait des faux
positifs, c'est-à-dire une perte de mouvement. En cas de doute, on montre les deux.

## 7. Future API

Aucune intégration n'est développée. L'architecture doit seulement garantir que demain,
`EXPORT_XLSX` et `API` alimentent **la même table**. Les lots aval ne doivent donc jamais dépendre
du format de transport, mais des mouvements en base.

## 8. Cut-over futur — non exécuté

Au changement de structure et de banque :

| Domaine | Traitement |
|---|---|
| Banque | **remise à zéro** — aucun report de solde d'ouverture automatique |
| Comptabilité | **remise à zéro** |
| Créances / dettes / soldes actifs | reset, selon les règles de cut-over déjà documentées |
| Historique métier, réservations, résultats, analytique | **conservés** |
| Logements, propriétaires, règles, tarifs | **conservés** |

`bank_account_id` est ce qui rendra ce cut-over propre : la nouvelle banque devient un contexte
distinct, sans mélange avec les mouvements anciens.

**Rien de tout cela n'est codé.** Seul le modèle est préparé.

## 9. Tests Banque — pourquoi aucun volume n'est figé

Les tests figeaient `52` mouvements bancaires non classés. Le dataset courant en donne `81`.
**Ni l'un ni l'autre n'a été inscrit dans les tests.**

Un nombre absolu de mouvements décrit un export, pas une règle. Il tombera au prochain relevé, et
le remplacer ne ferait que reporter l'échéance d'un mois. Deux familles de tests coexistent
désormais :

| Famille | Contenu |
|---|---|
| **Fixtures déterministes** | jeux synthétiques figés, avec des nombres exacts attendus — ils testent les règles |
| **Invariants sur données réelles** | cohérence agrégat/détail, absence de doublon, verdict déduit des comptes, aucun élément perdu — ils testent le système |

Exemples d'invariants retenus : le verdict du runner doit être la **conséquence** de
`n_avant`/`n_après`, jamais une affirmation indépendante ; un agrégat détaillable doit ouvrir au
moins un élément ; deux éléments ne peuvent pas partager un identifiant opaque ; le détail VRBO
doit compter exactement ce que le périmètre moteur compte.

## 10. Point ouvert — 128 contrôles bloquants, cause datée

La régénération de la chaîne aval a fait apparaître **128 contrôles `JOINTURE_PAYOUT_MANQUANTE`**,
tous sur les mois **2026-06 à 2026-12**.

Cause mesurée : `MASTER_CALC_HA_Payout.xlsx` a été **extrait le 2026-06-08** (1 380 payouts), alors
que les réservations vont jusqu'en décembre 2026. Toute réservation postérieure à cette extraction
n'a mécaniquement pas de payout.

Ce n'est **pas** une incohérence de calcul, et ce n'est pas une régression : c'est un décalage de
fraîcheur entre deux masters produits par Lot 1 à des dates différentes. Seul un nouveau run Lot 1
contre l'API Hostaway peut le résorber — hors périmètre de cette mission.

Conséquence pratique : **la baseline est fiable pour les mois clos, pas pour les mois récents.**
Le test de sévérité tolère ce code précis, daté et expliqué ; tout autre code bloquant le fait
toujours échouer.

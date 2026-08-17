# 96 — Dernière baseline Excel avant migration SQLite

> **VERSION FINALE ET FRAÎCHE — 2026-08-17.** La section 11 remplace les chiffres des
> sections 1 à 6, produits avant le rafraîchissement Hostaway. Celles-ci sont conservées
> pour montrer ce que le rafraîchissement a changé.

> **BASELINE_LEGACY_SQLITE_MIGRATION**
>
> Ces masters ne sont **pas la cible**. Ils constituent la dernière référence fiable de l'ancien
> moteur Excel, produite pour pouvoir comparer les futures tables SQLite ligne à ligne et montant à
> montant. Leur régénération n'est pas un retour à Excel : c'est ce qui rendra sa suppression
> vérifiable.

## 1. Production

| | |
|---|---|
| Date | 2026-08-16 |
| HEAD producteur | `c6d12e0` |
| Chaîne exécutée | lot4bis → lot4quater → lot9 → lot10 → lot11 → lot12 → lot13 |
| Interpréteur | `C:\Program Files\Python312\python.exe` (pandas) |
| Sources modifiées | **aucune** — REF_Setup et app.db réels d'empreinte inchangée |

### Pourquoi la chaîne démarre à lot4bis

L'audit préalable a montré que `guestCount` — l'entrée du calcul canapé — était **absent** de
`MASTER_CALC_Reservations.xlsx` (lot4bis) et donc du master résolu. Il est en revanche présent
dans le master Hostaway (lot1) et dans l'historique clos. Repartir de lot4bis suffisait donc, et
**aucun appel à l'API Hostaway n'a été nécessaire**.

## 2. Empreintes avant / après

| Master | SHA256 avant | SHA256 après | Lignes avant | Lignes après |
|---|---|---|---|---|
| `MASTER_CALC_Reservations_Resolues.xlsx` | `09de4790d58a6c21` | `c64598cef85da00c` | 2742 | 2863 |
| `MASTER_CALC_Flux.xlsx` | `b4e1d1c9f3820e4e` | `b4ac08fb218ad333` | 1394 | 1395 |
| `MASTER_CALC_Commissions.xlsx` | `68f0c150b16af755` | `41bda2d471829c65` | 1410 | 1367 |
| `MASTER_CALC_NetProprietaire.xlsx` | `f9e2666b95599442` | `f629c088f705917d` | 1843 | 1818 |
| `MASTER_CALC_Resultats.xlsx` | `599a502b735184cd` | `6437175ffce5a58e` | 947 | 957 |
| `MASTER_CTRL_Coherence.xlsx` | `d4504b33d5770434` | `f07e12e69fcd1789` | 53 | 331 |
| `MASTER_FACT_Proprietaires.xlsx` | `8cdca80f94ce3dfa` | `68f427bda63e087a` | 4065 | 4004 |

## 3. Préparation canapé — présence vérifiée

| Artefact | Constat |
|---|---|
| `MASTER_CALC_Commissions` / COMMISSIONS | `preparation_canape_voyageurs`, `controle_preparation_canape`, `source_preparation_canape` |
| — lignes avec canapé > 0 | 118 sur 1334 |
| — statuts de contrôle | OK 118 · NON_ELIGIBLE 425 · NON_APPLICABLE 791 |
| `MASTER_CALC_NetProprietaire` / VUE_MOIS | `total_preparation_canape_mois` |
| `MASTER_FACT_Proprietaires` (Lot 12) | 55 lignes `PREPARATION_CANAPE` |
| `PBI_Commissions.csv` (Lot 13) | `montant_preparation_canape`, `controle_preparation_canape` |

Total canapé : **1 180,00 €**, identique entre Commissions et NetProprietaire.

## 4. Parité métier — écart 0,00 €

`montant_du_conciergerie` = somme des **cinq** composants facturables, sur 217 lignes
mois × propriétaire :

| Composant | Montant |
|---|---|
| `total_commission_mois` | 37 058,38 € |
| `total_menage_mois` | 56 454,00 € |
| `total_preparation_canape_mois` | 1 180,00 € |
| `charge_fixe_mensuelle` | 7 475,00 € |
| `charges_exceptionnelles_refacturees` | 0,00 € |
| **`montant_du_conciergerie`** | **102 167,38 €** |

**0 ligne en écart. Écart financier total : 0,00 €.**

## 5. Totaux financiers de référence

| Indicateur | Montant |
|---|---|
| Net propriétaire après charges | 188 902,32 € |
| Résultat REEL | 284 855,70 € |
| Résultat COMPTABLE | 274 331,54 € |
| Résultat HORS_COMPTA | 10 524,16 € |
| **REEL − (COMPTABLE + HORS_COMPTA)** | **0,00 €** |

Lot 10 a par ailleurs relevé 0 contrôle bloquant.

## 6. Volumétrie des exports (Lot 13)

13 exports + dictionnaire (140 entrées), **aucune colonne manquante signalée** — c'était le
symptôme des masters périmés.

Flux 1394 · Résultats mensuels 535 · Réservations résolues 1527 · Commissions 1334 ·
Net propriétaire 217 · Préfactures 3223 · Contrôles ouverts 14 · Référentiels 19/17/12/19.

## 7. Ce que cette baseline sert à vérifier

Quand une chaîne sera portée en SQLite, la comparaison portera sur :

1. le **nombre de lignes** au grain de la table ;
2. les **clés** (mêmes identifiants, aucun perdu, aucun inventé) ;
3. les **cinq composants** ci-dessus, à l'euro ;
4. l'invariant **REEL = COMPTABLE + HORS_COMPTA** ;
5. les **contrôles** de Lot 11.

Un écart non expliqué invalide la migration de la chaîne concernée.

## 8. Ces masters ne sont pas retirés

Ils restent suivis par Git le temps des comparaisons OLD/NEW. Leur retrait suppose, pour chaque
chaîne : producteur SQLite, consommateur SQLite, parité vérifiée, tests verts, interface
fonctionnelle, redémarrage, et environnement neuf reconstructible.

---

## 11. Version finale — après rafraîchissement Hostaway (2026-08-17)

### 11.1 Pourquoi une seconde production

La première baseline portait **128 contrôles bloquants** `JOINTURE_PAYOUT_MANQUANTE`. Cause
mesurée : `MASTER_CALC_HA_Payout` datait du **2026-06-08** alors que les réservations allaient
jusqu'en décembre 2026. Ce n'était pas une erreur de calcul mais un décalage entre deux extractions
Lot 1.

Lot 1 a donc été relancé contre l'API Hostaway, **en lecture seule**.

### 11.2 Preuve de lecture seule

Les trois `POST` du lot visent tous `/v1/accessTokens` — OAuth2 `client_credentials`,
c'est-à-dire l'authentification. **Toute lecture de données passe par `_get()`.** Aucune route de
mutation, aucune création, aucune modification côté Hostaway.

### 11.3 Fraîcheur obtenue

| Sortie Lot 1 | Avant | Après |
|---|---|---|
| `MASTER_CALC_HA_Payout` | 1 380 lignes, extrait **2026-06-08** | **1 518 lignes, extrait 2026-08-17** |
| `MASTER_FACT_HA_Reservations` | 1 527 lignes, extrait 2026-08-10 | **1 542 lignes, extrait 2026-08-17** |
| `MASTER_FACT_HA_ReservationDetails` | 86 lignes | 113 lignes |
| `MASTER_REF_HA_Listings` | 17 lignes | 17 lignes, extrait 2026-08-17 |
| `MASTER_CTRL_HA_Anomalies` | 55 lignes | **31 lignes** |

Payouts et réservations portent désormais **la même date d'extraction**. L'écart de deux mois est
fermé.

### 11.3 bis — Ce que le run n'a PAS terminé

L'extraction des réservations et des payouts est allée à son terme : 1 542 réservations traitées,
288 sautées, et les sept tables écrites entre 07:26:53 et 07:26:55.

Le lot a ensuite enchaîné sur l'étape **tâches ménage (H6)**. Elle a tourné plus d'une heure en
butant à répétition sur les limites de débit de l'API (`HTTP 429`), puis le processus a été
interrompu par l'environnement d'exécution à 08:32.

Deux conséquences, sans effet sur la baseline :

- `MASTER_FACT_HA_CleaningTasks_Discovery` reste daté du **2026-06-09**. Il n'entre dans aucun des
  contrôles visés par cette mission.
- **`MASTER_RUN_Log` ne contient aucune entrée pour ce run** : le lot écrit son journal en fin de
  parcours, après l'étape ménages. Sa dernière entrée reste celle du 2026-06-08. Le rafraîchissement
  n'est donc pas tracé dans le journal du moteur — il l'est ici, et par les dates d'extraction
  inscrites dans chaque table.

Rien de tout cela n'affecte les chiffres ci-dessous : ils ont été mesurés directement dans les
fichiers produits, et vérifiés par la disparition des 128 contrôles bloquants.

### 11.4 Le point ouvert est refermé

| Contrôle | Avant | Après |
|---|---|---|
| `JOINTURE_PAYOUT_MANQUANTE` | **128** | **0** |
| Total `BLOQUANTS_OUVERTS` | 128 | **0** |

Aucun payout n'a été estimé, reconstitué depuis la Banque, ni déduit d'un montant de réservation.
Hostaway est resté la seule source : les jointures manquantes ont disparu parce que les payouts
existent désormais réellement.

Restent **14 contrôles `A_CONTROLER`**, tous légitimes et non bloquants :

| Nombre | Code |
|---|---|
| 9 | `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` |
| 1 | `VRBO_MONTANT_NON_RENSEIGNE` |
| 1 | `RESERVATION_A_CONTROLER_SANS_COMMISSION` |
| 1 | `MENAGE_EXTERNE_ECART_HOSTAWAY` |
| 1 | `MENAGE_EXTERNE_LOGEMENT_HORS_HA` |
| 1 | `SOURCE_SHEET_PROVENANCE_INCOMPLETE` |

### 11.5 Empreintes finales

| Master | SHA256 | Lignes |
|---|---|---|
| `MASTER_CALC_Reservations_Resolues` | `49a15e47470e` | 3020 |
| `MASTER_CALC_Flux` | `ce4bc14a4fb2` | 1537 |
| `MASTER_CALC_Commissions` | `7688dc37dd21` | 1520 |
| `MASTER_CALC_NetProprietaire` | `6b2160955aea` | 1997 |
| `MASTER_CALC_Resultats` | `959d55b9b4d8` | 1033 |
| `MASTER_CTRL_Coherence` | `416d474f7bda` | 71 |
| `MASTER_FACT_Proprietaires` | `a3fd50a464aa` | 4331 |

### 11.6 Parité métier — écart 0,00 €

233 lignes mois × propriétaire :

| Composant | Montant |
|---|---|
| `total_commission_mois` | 41 602,41 € |
| `total_menage_mois` | 62 609,00 € |
| `total_preparation_canape_mois` | 1 390,00 € |
| `charge_fixe_mensuelle` | 8 025,00 € |
| `charges_exceptionnelles_refacturees` | 0,00 € |
| **`montant_du_conciergerie`** | **113 626,41 €** |

**0 ligne en écart.**

### 11.7 Totaux financiers de référence

| Indicateur | Montant |
|---|---|
| Net propriétaire après charges | 209 242,15 € |
| Résultat REEL | 316 654,56 € |
| Résultat COMPTABLE | 306 130,40 € |
| Résultat HORS_COMPTA | 10 524,16 € |
| **REEL − (COMPTABLE + HORS_COMPTA)** | **0,00 €** |

Préparation canapé : **1 390,00 €**, 61 lignes `PREPARATION_CANAPE` au Lot 12, colonnes exportées
par Lot 13 sans aucune colonne manquante signalée.

### 11.8 Statut

**C'est la dernière baseline Excel légitime avant migration SQLite.** Elle est fraîche, sans
contrôle bloquant, et financièrement cohérente. C'est elle qui servira de référence pour valider
chaque table dérivée SQLite — et donc pour pouvoir supprimer ces masters.

## Mise à jour 2026-08-18 — baseline Hostaway et réservations

La baseline legacy reste la référence de comparaison, mais elle n'est plus la source de
fonctionnement. Volumes constatés au dernier rafraîchissement, et repris à l'identique en SQLite :

| Jeu | Volume |
|---|---|
| Réservations Hostaway | 1 542 |
| Payouts | 1 518 |
| Listings | 17 |
| Frais | 644 |
| Champs financiers | 892 |
| Anomalies d'extraction | 31 |
| Réservations calculées (Lot 4bis) | 1 542 |
| Réservations historisées (Lot 4ter) | 1 269 |
| Réservations résolues (Lot 4quater) | 1 542 |

Ces nombres décrivent l'état d'un jour donné. Ils servent à comparer deux exécutions, **jamais de
règle métier** : un export qui s'élargit ou un mois qui se clôture les fait bouger sans qu'aucune
régression n'ait eu lieu. Les tests vérifient des relations, pas ces volumes.

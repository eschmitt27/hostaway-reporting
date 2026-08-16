# 96 — Dernière baseline Excel avant migration SQLite

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

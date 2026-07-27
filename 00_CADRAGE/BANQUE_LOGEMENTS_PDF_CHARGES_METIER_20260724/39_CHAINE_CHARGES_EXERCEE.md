# 39 — Chaîne Charges exercée depuis l'application

Phase 2. Le module Charges était livré (`24`, `27`) mais sa chaîne n'avait **jamais tourné** depuis
`/calculs`. Ce document consigne ce que l'exécution a révélé et prouvé.

## 1. Audit ciblé

| Étape | Script | Entrées | Sorties | Contrôles |
|---|---|---|---|---|
| lot3 | `lot3_generateur_charges.generer()` via `runners/charges_post_write_runner.py` | `SAISIE_Charges_Flux.xlsx`, `REF_Setup.xlsm` | `Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx` (MASTER + VUE_MENAGE) | doublon `charge_id`, date invalide, `code_impact` inconnu, placeholders |
| lot7 | `lot7_generateur_avantages.generer()` | SAISIE, IK | `Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx` | — (NON_APPLICABLE en régénération) |
| lot11 (partiel) | `controles_suivi_associe` | SAISIE, IK | anomalies JSON | suivi associé |
| aval | lot9 → lot13 | MASTER charges | flux, résultats, préfactures, exports | CTR-9-*, CTR-11-* |

Orchestrateur **réutilisé**, aucun second moteur écrit.

## 2. Deux défauts trouvés en exécutant

### 2.1 La chaîne `charges` du pilotage ne pouvait pas fonctionner

`lot3_generateur_charges.py` est une **bibliothèque** : aucun bloc `__main__`. Le pilotage le
déclarait comme un script autonome. Lancé ainsi :

```
EXIT=0        et AUCUNE sortie produite
```

Le garde-fou « jamais de faux succès » le rattrapait bien (`Code retour 0 mais sortie absente`),
mais la chaîne restait inutilisable.

**Correction** : `Lot.runner` désigne un runner de `05_APPLICATION/runners/` lancé à la place du
script. `lot3` pointe désormais sur `charges_post_write_runner.py`, l'orchestrateur qui existait
déjà (lot3 + lot7 + lot11, chemins injectés, interpréteur moteur).

Point critique : **ce runner rend toujours le code 0** — il porte l'échec métier dans son JSON de
réponse. Sans lecture de ce JSON, une étape en échec passerait pour un succès dès qu'une sortie
d'un run précédent traîne sur le disque. `_verdict_runner()` lit donc la réponse et refuse tout
statut hors `OK` / `NON_APPLICABLE`. Vérifié en conditions réelles : une première tentative a rendu
`ECHEC — lot3=ECHEC (FileNotFoundError…)` avec un code retour 0.

### 2.2 Double comptage d'un frais bancaire dans le jeu de recette

Le jeu de recette portait `CHG_SEED_003` (frais bancaire, 8,90 €) **en charge manuelle**, alors que
Lot9 injecte déjà les mouvements `TYPE_FLUX_016 VALIDE` depuis `NORM_Banque`. Preuve :

```
FLUX-2026-06-IC-BNQ-0001 | BANQUE_LOT8_IMPORT_NORM_Banque | MVT-SEED-006
FLUX-2026-06-IC-CHG-0009 | MASTER_FACT_MAN_Charges        | CHG_SEED_003
```

Total charges 567,80 au lieu de 558,90 — **8,90 € comptés deux fois**. Le défaut préexistait
(l'ancien `charges_reel` de 232,80 valait 223,90 + 8,90) et **rien ne le détectait**.

**Corrections** : la charge manuelle est retirée du jeu de recette, et un test
(`test_pas_de_double_comptage_frais_bancaire`) signale désormais tout couple mois+montant présent à
la fois en flux bancaire et en charge manuelle.

## 3. Le MASTER n'est plus seedé à la main

Le jeu de recette écrivait 3 charges directement dans `MASTER_FACT_MAN_Charges.xlsx` — une seconde
vérité, que le premier passage de Lot3 aurait écrasée. Désormais :

- les charges sont écrites dans la **SAISIE**, qui est la vérité métier ;
- `build_charges()` fait produire le MASTER **par Lot3 lui-même** (openpyxl seul, donc exécutable
  avec l'interpréteur courant).

Le classeur MASTER doit néanmoins pré-exister : `ecrire_master` ouvre la cible pour en préserver les
autres onglets, il ne la crée pas.

## 4. Scénarios métier et réconciliation

10 charges fictives dans la SAISIE. Lot3 : 10 lignes MASTER, 5 en VUE_MENAGE, 0 anomalie bloquante.

| Scénario | Charges | Montant | Attendu | Constaté |
|---|---|--:|---|---|
| A — charge conciergerie | `CHG_A_LOGICIEL` | 60,00 | résultat conciergerie diminué, net propriétaire inchangé | ✅ |
| B — refacturable propriétaire | `CHG_B_REFACT` | 150,00 | somme à payer modifiée d'exactement 150,00 | ✅ |
| C — paiement personnel associé | `CHG_C_PERSO` | 40,00 | impact réel, hors comptabilité, associé résolu | ✅ |
| D — hors comptabilité | `CHG_D_HORSCOMPTA` | 25,00 | impact réel, aucun impact comptable, justification conservée | ✅ |
| E — ventilation multi-logements | `CHG_E_MULTI_1/2` | 30 + 30 | total 60 conservé, aucun doublon | ✅ |
| F — charge fournisseur liée à facture | `CHG_SEED_001/002` | 120 + 95 | une seule charge, aucun impact recréé par facture/règlement/banque | ✅ |
| Contrôle — non validée | `CHG_CTRL_NONVALID` | 500,00 | **jamais injectée** | ✅ absente du flux |
| Contrôle — exclue du résultat | `CHG_CTRL_EXCLUE` | 400,00 | **jamais injectée** | ✅ absente du flux |

### Réconciliation des totaux

```
charges manuelles VALIDE : 60 + 150 + 40 + 25 + 30 + 30 + 120 + 95 = 550,00
frais bancaire (Lot9 depuis NORM_Banque)                          =   8,90
                                                       REEL       = 558,90  ✅
dont hors comptabilité (C 40 + D 25)                              =  65,00
                                                       COMPTABLE  = 493,90  ✅
invariant : REEL = COMPTABLE + HORS_COMPTA  →  493,90 + 65,00 = 558,90  ✅
charges non validées, correctement écartées : 500 + 400           = 900,00
```

### Effet sur les résultats (run `RUN-C87382B8DA79`, SUCCES, 3,0 s)

| Indicateur | Avant charges | Après | Écart | Lecture |
|---|--:|--:|--:|---|
| `charges_reel` | 232,80 | 558,90 | +326,10 | dont −8,90 de double comptage supprimé |
| `resultat_reel` | 13 827,20 | 13 501,10 | −326,10 | cohérent au centime |
| `resultat_comptable` | 13 827,20 | 13 566,10 | −261,10 | = −326,10 + 65,00 (HC) |
| `resultat_hors_compta` | 0,00 | −65,00 | −65,00 | scénarios C + D |
| **`net_proprietaire`** | 11 629,40 | **11 629,40** | **0,00** | **scénario A prouvé** : une charge conciergerie ne touche pas le propriétaire |
| **`somme_a_payer`** | 2 430,60 | **2 580,60** | **+150,00** | **scénario B prouvé** : exactement le montant refacturé |

## 5. Idempotence et rollback

- Second run `RUN-653B6C4AE68F` : SUCCES, **tous écarts 0,00**.
- Restauration depuis la page du run : **1 fichier restauré**, run repassé `RESTAURE`.

## 6. Tests — `05_APPLICATION/tests/test_charges_pipeline.py`, 19 passés

Structure (5) : runner déclaré et présent ; Lot3 toujours sans point d'entrée ; verdict runner qui
refuse une réponse absente, refuse une étape en échec, accepte `NON_APPLICABLE`.

Double comptage (3) : frais bancaire, une charge = un flux, `flux_id` uniques.

Scénarios (6) : A à F.

Contrôles (2) : charge non validée et charge exclue absentes du flux.

Réconciliation (3) : invariant REEL = COMPTABLE + HC ; total Lot9 = total Lot10 ; net propriétaire
indépendant des charges conciergerie.

## 7. Statut : chaîne Charges **exercée et validée**

Ce qui reste hors périmètre de cette phase :
- les contrôles « facture liée deux fois », « propriétaire absent », « ventilation dont le total
  diffère » sont couverts par la page `/factures/controles` et la migration 0017 (index uniques),
  déjà livrées ; ils n'ont pas été re-testés ici ;
- « charge postérieure à une clôture validée » : aucun mécanisme applicatif ne l'interdit
  aujourd'hui. **Non construit, signalé** — la clôture applicative est un suivi, la clôture réelle
  reste portée par `REF_Cloture_Mensuelle`.

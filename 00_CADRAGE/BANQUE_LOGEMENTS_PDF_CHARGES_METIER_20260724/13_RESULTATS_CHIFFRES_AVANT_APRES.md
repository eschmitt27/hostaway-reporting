# 13 — Résultats chiffrés des scénarios

Chaque scénario : reset → prévisualisation → **confirmation réelle** (écriture SAISIE + Lot3 régénéré
+ Lot11 OK) → lecture des chiffres écrits. Montant identique 100 € partout. Reproductible via
`recette/run_scenarios.py` (test `test_recette_scenarios.py` : **ALL SCENARIOS OK**).

## Chiffres constatés (écrits dans les fichiers de recette)

| Scénario | Paramètres | code_impact | prise_en_compta | Quotes-parts (affectations) | Réserve/préfacture | Lot3 | Lot11 |
|---|---|---|---|---|---|---|---|
| **A** conciergerie | IC, LOG_A1, non refac. | IC | **OUI** | LOG_A1/PROP_A = 100 | — (0) | OK | OK |
| **B** propriétaire refacturable | IC, LOG_A1, **refac.** | IC | OUI | LOG_A1/PROP_A = 100 | **PROP_A = 100 €** | OK | OK |
| **C** payé compte perso associé | IC, LOG_A1, PAY_004, PERS_X | IC | OUI | LOG_A1/PROP_A = 100 | — (0) | OK | OK |
| **D** hors comptabilité | **HC**, LOG_A1 | **HC** | **NON** | LOG_A1/PROP_A = 100 | — (0) | OK | OK |
| **H** répartition même propriétaire | IC, LOG_A1+LOG_A2 | IC | OUI | **LOG_A1=50, LOG_A2=50** (PROP_A) | — (0) | OK | OK |
| **I** répartition multi-propriétaires | IC, LOG_A1+LOG_B1 | IC | OUI | **LOG_A1/PROP_A=50, LOG_B1/PROP_B=50** | — (0) | OK | OK |

## Différences prouvées (deux charges de 100 €, effets distincts)
- **A vs B** : mêmes paramètres sauf « refacturable ». B crée une **préfacture de 100 € pour PROP_A**
  (réserve de facturation), A non. → l'effet sur le net propriétaire est distinct.
- **A vs D** : A est IC (prise_en_compta OUI), D est HC (prise_en_compta **NON**). → la règle
  HORS_COMPTA est conservée : D est exclu du résultat comptable, pas du réel.
- **H** : 100 € ventilés **50/50** sur deux logements du même propriétaire, somme = 100 (0 centime perdu).
- **I** : 100 € ventilés **50/50** sur deux propriétaires différents (PROP_A, PROP_B) — quote-part
  distincte par propriétaire.
- **C** : payé via compte perso associé → ASSOC_MODE distinct (charge_id `…-FICTIF-…`), pas de
  préfacture, aucune diminution indue de trésorerie conciergerie.

## Effet sur résultat conciergerie / net propriétaire (déterminé par les drapeaux d'impact)
Une charge IC (A/C/H/I) porte `impact_resultat_reel=OUI` + `impact_resultat_comptable=OUI` → elle
diminue le résultat conciergerie réel ET comptable de 100 €. D (HC) diminue le réel de 100 € mais
**pas** le comptable. B (refacturable) diminue le résultat conciergerie mais génère une créance de
refacturation de 100 € sur PROP_A → **net propriétaire A −100 €** via la préfacture.

## Pipeline aval complet exécuté (lot9 + lot10) — impacts financiers FINAUX

Sources fictives construites (`recette/build_reservations_recette.py`) : **1010 réservations**
(dont 4 propres 2026-06 à 1000 € : LOG_A1/A2/B1/C1) + payout + banque/ménages (header seul). Lot9
**VERT** (1010 flux, 0 doublon), Lot10 **VERT** (0 contrôle bloquant). Une charge saisie doit être
**validée** (statut A_CONTROLER → VALIDE, étape humaine simulée) pour entrer dans le net — sinon
Lot9 l'ignore (règle métier constatée).

### Situation initiale 2026-06 (sans charge) — constatée dans MASTER_CALC_NetProprietaire / Resultats
| Propriétaire | payout | commission | net | reste à payer | résultat logement (réel=compta) |
|---|--:|--:|--:|--:|--:|
| PROP_A (LOG_A1 15% + LOG_A2 19%) | 2000 | 340 | **1660** | 340 | LOG_A1=1000, LOG_A2=1000 |
| PROP_B (LOG_B1 15%) | 1000 | 150 | **850** | 150 | LOG_B1=1000 |
| PROP_C (LOG_C1 19%) | 1000 | 190 | **810** | 190 | LOG_C1=1000 |

Le taux historisé est correctement résolu (LOG_A1 = 15 % via TX_A1_NEW pour 2026-06, LOG_A2 = 19 %).

### AVANT / APRÈS confirmés (charge 100 €, recalcul lot9+lot10)
| Scénario | résultat LOG_A1 RÉEL | résultat LOG_A1 COMPTABLE | net PROP_A | reste à payer |
|---|--:|--:|--:|--:|
| **A** conciergerie IC | 1000 → **900** (−100) | 1000 → **900** (−100) | 1660 → 1660 (0) | 340 → 340 |
| **D** hors compta HC | 1000 → **900** (−100) | 1000 → **1000** (0) | 1660 → 1660 (0) | 340 → 340 |

**Prouvé chiffré au résultat final :**
- Une charge **conciergerie IC** diminue le résultat du logement de 100 € en **réel ET comptable**,
  sans toucher le net propriétaire (la conciergerie la supporte).
- Une charge **HC** diminue le résultat **réel** de 100 € mais **PAS le comptable** (0) → règle
  HORS_COMPTA conservée de bout en bout.
- **A vs D** : effets distincts et chiffrés sur la comptabilité (IC : −100 ; HC : 0).

### Blocage reproduit — refacturation → préfacture
Pour une charge **refacturable** (B / B2), le résultat du logement baisse bien de 100 €, mais la
**préfacture propriétaire reste 0** et le net propriétaire inchangé : `aggregate_refacturable_charges`
(lot10) n'attribue la refacturation qu'à une charge portant un **propriétaire explicite ET validé**
(« jamais inféré » depuis le logement). La ligne MASTER de la charge saisie sur un logement porte
`proprietaire_id = None` ; la préfacture part alors en ligne de contrôle, pas sur le net du
propriétaire. Blocage précis, reproduit pour B et B2 : la matérialisation de la préfacture finale
nécessite que la saisie renseigne le propriétaire sur la charge (ou que le writer le dérive du
logement) — correction ciblée à faire avant de conclure sur le net refacturé.

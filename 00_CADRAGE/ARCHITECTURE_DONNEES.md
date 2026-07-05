# ARCHITECTURE_DONNEES.md

> **Version dÃ©finitive consolidÃ©e.** Socle ancrÃ© dans les fichiers rÃ©els (19 onglets `REF_Setup`, 9 CSV master du run `20260523_005752`), enrichi des raffinements d'implÃ©mentation.
> Ce document **n'implÃ©mente rien**. Il sert de rÃ©fÃ©rence pour construire le systÃ¨me lot par lot. Il doit Ãªtre assez clair pour qu'un autre dÃ©veloppeur ou assistant puisse construire le systÃ¨me.
>
> **DÃ©cisions mÃ©tier validÃ©es :**
> - Assiette commission = **payout plateforme encaissÃ© âˆ’ frais de mÃ©nage** (le payout inclut le mÃ©nage facturÃ© au voyageur).
> - RÃ©servations hors Hostaway = **une ligne par rÃ©servation**.
> - SÃ©jours `ownerStay` = **exclus totalement du rÃ©sultat**.
> - Convention table de flux = **montant positif + colonne `sens` (PRODUIT / CHARGE)**.
> - Identifiants manuels = **lisibles, stables, non dÃ©ductibles d'un montant seul** ; fichiers de saisie **contrÃ´lÃ©s** (listes dÃ©roulantes, colonnes obligatoires, statut, alertes doublons).

---

## Table des matiÃ¨res (lecture ciblÃ©e)

> Ce fichier est long (~18 k tokens). **Ne pas le lire en entier par dÃ©faut.** Voir `CLAUDE.md Â§5.bis` pour la matrice Â« lot â†’ sections Â». Sauter directement Ã  la section utile.

| Â§ | Section | Lire surtout pour |
|---|---|---|
| Â§1 | Objectif du systÃ¨me (+ Â§1.1 contexte juridique) | Onboarding |
| Â§2 | Principes structurants (upsert, flux, banque, familles de tables) | Tous lots |
| Â§3 | Vue d'ensemble des modules | Onboarding |
| Â§4 | Sources d'entrÃ©e (+ Â§4.1 GitHub/OneDrive, Â§4.2 fichiers saisie) | Lots 0, 2, 3 |
| Â§5 | Tables de sortie (existantes / Ã  construire / consultables) | Lots 1, 9, 10 |
| Â§6 | Architecture Hostaway (tables, statuts, perf) | Lot 1 |
| Â§7 | RÃ¨gles de payout Hostaway (Airbnb / Booking / Direct) | Lots 1, 10 |
| Â§8 | Commission & net propriÃ©taire (+ Â§8.3 mÃ©nage par canal) | Lot 10 |
| Â§9 | RÃ©servations hors Hostaway (+ Â§9.5 table commune) | Lots 4, 4 bis |
| Â§10 | Charges perso/liquide (+ Â§10.4 acomptes, Â§10.5 liquide, Â§10.6 corrections) | Lots 3, 5 |
| Â§11 | MÃ©nages (+ Â§11.4 M04, Â§11.5 mÃ©nages externes) | Lot 6 |
| Â§12 | IK & avantages associÃ©s | Lot 7 |
| Â§13 | Banque & rapprochement (+ Â§13.6 source rÃ©elle CM) | Lot 8 |
| Â§14 | Table de flux unifiÃ©e `MASTER_CALC_Flux` | Lot 9 |
| Â§15 | RÃ©sultats rÃ©el / comptable / hors compta | Lot 10 |
| Â§16 | ClÃ©s de liaison (+ Â§16.2 nomenclature, Â§16.3 rapprochements) | Lots 0, 2 |
| Â§17 | Livrables propriÃ©taires | Lot 12 |
| Â§18 | ContrÃ´les de cohÃ©rence (+ Â§18.5 contrÃ´les de saisie) | Lots 9, 11 |
| Â§19 | Ordre de construction recommandÃ© | Planification |
| Â§20 | Points de vigilance | Tous lots |
| Â§21 | Arbitrages non bloquants restants | DÃ©cisions ouvertes |
| Â§22 | RÃ©sultat du contrÃ´le de cohÃ©rence | â€” |
| Â§23 | Conventions transverses (statuts, clÃ´ture, arrondi, `REF_Statuts_Payout`) | Lots 0, 8, 10, 11 |

---

## 1. Objectif du systÃ¨me

Le systÃ¨me doit produire une vision financiÃ¨re et opÃ©rationnelle fiable d'une conciergerie courte durÃ©e gÃ©rant ~16 logements (rÃ©fÃ©rentiel : 17 logements dont 1 hors Hostaway), majoritairement Ã  Toulouse et Blagnac.

Il concilie plusieurs rÃ©alitÃ©s qui ne vivent pas dans le mÃªme outil :

- les **rÃ©servations Hostaway** (Airbnb, Booking, VRBO) issues de l'API ;
- les **rÃ©servations hors Hostaway** (canal `direct`), minoritaires, encaissÃ©es en liquide ou virement ;
- les **flux bancaires** du compte professionnel ;
- les **acomptes** sur factures propriÃ©taires ;
- les **charges** payÃ©es avec comptes personnels ou en liquide ;
- les **IK et avantages** des associÃ©s ;
- trois lectures de rÃ©sultat : **rÃ©el**, **comptable**, **hors compta** ;
- les **contrÃ´les de cohÃ©rence** entre sources.

Automatisation maximale ; les fichiers manuels ne couvrent que ce qu'aucune source automatique ne fournit. Le systÃ¨me doit rester **robuste, modulaire, maintenable** et **prioritairement exploitable dans Excel / Power Query** (livrable initial). Les tables et CSV sont **conÃ§us pour Ãªtre directement exploitables dans Power BI** par l'utilisateur lui-mÃªme, mais **aucun lot ne livre un dashboard Power BI** (D043).

### 1.1 Contexte juridique et consÃ©quence sur le rÃ©sultat comptable

L'activitÃ© est **opÃ©rationnelle** mais la **SAS porteuse est nouvelle** et son enregistrement n'est pas encore complÃ¨tement stabilisÃ©. Le systÃ¨me prÃ©pare les flux, les contrÃ´les et la distinction IC / HC / HR pour la future exploitation comptable, mais **ne suppose pas d'historique comptable existant**. Aucune Ã©criture comptable passÃ©e n'est Ã  rechercher. Le rÃ©sultat comptable (somme des flux `IC`) dÃ©marre Ã  partir des flux validÃ©s une fois la comptabilitÃ© opÃ©rationnelle ; les flux antÃ©rieurs restent visibles dans le rÃ©sultat rÃ©el (`IC + HC`) sans rÃ©troactivitÃ© comptable forcÃ©e.

### 1.2 Bascule sociÃ©tÃ© et sÃ©paration historique / production

> RÃ©fÃ©rence dÃ©cision : **D-LOT-PROD-01** (DECISIONS_METIER.md). Section de cadrage â€” n'implÃ©mente rien, aucune purge n'a lieu Ã  ce stade.

La nouvelle sociÃ©tÃ© est **en cours d'immatriculation**. Le **point de bascule** vers l'exploitation comptable de la nouvelle structure sera **paramÃ©trÃ© plus tard**. PrioritÃ© actuelle : vÃ©rifier que le pipeline complet (Lot 0 â†’ Lot 12) fonctionne avant toute mise en production.

Le systÃ¨me distingue deux natures de donnÃ©es :

| Nature | DonnÃ©es concernÃ©es | Traitement Ã  la bascule |
|---|---|---|
| **Historique de performance** (conservÃ©) | RÃ©servations, payouts, commissions, net propriÃ©taire, rÃ©sultats par logement / propriÃ©taire / mois, rÃ©fÃ©rentiels (`REF_*`) | **ConservÃ© intÃ©gralement.** Jamais purgÃ©. |
| **ComptabilitÃ© de production** (rÃ©initialisable) | Imports bancaires (`BRUT_Banque`, `NORM_Banque`, `IA_Classification`), rapprochements, clÃ´tures banque (`REF_Cloture_Mensuelle`), soldes, charges comptables, acomptes / rÃ¨glements bancaires, justificatifs sensibles | **Purgeable / rÃ©initialisable** au moment de la mise en production de la nouvelle sociÃ©tÃ©. |

**RÃ¨gles.**
- Aucune purge ni rÃ©initialisation **sans validation humaine explicite** ; jamais automatique.
- Purge autorisÃ©e **uniquement aprÃ¨s backup / snapshot** vÃ©rifiÃ©.
- **Interdiction de mÃ©langer** les donnÃ©es bancaires / comptables de l'ancienne structure avec la nouvelle sociÃ©tÃ©.

**Futurs paramÃ¨tres** (Ã  crÃ©er dans `REF_Parametres_Generaux` au moment de la bascule â€” non crÃ©Ã©s Ã  ce stade) :

| ParamÃ¨tre | RÃ´le |
|---|---|
| `DATE_BASCULE_SOCIETE` | Date de passage Ã  l'exploitation comptable de la nouvelle structure |
| `SOLDE_INITIAL_BANQUE` | Solde bancaire d'ouverture de la nouvelle sociÃ©tÃ© (fourni plus tard par l'utilisateur) |
| `STATUT_PERIODE` | `AVANT_BASCULE` / `APRES_BASCULE` â€” qualifie chaque pÃ©riode de flux |

Le solde initial de banque et les coordonnÃ©es dÃ©finitives de la sociÃ©tÃ© (nom lÃ©gal, SIRET, RCS, adresse, TVA intracom, IBAN, logo) seront **fournis plus tard** par l'utilisateur.

---

## 2. Principes structurants

### 2.1 Ne jamais supprimer une donnÃ©e connue (upsert par clÃ© stable)

Chaque table a une `PK` et un `ROW_HASH`.

| Cas | Traitement |
|---|---|
| `PK` nouvelle | Ajouter la ligne |
| `PK` existante + `ROW_HASH` modifiÃ© | Remplacer / mettre Ã  jour |
| `PK` existante + `ROW_HASH` identique | Ne rien modifier |
| Ancienne `PK` absente du nouvel extract | **Conserver** la ligne |

Jamais de suppression automatique.

### 2.2 SÃ©parer source brute / normalisÃ© / calcul / contrÃ´le

Pour chaque module : donnÃ©es sources â†’ tables normalisÃ©es â†’ tables calculÃ©es â†’ tables de contrÃ´le. Exemple Hostaway : JSON API â†’ rÃ©servations/listings/finance fields/fees â†’ payout â†’ anomalies.

### 2.3 La table de flux unifiÃ©e est la colonne vertÃ©brale

`MASTER_CALC_Flux` reÃ§oit **tous** les Ã©vÃ©nements Ã©conomiques sous une forme commune (un produit, une charge, un acompte, un avantage, une dÃ©duction, un remboursement, un mÃ©nage, une commission = une ligne). Les trois rÃ©sultats deviennent alors de **simples filtres** sur le code impact. C'est ce qui garantit la cohÃ©rence entre rÃ©sultat rÃ©el, comptable et hors compta.

### 2.4 Les contrÃ´les bloquent les zones dangereuses

Le systÃ¨me est bÃ¢ti avec des contrÃ´les, pas seulement des calculs. Sont bloquants uniquement les cas qui rendent un rÃ©sultat ou une facture faux ; le reste est Â« Ã  contrÃ´ler Â».

### 2.5 Les fichiers manuels doivent empÃªcher les erreurs simples

Toute table saisie Ã  la main utilise des listes dÃ©roulantes issues du rÃ©fÃ©rentiel, des colonnes obligatoires mises en Ã©vidence si vides, un statut de contrÃ´le, et des alertes visuelles (mise en forme conditionnelle) sur les doublons et incohÃ©rences de montant. Ces garde-fous sont faits en Excel / Power Query, sans macro complexe tant que des rÃ¨gles simples suffisent (dÃ©tail Â§10.5 et Â§18.5).

### 2.6 La banque ne doit pas doubler les flux mÃ©tier

La banque constate les encaissements et dÃ©caissements rÃ©els, mais ne recrÃ©e pas automatiquement un produit ou une charge dÃ©jÃ  portÃ© par une source mÃ©tier.

| Cas | Source Ã©conomique dans `MASTER_CALC_Flux` | RÃ´le de la banque |
|---|---|---|
| Payout Airbnb / Booking | Hostaway (`MASTER_CALC_HA_Payout`) | Rapprochement / contrÃ´le |
| RÃ©servation hors Hostaway liquide/virement | Table HH manuelle | Rapprochement caisse / banque |
| Charge payÃ©e par compte perso/liquide | Table charges manuelle | Rapprochement si remboursement associÃ© |
| DÃ©pense passÃ©e directement sur compte pro | Banque | Source Ã©conomique |
| Virement associÃ© | Banque | Source avantages / IK |
| Abonnement logiciel prÃ©levÃ© | Banque ou rÃ¨gle rÃ©currente validÃ©e | Source Ã©conomique + contrÃ´le anti-doublon |

RÃ¨gle gÃ©nÃ©rale : **source mÃ©tier structurÃ©e prioritaire ; banque en contrÃ´le**, sauf quand la banque est la seule source disponible.

### 2.7 Familles de tables

| PrÃ©fixe | RÃ´le | Alimentation |
|---|---|---|
| `REF_*` | RÃ©fÃ©rentiels | Manuelle (`REF_Setup`) |
| `MASTER_REF_HA_*`, `MASTER_FACT_HA_*` | Faits/dimensions Hostaway | Automatique |
| `MASTER_FACT_MAN_*` | Faits saisis manuellement | Manuelle / semi-auto |
| `BRUT_Banque`, `NORM_Banque`, `IA_Classification` | Pipeline bancaire (brut, normalisÃ©, classification) | Semi-auto |
| `MASTER_CALC_*` | Tables calculÃ©es (payout, flux, rÃ©sultats) | DÃ©rivÃ©es |
| `MASTER_CTRL_*` | ContrÃ´les de cohÃ©rence | DÃ©rivÃ©es |

---

## 3. Vue d'ensemble des modules

```text
MODULE 0  - RÃ©fÃ©rentiels (REF_Setup)           [Ã€ PRÃ‰PARER]
MODULE 1  - Hostaway API                        [extraction existante, non validÃ©e]
MODULE 2  - RÃ©servations hors Hostaway
MODULE 3  - Charges perso / liquide / compte pro + acomptes
MODULE 4  - MÃ©nages
MODULE 5  - IK & avantages associÃ©s
MODULE 6  - Banque & rapprochement bancaire
MODULE 7  - Table de flux unifiÃ©e (MASTER_CALC_Flux)
MODULE 8  - RÃ©sultats rÃ©el / comptable / hors compta
MODULE 9  - ContrÃ´les de cohÃ©rence
MODULE 10 - Livrables propriÃ©taires / exports Excel (Power BI = utilisateur, hors lots)
```

Le Module 1 (Hostaway) est dÃ©jÃ  construit. Tous les autres dÃ©pendent du Module 0 et convergent vers le Module 7.

---

## 4. Sources d'entrÃ©e

| Source | Type | Alimentation | Ã‰tat |
|---|---|---|---|
| API Hostaway | API | Automatique | **OpÃ©rationnelle** (`hostaway_master_upsert_fast.py`) |
| `REF_Setup` | Excel maÃ®tre (19 onglets) | Manuelle | **OpÃ©rationnel** |
| CrÃ©dit Mutuel (banque pro) | Export Excel | Manuelle â†’ auto | Ã€ brancher (Module 6) |
| Factures fournisseurs | PDF â†’ Excel | Semi-auto | Ã€ brancher |
| Factures mÃ©nage externe | Excel standardisÃ© | Semi-auto | Ã€ brancher |
| Suivi mÃ©nage interne | Excel | Manuelle | Ã€ brancher |
| `M04_MENAGES_PowerQuery.xlsx` | Excel + Power Query actualisable | Semi-auto (refresh requis avant lecture) | En cours â€” produit `tbl_MASTER_FACT_MEN_Menages` (Â§11.4) |
| `2026_03_BRUT_Banque_CreditMutuel.xlsx` | Export bancaire brut CrÃ©dit Mutuel | Manuel â†’ pipeline banque | Ã€ brancher (Module 6) â€” dÃ©tail Â§13.6 |
| DÃ©penses terrain | Formulaire mobile | Manuelle | Ã€ brancher |
| Caisse espÃ¨ces | Excel caisse | Manuelle | Ã€ brancher |
| `SAISIE_Charges_Flux.xlsx` | **Source unique** des achats, charges, consommables, produits mÃ©nage, linge, lavage, matÃ©riel, charges perso/liquide, dÃ©penses perso sur compte pro | Manuelle | Ã€ construire (Lot 3) |
| Facturation propriÃ©taires | Excel / gÃ©nÃ©ration | Semi-auto | Ã€ brancher (livrable) |

**Sources manuelles strictement nÃ©cessaires** : rÃ©servations hors Hostaway (montant rÃ©ellement encaissÃ©/reversÃ©), charges perso/liquide (invisibles dans le compte pro), IK/avantages (dÃ©cision mÃ©tier), acomptes propriÃ©taires (rattachement facture), caisse espÃ¨ces, corrections manuelles.

**RÃ©fÃ©rentiel central `REF_Setup`** : source de vÃ©ritÃ© pour logements (dont `charge_fixe_mensuelle` par logement â€” D039), propriÃ©taires, associÃ©s, types de logement, taux de commission, coÃ»ts standards mÃ©nage (exÃ©cution, D037), types de flux, catÃ©gories de charges, codes impact (`IC`/`HC`/`HR`), **statuts de contrÃ´le (`REF_Statuts`, valeurs fermÃ©es â€” Â§23.1)**, modes de paiement, cartes/personnes, et mappings libellÃ©sâ†’logements.

### 4.1 Synchronisation GitHub â†’ OneDrive

Le dÃ©pÃ´t GitHub automatise les extractions Hostaway, mais Excel / Power Query doit lire les CSV depuis le dossier local OneDrive synchronisÃ©, **pas** depuis les artefacts temporaires de GitHub Actions.

```text
GitHub Actions met Ã  jour exports/hostaway/master/
â†’ le dÃ©pÃ´t local (dans OneDrive) est mis Ã  jour par git pull
â†’ Excel / Power Query lit les CSV locaux dans exports/hostaway/master/tables/
```

Le `git pull` peut Ãªtre manuel au dÃ©part, puis automatisÃ© (tÃ¢che planifiÃ©e Windows ou script PowerShell). Les requÃªtes Power Query pointent vers le dossier local synchronisÃ© pour rester utilisables sans ouvrir GitHub.

### 4.2 RÃ¨gles minimales des fichiers de saisie manuelle

| RÃ¨gle | Application attendue |
|---|---|
| Identifiant stable | une colonne `*_id` lisible, non recalculÃ©e Ã  chaque ouverture |
| Listes dÃ©roulantes | valeurs issues du `REF_Setup` : logement, propriÃ©taire, associÃ©, type de flux, mode de paiement, statut |
| Colonnes obligatoires | mises en Ã©vidence si vides |
| Statut de contrÃ´le | au minimum : Ã  contrÃ´ler / validÃ© / bloquant / ignorÃ© |
| DÃ©tection doublons | mise en forme conditionnelle sur les clÃ©s ou quasi-clÃ©s |
| ContrÃ´le montants | alertes si total perÃ§u, reversÃ©, acompte, charge ou avantage incohÃ©rents |
| Justificatif | rÃ©fÃ©rence ou lien pour les charges et remboursements |

S'applique en prioritÃ© aux rÃ©servations hors Hostaway, charges perso/liquide, acomptes propriÃ©taires, IK/avantages et caisse espÃ¨ces.

---

## 5. Tables de sortie attendues

### 5.1 Tables produites â€” Module Hostaway (run `20260523_005752` â€” **non validÃ©es sur donnÃ©es rÃ©elles**)

| Table | Lignes | PK |
|---|---|---|
| `MASTER_REF_HA_Listings` | 16 | `listingMapId` |
| `MASTER_FACT_HA_Reservations` | 1505 | `reservation_id` |
| `MASTER_FACT_HA_ReservationDetails` | 1505 | `reservation_id` |
| `MASTER_FACT_HA_ReservationFinanceFields` | 13367 | `reservation_id + financeField_name` |
| `MASTER_FACT_HA_ReservationFees` | 614 | `reservation_id + fee_id` |
| `MASTER_CALC_HA_Payout` | 1505 | `reservation_id` |
| `MASTER_CTRL_HA_Anomalies` | 28 | `reservation_id + code` |
| `MASTER_FACT_HA_CleaningTasks_Discovery` | 451 | `task_id` |
| `MASTER_RUN_Log` | 1 | `run_id` |

### 5.2 Tables Ã  construire

| Table | PK | Module |
|---|---|---|
| `MASTER_FACT_MAN_ReservationsHorsHostaway` | `reservation_hh_id` | 2 |
| `MASTER_CALC_Reservations` | `reservation_calc_id` | 2/7 (table commune) |
| `MASTER_FACT_MAN_Charges` | `charge_id` | 3 |
| `MASTER_FACT_MAN_AcomptesProprietaires` | `acompte_id` | 3 |
| `MASTER_FACT_MEN_Menages` | `task_id` / composite | 4 |
| `MASTER_FACT_MEN_MenagesExternes` | `menage_externe_id` | 4 (Lot 6c) |
| `MASTER_FACT_MAN_IK_Avantages` | `avantage_id` (saisie des flux associÃ©s uniquement) | 5 |
| `MASTER_CALC_AvantagesAssocies` | `personne_id + mois` (calculÃ©e) | 5 |
| `MASTER_FACT_MAN_Corrections` | `correction_id` | transverse |
| `MASTER_CALC_Flux` | `flux_id` | 7 |
| `MASTER_CALC_Resultats` | `mois + pÃ©rimÃ¨tre + vision` | 8 |
| `MASTER_CALC_NetProprietaire` | `proprietaire_id + logement_id + mois` ou `facture_id` | 10 |
| `MASTER_CALC_Commissions` | `reservation_id` / composite | 8 |
| `MASTER_CTRL_Coherence` | `source_pk + code_controle` | 9 |
| `FACT_FACTURE_ENTETE` | `facture_id` | 12 (D040) |
| `FACT_FACTURE_LIGNES` | `facture_id + ligne_num` | 12 (D040) |
| `BRUT_Banque` | `import_id + ligne_source` | 6 |
| `NORM_Banque` | `mouvement_id` | 6 |
| `IA_Classification` | `mouvement_id` | 6 |
| `CTRL_A_CONTROLER` | `mouvement_id + code` | 6 |
| `LOG_Traitement` | `run_id + Ã©tape` | 6 |
| `REF_Cloture_Mensuelle` | `mois` | **0** (structure crÃ©Ã©e au Lot 0 ; exploitÃ©e au Lot 8) |
| `REF_Statuts_Payout` | `statut_calcul_payout` | **0** (crÃ©Ã©e au Lot 0 â€” voir D021) |

> Les tables bancaires (`BRUT_Banque`, `NORM_Banque`, `IA_Classification`, `CTRL_A_CONTROLER`, `LOG_Traitement`) gardent les noms dÃ©jÃ  prÃ©vus pour le pipeline banque afin de rester compatibles avec les macros / scripts futurs (dÃ©tail Â§13).

### 5.3 RÃ©sultats consultables attendus

RÃ©sultat rÃ©el/comptable/hors compta par mois ; rÃ©sultat par logement, par propriÃ©taire, global ; net propriÃ©taire ; commission ; mÃ©nages retenus ; acomptes ; avantages bruts/nets par associÃ© ; charges perso/liquide ; anomalies bloquantes et Ã  contrÃ´ler.

---

## 6. Architecture Hostaway (Module 1 â€” extraction existante, non validÃ©e)

### 6.1 Tables et clÃ©s

| Table | ClÃ© | RÃ´le |
|---|---|---|
| `MASTER_REF_HA_Listings` | `listingMapId` | Logements Hostaway |
| `MASTER_FACT_HA_Reservations` | `reservation_id` | RÃ©servations |
| `MASTER_FACT_HA_ReservationDetails` | `reservation_id` | DÃ©tail JSON |
| `MASTER_FACT_HA_ReservationFinanceFields` | `reservation_id + financeField_name` | Champs financiers dÃ©taillÃ©s |
| `MASTER_FACT_HA_ReservationFees` | `reservation_id + fee_id` (fallback si absent) | Frais |
| `MASTER_CALC_HA_Payout` | `reservation_id` | Payout calculÃ© |
| `MASTER_CTRL_HA_Anomalies` | `reservation_id + code` | Anomalies |
| `MASTER_FACT_HA_CleaningTasks_Discovery` | `task_id` | TÃ¢ches mÃ©nage |

### 6.2 DonnÃ©es rÃ©elles observÃ©es

RÃ©partition par canal : `airbnbOfficial` 1335, `bookingcom` 110, `direct` 31, `vrboical` 32.
Champ dÃ©jÃ  calculÃ© `source_financiere_prevue` : HOSTAWAY_AIRBNB / HOSTAWAY_BOOKING / MANUEL_HORS_HOSTAWAY / A_CONTROLER.
Finance fields les plus frÃ©quents : `baseRate`, `totalPriceFromChannel`, `hostChannelFee`, `cleaningFee`, `totalPaid`, `airbnbPayoutSum`, `cityTax`, `otaPaymentProcessingFee`, `vat`.
Reservation fees : tous les `fee_id` renseignÃ©s au dernier run (fallback non sollicitÃ© mais Ã  conserver).

### 6.3 RÃ¨gle de performance (extraction incrÃ©mentale)

```text
si reservation_id inconnue                                   â†’ rÃ©cupÃ©rer le dÃ©tail API
si reservation_id connue mais updatedOn/latestActivityOn changÃ© â†’ rÃ©cupÃ©rer le dÃ©tail API
si reservation_id connue et inchangÃ©e                        â†’ rÃ©utiliser le dÃ©tail stockÃ©
```

MÃªme rÃ©sultat final avec moins d'appels API.

### 6.4 Traitement des statuts

| Statut | Traitement | Volume rÃ©el |
|---|---|---|
| `new` | Inclus | 1269 |
| `modified` | Inclus | 62 |
| `cancelled` | Exclu, **sauf contrÃ´le si montant/payout prÃ©sent** | 77 |
| `ownerStay` | **Exclu du rÃ©sultat**, Ã©ventuellement tracÃ© pour l'occupation | 9 |
| `inquiry`, `declined`, `expired`, `inquiryPreapproved`, `inquiryNotPossible` | Exclus | 88 |

---

## 7. RÃ¨gles de payout Hostaway

### 7.1 Airbnb (vÃ©rifiÃ©)

```text
PayoutPlateforme = airbnbExpectedPayoutAmount
   fallback : financeField[airbnbPayoutSum]
```

### 7.2 Booking (vÃ©rifiÃ©)

```text
PayoutPlateforme = financeField[totalPriceFromChannel]
                 - financeField[cityTax]
                 - financeField[otaPaymentProcessingFee]
                 - financeField[hostChannelFee]

fallback si financeField absents (marquÃ© moins fiable) :
PayoutPlateforme = totalPrice - taxe de sÃ©jour (reservationFees)
                 - payment charge (guestNote) - channelCommissionAmount
```

### 7.3 Direct / hors Hostaway

Hostaway n'est jamais la source financiÃ¨re. Il sert d'existence/planning ; le montant vient de la table manuelle (Â§9).

`InclureResultatAuto` (=1 si OK) et `StatutCalculPayout` pilotent l'inclusion automatique au rÃ©sultat.

### 7.4 Statuts de calcul payout â€” valeurs fermÃ©es (D021)

Le champ `statut_calcul_payout` dans `MASTER_CALC_HA_Payout` utilise les valeurs ci-dessous (rÃ©fÃ©rentiel `REF_Statuts_Payout`, crÃ©Ã© au Lot 0).

| `statut_calcul_payout` | Signification |
|---|---|
| `NORMAL` | Payout calculÃ©, rÃ©servation active |
| `ANNULE_SANS_PAYOUT` | AnnulÃ©e, aucun montant |
| `ANNULE_AVEC_PAYOUT` | AnnulÃ©e avec indemnitÃ© â†’ rÃ¨gle D030 s'applique |
| `PAYOUT_ABSENT` | RÃ©servation active sans payout calculable â†’ **BLOQUANT** |
| `PAYOUT_INCOMPLET` | Champs financiers partiels â†’ `A_CONTROLER` |
| `A_CONTROLER` | Cas non rÃ©solu (VRBO Unknown, direct sans montant) |

---

## 8. Commission et net propriÃ©taire

### 8.1 Assiette et commission (validÃ©)

Le `PayoutPlateforme` **inclut le mÃ©nage facturÃ© au voyageur**. L'assiette de commission s'obtient donc en retirant le mÃ©nage :

```text
Assiette       = PayoutPlateforme - MenageRetenu
CommissionGestion = Assiette Ã— TauxCommission        (REF_Proprietaires, 0,12 Ã  0,19)
```

### 8.2 Net propriÃ©taire

```text
NetProprietaire = PayoutPlateforme - MenageRetenu - CommissionGestion
                = (PayoutPlateforme - MenageRetenu) Ã— (1 - TauxCommission)
```

Exemple vÃ©rifiÃ© (Booking, taux 15 % illustratif) : payout 257,12 âˆ’ mÃ©nage 55 = assiette 202,12 â†’ commission 30,32 â†’ net propriÃ©taire 171,80.

### 8.3 Source du mÃ©nage retenu â€” POINT TECHNIQUE CRITIQUE

Le mÃ©nage Ã  soustraire **ne se trouve pas au mÃªme endroit selon le canal** :

| Canal | OÃ¹ lire le mÃ©nage | Constat sur les donnÃ©es |
|---|---|---|
| **Airbnb** (1335) | `MASTER_FACT_HA_ReservationFinanceFields` â†’ `financeField_name = cleaningFee` (1210/1252 renseignÃ©s) | La colonne `CleaningFee` de la table payout est **toujours vide** pour Airbnb |
| **Booking** (110) | Colonne `CleaningFee` de la table payout (95/110) | Utilisable directement |
| **VRBO / Direct** | Finance fields ou saisie manuelle | MÃ©nage non isolÃ© |

> âš ï¸ **Sans cette logique par canal, la commission Airbnb serait calculÃ©e sur une assiette incluant le mÃ©nage â†’ surcommission systÃ©matique sur ~88 % du volume.** Ã€ implÃ©menter avec une rÃ©cupÃ©ration du mÃ©nage canal par canal.

Distinction Ã  garder : le **mÃ©nage retenu** (facturÃ© au voyageur) sert au calcul propriÃ©taire ; le **coÃ»t rÃ©el mÃ©nage** (facture prestataire) sert au rÃ©sultat rÃ©el et au contrÃ´le d'Ã©cart (Â§11).

### 8.4 RÃ©servation annulÃ©e avec indemnitÃ© â€” `CancellationPayout` (D030)

Pour toute rÃ©servation avec `statut_calcul_payout = ANNULE_AVEC_PAYOUT` (`CancellationPayout > 0`) :

```text
BaseCommission         = CancellationPayout
CommissionConciergerie = CancellationPayout Ã— TauxCommission
NetProprietaire        = CancellationPayout âˆ’ CommissionConciergerie
```

**Aucun mÃ©nage n'est dÃ©duit** (pas de prestation rÃ©alisÃ©e). L'ancienne anomalie `CANCELLED_AVEC_MONTANT` (Ã  contrÃ´ler) devient une rÃ¨gle active via ce statut.

### 8.5 `revenu_net_exploitation_proprietaire` (D031)

Indicateur Ã©conomique pur. **Exclut impÃ©rativement** : avances, acomptes Airbnb versÃ©s Ã  la conciergerie, paiements dÃ©jÃ  reÃ§us, montants rÃ©glÃ©s par le propriÃ©taire, remboursements, rÃ©gularisations de trÃ©sorerie, achats exceptionnels, matÃ©riel exceptionnel, charges exceptionnelles non rÃ©currentes, ajustements ponctuels.

```text
CommissionConciergerie               = (TotalPayout âˆ’ MenageFacture) Ã— TauxCommission
revenu_net_exploitation_proprietaire = TotalPayout âˆ’ MenageFacture âˆ’ CommissionConciergerie âˆ’ charge_fixe_mensuelle
```

`charge_fixe_mensuelle` = montant rÃ©current facturÃ© contractuellement chaque mois (forfait logiciel, forfait consommables rÃ©current, forfait contractuel fixe). **ParamÃ©trable par propriÃ©taire/logement dans `REF_Logements`** (D039). Valeur = 0 si aucun forfait dÃ©fini. Jamais une charge exceptionnelle.

> Relation avec Â§8.2 : `revenu_net_exploitation = NetProprietaire âˆ’ charge_fixe_mensuelle`. Les deux indicateurs sont distincts et doivent coexister.

### 8.6 SÃ©paration exploitation / rÃ¨glement â€” deux blocs non communicants (D033)

**Bloc exploitation** (performance Ã©conomique â€” ne varie qu'avec le sÃ©jour et les tarifs) :

| Champ | Formule |
|---|---|
| `total_payout` | PayoutPlateforme |
| `menage_facture` | MÃ©nage retenu (Â§8.3) |
| `base_commission` | `total_payout âˆ’ menage_facture` |
| `taux_commission` | Taux dat? r?solu depuis `REF_Taux_Commission` |
| `commission_conciergerie` | `base_commission Ã— taux_commission` |
| `charge_fixe_mensuelle` | Forfait fixe contractuel |
| `revenu_net_exploitation_proprietaire` | `total_payout âˆ’ menage_facture âˆ’ commission_conciergerie âˆ’ charge_fixe_mensuelle` |

**Bloc rÃ¨glement / trÃ©sorerie** (mouvements de cash â€” ne modifie jamais le bloc exploitation) :

| Champ | Formule / source |
|---|---|
| `montant_du_conciergerie` | `commission_conciergerie + menage_facture + charge_fixe_mensuelle + charges_exceptionnelles_refacturees` |
| `aircover_recu_par_proprietaire_montant` | **Information uniquement** (D042/AC2) â€” montant AirCover perÃ§u directement par le propriÃ©taire. Ne modifie ni revenu net ni rÃ¨glement. |
| `aircover_recu_par_proprietaire_date` | Date Ã  laquelle le remboursement AirCover a Ã©tÃ© reÃ§u par le propriÃ©taire. |
| `aircover_recu_par_proprietaire_motif` | Motif du remboursement AirCover (description libre). |
| `acompte_conciergerie_recu_via_airbnb` | Versement Airbnb â†’ conciergerie uniquement (D032) |
| `autres_acomptes_conciergerie_recus` | Autres avances reÃ§ues |
| `paiement_deja_recu` | RÃ¨glements directs reÃ§us |
| `reste_a_payer_conciergerie` | `montant_du_conciergerie âˆ’ acomptes âˆ’ paiements` |
| `statut_reglement_conciergerie` | REF_Statuts (VALIDE / A_CONTROLER / BLOQUANT) |

`charges_exceptionnelles_refacturees` : modifie `montant_du_conciergerie` uniquement, **jamais** `revenu_net_exploitation_proprietaire` (D034).

---

## 9. RÃ©servations hors Hostaway (Module 2)

**Besoin confirmÃ©** : sur 31 `direct`, seules **13 sont des rÃ©servations payantes** (`new`), 9 `ownerStay` (montant 0, exclus), 7 `cancelled`, 2 `modified`. Les 32 VRBO sont en `paymentStatus = Unknown` (flag `A_CONTROLER`). **GranularitÃ© retenue : une ligne par rÃ©servation.**

### 9.1 Cas de figure rÃ©els

Une rÃ©servation hors Hostaway peut combiner : l'associÃ© rÃ©cupÃ¨re du liquide/virement ; il en reverse une partie au propriÃ©taire ; il garde la part commission + mÃ©nage ; l'argent peut servir Ã  payer un prestataire ; le solde devient un acompte sur facture.

### 9.2 Table `MASTER_FACT_MAN_ReservationsHorsHostaway`

Type de flux `RESERVATION_HORS_HOSTAWAY` (code impact dÃ©faut `HC`, sauf `comptabilisation` explicite).

| Colonne | RÃ´le |
|---|---|
| `reservation_hh_id` (PK) | ClÃ© sÃ©quentielle (`RESHH_0001`) |
| `ROW_HASH` | Hash de ligne |
| `mois` | Mois de rattachement |
| `proprietaire_id`, `logement_id` | Affectation : `logement_id` saisi ou mapp?, `proprietaire_id` d?riv? depuis `REF_Gestion_Logements_Hist` |
| `reservation_id_hostaway` | Lien optionnel si la rÃ©sa existe dans Hostaway |
| `date_arrivee`, `date_depart`, `nuits` | SÃ©jour |
| `total_percu` | Total rÃ©ellement encaissÃ© |
| `menage` | MÃ©nage retenu |
| `taux_commission`, `commission` | Taux et commission calculÃ©e |
| `montant_recupere`, `associe_id_recuperateur` | Montant rÃ©cupÃ©rÃ© et associÃ© concernÃ© (`REF_Associes`) |
| `montant_reverse_proprietaire` | ReversÃ© au propriÃ©taire |
| `acompte_facture` | Ã€ reprendre sur facture propriÃ©taire |
| `mode_paiement_id` | EspÃ¨ces / virement / autre (`REF_Modes_Paiement`) |
| `code_impact` | `HC` par dÃ©faut |
| `comptabilisation` | OUI/NON (passage en compta) |
| `statut_controle` | OK / Ã  contrÃ´ler / bloquant |
| `commentaire` | Note libre |

### 9.3 Formule d'acompte (validÃ©e, pÃ©rimÃ¨tre limitÃ©)

```text
AcompteFacture = TotalPercu - Menage - Commission - MontantReverseProprietaire
```

> Applicable **uniquement** aux rÃ©servations hors Hostaway, oÃ¹ chaque composant est saisi manuellement. **Ne pas rÃ©utiliser comme calcul d'acompte gÃ©nÃ©rique** : sur Hostaway, mÃ©nage et commission ne sont pas isolÃ©s de la mÃªme faÃ§on (Â§8.3). Cette formule doit Ãªtre contrÃ´lÃ©e automatiquement.

### 9.4 Avantage associÃ© liÃ©

Le `montant_recupere` alimente les avantages bruts de l'associÃ©. Si ce montant sert Ã  payer une charge, celle-ci rÃ©duit l'avantage net :

```text
Montant rÃ©cupÃ©rÃ© = 100   â†’  avantage brut +100
Charge payÃ©e avec  = 100   â†’  dÃ©duction      -100
Avantage net       =   0
```

### 9.5 Table commune des rÃ©servations â€” `MASTER_CALC_Reservations`

**Objectif.** Consolider **toutes** les rÃ©servations (Hostaway, hors Hostaway, VRBO manuelles, manuelles hors plateforme) sous un schÃ©ma unique, pour empÃªcher tout double comptage avant dÃ©versement dans `MASTER_CALC_Flux`.

Cette table ne crÃ©e pas de donnÃ©es par elle-mÃªme : elle rÃ©concilie les sources existantes.

| Colonne | RÃ´le |
|---|---|
| `reservation_calc_id` (PK) | Identifiant consolidÃ© (`RES-AAAA-MM-{SOURCE}-{COMPTEUR}`) |
| `ROW_HASH` | Hash de ligne |
| `source` | `HOSTAWAY_AIRBNB` / `HOSTAWAY_BOOKING` / `HOSTAWAY_VRBO` / `HOSTAWAY_DIRECT` / `MANUEL_HORS_HOSTAWAY` |
| `reservation_id_hostaway` | Lien Hostaway si applicable |
| `reservation_hh_id` | Lien table manuelle si applicable |
| `logement_id`, `proprietaire_id` | Affectation |
| `date_arrivee`, `date_depart`, `nuits` | SÃ©jour |
| `montant_retenu` | Montant qui alimentera `MASTER_CALC_Flux` (un seul par rÃ©servation) |
| `source_montant` | `HOSTAWAY` / `MANUEL` / `MANUEL_VRBO` |
| `code_impact` | `IC` / `HC` selon la source |
| `statut_controle` | ValidÃ© / Ã  contrÃ´ler / bloquant |
| `commentaire` | |

**RÃ¨gles de rÃ©conciliation.**
- Une rÃ©servation `direct` Hostaway avec `totalPrice > 0` **et** une ligne hors Hostaway liÃ©e par `reservation_id_hostaway` : la table commune retient une seule ligne, `source_montant = MANUEL`.
- Une rÃ©servation VRBO `paymentStatus = Unknown` : tant que le montant n'est pas renseignÃ© manuellement, la ligne est `statut_controle = A_CONTROLER` et n'alimente pas `MASTER_CALC_Flux`.
- Une rÃ©servation Hostaway Airbnb / Booking sans contrepartie manuelle : `source_montant = HOSTAWAY`, alimentation directe.

**ContrÃ´les dÃ©diÃ©s.**
- `RESERVATION_DOUBLON_HOSTAWAY_HH` (bloquant) : `reservation_id_hostaway` rattachÃ© Ã  2+ lignes sans lien explicite.
- `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH` (Ã  contrÃ´ler) : `direct` Hostaway avec `totalPrice > 0` mais aucune ligne hors Hostaway â†’ vÃ©rifier si saisie manquante.
- `RESERVATION_VRBO_MONTANT_NON_RENSEIGNE` (Ã  contrÃ´ler) : VRBO `Unknown` sans saisie manuelle.

**Position dans le pipeline.** `MASTER_FACT_HA_Reservations` + `MASTER_FACT_MAN_ReservationsHorsHostaway` â†’ `MASTER_CALC_Reservations` â†’ (`MASTER_CALC_Reservations_Resolues`) â†’ `MASTER_CALC_Flux`. La table commune est le seul point d'entrÃ©e des rÃ©servations dans le flux.

### 9.6 Historique des rÃ©servations clÃ´turÃ©es et rÃ©solution de source (D097 / D098)

**But.** Ne pas dÃ©pendre de l'API Hostaway pour l'historique des mois clÃ´turÃ©s. Source de vÃ©ritÃ© figÃ©e + protection contre la perte de rÃ©servations cÃ´tÃ© API.

**Trois Ã©tages (bloc rÃ©servations, aprÃ¨s `lot4bis` qui reste live-only) :**

| Ã‰tage | Script | Sortie | RÃ´le |
|---|---|---|---|
| Historisation | `lot4ter_historiser_reservations_cloturees.py` | `HIST_Reservations_Cloturees.xlsx` | Archive **toutes** les rÃ©servations validÃ©es des mois `CLOTURE` (tous canaux). Upsert sans suppression sur clÃ© stable (`reservation_id_hostaway`, sinon `reservation_hh_id`). |
| RÃ©solution | `lot4quater_resoudre_source_reservations.py` | `MASTER_CALC_Reservations_Resolues.xlsx` (MASTER + VUE_FLUX) | Mois ouvert = live (`MASTER_CALC_Reservations`) ; mois `CLOTURE` = HIST. Sortie unique consommÃ©e par lot9/10/11/12. |
| Consommation | `lot9`, `lot10`, `lot11`, `lot12` | â€” | Lisent `MASTER_CALC_Reservations_Resolues`. **Ne portent plus la bascule open/closed.** |

**RÃ¨gles clÃ©s.**
- ClÃ´ture pilotÃ©e **uniquement** par `REF_Cloture_Mensuelle.statut_mois = CLOTURE` (pas de rÃ¨gle automatique Â« mois < courant Â»). REF vide â‡’ tout reste live.
- Date de rÃ©fÃ©rence = **check-in** ; `mois` dÃ©rivÃ© du check-in.
- HIST **prime** l'extract pour un mois clÃ´turÃ© ; rÃ©servation disparue de l'API aprÃ¨s clÃ´ture = conservÃ©e/rÃ©injectÃ©e ; Ã©cart live vs HIST = **alerte** (jamais d'Ã©crasement silencieux).
- Mois `CLOTURE` sans ligne HIST â‡’ repli live flaggÃ© `MOIS_CLOTURE_SANS_HISTORIQUE` (non bloquant).
- Valeurs : `source_ligne`/`source_montant = HIST_RESERVATIONS_CLOTUREES`, `methode = HIST_PRIME_MOIS_CLOTURE`, `origine_initiale âˆˆ {API_HOSTAWAY, SAISIE_HH, BACKFILL_VRBO, CORRECTION_VALIDEE}`, `canal âˆˆ {AIRBNB, BOOKING, VRBO, DIRECT, HH}`.

**Backfill VRBO (D098).** Origine de correction ponctuelle dans l'historique (pas une table ni une branche VRBO). Commission VRBO : `assiette = payout âˆ’ coÃ»t mÃ©nage standard`, `commission = assiette Ã— taux`, branche dÃ©diÃ©e dans lot10 (jamais routÃ© en HH).

**Pipeline complet rÃ©servations :** `MASTER_FACT_HA_Reservations` + `MASTER_FACT_MAN_ReservationsHorsHostaway` â†’ `MASTER_CALC_Reservations` (live, lot4bis) â†’ `lot4ter` (HIST mois clÃ´turÃ©s) â†’ `lot4quater` (rÃ©solution) â†’ `MASTER_CALC_Reservations_Resolues` â†’ `MASTER_CALC_Flux`.

---

## 10. Charges perso / liquide / compte pro (Module 3)

### 10.1 Fichier de saisie `SAISIE_Charges_Flux.xlsx` â†’ table `MASTER_FACT_MAN_Charges`

> `SAISIE_Charges_Flux.xlsx` est la **source unique** de toutes les charges, achats, consommables, produits mÃ©nage, linge/lavage, matÃ©riel, charges perso/liquide, dÃ©penses perso sur compte pro (D026). Ce fichier exclut IK et virements associÃ©s.

Types de flux : `DEPENSE_PERSO_COMPTE_PRO`, `CHARGE_PAYEE_PERSO_OU_LIQUIDE`, `PAIEMENT_PRESTATAIRE_LIQUIDE`, `ACHAT_MENAGE`, `FRAIS_LOCAL`, `CHARGE_EXCEPTIONNELLE_REFACTURABLE`, **`INCIDENT_VOYAGEUR`** (D041 â€” `reservation_id` obligatoire), **`PRESTATION_AIRCOVER_REFACTUREE`** (D042 â€” gestion sinistre facturÃ©e au propriÃ©taire).

| Colonne | RÃ´le |
|---|---|
| `charge_id` (PK) | Identifiant parlant, nomenclature Â§16.2 (`CHG-2026-04-HC-WAFA-CB-001`) |
| `ROW_HASH` | Hash de ligne |
| `date_charge`, `mois` | Date et mois |
| `associe_id` | AssociÃ© (`REF_Associes`) |
| `montant` | Montant TTC |
| `categorie_charge_id` | `REF_Categories_Charges` (20 catÃ©gories) |
| `type_flux_id` | `REF_Types_Flux` |
| `mode_paiement_id` | Banque pro / espÃ¨ces / carte / compte perso (`REF_Modes_Paiement`) |
| `carte_id` | `REF_Cartes_Paiement` si applicable |
| `logement_id`, `proprietaire_id` | Affectation |
| `affectation_type` | Logement / propriÃ©taire / global / non affectable (`REF_Types_Affectation`) |
| `code_impact` | `IC` / `HC` / `HR` |
| `prise_en_compta` | OUI / NON |
| `paye_avec_montant_recupere` | Lien rÃ©servation HH si l'argent vient d'un montant rÃ©cupÃ©rÃ© |
| `lien_virement_banque` | Lien optionnel avec un mouvement bancaire |
| `refacturable` | DÃ©faut `REF_Categories_Charges`, surchargeable |
| `justificatif` | Oui / non / lien |
| `statut_controle` | OK / Ã  contrÃ´ler / bloquant |
| `commentaire` | Note |

`REF_Categories_Charges` prÃ©-remplit via `impact_resultat`, `refacturable_defaut`, `hors_compta_defaut`.

### 10.2 Effets mÃ©tier

| Cas | RÃ©sultat rÃ©el | RÃ©sultat comptable | Avantages |
|---|---|---|---|
| Charge sociÃ©tÃ© payÃ©e par compte perso | Diminue | Non, sauf `prise_en_compta = OUI` | RÃ©duit l'avantage net |
| Paiement prestataire en liquide | Diminue | Non, sauf instruction contraire | RÃ©duit l'avantage net si payÃ© avec argent rÃ©cupÃ©rÃ© |
| DÃ©pense perso sur compte pro | Diminue le rÃ©sultat concernÃ© | Oui (banque pro) | Augmente l'avantage brut |
| Remboursement associÃ© | Neutralise selon lien d'origine | Selon code impact | Peut neutraliser avantage/charge |

### 10.3 ClÃ©

Identifiant parlant gÃ©nÃ©rÃ© Ã  la saisie, suivant la nomenclature unique du Â§16.2 : `CHG-AAAA-MM-IMPACT-ASSOCIE/MODE-COMPTEUR` (ex. `CHG-2026-04-HC-WAFA-CB-001`). **Ã‰viter** une clÃ© date+montant+catÃ©gorie (deux charges identiques peuvent exister le mÃªme jour).

### 10.4 Acomptes propriÃ©taires â€” `MASTER_FACT_MAN_AcomptesProprietaires`

Type de flux `TYPE_FLUX_006 = ACOMPTE_FACTURE_PROPRIETAIRE` (`HC`). Rattachement Ã  une facture obligatoire (`facture_ref`). `report_mois_suivant` supprimÃ© du Lot 5 (diffÃ©rÃ© Lot 10/12 â€” D061). GranularitÃ© : `proprietaire_id + logement_id + mois + facture_ref + source_acompte`.

**Structure : 18 colonnes SAISIE + 4 colonnes PQ = 22 colonnes MASTER**

| # | Colonne | Bloc | RÃ¨gle |
|---|---|---|---|
| 1 | `acompte_id` (PK) | Identification | `ACC-AAAA-MM-NNN` â€” stable â€” reset 001/mois â€” saisi manuellement (D063) |
| 2 | `ROW_HASH` | Identification | Hash ligne |
| 3 | `mois` | Rattachement | `YYYY-MM` extrait de `acompte_id` |
| 4 | `proprietaire_id` | Rattachement | DV REF_Proprietaires â€” BLOQUANT si absent |
| 5 | `logement_id` | Rattachement | DV REF_Logements â€” A_CONTROLER si absent |
| 6 | `facture_ref` | Rattachement | `FAC-AAAA-MM-PROP-NNN` â€” BLOQUANT si absent pour ligne VALIDE (D059) |
| 7 | `source_acompte` | Rattachement | `HH_RESERVATION` / `VIREMENT_DIRECT` / `AUTRE` (D058) |
| 8 | `source_hh_id` | Rattachement | `reservation_hh_id` si `HH_RESERVATION` / null sinon (D064) |
| 9 | `montant_acompte` | Financier | `= acompte_facture` (HH) / saisi sinon â€” BLOQUANT si â‰¤ 0 |
| 10 | `report_mois_precedent` | Financier | Informatif â€” non calculÃ© au Lot 5 â€” A_CONTROLER si sans commentaire (D061) |
| 11 | `mode_paiement_id` | Mode | DV REF_Modes_Paiement |
| 12 | `code_impact` | Impact | `HC` fixe |
| 13 | `impact_resultat_reel` | Impact | `OUI` fixe (HC â†’ OUI) |
| 14 | `impact_resultat_comptable` | Impact | `NON` fixe (HC â†’ NON) |
| 15 | `statut_controle` | Statut | `VALIDE` / `A_CONTROLER` / `EXCLU_RESULTAT` |
| 16 | `niveau_anomalie` | Statut | `BLOQUANT` / `A_CONTROLER` / `INFO` |
| 17 | `code_anomalie` | Statut | Premier code dÃ©tectÃ© par prioritÃ© |
| 18 | `commentaire` | Statut | Libre â€” obligatoire si `report_mois_precedent` renseignÃ© |
| 19 | `source_module` | SystÃ¨me PQ | `LOT5_ACOMPTES_PROPRIETAIRES` |
| 20 | `source_table` | SystÃ¨me PQ | `SAISIE_AcomptesProprietaires` (toujours â€” D064) |
| 21 | `source_pk` | SystÃ¨me PQ | `acompte_id` (toujours â€” D064) |
| 22 | `date_integration` | SystÃ¨me PQ | `DateTime.LocalNow()` |

ContrÃ´les BLOQUANT (5) : `ACOMPTE_NON_RATTACHE_FACTURE` / `ACOMPTE_HH_INCOHERENT` / `ACOMPTE_CALC_ID_DUPLIQUE` / `ACOMPTE_PROPRIETAIRE_ABSENT` / `ACOMPTE_MONTANT_INVALIDE`.
ContrÃ´les A_CONTROLER (5) : `ACOMPTE_LOGEMENT_ABSENT` / `ACOMPTE_SOURCE_HH_INTROUVABLE` / `ACOMPTE_REPORT_INCOHERENT` / `ACOMPTE_SOURCE_A_CONTROLER` / `ACOMPTE_FACTURE_REF_FORMAT_INVALIDE`.
Sources : `SAISIE_AcomptesProprietaires.xlsx` (toutes lignes) + `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx` (ref croisÃ©e HH, filtre VALIDE + acompte_facture > 0 â€” D058/D064).

### 10.5 TraÃ§abilitÃ© du liquide et caisse thÃ©orique

Le liquide n'est pas un avantage par dÃ©faut : il doit Ãªtre traÃ§able par **origine** et par **usage**.

| Origine | Usage | Impact |
|---|---|---|
| RÃ©servation hors Hostaway (paiement espÃ¨ces) | Montant conservÃ© par associÃ© | Avantage brut |
| Remboursement / ajustement documentÃ© | Paiement prestataire | Charge HC ou comptabilisable selon `prise_en_compta`, dÃ©duit de l'avantage net |
| | Reversement propriÃ©taire | Diminue le montant conservÃ© / impacte acompte ou net propriÃ©taire |
| | Acompte facture | RattachÃ© Ã  une facture propriÃ©taire |
| | Solde non utilisÃ© | Reste en caisse thÃ©orique, Ã  contrÃ´ler |

ContrÃ´le minimal :

```text
Solde liquide thÃ©orique = liquide rÃ©cupÃ©rÃ© âˆ’ liquide reversÃ© âˆ’ liquide utilisÃ© en charge âˆ’ liquide affectÃ© en acompte
```

Toute ligne de liquide porte un lien d'origine (`reservation_hh_id` si possible) et un usage. Une ligne sans usage est tolÃ©rÃ©e temporairement mais apparaÃ®t en contrÃ´le.

---

### 10.6 Corrections manuelles â€” `MASTER_FACT_MAN_Corrections`

Table transverse pour les ajustements validÃ©s qui ne relÃ¨vent d'aucune autre table : rÃ©gularisation d'un Ã©cart constatÃ©, neutralisation, correction d'imputation. Chaque correction est tracÃ©e et porte un code impact.

| Colonne | RÃ´le |
|---|---|
| `correction_id` (PK) / `ROW_HASH` | Nomenclature Â§16.2 |
| `mois` | Mois de rattachement |
| `cible_module`, `cible_pk` | Ligne ou pÃ©rimÃ¨tre corrigÃ© |
| `montant`, `sens` | Montant positif + PRODUIT / CHARGE / NEUTRALISATION |
| `code_impact` | `IC` / `HC` / `HR` |
| `motif` | Justification obligatoire |
| `statut_controle` | ValidÃ© / Ã  contrÃ´ler |

Une correction n'est jamais silencieuse : motif obligatoire et visible en contrÃ´le.

---

## 11. MÃ©nages (Module 4)

### 11.1 Trois notions distinctes

| Notion | Source | Usage |
|---|---|---|
| MÃ©nage facturÃ© au voyageur (retenu) | Hostaway / hors Hostaway | Calcul propriÃ©taire & payout |
| CoÃ»t standard mÃ©nage (exÃ©cution) | `REF_Couts_Standards_Menage` â€” **standards rebasÃ©s sur l'exÃ©cution uniquement (D037)**. Valeurs actuelles (Studio 29, T2 39, T3 55, T4 69, T6/Duo 110 â‚¬) Ã  revalider au Lot 0 | ContrÃ´le d'Ã©cart exÃ©cution vs standard |
| CoÃ»t rÃ©el mÃ©nage | Facture prestataire / suivi interne | RÃ©sultat rÃ©el & Ã©cart |

Le prix de mÃ©nage Hostaway **n'est pas** le coÃ»t rÃ©el prestataire.

### 11.2 Deux sources et logique de comptage

TÃ¢ches Hostaway (451, endpoint `/tasks`) : 450/451 ont un `reservationId`, 451/451 un `listingMapId`, mais **22/451 seulement ont un `cost`**. Statuts : `completed` 280, `cancelled` 72, `confirmed` 72, `pending` 27.

`REF_Types_Lignes_Menage` : `MENAGE_STANDARD` et `REMISE_EN_ETAT` comptent comme mÃ©nage ; `FRAIS_DEPLACEMENT` jamais (mais rÃ©partissable) ; `LINGE` et `ACHAT_PRODUIT` non comptÃ©s.

> **DÃ©cision (validÃ©e par les donnÃ©es)** : le `cost` Hostaway Ã©tant vide Ã  95 %, **Hostaway sert au comptage, pas Ã  la valorisation**. Le coÃ»t rÃ©el vient des factures / suivi interne.

### 11.3 Table `MASTER_FACT_MEN_Menages`

PK = `task_id` (Hostaway), sinon composite `logement_id + date_menage + intervenant_id` (+ suffixe si collision). Colonnes : `reservation_id`, `logement_id`, `proprietaire_id`, `intervenant_id` (`REF_Intervenants`), `date_menage`, `type_ligne_menage_id`, `compte_comme_menage`, `cout_reel`, `cout_standard`, `ecart_cout`, `source`, `statut_controle`.

### 11.4 Fichier de production `M04_MENAGES_PowerQuery.xlsx`

**Emplacement**

```text
C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\02_DONNEES_NORMALISEES\menages\M04_MENAGES_PowerQuery.xlsx
```

**PÃ©rimÃ¨tre aprÃ¨s dÃ©cision D027 â€” IRRÃ‰VOCABLE.** M04 traite uniquement :

- la main-d'Å“uvre de mÃ©nage directe (heures Ã— taux horaire) ;
- le rangement (main-d'Å“uvre opÃ©rationnelle uniquement â€” D038 : si le rangement inclut un achat, du linge, du matÃ©riel, des consommables ou un coÃ»t exceptionnel, il sort de M04 vers `SAISIE_Charges_Flux.xlsx`) ;
- la comparaison avec le coÃ»t standard d'exÃ©cution mÃ©nage (`REF_Couts_Standards_Menage`).

M04 **ne contient plus** : onglet `achats`, colonne `CoÃ»t du lavage`, colonne `Courses`, heures de courses, consommables, linge, matÃ©riel, forfait local 50 â‚¬. Ces postes passent par `SAISIE_Charges_Flux.xlsx`. Le coÃ»t complet mÃ©nage (exÃ©cution + charges) est reconstruit hors M04 via `VUE_ACHATS_MENAGE_VALIDES` (Â§11.6, D028).

**Statut.** Ce classeur n'est pas une source brute. C'est un fichier de transformation actualisable. La requÃªte Power Query `tbl_MASTER_FACT_MEN_Menages` produit la table mÃ©tier exploitable. Les lignes ne se modifient pas Ã  la main. La requÃªte doit Ãªtre actualisÃ©e avant toute exploitation.

**GranularitÃ©.** Table mensuelle agrÃ©gÃ©e par `mois Ã— intervenant Ã— appartement`, alimentÃ©e par le Google Sheet `Suivi mÃ©nage` (main-d'Å“uvre). Niveau distinct du comptage Hostaway (par tÃ¢che, Â§11.2).

**Sources amont (simplifiÃ©es).** Google Sheet `Suivi mÃ©nage` â€” onglet principal uniquement : heures, nombre de mÃ©nages, intervenant, mois, appartement, Rangement. L'onglet `achats` du Google Sheet est **retirÃ©** (D027).

**SchÃ©ma attendu du Google Sheet `Suivi mÃ©nage` (onglet principal).**

| Colonne | Type | Obligatoire | Notes |
|---|---|---|---|
| `Mois` | texte ou numÃ©rique | OUI | Power Query convertit en numÃ©rique |
| `AnnÃ©e` | numÃ©rique | OUI | |
| `Intervenant` | texte | OUI | RÃ©solu via `REF_Intervenants` (`INTERNE` attendu) |
| Une colonne par appartement | numÃ©rique (nb mÃ©nages) | au moins une | DÃ©pivotÃ©es. LibellÃ©s mappÃ©s dans `REF_Mapping_Logements` (Lot 2). |
| `Rangement` | numÃ©rique | NON | Devient une ligne `Type = Rangement` aprÃ¨s dÃ©pivotage |
| `Nombre d'heures` | numÃ©rique | OUI | Heures de main-d'Å“uvre directe |

> `Courses` et `CoÃ»t du lavage` sont **supprimÃ©s** du schÃ©ma source. Si ces colonnes subsistent dans le Google Sheet, elles sont ignorÃ©es Ã  l'import et ne participent Ã  aucun calcul M04.

**Comportement si le schÃ©ma amont change.**
- Colonne appartement renommÃ©e â†’ `MENAGE_SANS_LOGEMENT_ID` (bloquant Lot 11).
- Colonne obligatoire absente â†’ `M04_SCHEMA_SOURCE_INVALIDE` (bloquant).
- Nouvelle colonne appartement â†’ ajouter Ã  `REF_Mapping_Logements` avant actualisation.

**Formules mÃ©tier (simplifiÃ©es, M04 = exÃ©cution uniquement).**

```text
CoÃ»t d'exÃ©cution mÃ©nage  = Nombre d'heures Ã— TAUX_HORAIRE_MENAGE_INTERNE
Prix d'exÃ©cution unitaire = CoÃ»t d'exÃ©cution mÃ©nage / Nombre de mÃ©nages
TotalRangement           = Î£ coÃ»t d'exÃ©cution des lignes Type = Rangement
Ecart_standard           = REF_Couts_Standards_Menage âˆ’ Prix d'exÃ©cution unitaire   (informatif)
Total_execution          = Nombre de mÃ©nages Ã— Prix d'exÃ©cution unitaire
```

`TAUX_HORAIRE_MENAGE_INTERNE` : actuellement 10 â‚¬/h (codÃ© en dur, Ã  migrer vers `REF_Parametres_Generaux`).

> **Plus de quote-part Courses / forfait local dans M04.** La quote-part par mÃ©nage et le coÃ»t complet sont calculÃ©s hors M04 (Â§11.6).

**Colonnes attendues en sortie.**

| Colonne | Source |
|---|---|
| `hostaway_listing_id` | Via `REF_Mapping_Logements` |
| `Mois`, `AnnÃ©e` | Google Sheet |
| `Intervenant`, `Appartement`, `Type` | Google Sheet dÃ©pivotage |
| `Nombre de mÃ©nages`, `Nombre d'heures` | Google Sheet |
| `cout_execution_unitaire` | CoÃ»t exÃ©cution / Nb mÃ©nages |
| `cout_standard` | `REF_Couts_Standards_Menage` |
| `ecart_execution_vs_standard` | `cout_standard âˆ’ cout_execution_unitaire` (informatif) |
| `total_execution` | `Nombre de mÃ©nages Ã— cout_execution_unitaire` |
| `menage_calc_id` | ClÃ© composite |
| `statut_controle`, `ROW_HASH` | ContrÃ´le et upsert |

**ClÃ© et upsert.**

```text
menage_calc_id = MEN-{AAAA-MM}-{hostaway_listing_id ou APP_SANITIZED}-{INTERVENANT}-{compteur}
ex. MEN-2026-04-480140-IMENE-001
```

**Alimentation de `MASTER_CALC_Flux`** (Lot 9) :

| Champ `MASTER_CALC_Flux` | Valeur |
|---|---|
| `source_module` | `MENAGES_INTERNES` |
| `source_table` | `tbl_MASTER_FACT_MEN_Menages` |
| `source_pk` | `menage_calc_id` |
| `date_flux` | Dernier jour du mois |
| `mois` | `AnnÃ©e` + `Mois` |
| `logement_id` | Via `REF_Mapping_Logements` |
| `type_flux_id` | `COUT_EXECUTION_MENAGE_INTERNE` |
| `sens` | `CHARGE` |
| `montant` | `total_execution` |
| `code_impact` | `HC` **obligatoire** |

**ContrÃ´le dÃ©diÃ©.** `MENAGE_INTERNE_CODE_IMPACT_NON_HC` (bloquant). `M04_SCHEMA_SOURCE_INVALIDE` (bloquant).

### 11.5 MÃ©nages externes â€” `MASTER_FACT_MEN_MenagesExternes`

**Statut.** Module **Ã  construire** au Lot 6c. La source initiale est constituÃ©e des **factures PDF des prestataires de mÃ©nage externes**, transformÃ©es par IA dans le format structurÃ© ci-dessous. La saisie ligne par ligne par l'humain n'est pas prÃ©vue ; l'IA produit la table.

**GranularitÃ© obligatoire.** 1 ligne = 1 mÃ©nage Ã— 1 appartement Ã— 1 date Ã— 1 prestataire. La rÃ©fÃ©rence facture est conservÃ©e, mais la table de travail est dÃ©taillÃ©e â€” jamais agrÃ©gÃ©e Ã  la facture.

**SchÃ©ma cible.**

| Colonne | RÃ´le |
|---|---|
| `menage_externe_id` (PK) | Nomenclature : `MENEXT-{AAAA-MM}-{prestataire}-{COMPTEUR}` |
| `ROW_HASH` | Hash de ligne |
| `facture_id` | Identifiant de la facture d'origine (groupe de lignes) |
| `date_facture` | **Date administrative/comptable** de la facture prestataire (suivi fournisseur, compta). |
| `date_menage` | **Date de prestation** (date rÃ©elle d'exÃ©cution du mÃ©nage) â€” **pilote le rattachement Ã©conomique** (mois/logement/rÃ©servation, ME5). Peut Ãªtre diffÃ©rente de `date_facture`. |
| `mois`, `annee` | Mois de rattachement (`date_menage` fait foi, jamais `date_facture`) |
| `prestataire_id`, `nom_prestataire` | `REF_Intervenants` |
| `type_intervenant` | `EXTERNE` obligatoire ici |
| `logement_id`, `hostaway_listing_id` | Via `REF_Mapping_Logements` |
| `appartement_source` | LibellÃ© brut tel qu'il apparaÃ®t sur la facture |
| `type_ligne_menage_id` | `REF_Types_Lignes_Menage` |
| `nombre_menages` | Compteur |
| `montant_ligne_ht`, `montant_ligne_ttc` | Montant unitaire ligne |
| `montant_facture_total_ht`, `montant_facture_total_ttc` | Montant total facture (sert au contrÃ´le de rÃ©conciliation) |
| `code_impact` | **`IC` par dÃ©faut**, sÃ©lectionnable `IC` / `HC` / `HR` |
| `prise_en_compta` | `OUI` / `NON` |
| `statut_controle` | ValidÃ© / Ã  contrÃ´ler / bloquant |
| `source_document`, `nom_fichier_source` | Trace du PDF d'origine |
| `commentaire` | |

**RÃ¨gles.**
- Code impact : **`IC` par dÃ©faut**. Modifiable ligne par ligne (jamais par dÃ©faut Ã  `HC`).
- `type_intervenant` doit Ãªtre `EXTERNE` sur toute ligne de cette table. Une ligne avec `type_intervenant = INTERNE` doit aller dans M04, pas ici.
- RÃ©conciliation : `Î£(montant_ligne_ttc) â‰ˆ montant_facture_total_ttc` (tolÃ©rance arrondis), sinon `MENAGE_EXTERNE_FACTURE_NON_RECONCILIEE`.

**Alimentation de `MASTER_CALC_Flux`.**

| Champ `MASTER_CALC_Flux` | Valeur issue de la table |
|---|---|
| `source_module` | `MENAGES_EXTERNES` |
| `source_table` | `MASTER_FACT_MEN_MenagesExternes` |
| `source_pk` | `menage_externe_id` |
| `date_flux` | `date_menage` |
| `mois` | DÃ©rivÃ© de `date_menage` |
| `logement_id` | Via `REF_Mapping_Logements` |
| `type_flux_id` | `COUT_REEL_MENAGE_EXTERNE` |
| `sens` | `CHARGE` |
| `montant` | `montant_ligne_ttc` |
| `code_impact` | Selon la ligne (`IC` par dÃ©faut) |

**ContrÃ´les dÃ©diÃ©s** (cf. `PLAN_CONSTRUCTION.md` Lot 6c) : `MENAGE_EXTERNE_LOGEMENT_ABSENT`, `MENAGE_EXTERNE_DATE_ABSENTE`, `MENAGE_EXTERNE_PRESTATAIRE_INCONNU`, `MENAGE_EXTERNE_CODE_IMPACT_ABSENT`, `MENAGE_EXTERNE_FACTURE_NON_RECONCILIEE`, `MENAGE_EXTERNE_A_VENTILER`.

> **Ã€ confirmer avant Lot 6c.** Format rÃ©el des factures PDF prestataires (1 Ã  2 factures anonymisÃ©es suffisent Ã  figer le pipeline d'extraction IA).

### 11.6 CoÃ»t complet mÃ©nage hors M04 â€” `VUE_ACHATS_MENAGE_VALIDES` (D028)

> M04 produit uniquement le coÃ»t d'exÃ©cution (main-d'Å“uvre). Le coÃ»t complet mÃ©nage est reconstruit dans le flux analytique global Ã  partir de deux sources distinctes. Aucun double comptage n'est possible car les sources sont exclusives.

**`VUE_ACHATS_MENAGE_VALIDES`**

Vue dÃ©rivÃ©e de `MASTER_FACT_MAN_Charges` (alimentÃ©e par `SAISIE_Charges_Flux.xlsx`). Filtre les lignes oÃ¹ :

```text
type_charge IN ('LINGE', 'CONSOMMABLE_MENAGE', 'PRODUIT_MENAGE', 'MATERIEL_MENAGE', 'FRAIS_LOCAL')
ET statut_controle = 'VALIDE'
```

Dimensions disponibles : `logement_id`, `proprietaire_id`, `mois`, `montant`, `code_impact`, `associe_id`.

Cette vue ne remplace pas M04. Elle complÃ¨te le coÃ»t analytique au niveau du flux unifiÃ© (`MASTER_CALC_Flux`, Lot 9).

**Reconstruction du coÃ»t complet analytique (Lot 9 / Lot 10)**

```text
CoÃ»t complet mÃ©nage interne (analytique) par logement Ã— mois :
  = Î£ total_execution de tbl_MASTER_FACT_MEN_Menages (M04)
  + Î£ montant de VUE_ACHATS_MENAGE_VALIDES pour le mÃªme logement Ã— mois
```

Cette reconstruction est un calcul de lecture/reporting (Power BI / Power Query). Elle ne modifie pas `MASTER_CALC_Flux` directement.

**RÃ¨gle de cohÃ©rence.**

ContrÃ´le `ACHATS_DEJA_EN_SAISIE_CHARGES` (remplace `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL`) : si un poste de charge prÃ©sent dans `SAISIE_Charges_Flux.xlsx` est aussi injectÃ© depuis M04, il est signalÃ© en doublon.

---

## 12. IK & avantages (Module 5)

### 12.1 Logique

Chaque type de flux porte des drapeaux (`avantage_brut_defaut`, `deduit_avantage_defaut`) :

| Type de flux | Avantage brut | DÃ©duit avantage |
|---|---|---|
| `VIREMENT_ASSOCIE` | OUI | NON |
| `DEPENSE_PERSO_COMPTE_PRO` | OUI | NON |
| `RESERVATION_HORS_HOSTAWAY` (montant rÃ©cupÃ©rÃ©) | OUI | NON |
| `CHARGE_PAYEE_PERSO_OU_LIQUIDE` | NON | OUI |
| `REMBOURSEMENT_ASSOCIE` | NON (neutralise, `HR`) | NON |
| `PAIEMENT_PRESTATAIRE_LIQUIDE` | NON | OUI |

Trois sources d'avantage brut : virement reÃ§u sur compte perso ; dÃ©pense perso sur compte pro ; `montant_recupere` des rÃ©servations hors Hostaway. Une partie est **dÃ©rivÃ©e** des modules 2 et 3 (pas de double saisie). Le strict manuel : virements associÃ©s sans dÃ©tail (Â« seul le total compte Â») et IK kilomÃ©triques.

### 12.2 SÃ©paration saisie / calcul (deux tables)

Il ne faut pas mÃ©langer les lignes saisies et le rÃ©sultat calculÃ©, sous peine de double comptage. Deux tables distinctes.

**Table de saisie `MASTER_FACT_MAN_IK_Avantages`** â€” uniquement les flux associÃ©s non disponibles ailleurs : virements associÃ©s sans dÃ©tail, IK en montant direct, avances, corrections validÃ©es. Les dÃ©penses perso sur compte pro, les charges payÃ©es pour la sociÃ©tÃ© et les montants rÃ©cupÃ©rÃ©s hors Hostaway **ne sont pas ressaisis ici** s'ils existent dÃ©jÃ  dans leurs tables sources.

| Colonne | RÃ´le |
|---|---|
| `avantage_id` (PK) / `ROW_HASH` | Identifiant stable (nomenclature Â§16.2) |
| `mois` | Mois de rattachement |
| `associe_id` | AssociÃ© (`REF_Associes`) |
| `type_flux` | IK / virement associÃ© / avance / correction |
| `nature` | Description courte (ex. trajet, avance, virement mensuel) |
| `montant` | Montant saisi directement â€” pas de barÃ¨me auto (D036) |
| `code_impact` | `IC` / `HC` / `HR` |
| `impact_resultat_reel` | OUI / NON |
| `impact_resultat_comptable` | OUI / NON |
| `commentaire` | Note libre |
| `lien_origine` | Lien optionnel vers banque ou justificatif |
| `statut_controle` | REF_Statuts (VALIDE / A_CONTROLER / BLOQUANT / IGNORE_JUSTIFIE) |

**Table calculÃ©e `MASTER_CALC_AvantagesAssocies`** â€” consolide par associÃ© et par mois en empilant les sources (virements, dÃ©penses perso compte pro, montants rÃ©cupÃ©rÃ©s HH, IK), puis dÃ©duit les charges payÃ©es pour la sociÃ©tÃ©.

| Colonne | RÃ´le |
|---|---|
| `PK` = `personne_id + mois` | ClÃ© de synthÃ¨se |
| `avantages_bruts` | Virements + dÃ©penses perso compte pro + montants rÃ©cupÃ©rÃ©s HH + IK |
| `charges_payees_pour_societe` | Charges payÃ©es par l'associÃ© pour la sociÃ©tÃ© |
| `avantages_nets` | `avantages_bruts âˆ’ charges_payees_pour_societe` |
| `detail_sources` | Liste ou lien vers les lignes sources |

```text
Avantage net = Avantages bruts (+ IK) âˆ’ Charges payÃ©es pour la sociÃ©tÃ© âˆ’ Remboursements neutralisateurs
```

IK : montant direct (D036). BarÃ¨me kilomÃ©trique pourra Ãªtre ajoutÃ© plus tard â€” non bloquant.

---

## 13. Banque & rapprochement bancaire (Module 6)

### 13.1 Objectif

Rapprocher les mouvements du compte pro avec : payouts plateformes, virements associÃ©s, dÃ©penses perso sur compte pro, remboursements, abonnements logiciels, factures fournisseurs, autres charges.

### 13.2 Pipeline cible (stack rÃ©elle)

```text
1. VBA importe + normalise + applique les rÃ¨gles manuelles
2. Les lignes A_ENVOYER_IA restent dans NORM_Banque
3. Un script/macro exporte ces lignes en JSON/CSV propre
4. Claude Code lit ce fichier localement
5. Claude Code gÃ©nÃ¨re un fichier rÃ©sultat JSON/Excel
6. Une macro importe le rÃ©sultat dans IA_Classification et CTRL_A_CONTROLER
```

### 13.3 Tables du pipeline bancaire

| Table | RÃ´le | ClÃ© |
|---|---|---|
| `BRUT_Banque` | Copie brute de l'export importÃ© | `import_id + ligne_source` |
| `NORM_Banque` | Mouvements normalisÃ©s (date, libellÃ© nettoyÃ©, montant, sens, compte, empreinte) | `mouvement_id` |
| `IA_Classification` | Classification IA des lignes non reconnues | `mouvement_id` |
| `CTRL_A_CONTROLER` | Lignes douteuses Ã  valider | `mouvement_id + code` |
| `LOG_Traitement` | Historique des imports et traitements | `run_id + Ã©tape` |

Colonnes principales de `NORM_Banque` : `mouvement_id` (PK), `ROW_HASH`, `date_operation`, `date_valeur`, `libelle`, `montant`, `sens` (dÃ©bit/crÃ©dit), `compte_id`, `tiers_detecte`, `categorie`, `type_flux_id`, `code_impact`, `source_classification` (rÃ¨gle/IA/manuel), `statut_controle`.

### 13.4 Logique anti-doublon

Chaque mouvement reÃ§oit une empreinte technique pour Ã©viter les doublons entre exports qui se chevauchent. Si la banque fournit un identifiant stable, il prime ; sinon l'empreinte sert de clÃ© de dÃ©duplication :

```text
empreinte = date_operation + date_valeur + montant + sens + libellÃ©_normalisÃ© + compte
```

**Distinction BRUT vs NORM (pÃ©riode).** `BRUT_Banque` contient **toutes** les lignes importÃ©es depuis le fichier brut, telles quelles, sans filtre temporel â€” mÃªme celles qui dÃ©bordent du mois nominal. `NORM_Banque` ne contient que les **lignes rattachÃ©es Ã  la pÃ©riode rÃ©elle** concernÃ©e (mois courant), normalisÃ©es et filtrÃ©es. Le rattachement temporel se fait **toujours par les colonnes `Date` ou `Valeur`**, jamais par le nom du fichier. Toutes les lignes de la pÃ©riode doivent Ãªtre validÃ©es avant clÃ´ture du mois.

### 13.4 bis Banque et table de flux : Ã©viter le double comptage

`NORM_Banque` n'alimente `MASTER_CALC_Flux` comme produit ou charge que lorsque la banque est la **source Ã©conomique principale**. Quand un flux est dÃ©jÃ  portÃ© par Hostaway, une rÃ©servation hors Hostaway, une charge manuelle ou une rÃ¨gle rÃ©currente, la banque sert au rapprochement.

| Mouvement bancaire | Traitement dans `MASTER_CALC_Flux` |
|---|---|
| Virement Airbnb/Booking rapprochÃ© Ã  `MASTER_CALC_HA_Payout` | pas de produit bancaire ; rapprochement uniquement |
| DÃ©pense fournisseur inconnue du systÃ¨me | crÃ©e une charge si validÃ©e |
| DÃ©pense perso sur compte pro | crÃ©e un avantage brut + charge selon catÃ©gorie |
| Virement associÃ© | crÃ©e une ligne d'avantage / IK si non dÃ©jÃ  saisie |
| Remboursement associÃ© | neutralisation ou contrÃ´le selon lien d'origine |
| Paiement d'une charge dÃ©jÃ  saisie manuellement | rapprochement uniquement, sauf si la ligne manuelle Ã©tait prÃ©visionnelle |

RÃ¨gle obligatoire pour empÃªcher le double comptage des produits et charges.

### 13.5 Place de l'IA

L'IA ne traite que les lignes restantes aprÃ¨s les rÃ¨gles dÃ©terministes. Elle **n'Ã©crase jamais** une rÃ¨gle manuelle validÃ©e ; une correction humaine peut au contraire **devenir** une nouvelle rÃ¨gle dÃ©terministe.

> Module autonome : il ne bloque pas le cÅ“ur du systÃ¨me, mais devient prioritaire dÃ¨s que les tables Charges, IK/Avantages et Payout sont stabilisÃ©es, car il permet de vÃ©rifier versements plateformes, dÃ©penses perso sur compte pro et virements associÃ©s.

### 13.6 Source brute observÃ©e â€” `2026_03_BRUT_Banque_CreditMutuel.xlsx`

**Emplacement officiel**

```text
C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\01_SOURCES_BRUTES\Banque\2026_03_BRUT_Banque_CreditMutuel.xlsx
```

**Statut.** Source brute bancaire CrÃ©dit Mutuel. **Ne jamais modifier le fichier.** Toutes les transformations produisent des tables dÃ©rivÃ©es (`BRUT_Banque`, `NORM_Banque`, `IA_Classification`, `CTRL_A_CONTROLER`, `LOG_Traitement`).

**Compte concernÃ©**

| Ã‰lÃ©ment | Valeur |
|---|---|
| Compte | C/C EUROCOMPTE PRO WONDERBNB |
| RIB | 10278 02211 00021321603 |
| `compte_id` retenu | `CM_02211_00021321603` |
| Devise | EUR |

**Feuilles du classeur**

| Feuille | RÃ´le | Utilisable pour les calculs ? |
|---|---|---|
| `Vos comptes` | SynthÃ¨se (RIB, solde, situation au 25/04/2026) | Non â€” identification uniquement |
| `Cpt 02211 00021321603` | **Mouvements bancaires exploitables** | **Oui â€” feuille mÃ©tier** |
| `hidden_data` | MÃ©tadonnÃ©es techniques de l'export (`Range A6:E137`) | Non |
| `hidden` | RÃ©sidu de modÃ¨le d'export | Non |

**Structure de la feuille mÃ©tier `Cpt 02211 00021321603`**

| Ã‰lÃ©ment | Valeur observÃ©e |
|---|---|
| Ligne d'en-tÃªte | Ligne 5 |
| PremiÃ¨re ligne de mouvements | Ligne 6 |
| Colonnes brutes | `Date`, `Valeur`, `LibellÃ©`, `DÃ©bit`, `CrÃ©dit`, `Solde`, `Dev` |
| Nombre de mouvements exploitables | 132 |
| PremiÃ¨re date observÃ©e | 25/02/2026 |
| DerniÃ¨re date observÃ©e | 25/04/2026 |
| Total dÃ©bits observÃ©s | 11 055,83 â‚¬ |
| Total crÃ©dits observÃ©s | 11 955,26 â‚¬ |

> **RÃ¨gle nom de fichier â‰  pÃ©riode rÃ©elle.** Le fichier s'appelle `2026_03_...` mais couvre du **25/02/2026 au 25/04/2026**. Le rattachement temporel doit **toujours** se faire par les colonnes `Date` ou `Valeur`, jamais par le nom du fichier. Ce constat motive le contrÃ´le `BANQUE_FICHIER_PERIODE_INCOHERENTE` (Â§18.3).

**Lignes Ã  ignorer Ã  l'import** : lignes d'introduction de compte, en-tÃªtes non mouvement, lignes totalement vides, ligne de solde final, ligne "Liste de vos comptes", feuilles `hidden` / `hidden_data`. Une ligne bancaire exploitable doit avoir au minimum : `Date` non vide, `LibellÃ©` non vide, `DÃ©bit` ou `CrÃ©dit` renseignÃ©, devise renseignÃ©e ou dÃ©ductible.

**Identifiants recommandÃ©s pour ce fichier**

```text
import_id     = IMP-BQ-CM-2026-03-001
compte_id     = CM_02211_00021321603
mouvement_id  = MVT-{compte_id}-{YYYYMMDD}-{sens}-{montant_centimes}-{hash_court}
ex.           = MVT-CM_02211_00021321603-20260315-DEBIT-2590-A1B2C3
```

Le `2026-03` de l'`import_id` correspond au mois nominal du fichier, **pas** Ã  la pÃ©riode rÃ©elle des mouvements.

**Conventions de normalisation** (cohÃ©rentes avec Â§13.2 et Â§14.3)

```text
Si DÃ©bit renseignÃ©  : sens = DEBIT  ; montant = |DÃ©bit|
Si CrÃ©dit renseignÃ© : sens = CREDIT ; montant = CrÃ©dit
```

Cas Ã  contrÃ´ler : DÃ©bit et CrÃ©dit simultanÃ©s, les deux vides, montant nul, montant non numÃ©rique (cf. Â§18.3).

**Familles de libellÃ©s observÃ©es et rÃ¨gle de classification**

| Famille de libellÃ© | Usage probable | RÃ¨gle |
|---|---|---|
| `VIR AIRBNB PAYMENTS LUXEMBOU...` | Versement Airbnb | **Rapprochement Hostaway, jamais nouveau produit** (Â§13.4 bis) |
| `PAIEMENT CB ... CARTE 8259` | Carte compte pro | Charge ou dÃ©pense perso compte pro selon tiers |
| `PRLV SEPA ...` | PrÃ©lÃ¨vement SEPA | Charge rÃ©currente Ã  classifier |
| `VIR INST WAFA SOUCI...` | Virement instantanÃ© associÃ© | Avantage / IK / remboursement Ã  classifier |
| `VIR INST ...` (reÃ§u) | Encaissement non plateforme | Ã€ classifier (caution, propriÃ©taire, associÃ©, autre) |
| `FRAIS ...` | Frais bancaires | Charge bancaire si validÃ©e |
| `IMPAYE ...` | ImpayÃ© / retour | Ã€ contrÃ´ler |

Aucune rÃ¨gle dÃ©finitive n'est dÃ©duite du seul texte tant qu'elle n'est pas inscrite dans un rÃ©fÃ©rentiel de classification bancaire.

---

## 14. Table de flux unifiÃ©e (Module 7)

### 14.1 RÃ´le

`MASTER_CALC_Flux` empile toutes les lignes Ã©conomiques : Hostaway, rÃ©servations hors Hostaway, banque, charges perso/liquide, mÃ©nages, IK & avantages, acomptes, corrections.

### 14.2 Structure

| Colonne | RÃ´le |
|---|---|
| `flux_id` (PK) / `ROW_HASH` | |
| `source_module`, `source_table`, `source_pk` | TraÃ§abilitÃ© vers la ligne d'origine |
| `date_flux`, `mois` | |
| `logement_id`, `proprietaire_id`, `associe_id` | Dimensions d'affectation |
| `type_flux_id`, `categorie` | Nature |
| `sens` | **PRODUIT / CHARGE / NEUTRALISATION** |
| `montant` | **Toujours positif** |
| `code_impact` | `IC` / `HC` / `HR` |
| `inclure_resultat_comptable` | OUI/NON (dÃ©rivÃ© du code impact) |
| `inclure_resultat_hors_compta` | OUI/NON (dÃ©rivÃ©) |
| `inclure_resultat_reel` | OUI/NON (dÃ©rivÃ©) |
| `statut_controle`, `commentaire` | |

### 14.3 Convention de calcul (retenue : la plus robuste)

```text
montant = toujours positif
sens    = PRODUIT | CHARGE | NEUTRALISATION
Resultat = Somme(PRODUITS) - Somme(CHARGES)
```

Montant positif + sens explicite rend les contrÃ´les plus lisibles et Ã©vite les erreurs de signe. Les trois colonnes `inclure_resultat_*` sont prÃ©-calculÃ©es depuis `code_impact` pour faciliter l'exploitation Power BI.

---

## 15. RÃ©sultats rÃ©el / comptable / hors compta (Module 8)

### 15.1 Codes impact (pivot)

| Code | Signification | Comptable | Hors compta | RÃ©el |
|---|---|---:|---:|---:|
| `IC` | Intra-comptable | Oui | Non | Oui |
| `HC` | Hors compta / extra | Non | Oui | Oui |
| `HR` | Hors rÃ©sultat | Non | Non | Non |

- **Comptable** = flux `IC`.
- **Hors compta** = flux `HC`.
- **RÃ©el (pilotage)** = `IC` + `HC` (tout sauf `HR`). Vision par dÃ©faut (`REF_Parametres_Generaux` â†’ `resultat_par_defaut = PILOTAGE`). DÃ©marrage `2026-03`.

### 15.2 Table `MASTER_CALC_Resultats`

Dimensions : mois, logement, propriÃ©taire, activitÃ© globale, vision (rÃ©el/comptable/hors compta). Mesures : produits, charges, rÃ©sultat, commission, mÃ©nage, net propriÃ©taire, avantages associÃ©s, anomalies bloquantes.

### 15.3 Vision par associÃ©

Le rÃ©sultat global n'est **pas** dÃ©coupÃ© par associÃ©. Les avantages sont consultables par associÃ© dans une vue dÃ©diÃ©e. Sont exclus du rÃ©sultat : statuts non productifs et `ownerStay`.

---

## 16. ClÃ©s de liaison

### 16.1 ClÃ©s primaires

| Table | PK | VÃ©rifiÃ© |
|---|---|---|
| Listings Hostaway | `listingMapId` | OUI |
| RÃ©servations / DÃ©tail / Payout | `reservation_id` | OUI |
| Finance fields | `reservation_id + financeField_name` | OUI |
| Reservation fees | `reservation_id + fee_id` (fallback) | OUI (0 fallback au dernier run) |
| Anomalies | `reservation_id + code` | OUI |
| TÃ¢ches mÃ©nage | `task_id` | OUI |
| Calendrier | `listingMapId + date` | (non fourni) |
| RÃ©servations hors Hostaway | `reservation_hh_id` | Ã€ crÃ©er |
| Charges | `charge_id` | Ã€ crÃ©er |
| Acomptes | `acompte_id` | Ã€ crÃ©er |
| IK & avantages saisis | `avantage_id` | Ã€ crÃ©er |
| Avantages associÃ©s calculÃ©s | `personne_id + mois` | Ã€ crÃ©er |
| Net propriÃ©taire | `proprietaire_id + logement_id + mois` ou `facture_id` | Ã€ crÃ©er |
| Banque normalisÃ©e | `mouvement_id` (empreinte si pas d'ID stable) | Ã€ crÃ©er |
| Flux unifiÃ© | `flux_id` | Ã€ crÃ©er |

### 16.2 Nomenclature des identifiants manuels

Les identifiants manuels sont lisibles et stables, et ne dÃ©pendent pas du seul montant (deux flux identiques peuvent exister le mÃªme jour).

```text
TYPE-AAAA-MM-IMPACT-ASSOCIE/MODE-COMPTEUR
```

Exemples : `FLUX-2026-04-HC-EWAN-LIQ-001`, `CHG-2026-04-HC-WAFA-CB-001`, `RESHH-2026-04-HC-DIRECT-001`, `ACOMPTE-2026-04-PROP001-APP002-001`.

| Ã‰lÃ©ment | Exemple | RÃ´le |
|---|---|---|
| `TYPE` | `FLUX`, `CHG`, `RESHH`, `ACOMPTE`, `AVTG` | nature de la ligne |
| `AAAA-MM` | `2026-04` | pÃ©riode de rattachement |
| `IMPACT` | `IC`, `HC`, `HR` | impact comptable / hors compta / hors rÃ©sultat |
| associÃ© / mode | `EWAN`, `WAFA`, `LIQ`, `CB` | lecture rapide |
| compteur | `001`, `002` | Ã©vite les collisions |

Obligatoire pour les tables manuelles ; les tables Hostaway conservent les IDs API comme clÃ©s.

### 16.3 Rapprochements

```text
REF_Logements.hostaway_listing_id â”€â”€â–º MASTER_REF_HA_Listings.listingMapId
REF_Logements.logement_id â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º toutes les tables MAN_* (affectation)
REF_Gestion_Logements_Hist.proprietaire_id ??????? REF_Proprietaires.proprietaire_id
REF_Logements.type_logement_id â”€â”€â”€â”€â”€â–º REF_Types_Logements / REF_Couts_Standards_Menage
REF_Mapping_Logements â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º rÃ©solution libellÃ©s sources â†’ logement_id
toute charge/flux â”€â”€â–º REF_Categories_Charges / REF_Types_Flux / REF_Codes_Impact
```

`REF_Mapping_Logements` (81 lignes) relie les libellÃ©s hÃ©tÃ©rogÃ¨nes (Hostaway `listingMapId`/`listingName`, libellÃ©s factures mÃ©nage, noms courts, adresses) Ã  un `logement_id` unique avec un `niveau_confiance`. C'est le maillon qui rattache une facture mÃ©nage au bon logement.

---

## 17. Livrables propriÃ©taires (Module 10)

> RÃ¨gle centrale : **Revenu net d'exploitation = performance Ã©conomique du logement.** **Solde Ã  payer = rÃ¨glement rÃ©el.** Ces deux notions sont **strictement sÃ©parÃ©es** (D033).

### 17.1 Bloc exploitation (par rÃ©servation ou agrÃ©gÃ© par mois)

```text
base_commission                      = total_payout âˆ’ menage_facture
commission_conciergerie              = base_commission Ã— taux_commission
revenu_net_exploitation_proprietaire = total_payout âˆ’ menage_facture âˆ’ commission_conciergerie âˆ’ charge_fixe_mensuelle
```

Cas particulier â€” annulation avec indemnitÃ© (D030) :
```text
base_commission         = CancellationPayout
commission_conciergerie = CancellationPayout Ã— taux_commission
revenu_net_exploitation = CancellationPayout âˆ’ commission_conciergerie   (charge_fixe_mensuelle non dÃ©duite par rÃ©servation)
```

### 17.2 Bloc rÃ¨glement / trÃ©sorerie

```text
montant_du_conciergerie = commission_conciergerie + menage_facture + charge_fixe_mensuelle + charges_exceptionnelles_refacturees
reste_a_payer           = montant_du_conciergerie âˆ’ acompte_conciergerie_recu_via_airbnb âˆ’ autres_acomptes_recus âˆ’ paiement_deja_recu
```

`charges_exceptionnelles_refacturees` impacte le `montant_du_conciergerie` uniquement â€” **jamais** `revenu_net_exploitation_proprietaire`.

### 17.3 Format de la facture propriÃ©taire (12 lignes obligatoires)

La facture affiche sÃ©parÃ©ment et dans cet ordre :

| Ligne | Champ | Bloc |
|---|---|---|
| 1 | Total payout | Exploitation |
| 2 | MÃ©nage facturÃ© | Exploitation |
| 3 | Commission conciergerie | Exploitation |
| 4 | Charge fixe mensuelle | Exploitation |
| 5 | **Revenu net d'exploitation propriÃ©taire** | Exploitation |
| 6 | Montant total dÃ» Ã  la conciergerie | RÃ¨glement |
| 7 | Acompte reÃ§u via Airbnb | RÃ¨glement |
| 8 | Autres paiements dÃ©jÃ  reÃ§us | RÃ¨glement |
| 9 | **Reste Ã  payer Ã  la conciergerie** | RÃ¨glement |
| 10 | Charges / achats exceptionnels refacturÃ©s (hors revenu net) | RÃ¨glement |
| 11 | Acomptes propriÃ©taires (rÃ©servations hors Hostaway) | RÃ¨glement |
| 12 | Statut rÃ¨glement | RÃ¨glement |

L'acompte issu des rÃ©servations hors Hostaway (Â§9.3) apparaÃ®t en ligne 11, **sans dÃ©tailler toute l'origine**, mais le lien de contrÃ´le est conservÃ© en interne.

**Note D042 (AirCover) â€” non inscrite comme ligne de facture** : les Ã©ventuels champs `aircover_recu_par_proprietaire_montant`, `_date` et `_motif` apparaissent en encadrÃ© d'information sur l'Excel de contrÃ´le uniquement (pas dans les 12 lignes), pour rappeler au propriÃ©taire qu'un remboursement plateforme lui a Ã©tÃ© versÃ© directement. Il ne modifie ni le bloc exploitation ni le bloc rÃ¨glement (AC2).

### 17.4 Structure de sortie facture (D040)

La facture propriÃ©taire produit les sorties logiques suivantes (mise en forme visuelle dÃ©cidÃ©e au Lot 12) :

| Table / sortie | RÃ´le |
|---|---|
| Excel de contrÃ´le | Feuille rÃ©capitulative par mois / propriÃ©taire / logement |
| `FACT_FACTURE_ENTETE` | Identifiants, `proprietaire_id`, `logement_id`, `mois`, `statut_generation`, dates, totaux blocs exploitation et rÃ¨glement |
| `FACT_FACTURE_LIGNES` | Les 12 lignes de Â§17.3 avec `type_ligne`, libellÃ©, montant, `bloc` (EXPLOITATION / REGLEMENT) |
| `statut_generation` | BROUILLON / VALIDE / EMIS / ANNULE (REF_Statuts) |
| Future sortie PDF | **Aucun PDF propriÃ©taire produit au dÃ©marrage** (D040/P11). Les champs et tables sont conÃ§us dÃ¨s maintenant pour qu'un PDF puisse Ãªtre gÃ©nÃ©rÃ© au Lot 12 sans refactoring, mais la production PDF n'est pas un livrable des lots initiaux. |

La structure logique des 12 lignes (Â§17.3) est verrouillÃ©e et doit Ãªtre respectÃ©e avant toute mise en forme visuelle.

---

## 18. ContrÃ´les de cohÃ©rence (Module 9)

### 18.1 En place (Hostaway)

`MASTER_CTRL_HA_Anomalies` : `CANCELLED_AVEC_MONTANT` (rÃ¨gle active via D030 â€” `statut_calcul_payout = ANNULE_AVEC_PAYOUT`), `BOOKING_PAYOUT_INCOMPLET` (bloquant).

### 18.2 Bloquants

> Codes Ã  l'identique de `JOURNAL_CONTROLES.md` (registre faisant foi).

| Code | Module | Pourquoi |
|---|---|---|
| `PK_MANQUANTE_OU_DOUBLONNEE` | Transverse | Upsert impossible |
| `BOOKING_PAYOUT_INCOMPLET` | Hostaway | RÃ©servation Booking active sans payout calculable |
| `ACOMPTE_NON_RATTACHE_FACTURE` | Acomptes | Acompte sans `facture_ref` |
| `MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES` | HH / Avantages | `montant_recupere` non reflÃ©tÃ© dans les avantages |
| `CHARGE_LOGEMENT_SANS_LOGEMENT_ID` | Charges | Charge affectation LOGEMENT sans `logement_id` |
| `CHARGE_PERSO_SANS_ASSOCIE` | Charges | Charge perso/liquide sans `associe_id` |
| `RESERVATION_HH_SANS_PROPRIETAIRE` | HH | RÃ©servation HH sans `proprietaire_id` |
| `ACOMPTE_HH_INCOHERENT` | HH | Acompte â‰  Total âˆ’ MÃ©nage âˆ’ Commission âˆ’ ReversÃ© |
| `MENAGE_SANS_LOGEMENT_ID` | M04 / MÃ©nages | Appartement mÃ©nage non rattachÃ© Ã  un logement |
| `MENAGE_INTERNE_CODE_IMPACT_NON_HC` | M04 | Ligne M04 avec `code_impact` â‰  `HC` |
| `BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY` | Banque | Tentative double comptage banque â†” Hostaway (Â§13.4 bis) |
| `BANQUE_DATE_INEXPLOITABLE` | Banque | **Date ET Valeur absentes ou toutes deux non parsables** â†’ ligne inutilisable |
| `BANQUE_DEBIT_CREDIT_VIDES` | Banque | DÃ©bit et CrÃ©dit simultanÃ©ment vides |
| `BANQUE_DEBIT_CREDIT_DOUBLES` | Banque | DÃ©bit et CrÃ©dit simultanÃ©ment renseignÃ©s |
| `BANQUE_MONTANT_NON_NUMERIQUE` | Banque | Montant non convertible |
| `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Ligne non classÃ©e ouverte â†’ mois non clÃ´turable |
| `M04_SCHEMA_SOURCE_INVALIDE` | M04 | Colonne obligatoire absente / requÃªte PQ Ã©choue |
| `RESERVATION_DOUBLON_HOSTAWAY_HH` | Table commune | `reservation_id_hostaway` rattachÃ© Ã  2+ lignes |
| `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL` | ObsolÃ¨te | **RemplacÃ© par `ACHATS_DEJA_EN_SAISIE_CHARGES`** |
| `ACHATS_DEJA_EN_SAISIE_CHARGES` | M04 / Charges | Charge prÃ©sente dans `SAISIE_Charges_Flux` aussi injectÃ©e depuis M04 |
| `ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION` | Exploitation | `acompte_conciergerie_recu_via_airbnb` comptabilisÃ© dans le revenu net |
| `ACHAT_EXCEPTIONNEL_INCLUS_NET_EXPLOITATION` | Exploitation | Achat ou charge exceptionnelle inclus dans `revenu_net_exploitation` |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | Exploitation | Charge non rÃ©currente dans `charge_fixe_mensuelle` |
| `PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT` | Exploitation | Paiement dÃ©duit du `total_payout` au lieu du `reste_a_payer` |
| `CONFUSION_PAYOUT_SOLDE_FACTURE` | Exploitation | Confusion entre `total_payout`, `montant_du_conciergerie`, `reste_a_payer` |
| `INCIDENT_VOYAGEUR_SANS_RESERVATION` | Incidents | Ligne `INCIDENT_VOYAGEUR` sans `reservation_id` (D041/IV2) |
| `AIRCOVER_CONFONDU_AVEC_PAYOUT` | AirCover | Montant AirCover apparaÃ®t dans `total_payout` (D042/AC5) |

### 18.3 Ã€ contrÃ´ler (non bloquants)

| Code | Module | Cas rÃ©el / raison |
|---|---|---|
| `LISTING_ORPHELIN_A_CONTROLER` | Hostaway | `listingMapId 515523` dans l'export, absent du REF |
| `REFERENTIEL_ORPHELIN` | REF | Logement `sur_hostaway=OUI` absent de l'export |
| `VRBO_MONTANT_NON_RENSEIGNE` | VRBO | RÃ©servation VRBO Unknown sans saisie manuelle |
| `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH` | Table commune | `direct` Hostaway avec `totalPrice > 0`, pas de ligne HH |
| `CANCELLED_AVEC_MONTANT` | Hostaway | RÃ©servation annulÃ©e avec montant â†’ rÃ¨gle D030 s'applique |
| `BANQUE_LIGNE_SANS_DATE` | Banque | **Colonne Date vide** mais colonne Valeur prÃ©sente et parsable â†’ non bloquant |
| `BANQUE_FICHIER_PERIODE_INCOHERENTE` | Banque | Dates rÃ©elles hors pÃ©riode nominale du nom de fichier |
| `BANQUE_DEVISE_NON_EUR` | Banque | Devise â‰  EUR |
| `DOUBLON_BANCAIRE_POTENTIEL` | Banque | Empreinte bancaire dÃ©jÃ  connue |
| `LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Aucune rÃ¨gle ni classification fiable |
| `IA_CONFIANCE_INSUFFISANTE` | Banque | Score de confiance IA sous le seuil |
| `MENAGE_SANS_COUT_STANDARD` | MÃ©nages | Type ou coÃ»t standard absent â†’ contrÃ´le Ã©cart impossible |
| `MENAGE_ECART_NEGATIF_IMPORTANT` | MÃ©nages | Ã‰cart coÃ»t rÃ©el vs standard > seuil |
| `MENAGE_DOUBLON_POTENTIEL` | MÃ©nages | MÃªme mois Ã— logement Ã— intervenant en double |
| `MENAGE_STATUT_NON_VALIDE` | M04 | Ligne non validÃ©e, exclue du calcul |
| `TYPE_INTERVENANT_ABSENT` | MÃ©nages | Intervenant sans type (prioritaire sur identitÃ© exacte) |
| `AIRCOVER_NON_TRACE` | AirCover | Ã‰vÃ©nement AirCover documentÃ© sans ligne associÃ©e (D042/AC5) |

### 18.4 Table `MASTER_CTRL_Coherence`

Colonnes : `PK` (`source_pk + code_controle`), `source_module`, `source_pk`, `code_controle`, `severity` (bloquant/Ã  contrÃ´ler/information), `message`, `statut_resolution` (ouvert/corrigÃ©/ignorÃ© justifiÃ©), `commentaire`.

### 18.5 ContrÃ´les de saisie dans les fichiers manuels

Faits directement en Excel (formules, mise en forme conditionnelle, validation de donnÃ©es, Power Query), sans macro complexe tant que des rÃ¨gles simples suffisent.

| Fichier | ContrÃ´les minimums |
|---|---|
| RÃ©servations hors Hostaway | doublon `logement_id + date_arrivee + total_percu` ; acompte incohÃ©rent ; propriÃ©taire manquant ; montant rÃ©cupÃ©rÃ© sans associÃ© |
| Charges perso/liquide | doublon `date + montant + tiers + personne` ; charge sans affectation ; charge sans justificatif ; `prise_en_compta` vide |
| IK & avantages | associÃ© manquant ; mois manquant ; montant nÃ©gatif non justifiÃ© ; lien origine absent si avantage dÃ©rivÃ© |
| Acomptes propriÃ©taires | facture absente ; logement absent ; montant reportÃ© incohÃ©rent |
| Caisse / liquide | origine absente ; usage absent ; solde thÃ©orique nÃ©gatif |

### 18.6 ContrÃ´le structurel permanent

Ã€ chaque run : `PK` unique, `ROW_HASH` prÃ©sent, compteurs dans `MASTER_RUN_Log`.

---

## 19. Ordre de construction recommandÃ©

```
Lot 0  â”‚ Stabiliser REF_Setup                                   [Ã€ PRÃ‰PARER]
Lot 1  â”‚ Module Hostaway (extraction + payout + anomalies)      [extraction existante, non validÃ©e]
Lot 2  â”‚ RÃ©conciliation logements (orphelin 515523, encodage, dates)
Lot 3  â”‚ Charges perso/liquide (Module 3)
Lot 4  â”‚ RÃ©servations hors Hostaway (Module 2) â€” 1 ligne/rÃ©sa, acompte
Lot 5  â”‚ Acomptes propriÃ©taires (Module 3)
Lot 6  â”‚ MÃ©nages (Module 4) â€” tasks (comptage) + factures (valorisation)
Lot 7  â”‚ IK & Avantages (Module 5) â€” dÃ©rivÃ© des Lots 3 et 4
Lot 8  â”‚ Banque / rapprochement (Module 6) â€” autonome
Lot 9  â”‚ Table de flux unifiÃ©e MASTER_CALC_Flux (Module 7)
Lot 10 â”‚ RÃ©sultats + Commissions (Module 8) â€” filtres IC/HC/HR
Lot 11 â”‚ ContrÃ´les globaux (Module 9)
Lot 12 â”‚ Livrables propriÃ©taires Excel / donnÃ©es prÃªtes Power BI (Module 10)
```

Justification : tout dÃ©pend du rÃ©fÃ©rentiel (Lot 0) et de la rÃ©conciliation logements (Lot 2). Les charges (Lot 3) prÃ©cÃ¨dent les IK (Lot 7), qui s'en dÃ©duisent. La banque (Lot 8) est autonome et peut Ãªtre menÃ©e en parallÃ¨le dÃ¨s que les charges sont stabilisÃ©es. La table de flux (Lot 9) prÃ©cÃ¨de les rÃ©sultats (Lot 10), simples agrÃ©gations filtrÃ©es. Les contrÃ´les (Lot 11) consomment toutes les tables.

---

## 20. Points de vigilance

1. **Assiette commission par canal** (Â§8.3). Le mÃ©nage Ã  soustraire n'est pas au mÃªme endroit selon Airbnb / Booking / VRBO / Direct. **Risque de surcommission Airbnb (~88 % du volume) si mal gÃ©rÃ©. Point nÂ°1 Ã  l'implÃ©mentation.**
2. **DÃ©calage rÃ©fÃ©rentiel â†” Hostaway.** `515523` dans l'export, absent du REF ; `480780` (LOG_0016, `sur_hostaway=NON`, cohÃ©rent) et `497801` du REF absents de l'export. Ã€ trancher avant de fiabiliser les jointures.
3. **Encodage du rÃ©fÃ©rentiel.** `REF_Associes`, `REF_Codes_Impact`, `REF_Types_Flux` contiennent des caractÃ¨res cassÃ©s (Â« associÃƒÂ© Â»). Ã€ corriger Ã  la source.
4. **Dates en sÃ©rie Excel** (ex. `46023`). Ã€ normaliser Ã  l'import.
5. **Ne pas confondre prix total canal et payout rÃ©el.**
6. **Hostaway â‰  source financiÃ¨re hors Hostaway.** La table manuelle est la vÃ©ritÃ©.
7. **Prix mÃ©nage Hostaway â‰  coÃ»t rÃ©el mÃ©nage.** CoÃ»t rÃ©el via factures/suivi.
8. **Pas de double saisie** : une charge qui alimente dÃ©jÃ  les avantages n'est pas ressaisie.
9. **Ne jamais supprimer** une ligne absente d'un extract.
10. **SÃ©vÃ©ritÃ© juste** : ne rendre bloquant que ce qui rend un rÃ©sultat ou une facture faux.
11. **Formule d'acompte HH non gÃ©nÃ©rique** (Â§9.3).
12. **ClÃ©s sur identifiant stable, pas sur montant** (collisions).
13. **Construire lot par lot** : ne pas demander Ã  un outil IA de tout construire en une passe. Les rÃ¨gles restent dans les fichiers `.md` de rÃ©fÃ©rence.
14. **Synchronisation GitHub / OneDrive** (Â§4.1) : Power Query doit pointer vers le dÃ©pÃ´t local synchronisÃ©, pas vers les artefacts GitHub temporaires. Le `git pull` local doit devenir une Ã©tape maÃ®trisÃ©e.
15. **Fichiers manuels contrÃ´lÃ©s** (Â§4.2, Â§18.5) : pas de tableaux libres. Listes dÃ©roulantes, colonnes obligatoires, statuts, alertes doublons.
16. **TraÃ§abilitÃ© du liquide** (Â§10.5) : tout liquide rÃ©cupÃ©rÃ© a une origine et un usage. Un solde thÃ©orique non expliquÃ© apparaÃ®t en contrÃ´le.
17. **Table de flux = colonne vertÃ©brale** : c'est elle qui Ã©vite les incohÃ©rences entre les trois rÃ©sultats.

---

## 21. DÃ©cisions verrouillÃ©es et points non bloquants restants

### 21.1 DÃ©cisions verrouillÃ©es (rappel â€” ne pas rouvrir sans nouvelle dÃ©cision explicite)

| Point | DÃ©cision verrouillÃ©e |
|---|---|
| Cancellation payout | **D030 / EP4** â€” BaseCommission = CancellationPayout, pas de mÃ©nage dÃ©duit |
| Seuil tolÃ©rance arrondi | **D035** â€” 0,10 â‚¬/ligne, 1,00 â‚¬ cumulÃ©/facture |
| BarÃ¨me IK | **D036** â€” montant direct au dÃ©marrage |
| PÃ©rimÃ¨tre `REF_Couts_Standards_Menage` | **D037** â€” exÃ©cution seule. Valeurs Ã  revalider au Lot 0 |
| `charge_fixe_mensuelle` | **D039** â€” paramÃ©trable dans `REF_Logements`, valeur 0 si absent |
| Structure facture | **D040** â€” `FACT_FACTURE_ENTETE` + `FACT_FACTURE_LIGNES` |

### 21.2 Points rÃ©ellement non bloquants restants

| Point | Solution provisoire / report |
|---|---|
| Mise en forme visuelle PDF facture | PrÃ©parÃ©e par la structure des donnÃ©es (tables et champs conformes), mais **non livrÃ©e au dÃ©marrage** (D040/P11). DÃ©cision future si besoin â€” non bloquante pour tous les lots. |
| VRBO / iCal | Â« Ã  contrÃ´ler Â» tant que la source financiÃ¨re n'est pas validÃ©e |
| Remboursements associÃ©s | Lier Ã  une charge si possible, sinon Â« Ã  contrÃ´ler Â» |
| Caisse espÃ¨ces | Suivi manuel au dÃ©part |
| PÃ©riodicitÃ© de validation manuelle | hebdo / mensuelle / avant facturation â€” Ã  confirmer Ã  l'usage |

---

## 22. RÃ©sultat du contrÃ´le de cohÃ©rence

L'architecture est **cohÃ©rente et constructible** sous rÃ©serve de respecter les rÃ¨gles anti-casse ci-dessus. Chaque donnÃ©e utile est rattachÃ©e Ã  une source, une clÃ©, un hash, un statut de contrÃ´le et, si nÃ©cessaire, une ligne de rapprochement.

| Axe | Ã‰valuation |
|---|---|
| ConstructibilitÃ© | Oui, par lots successifs |
| RÃ©sistance aux doublons | Oui, si `PK` / `ROW_HASH` / empreinte bancaire appliquÃ©s |
| RÃ©sistance au double comptage | Oui, grÃ¢ce Ã  la rÃ¨gle source mÃ©tier prioritaire / banque rapprochement (Â§2.6, Â§13.4 bis) |
| VÃ©rification des montants | Oui, via `MASTER_CTRL_*`, `CTRL_A_CONTROLER`, caisse thÃ©orique, rapprochement bancaire |
| MaintenabilitÃ© | Oui, si les rÃ¨gles mÃ©tier restent dans les `.md` et rÃ©fÃ©rentiels, pas dans du code dispersÃ© |

---

## 23. Conventions transverses (statuts, clÃ´ture, arrondi)

> Section ajoutÃ©e pour combler trois manques verrouillÃ©s ailleurs en rÃ¨gle mais sans support structurel : un rÃ©fÃ©rentiel de statuts fermÃ©, une table de clÃ´ture mensuelle, et une convention d'arrondi unique. Les conventions ci-dessous sont **verrouillÃ©es** ; les seuls points non bloquants restants sont listÃ©s au Â§21.

### 23.1 RÃ©fÃ©rentiel des statuts â€” `REF_Statuts` (onglet `REF_Setup`)

Le champ `statut_controle` est prÃ©sent dans toutes les tables de saisie et de calcul. Pour Ã©viter la dÃ©rive de valeurs (`OK`, `ValidÃ©`, `VALIDE`, `Ã€ contrÃ´ler`, `A_CONTROLER`â€¦), les valeurs autorisÃ©es sont **fermÃ©es** et viennent d'un onglet `REF_Statuts` dans `REF_Setup` (Ã  crÃ©er au Lot 0).

| `statut_id` | LibellÃ© | Effet |
|---|---|---|
| `VALIDE` | ValidÃ© | Ligne intÃ©grÃ©e au calcul |
| `A_CONTROLER` | Ã€ contrÃ´ler | Visible en contrÃ´le, intÃ©grÃ©e sauf rÃ¨gle contraire du module |
| `BLOQUANT` | Bloquant | Exclue du calcul, bloque la clÃ´ture / la facturation |
| `IGNORE_JUSTIFIE` | IgnorÃ© justifiÃ© | Exclue volontairement, motif obligatoire |

Toute table manuelle (`MASTER_FACT_MAN_*`), calculÃ©e et de contrÃ´le utilise **uniquement** ces valeurs. Les listes dÃ©roulantes des fichiers de saisie (Â§4.2) pointent vers `REF_Statuts`.

> **D044 (2026-06-08) â€” Lot 3+ :** `statut_controle` = `VALIDE` / `A_CONTROLER` / `EXCLU_RESULTAT` / `A_VENTILER`.
> Nouvelle colonne `niveau_anomalie` (famille `niveau_anomalie` dans `REF_Statuts`) = `INFO` / `A_CONTROLER` / `BLOQUANT`.
> `BLOQUANT` n'est plus un statut de ligne â€” c'est un niveau d'anomalie. `IGNORE_JUSTIFIE` reste valide pour Lots 0-2.
> `REF_Statuts` Ã©tendu : STAT_022 dÃ©sactivÃ© ; STAT_024-026 (statut_controle) + STAT_027-029 (niveau_anomalie) ajoutÃ©s.

> **Ã€ ne pas confondre** avec les statuts de rÃ©servation Hostaway (`new`/`cancelled`/`ownerStay`â€¦ Â§6.4) ni avec les statuts de mois (Â§23.2), qui sont des familles distinctes.

### 23.2 ClÃ´ture mensuelle â€” `REF_Cloture_Mensuelle`

La rÃ¨gle de clÃ´ture (REGLES Â§11, C1-C7 ; PLAN Lot 8) impose trois Ã©tats de mois. La table de stockage est crÃ©Ã©e **au Lot 0** (structure vide dans `REF_Setup.xlsm` ou CSV dÃ©diÃ©) et exploitÃ©e/alimentÃ©e **au Lot 8**.

| Colonne | RÃ´le |
|---|---|
| `mois` (PK) | `AAAA-MM` |
| `statut_mois` | `OUVERT` / `EN_CONTROLE` / `CLOTURE` |
| `date_passage_controle`, `date_cloture` | TraÃ§abilitÃ© |
| `nb_lignes_bancaires_non_classees` | Compteur ; > 0 â‡’ `CLOTURE` impossible (REGLES C5/C6) |
| `nb_controles_bloquants_ouverts` | Compteur de bloquants ouverts |
| `commentaire` | |

RÃ¨gles : un mois ne peut passer Ã  `CLOTURE` que si `nb_lignes_bancaires_non_classees = 0` **et** `nb_controles_bloquants_ouverts = 0`. La facturation propriÃ©taire (Â§17, Lot 12) n'est Ã©mise qu'aprÃ¨s `CLOTURE`. ContrÃ´le associÃ© : `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` (Â§18.2, PLAN Lot 8). Un calcul provisoire reste possible sur un mois `OUVERT` / `EN_CONTROLE`.

### 23.2bis `REF_Statuts_Payout` â€” Ã  distinguer de `REF_Statuts`

Deux rÃ©fÃ©rentiels de statuts distincts, crÃ©Ã©s **tous les deux au Lot 0** :

| RÃ©fÃ©rentiel | Onglet REF_Setup | Champ cible | Valeurs |
|---|---|---|---|
| `REF_Statuts` | `REF_Statuts` | `statut_controle` (toutes tables) | VALIDE / A_CONTROLER / BLOQUANT / IGNORE_JUSTIFIE |
| `REF_Statuts_Payout` | `REF_Statuts_Payout` | `statut_calcul_payout` (`MASTER_CALC_HA_Payout`) | NORMAL / ANNULE_SANS_PAYOUT / ANNULE_AVEC_PAYOUT / PAYOUT_ABSENT / PAYOUT_INCOMPLET / A_CONTROLER |

Ne jamais mÃ©langer les deux. `REF_Statuts` = traitement/contrÃ´le. `REF_Statuts_Payout` = statut de calcul payout.

### 23.3 Convention d'arrondi et double seuil de tolÃ©rance (D035)

Pour Ã©viter les Ã©carts entre Excel, Power Query, Python et Power BI :

```text
Calcul interne     : pleine prÃ©cision disponible â€” jamais d'arrondi intermÃ©diaire.
Stockage/affichage : 2 dÃ©cimales, arrondi demi-vers-le-haut (ROUND).
TolÃ©rance ligne    : 0,10 â‚¬ par ligne.
TolÃ©rance cumulÃ©e  : 1,00 â‚¬ par facture / propriÃ©taire / mois.
Ã‰cart â‰¤ seuil      : acceptable, traÃ§able si nÃ©cessaire.
Ã‰cart > seuil ligne   â†’ ECART_ARRONDI_LIGNE_SUPERIEUR_TOLERANCE (Ã  contrÃ´ler).
Ã‰cart > seuil cumulÃ©  â†’ ECART_ARRONDI_FACTURE_SUPERIEUR_TOLERANCE (Ã  contrÃ´ler).
```

ParamÃ¨tres externalisÃ©s dans `REF_Parametres_Generaux` (avec dates de validitÃ©) :

| ParamÃ¨tre | Valeur | Type |
|---|---|---|
| `ARRONDI_DECIMALES` | 2 | ENTIER |
| `TOLERANCE_ARRONDI_LIGNE_EUR` | 0.10 | MONTANT |
| `TOLERANCE_ARRONDI_CUMUL_EUR` | 1.00 | MONTANT |

---

## Vision proposÃ©e par Claude

Je m'appuierais sur **une table de flux unifiÃ©e comme colonne vertÃ©brale**, tout le reste Ã©tant alimentation ou lecture de cette table.

**Principe central.** Les donnÃ©es vivent aujourd'hui en silos (Hostaway, charges, mÃ©nages, acomptes, banque) avec des schÃ©mas diffÃ©rents. Calculer les rÃ©sultats depuis chaque silo multiplierait les rÃ¨gles et rendrait les trois visions incohÃ©rentes. `MASTER_CALC_Flux` normalise tout : un Ã©vÃ©nement Ã©conomique = une ligne, mÃªmes colonnes, montant positif + sens. Les trois rÃ©sultats deviennent **trois filtres** sur `code_impact` (REEL = IC+HC, COMPTABLE = IC, EXTRA = HC). Robuste, vÃ©rifiable, exploitable en Power BI comme schÃ©ma en Ã©toile (une table de faits, les `REF_*` en dimensions).

**Lecture en trois couches.** (1) **Automatique** : Hostaway, puis banque â€” tables propres, stables, indÃ©pendantes des saisies manuelles. (2) **MÃ©tier** : `REF_Setup`, rÃ©servations hors Hostaway, charges perso/liquide, avantages â€” complÃ¨te ce que les API ne fournissent pas. (3) **Pilotage** : flux unifiÃ©, rÃ©sultats, commissions, factures propriÃ©taires, contrÃ´les.

**PrioritÃ©s et dÃ©pendances.** (1) Fiabiliser la rÃ©conciliation logements (Lot 2), maillon le plus risquÃ© : une facture mÃ©nage mal rattachÃ©e fausse tout le rÃ©sultat par logement. (2) Charges (Lot 3) avant IK (Lot 7), car les avantages s'en dÃ©duisent. (3) RÃ©servations hors Hostaway (Lot 4), porteuses de la logique la plus subtile (montant rÃ©cupÃ©rÃ© â†’ avantage â†’ charge payÃ©e avec â†’ dÃ©duction). (4) Table de flux (Lot 9) avant rÃ©sultats (Lot 10). (5) ContrÃ´les (Lot 11) en filet de sÃ©curitÃ© avant les livrables.

**Garder simple.** Pas de base de donnÃ©es ni d'ETL lourd : scripts Python d'upsert + CSV master + Power Query/Power BI suffisent, dÃ©jÃ  en place pour Hostaway. La complexitÃ© reste dans les **rÃ¨gles mÃ©tier** (le rÃ©fÃ©rentiel), pas dans la plomberie. Les fichiers manuels se limitent Ã  ce qui n'existe nulle part ailleurs.

**Point d'attention nÂ°1 Ã  l'implÃ©mentation** : l'assiette de commission par canal (Â§8.3), la rÃ¨gle validÃ©e la plus piÃ©geuse techniquement, parce que le mÃ©nage Ã  soustraire se cache Ã  un endroit diffÃ©rent selon Airbnb, Booking, VRBO ou Direct.

**En une phrase.** Un rÃ©fÃ©rentiel solide + une table de flux unifiÃ©e alimentÃ©e par tous les modules + trois lectures par simple filtre sur le code impact = un systÃ¨me modulaire, maintenable et directement branchable sur Power BI.

---

## Note 2026-06-29 - statut_parc et codes techniques hors parc

`REF_Logements.actif` indique la disponibilite technique d'un code referentiel. Un code peut rester disponible pour le controle, le mapping ou l'anti-mauvais-mapping sans etre eligible aux calculs metier.

`REF_Logements.statut_parc` indique l'eligibilite du code au parc de logements geres. Les seules valeurs valides sont `GERE` et `HORS_PARC_TECHNIQUE`.

- `GERE` : logement reellement gere, eligible aux calculs metier si les autres referentiels obligatoires sont valides (proprietaire historise, taux de commission, etc.).
- `HORS_PARC_TECHNIQUE` : code conserve dans le referentiel pour controle ou anti-mauvais-mapping, explicitement exclu de tout calcul economique et operationnel.
- `statut_parc` vide, invalide ou inconnu : traitement `A_CONTROLER`, code anomalie `STATUT_PARC_INVALIDE`, sans calcul economique.

Cette distinction est volontairement separee de `actif` : `actif` ne vaut pas eligibilite au parc gere.
### Note APP-2b - Extension contrÃ´lÃ©e saisie HH (2026-07-03)

La saisie APP-2b `SAISIE_ReservationsHorsHostaway.xlsx` conserve temporairement les 30 colonnes historiques pour compatibilitÃ©, mais le schÃ©ma cible testÃ© sur copie ajoute en fin de ligne :

| Champ cible | RÃ´le |
|---|---|
| `taux_commission_override` | Taux dÃ©rogatoire dÃ©cimal canonique entre 0 et 1 inclus ; exemple : 18 % utilisateur = `0.18` stockÃ© |
| `motif_override_taux_commission` | Motif obligatoire de dÃ©rogation taux |
| `confirmation_override_taux_commission` | Confirmation explicite de dÃ©rogation taux |
| `menage_override` | Montant mÃ©nage dÃ©rogatoire demandÃ© |
| `motif_override_menage` | Motif obligatoire de dÃ©rogation mÃ©nage |
| `confirmation_override_menage` | Confirmation explicite de dÃ©rogation mÃ©nage |
| `source_acompte_facture` | Source de calcul de l'acompte propriÃ©taire |

RÃ¨gles associÃ©es :
- `reservation_id_hostaway` n'est plus utilisÃ© par APP-2b ; il reste physiquement possible dans le classeur uniquement pour compatibilitÃ© temporaire.
- `source_financiere` par dÃ©faut = `SAISIE_MANUELLE`.
- `comptabilisation` est dÃ©rivÃ©e de `REF_Codes_Impact.impact_resultat_comptable`.
- `Direct propriÃ©taire` est prÃ©parÃ© sous `PAY_006` / `DIRECT_PROPRIETAIRE` dans la migration de copie de `REF_Modes_Paiement`.
- Lot4A publie les nouveaux champs de traÃ§abilitÃ© Ã  droite des 34 colonnes MASTER historiques.
- L'acompte propriÃ©taire est calculÃ© par mode de paiement : banque pro = `total_percu`, compte perso associÃ©e/carte associÃ©e = `total_percu`, espÃ¨ces = `total_percu - montant_reverse_proprietaire`, direct propriÃ©taire = `0`. Le `montant_recupere` reste une donnÃ©e de contrÃ´le/traÃ§abilitÃ©.

### Note APP-2b REV2 - interface et source acompte (2026-07-03)

- Les colonnes de traÃ§abilitÃ© de dÃ©rogation restent sÃ©parÃ©es du taux et du mÃ©nage calculÃ©s. L'interface ne prÃ©sente le motif et la confirmation qu'en modale au moment de la vÃ©rification, jamais comme champs visibles permanents.
- `source_acompte_facture` prend les valeurs Lot4A `TOTAL_PERCU`, `TOTAL_PERCU_ASSOCIE`, `TOTAL_PERCU_MOINS_REVERSE_ESPECES` ou `DIRECT_PROPRIETAIRE` selon le mode de paiement.
- Les champs conditionnels masquÃ©s par l'interface (`montant_recupere`, `associe_id_recuperateur`, `montant_reverse_proprietaire`) sont aussi nettoyÃ©s/ignorÃ©s par le service backend quand le mode de paiement ne les autorise pas.
- REV2 ne modifie aucun fichier Excel rÃ©el ; `HH_REAL_WRITE_ENABLED` reste `False`.

### Note APP-2b REV3 - format taux dÃ©rogatoire (2026-07-04)

- Formulaire : `taux_commission_override_pct` est le pourcentage utilisateur temporaire, jamais persistant.
- Service APP-2b : `taux_commission_override_pct` est converti en `Decimal` puis divisÃ© par 100.
- Preview / orchestrateur / writer futur / colonnes cible SAISIE HH / Lot4A : seul `taux_commission_override` est transmis, au format dÃ©cimal canonique `[0, 1]`.
- Lot4A refuse une valeur confirmÃ©e hors `[0, 1]` au lieu de tenter une conversion implicite.
- REV3 ne modifie aucun fichier Excel rÃ©el ; `HH_REAL_WRITE_ENABLED` reste `False`.

### Note APP-2c - Dry-run applicatif sur copies (2026-07-04)

APP-2c ajoute un espace applicatif non versionne `05_APPLICATION/data/dryruns/`. Chaque previsualisation cree un sous-dossier unique contenant :

| Fichier | Role |
|---|---|
| `manifest.json` | Horodatage UTC, identifiant de simulation, hashes des sources lues, hashes des copies, resume payload, statut, erreurs |
| `SAISIE_ReservationsHorsHostaway_copie.xlsx` | Copie isolee de SAISIE HH, migree vers le schema cible APP-2b/APP-2c |
| `REF_Setup_copie.xlsm` | Copie isolee du referentiel, adaptee si une migration de copie est necessaire |
| `MASTER_FACT_MAN_ReservationsHorsHostaway_simule.xlsx` | Sortie MASTER simulee issue du moteur Lot4A sur copies |
| `resultat_lot4a.json` | Resultat structure du moteur Lot4A, anomalies et ligne simulee |

Regles d'architecture :
- les resultats affiches par l'ecran APP-2c proviennent du backend et du moteur Lot4A, pas du JavaScript ;
- le writer reel reste protege par `HH_REAL_WRITE_ENABLED=False` et n'est pas appele par APP-2c ;
- les formules critiques B/C/K/N/O/Q/V/Y/Z restent preservees dans la copie ;
- l'activation d'une ecriture reelle est une etape separee, hors APP-2c, apres validation humaine et migration controlee du classeur source reel.

### Note APP-2c - execution Lot4A reelle hors processus FastAPI (2026-07-04)

Decision technique de correction : Option B.

Motif : l'application locale est lancee avec `C:\Users\Ewan\miniconda3\python.exe`, environnement qui ne contient ni NumPy ni pandas. Installer ces dependances dans Miniconda modifierait un environnement local hors depot et ne constituerait pas un correctif versionne. APP-2c execute donc Lot4A reel dans un sous-processus controle avec `LOT4A_ENGINE_PYTHON` (par defaut `C:\Program Files\Python312\python.exe`).

Contraintes appliquees :
- aucun import Lot4A dans le processus FastAPI ;
- chemins `saisie_path`, `ref_path`, `master_path`, requete et reponse obligatoirement sous le sous-dossier dry-run ;
- timeout configure par `LOT4A_ENGINE_TIMEOUT_SECONDS` ;
- sortie JSON standardisee avec interpreteur moteur, versions NumPy/pandas et chemin du module Lot4A ;
- le MASTER simule est produit par le runner Lot4A, toujours dans le dossier dry-run ;
- le manifest APP-2c porte les informations moteur et le statut de cache des formules Excel.

Formules Excel : openpyxl ne recalcule pas les formules, il ne fait que poser `fullCalcOnLoad`. APP-2c controle la presence des formules critiques et trace l'absence eventuelle de cache Excel. Lot4A reste certifiable sans recalcul Excel car il recalcule en Python les champs derives qu'il publie dans le MASTER (`ROW_HASH`, `mois`, `nuits`, taux, commission, acompte, impacts).

### Note APP-2d - ecriture reelle controlee et reversible (2026-07-04)

APP-2d ajoute une couche d'activation au-dessus du dry-run APP-2c et de l'orchestrateur APP-2b. L'ecriture reelle reste impossible tant que `HH_REAL_WRITE_ENABLED` et `HH_REAL_WRITE_CONFIRMATION_ENABLED` ne sont pas tous deux explicitement actives par configuration.

Prerequis techniques controles avant appel du writer :
- simulation APP-2c presente, `OK`, agee de moins de 30 minutes ;
- `lot4a_status = ANALYSE_TERMINEE` et aucune anomalie bloquante dans le manifest ;
- hashes reels de REF_Setup et SAISIE strictement egaux aux hashes sources du manifest ;
- PK HH toujours absente du fichier reel ;
- mois de `date_arrivee` toujours `OUVERT` dans `REF_Cloture_Mensuelle` ;
- schema reel deja prepare : champs cibles APP-2b/APP-2c presents dans SAISIE et mode `PAY_006 / DIRECT_PROPRIETAIRE` present dans REF ;
- texte de confirmation humaine exact `ENREGISTRER <reservation_hh_id>`.

Contrat de rollback :
- APP-2d cree une copie de rollback avant d'appeler l'orchestrateur ;
- l'orchestrateur conserve ses propres controles atomiques, snapshots, locks et validations structurelles ;
- toute divergence post-ecriture ou erreur apres debut d'ecriture restaure la copie APP-2d, verifie le hash restaure et journalise le statut ;
- Lot4A post-ecriture est execute uniquement sur une copie post-ecriture, jamais sur les fichiers source en modification directe.

La migration du schema reel reste une operation separee, manuelle, sauvegardee et validee avant toute premiere activation effective.

### Note APP-2e - preparation schema reel et recette sur copies (2026-07-04)

APP-2e ajoute un service de preparation distinct pour diagnostiquer et preparer le schema HH sans modifier les classeurs reels.

Fonctions applicatives :
- `diagnostiquer_schema_hh(...)` lit SAISIE HH et REF_Setup, calcule les hashes et liste les colonnes APP-2b/APP-2c manquantes, le mode `PAY_006 / DIRECT_PROPRIETAIRE`, les feuilles, formules critiques, validations, tables, plages nommees, MFC, protections, liens externes et presence VBA.
- `preparer_migration_hh_sur_copies(...)` copie les deux classeurs dans un dossier de travail, ajoute uniquement les champs cibles manquants et le mode de paiement cible absent, puis valide la preservation structurelle.
- `executer_migration_hh_reelle(...)` est reservee a une execution future explicite avec confirmation exacte, snapshots doubles, temporaires valides, remplacement atomique et rollback des deux fichiers.

Commande locale future :

```powershell
& "C:\Users\Ewan\miniconda3\python.exe" 05_APPLICATION\tools\preparer_schema_hh_reel.py
& "C:\Users\Ewan\miniconda3\python.exe" 05_APPLICATION\tools\preparer_schema_hh_reel.py --execute
```

La commande sans option est un diagnostic seul. L'option `--execute` demandera `MIGRER_SCHEMA_HH_REELLE` et ne doit etre utilisee qu'apres sauvegarde et validation humaine.

Propagation cible :
- `saisie_svc.build_row_data()` inclut les champs de derogation et de source d'acompte.
- Le writer ecrit ces champs uniquement si les colonnes cible existent dans SAISIE.
- Une valeur de taux derogatoire `0.005` ou `0.00` est une donnee valide ; elle n'est pas assimilee a une valeur vide.
- La comparaison APP-2d simulation/ecriture couvre les champs economiques et de tracabilite necessaires, dont `taux_commission_override` et `menage_override`.

Aucune migration reelle automatique n'est exposee dans l'interface. L'ecran de previsualisation affiche seulement le diagnostic schema et continue de bloquer l'ecriture reelle tant que le schema reel n'est pas prepare.

## ContrÃ´le intÃ©gritÃ© VBA et package ZIP (APP-2e durcissement â€” 2026-07-05)

Distinction des trois niveaux de controle VBA :

**Presence VBA** (`has_vba`) : verifie que `wb.vba_archive is not None` apres chargement openpyxl avec `keep_vba=True`. Necesssaire mais non suffisant.

**Integrite binaire VBA** : compare le SHA-256 du contenu decompresse de `xl/vbaProject.bin` entre la copie reference (avant migration) et la copie de travail (apres migration). Un seul octet modifie produit `VBA_PRESERVATION_ECHEC`. Verifie aussi `xl/vbaProjectSignature.bin` si present, le marqueur `macroEnabled` dans `[Content_Types].xml`, et la relation `relationships/vbaProject` dans `xl/_rels/workbook.xml.rels`.

**Preservation des parties ZIP sensibles** : protege les parties du package Excel qui ne sont pas reconstruites par openpyxl et qui representent des configurations metier ou systeme : `xl/activeX/`, `xl/ctrlProps/`, `xl/embeddings/`, `xl/externalLinks/`, `xl/connections.xml`, `customUI/`, `docProps/custom.xml`, `xl/printerSettings/`. Toute disparition ou modification produit `PACKAGE_SENSIBLE_PRESERVATION_ECHEC`.

Nouveaux helpers dans `saisie_hh_schema_real_prepare_service.py` :
- `_zip_sha256_entry(zf, name)` â€” SHA-256 contenu decompresse d'une entree ZIP
- `_vba_snapshot(path)` â€” empreinte complete des parties VBA pour manifest
- `_check_zip_vba_integrity(ref_path, work_path)` â€” controle binaire VBA complet
- `_check_zip_sensitive_parts(ref_path, work_path)` â€” controle parties sensibles

Le manifest APP-2e inclut desormais `vba_snapshots.saisie.avant/apres` et `vba_snapshots.ref_setup.avant/apres` pour traÃ§abilite complete.

## Migration rÃ©elle APP-2e exÃ©cutÃ©e (2026-07-05)

La migration reelle a ete executee le 05/07/2026. Les fichiers sources reels sont desormais au nouveau schema.

**Schema SAISIE post-migration** : 37 colonnes (A-AK). Les 7 nouvelles colonnes de derogation sont en colonnes AE-AK. Toutes les colonnes historiques et formules sont preservees.

**Schema REF_Setup post-migration** : feuille REF_Modes_Paiement inclut desormais PAY_006 / DIRECT_PROPRIETAIRE. VBA intact (`xl/vbaProject.bin` SHA-256 : `09eb44f98025583fad807b0784123e2c3d402ad3d38093b163170def3de8e5c2`).

**Hashes avant** : SAISIE `c3c00e73017212e08bb3f9e9aef73a26bd3c828804e4fa21f7f2b37713d54c5c`, REF `6d9f21de919e80c1903ae5acdb2f64a3d776c858857dda52fb39b8335ab726da`

**Hashes apres** : SAISIE `60b7bc85f7d59530e0a0fcdb9596162012db44611aeefaa3b1f0a97d32b18943`, REF `3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8`

**Sauvegarde** : `99_ARCHIVES\APP2E_SCHEMA_HH_20260705_020543\` (hors staging, hors git)

**Flags ecriture reservations** : `HH_REAL_WRITE_ENABLED = False`, `HH_REAL_WRITE_CONFIRMATION_ENABLED = False` â€” inchanges. Le nouveau schema est en place mais l'ecriture reelle des reservations reste desactivee.

---

## APP-2 MÃ©nages â€” Lecteur, service et routes (2026-07-05)

### Lecteur `menages_reader.py`

- `rapprochement_available()` â†’ bool (MASTER prÃ©sent)
- `gainperte_available()` â†’ bool
- `read_tableau_comparaison()` â†’ liste brute (TABLEAU_COMPARAISON)
- `find_ligne(mois, logement_id, intervenant_id)` â†’ dict ou None
- `read_controles()` â†’ liste (CONTROLES)
- `read_gainperte_detail()` â†’ liste (DETAIL_ECART_COUT)
- `find_gainperte(mois, logement_id, intervenant_id)` â†’ dict ou None

Sources : `MASTER_CTRL_Rapprochement_Menages.xlsx`, `MASTER_CALC_GainPerte_Menages.xlsx`. Lecture seule stricte. Aucune valorisation Hostaway.

### Service `menages_service.py`

- `load_list(mois, logement_id, type_intervenant, statut_controle)` â†’ dict {status, rows, filters, applied}
- `load_detail(mois, logement_id, intervenant_id)` â†’ dict ou None (404 propre)
- `enregistrer_outrepassage(mois, logement_id, intervenant_id, motif)` â†’ dict {ok, error?}

Les 3 flux (HA tasks / M04 internes / externes) restent des champs sÃ©parÃ©s dans chaque ligne. `statut_effectif` = JUSTIFIE si override prÃ©sent, sinon `statut_controle` du MASTER.

### Table SQLite `menage_overrides` (migration 0003)

| Colonne | Type | Contrainte |
|---|---|---|
| id | INTEGER PK | AUTOINCREMENT |
| ts | TEXT | DEFAULT strftime ISO |
| mois | TEXT | NOT NULL |
| logement_id | TEXT | NOT NULL |
| intervenant_id | TEXT | NOT NULL |
| motif | TEXT | NOT NULL |
| statut_override | TEXT | DEFAULT 'JUSTIFIE' |
| â€” | â€” | UNIQUE(mois, logement_id, intervenant_id) |

### Config

```python
MASTER_RAPPROCHEMENT_MENAGES = TRAVAIL / "Lot6d_Rapprochement_Menages" / "MASTER_CTRL_Rapprochement_Menages.xlsx"
MASTER_GAINPERTE_MENAGES     = TRAVAIL / "Lot6e_GainPerte_Menages"       / "MASTER_CALC_GainPerte_Menages.xlsx"
MASTER_COUTCOMPLET_MENAGES   = TRAVAIL / "Lot6f_CoutComplet_Menages"     / "MASTER_CALC_CoutComplet_Menages.xlsx"
```

### Routes

- `GET /menages` â€” liste avec filtres (mois, logement_id, type_intervenant, statut_controle)
- `GET /menages/{mois}/{logement_id}/{intervenant_id}` â€” fiche dÃ©tail
- `POST /menages/{mois}/{logement_id}/{intervenant_id}/outrepasser` â€” outrepassage tracÃ©

---

## §21 — APP-3a : Charges & Fournisseurs (lecture seule)

### Sources

- MASTER_CHARGES = TRAVAIL / "Lot3_Charges" / "MASTER_FACT_MAN_Charges.xlsx" — sortie Power Query Lot3, onglet MASTER. Données présentes après refresh Excel.
- SAISIE_CHARGES = SOURCES_BRUTES / "Charges" / "SAISIE_Charges_Flux.xlsx" — source amont. Jamais lue ni écrite par l'app (hash-check uniquement).

### Structure MASTER (37 colonnes)

31 colonnes SAISIE + 6 colonnes Power Query : sens, iltre_vue_menage, source_module, source_table, source_pk, date_integration.

Clé logique : charge_id.

### Règles d'affichage

- Lignes placeholder PQ (charge_id commence par [) filtrées avant affichage (D-C2).
- IK et VIREMENT_ASSOCIE exclus (D025 / D-C3).
- Statuts affichés tels quels (D044 / D-C4).
- Aucun recalcul, aucune écriture, aucun accès SQLite.

### Routes

- GET /fournisseurs — liste filtrable (mois, logement_id, categorie_charge_id, code_impact, statut_controle, associe_id)
- GET /fournisseurs/{charge_id} — fiche détail

---

## §22 APP-3c — Propriétaires & règlements (2026-07-05)

### Sources

| Fichier | Onglet | Lignes | Rôle |
|---------|--------|--------|------|
| REF_Setup.xlsm | REF_Proprietaires | 12 | Liste propriétaires |
| MASTER_CALC_NetProprietaire.xlsx | REGLEMENT | 270 | Relevé par prop×log×mois (clé unique) |
| MASTER_CALC_NetProprietaire.xlsx | VUE_MOIS | 221 | Agrégation par prop×mois |
| MASTER_FACT_Proprietaires.xlsx | FACT_FACTURE_ENTETE | 270 | En-tête préfacture |
| MASTER_FACT_Proprietaires.xlsx | FACT_FACTURE_LIGNES | 3240 | 12 lignes × 270 factures |

### Structure préfacture (12 lignes fixes)

Bloc EXPLOITATION (5) : TOTAL_PAYOUT, MENAGE_FACTURE, COMMISSION_CONCIERGERIE, CHARGE_FIXE, REVENU_NET_EXPLOITATION.
Bloc REGLEMENT (7) : MONTANT_DU, ACOMPTE_AIRBNB, PAIEMENT_DEJA_RECU, RESTE_A_PAYER, CHARGES_EXCEPT_REFAC, ACOMPTES_PROPRIETAIRES, STATUT_REGLEMENT.

### Règles d'affichage

- Bloc EXPLOITATION et bloc REGLEMENT séparés structurellement (D033, EP1-EP7).
- revenu_net_exploitation lu depuis MASTER, jamais recalculé (D-P3).
- AirCover = ligne ACOMPTE_AIRBNB uniquement (D-P4).
- Lot12 absent → status=UNAVAILABLE, relevé non bloqué (D-P6).
- Aucun recalcul, aucune écriture, aucun accès SQLite.

### Routes

- GET /proprietaires — liste propriétaires
- GET /proprietaires/{prop_id} — fiche + mois disponibles
- GET /proprietaires/{prop_id}/{mois} — relevé (blocs séparés)
- GET /proprietaires/{prop_id}/{mois}/prefacture — 12 lignes préfacture

---

## §22 — APP-3b-0 : Référentiel REF_Assoc_Mode

Résout l'ambiguïté du segment ASSOC_MODE dans la nomenclature `charge_id` (§16.2).

### Feuille REF_Assoc_Mode (REF_Setup.xlsm)

| Colonne | Rôle |
|---|---|
| assoc_mode_id (PK) | Identifiant AM_001..AM_007 |
| mode_paiement_id | FK REF_Modes_Paiement |
| associe_id | FK REF_Associes (nullable) |
| assoc_mode | Segment généré dans charge_id (BANQUE, LIQ, EWAN-CB, WAFA-CB, EWAN-PERSO, WAFA-PERSO, ADEF) |
| actif | OUI/NON |
| commentaire | Description métier |

Unicité : assoc_mode_id / (mode_paiement_id, associe_id) / assoc_mode.

Table Excel structurée : tblRefAssocMode.

Migration : outil `tools/preparer_ref_assoc_mode.py`, service `app/services/ref_assoc_mode_prepare_service.py`. Gated par cfg.REF_ASSOC_MODE_REAL_WRITE_ENABLED. Non exécutée en APP-3b-0 (préparation sur copie uniquement).

---

## §22.1 -- APP-3b-0 bis : Migration reelle REF_Assoc_Mode (2026-07-05)

REF_Assoc_Mode ajoutee dans REF_Setup.xlsm (migration reelle du 2026-07-05).

Hash REF_Setup.xlsm avant : 3354ce22e1ad667e1a672e4f793af091c2907b3cd5469da9661a1997c16149e8
Hash REF_Setup.xlsm apres : c5a544e6a73f2815fbbec7ee0b2777c230085086d3f417bf42c4747ce9a78d9a
Hash VBA inchange : 09eb44f98025583fad807b0784123e2c3d402ad3d38093b163170def3de8e5c2

Contenu valide :

| assoc_mode_id | mode_paiement_id | associe_id | assoc_mode | actif | commentaire |
|---|---|---|---|---|---|
| AM_001 | PAY_001 | | BANQUE | OUI | Banque professionnelle |
| AM_002 | PAY_002 | | LIQ | OUI | Especes caisse |
| AM_003 | PAY_003 | PERS_EWAN | EWAN-CB | OUI | Carte associee Ewan |
| AM_004 | PAY_003 | PERS_WAFA | WAFA-CB | OUI | Carte associee Wafa |
| AM_005 | PAY_004 | PERS_EWAN | EWAN-PERSO | OUI | Compte personnel Ewan |
| AM_006 | PAY_004 | PERS_WAFA | WAFA-PERSO | OUI | Compte personnel Wafa |
| AM_007 | PAY_005 | | ADEF | OUI | Mode a definir -- controle obligatoire |

Table Excel : tblRefAssocMode. Sauvegarde : 99_ARCHIVES/APP3B0_REF_ASSOC_MODE_20260705_182335/.

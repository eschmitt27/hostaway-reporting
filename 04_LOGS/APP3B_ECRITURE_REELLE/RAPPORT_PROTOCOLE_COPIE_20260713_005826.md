# APP-3b — Protocole d'écriture réelle exécuté sur COPIE ISOLÉE (20260713_005826)

**Verdict : PROTOCOLE VERT** — 59/59 contrôles passés.

> Aucun fichier métier réel n'a été modifié. Aucun flag n'a été activé (`CHARGES_REAL_WRITE_ENABLED` reste **False**).

## 1. Empreintes des fichiers réels (preuve de non-modification)

| Fichier réel | SHA256 avant | SHA256 après | Verdict |
|---|---|---|---|
| `SAISIE_Charges_Flux.xlsx` | `c2adaaf5dc9606a5…` | `c2adaaf5dc9606a5…` | INTACT |
| `SAISIE_Charges_Impacts.xlsx` | `21cd9fc41c5cf321…` | `21cd9fc41c5cf321…` | INTACT |
| `MASTER_FACT_MAN_IK_Avantages.xlsx` | `39d2ff8bc3fe2583…` | `39d2ff8bc3fe2583…` | INTACT |

## 2. Copies isolées (hors dépôt, jamais commitées)

- Scratch : `C:\Users\Ewan\AppData\Local\Temp\app3b_ecriture_20260713_005826`
- Une copie SAISIE + une copie Impacts par cas testé (sous `dryruns/<token>/`).
- Copie Lot7 régénérée : `C:\Users\Ewan\AppData\Local\Temp\app3b_ecriture_20260713_005826\MASTER_FACT_MAN_IK_Avantages.xlsx`

## 3. Cas testés (§8.1 du protocole)

| Cas | Charge | Attendu |
|---|---|---|
| A | CHG_025 Repas, 100 €, PAY_001, avantage `PERS_EWAN` | 1 ligne SAISIE, aucun impact ménage/réserve, avantage HR en Lot7 |
| B | CHG_008 Maintenance, 100 €, PAY_001, refacturable, LOG_0001 | 1 AFFECTATION (Σ=100), 1 RESERVE `EN_ATTENTE` |
| C | CHG_004 Achat ménage, 100 €, PAY_001, INT_0001 | 1 ligne MENAGE (intervenant XOR logement), aucune réserve |

- Cas A — `charge_id` : **CHG-2026-06-IC-BANQUE-001** (ligne 2), `type_flux_id` dérivé : TYPE_FLUX_020, `assoc_mode` : BANQUE.

### Ligne produite dans MASTER_CALC_AVANTAGES (Lot7, sur copie)

| pk_id | mois | associe_id | bruts | nets | code_impact | source_calcul | sens_suivi |
|---|---|---|---|---|---|---|---|
| PERS_EWAN-2026-06 | 2026-06 | PERS_EWAN | 100 | 100 | **HR** | LOT7 | A_CONTROLER_POSITIF |

## 4. Contrôles

| Contrôle | Résultat | Détail |
|---|---|---|
| Flag CHARGES_REAL_WRITE_ENABLED = False (non modifié) | OK |  |
| Flag CHARGES_REAL_WRITE_CONFIRMATION_ENABLED = False (non modifié) | OK |  |
| persister_reel() verrouillé (PermissionError tant que le flag est off) | OK |  |
| Fichier réel présent : SAISIE_Charges_Flux.xlsx | OK | C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\01_SOURCES_BRUTES\Charges\SAISIE_Charges_Flux.xlsx |
| Fichier réel présent : SAISIE_Charges_Impacts.xlsx | OK | C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\01_SOURCES_BRUTES\Charges\SAISIE_Charges_Impacts.xlsx |
| Fichier réel présent : MASTER_FACT_MAN_IK_Avantages.xlsx | OK | C:\Users\Ewan\OneDrive\Documents\Conciergerie\Pilotage_Conciergerie\02_TRAVAIL\Lot7_IK_Avantages\MASTER_FACT_MAN_IK_Avantages.xlsx |
| [A] Prévisualisation acceptée (validations métier vertes) | OK | [] |
| [A] Colonnes formule C/I/J/AD non écrasées (formules vivantes) | OK | C=formule; I=formule; J=formule; AD=formule |
| [A] Exactement 1 ligne charge après DOUBLE écriture (pas de doublon) | OK | 1 ligne(s) |
| [A] charge_id unique dans la feuille SAISIE | OK | 1 charge(s) |
| [A] Colonnes manuelles écrites (montant / catégorie / mode / type_flux) | OK | montant=100 cat=CHG_025 mode=PAY_001 tf=TYPE_FLUX_020 |
| [A] avantage_associe_id = PERS_EWAN | OK | PERS_EWAN |
| [A] AFFECTATIONS : 0 ligne(s) (après double écriture) | OK | 0 ligne(s) |
| [A] MENAGE : 0 ligne(s) | OK | 0 ligne(s) |
| [A] RESERVE_REFACTURATION : 0 ligne(s) | OK | 0 ligne(s) |
| [A] Intégrité Excel SAISIE_Charges_Flux.xlsx (onglets / validations / tables / VBA) | OK | sheets=4 validations=20 |
| [A] Intégrité Excel SAISIE_Charges_Impacts.xlsx (onglets / validations / tables / VBA) | OK | sheets=4 validations=0 |
| [B] Prévisualisation acceptée (validations métier vertes) | OK | [] |
| [B] Colonnes formule C/I/J/AD non écrasées (formules vivantes) | OK | C=formule; I=formule; J=formule; AD=formule |
| [B] Exactement 1 ligne charge après DOUBLE écriture (pas de doublon) | OK | 1 ligne(s) |
| [B] charge_id unique dans la feuille SAISIE | OK | 1 charge(s) |
| [B] Colonnes manuelles écrites (montant / catégorie / mode / type_flux) | OK | montant=100 cat=CHG_008 mode=PAY_001 tf=TYPE_FLUX_011 |
| [B] avantage_associe_id = (vide) | OK | None |
| [B] AFFECTATIONS : 1 ligne(s) (après double écriture) | OK | 1 ligne(s) |
| [B] AFFECTATIONS : Σ quote_part = montant (100.0) | OK | Σ=100.0 |
| [B] MENAGE : 0 ligne(s) | OK | 0 ligne(s) |
| [B] RESERVE_REFACTURATION : 1 ligne(s) | OK | 1 ligne(s) |
| [B] RESERVE : statut EN_ATTENTE + Σ montant_refacturable = montant | OK | Σ=100.0 |
| [B] Intégrité Excel SAISIE_Charges_Flux.xlsx (onglets / validations / tables / VBA) | OK | sheets=4 validations=20 |
| [B] Intégrité Excel SAISIE_Charges_Impacts.xlsx (onglets / validations / tables / VBA) | OK | sheets=4 validations=0 |
| [C] Prévisualisation acceptée (validations métier vertes) | OK | [] |
| [C] Colonnes formule C/I/J/AD non écrasées (formules vivantes) | OK | C=formule; I=formule; J=formule; AD=formule |
| [C] Exactement 1 ligne charge après DOUBLE écriture (pas de doublon) | OK | 1 ligne(s) |
| [C] charge_id unique dans la feuille SAISIE | OK | 1 charge(s) |
| [C] Colonnes manuelles écrites (montant / catégorie / mode / type_flux) | OK | montant=100 cat=CHG_004 mode=PAY_001 tf=TYPE_FLUX_020 |
| [C] avantage_associe_id = (vide) | OK | None |
| [C] AFFECTATIONS : 0 ligne(s) (après double écriture) | OK | 0 ligne(s) |
| [C] MENAGE : 1 ligne(s) | OK | 1 ligne(s) |
| [C] MENAGE : intervenant XOR logement (jamais les deux) | OK | intervenant=True logement=False (cols=10) |
| [C] RESERVE_REFACTURATION : 0 ligne(s) | OK | 0 ligne(s) |
| [C] Charge ménage : JAMAIS de réserve de refacturation | OK |  |
| [C] Intégrité Excel SAISIE_Charges_Flux.xlsx (onglets / validations / tables / VBA) | OK | sheets=4 validations=20 |
| [C] Intégrité Excel SAISIE_Charges_Impacts.xlsx (onglets / validations / tables / VBA) | OK | sheets=4 validations=0 |
| Aucune ligne SOURCE_SAISIE Lot7 créée (avantage porté par la charge, jamais ressaisi) | OK | 0 ligne(s) résiduelle(s) réelle(s) |
| Lot7 idempotent (2e génération identique — jamais ×2) | OK | 1 → 1 ligne(s) |
| Lot7 : la charge test ressort pour le bon associé/mois, exactement 1 fois | OK | 1 ligne(s) |
| Lot7 : aucune ligne parasite (gabarit 'AAAA-MM' de SOURCE_SAISIE rejeté) | OK | 1 ligne(s) au total |
| Lot7 : avantage_brut_depenses_perso = 100.00 (montant de la charge) | OK | 100 |
| Lot7 : avantages_nets = 100.00 (bruts − charges_payées − remboursements) | OK | 100 |
| Lot7 : code_impact = HR (hors résultat réel ET comptable) | OK | HR |
| Lot7 : source_calcul = LOT7 | OK |  |
| Lot7 : aucune anomalie de génération | OK | [] |
| MASTER_CALC_AVANTAGES : aucune colonne propriétaire / résultat / préfacture | OK | aucune |
| Persistable : aucune table de règlement / préfacture / net propriétaire | OK | affectations, avantage, charge_id, controle_somme_quotes, menage, reserve |
| Persistable : 1 seul marqueur avantage, porté par la charge (jamais écrit en Lot7) | OK | {"charge_id": "CHG-2026-06-IC-BANQUE-001", "mois": "2026-06", "avantage_associe_id": "PERS_EWAN", "montant": 100.0, "porte_par": "SAISIE_Charges_Flux.avantage_associe_id"} |
| Lot11 : aucune anomalie BLOQUANTE | OK | [] |
| Lot11 : aucune anomalie A_CONTROLER (aucun faux positif) | OK | [] |
| Lot11 : les 2 cross-contrôles s'activent (calc régénéré, non vide) | OK | actifs |
| FICHIERS RÉELS INTACTS (SHA256 avant == après, les 3 fichiers) | OK | SAISIE_Charges_Flux.xlsx: inchangé; SAISIE_Charges_Impacts.xlsx: inchangé; MASTER_FACT_MAN_IK_Avantages.xlsx: inchangé |

## 5. Contrôles Lot11 (suivi associé)

- Anomalies remontées : **0**

## 6. Rollback

- Les fichiers réels n'ont jamais été ouverts en écriture : **aucun rollback nécessaire**.
- Rollback des copies = suppression du dossier scratch `C:\Users\Ewan\AppData\Local\Temp\app3b_ecriture_20260713_005826`.

## 7. Limites constatées (bloquantes pour l'activation)

1. **Aucun writer réel n'existe.** `persister_reel()` est un stub : il lève `PermissionError` tant que le flag est off, et `NotImplementedError` même si on l'activait. Ce protocole prouve le **moteur** (`build_persistable` + `_inject_row` + `persister_sur_copie`) sur copie ; activer `CHARGES_REAL_WRITE_ENABLED` ne suffirait donc à rien — il faut d'abord écrire la chaîne réelle : sauvegarde → injection de la ligne dans le vrai `SAISIE_Charges_Flux` → impacts dans le vrai `SAISIE_Charges_Impacts` → régénération Lot7.
2. **Séquence `charge_id` non réservée.** `count_charges_with_prefix` compte les charges du fichier *source* ; en dry-run la source n'est jamais écrite, donc les trois cas obtiennent tous `-001` (attendu ici). En écriture réelle le compteur avancerait, mais **rien ne réserve l'identifiant** : deux saisies concurrentes produiraient le même `charge_id`. À verrouiller avant activation.
3. **Classeur ouvert dans Excel / synchronisé OneDrive : non testé.** L'écriture réelle échouera (ou écrasera une version en cours) si le classeur est ouvert. Prévoir un contrôle de verrou avant écriture.
4. **Recalcul des formules différé.** openpyxl pose `fullCalcOnLoad = True` : les colonnes `C/I/J/AD` restent des formules vivantes mais **sans valeur en cache** tant qu'Excel n'a pas rouvert le fichier. Lot7 est immunisé (il dérive `mois` de `date_charge`), mais tout lecteur `data_only=True` qui dépendrait de `C/I/J` lirait vide. À vérifier lot par lot avant activation.
5. **Périmètre non couvert par cette exécution** : répartition multi-logements (cent-exacte) et `.xlsm` avec VBA (`keep_vba`) — les 3 fichiers cibles sont des `.xlsx` sans projet VBA. Ces points restent couverts uniquement par les tests unitaires.

## 8. Décision

- **Moteur d'écriture : PRÊT et prouvé sur copie** (3 cas, 59 contrôles, fichiers réels intacts).
- **Activation réelle : NON. `CHARGES_REAL_WRITE_ENABLED` doit rester `False`** tant que les limites 1 à 4 ci-dessus ne sont pas levées. Le §8 du protocole exige en plus une décision humaine tracée au JOURNAL_CONTROLES.

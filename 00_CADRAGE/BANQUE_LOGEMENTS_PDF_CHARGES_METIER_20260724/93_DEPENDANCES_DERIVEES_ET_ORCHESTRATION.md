# 93 — Dépendances dérivées de l'application et orchestration des traitements

Point de départ : `/logements` affichait « Source liste introuvable : PBI_Referentiel_Logements.csv ».
Ce n'était pas un défaut de l'écran. `03_EXPORTS/PowerBI/` est ignoré par Git : un worktree ne reçoit
que les fichiers suivis, donc ce dossier n'y a jamais existé. Le constat a ouvert une question plus
large — **quels fichiers l'application attend-elle sans que Git ne les fournisse ?**

## 1. Règle d'architecture conservée

L'application ne recalcule rien. Elle lit les sorties du moteur.

La correction n'a donc **pas** consisté à faire lire `REF_Setup` à `/logements`, mais à générer les
exports manquants et à brancher le service sur le bon export.

## 2. Inventaire des dépendances fichier (hors SQLite)

29 constantes de chemin `.xlsx` / `.xlsm` / `.csv` sont déclarées dans `app/config.py`.

| Consommateur app | Fichier attendu | Producteur | État |
|---|---|---|---|
| `logements_service` | `03_EXPORTS/PowerBI/PBI_Referentiel_Logements.csv` | lot13 | PRESENT (régénéré) |
| `logements_service` | `03_EXPORTS/PowerBI/PBI_Referentiel_Gestion_Logements.csv` | lot13 | PRESENT (régénéré) |
| `banques_reader`, `controles_detail_reader`, `banques_import_service`, `controles_runner_service` | `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` | lot8a → lot8b → lot8c | PARTIEL (voir §5) |
| tous les autres modules | 26 masters `02_TRAVAIL/**` + sources `01_SOURCES_BRUTES/**` | lots 1 à 12 | PRESENT — suivis par Git |

Après régénération : **0 fichier manquant sur 29**.

Le seul fichier absent au départ (hors exports Power BI) était `BANQUE_LOT8_IMPORT.xlsx`, exclu de
Git par `.gitignore` lignes 30/33-34 (données bancaires réelles, jamais versionnées).

## 3. Ce qui a été régénéré

| Traitement | Sorties | Fichiers suivis par Git ? |
|---|---|---|
| `lot13_export_powerbi.py` | 13 CSV + dictionnaire dans `03_EXPORTS/PowerBI/` | non (ignorés) |
| `lot8a_banque_import.py` | `BANQUE_LOT8_IMPORT.xlsx` — 541 mouvements, 6 onglets | non (ignorés) |
| `lot8c_rapprochement_banque.py` | 3 onglets d'attente dans le même classeur | non (ignorés) |

Aucune source métier n'a été modifiée. `REF_Setup.xlsm` conserve son empreinte d'avant mission.

**L'ancien CSV du dépôt principal n'a pas été copié** : il datait du 18 juin et portait encore
`date_entree_gestion = 2026-01-01` pour des périodes ramenées depuis à `2025-01-01`. Le régénérer
depuis les sources actuelles était la seule option correcte.

## 4. Ce qui n'a **pas** été relancé, et pourquoi

### 4.1 `lot8b_banque_regles.py` — écrit une source réelle

Ligne 415 : `wb.save(REF_PATH)` où `REF_PATH = 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`.
Le lot réécrit inconditionnellement le référentiel réel pour y semer `REF_Banque_Regles` — feuille
qui **existe déjà**. Aucune variable d'environnement ne permet de rediriger ce chemin (contrairement
à `lot8a`, qui expose `LOT8A_BRUT_FILE_OVERRIDE` / `LOT8A_OUT_FILE_OVERRIDE`).

`lot8a` l'annonce lui-même en fin de run : « Prochain lot : 8b — ne pas lancer avant validation
humaine de ce bilan. »

**Conséquence directe** : les 541 mouvements sont importés mais **540 restent
`EN_ATTENTE_CLASSIFICATION`**. `lot8c`, qui filtre sur les colonnes de classification produites par
`lot8b`, sort donc 0 ligne dans ses trois onglets d'attente. La chaîne Banque est **fonctionnelle
mais non classée**.

Décision requise : autoriser `lot8b` à réécrire `REF_Setup.xlsm`, ou lui ajouter un override de
chemin comme en possède `lot8a`.

### 4.2 Chaîne aval `lot4quater → lot9 → lot10 → lot11 → lot12` — sorties suivies par Git

Les 26 masters de `02_TRAVAIL/` **sont versionnés**. Les relancer modifierait des fichiers suivis :
ce n'est pas la régénération d'un dérivé jetable, c'est un recalcul du réel, qui relève de
`/calculs` et d'une décision utilisateur.

**Mais ces masters sont périmés.** Ils datent du 24 juillet, soit avant le commit `f2aa0a3`
« Préparation canapé ». Constats mesurés :

- `MASTER_CALC_Commissions.xlsx` : aucune colonne `preparation_canape_voyageurs` ni
  `controle_preparation_canape` ;
- `MASTER_CALC_NetProprietaire.xlsx` : aucune colonne `total_preparation_canape_mois` ;
- `MASTER_FACT_Proprietaires.xlsx` (Lot 12) : **aucune ligne `PREPARATION_CANAPE`** — 12 types de
  ligne présents, 270 factures chacun.

`lot13` a signalé les colonnes absentes et les a ignorées, sans échouer.

**Impact réel** : la préparation canapé est l'un des 5 types facturables de
`factures_proprietaires_service`. Tant que la chaîne aval n'est pas relancée, une facture produite
depuis cet environnement **omettrait silencieusement les lignes canapé**. Aucun service applicatif ne
lit directement une colonne canapé — l'impact passe entièrement par le master Lot 12.

### 4.3 `lot5_master_acomptes_proprietaires.py` — écrit une source de saisie

Le script ne calcule pas : il **crée le squelette** de `SAISIE_AcomptesProprietaires.xlsx`
(source réelle) et du master. Le relancer écraserait la saisie.

Le passage SAISIE → MASTER n'est pas fait par Python mais par **Power Query**, dont le code M vit
dans l'onglet `POWER_QUERY_CODE` (192 lignes) du master. Actualisation **manuelle dans Excel**.

État : `MASTER` vide (en-tête seul), `VUE_ACTIVE` 1 ligne. Cohérent avec le constat des missions
précédentes.

Régénérabilité : **non automatisable en l'état**. Ni CLI, ni UI — Excel.

## 5. Chaîne Banque — état exact

| Lot | Rôle | Rejouable sans risque ? | Résultat aujourd'hui |
|---|---|---|---|
| lot8a | import + normalisation | oui | 541 mouvements, 0 BLOQUANT, 1 A_CONTROLER |
| lot8b | classification déterministe | **non** — réécrit `REF_Setup.xlsm` | non lancé |
| lot8c | onglets d'attente | oui — n'écrit que le classeur Banque | 0 ligne (dépend de lot8b) |

`lot8a` a relevé `BANQUE_FICHIER_PERIODE_INCOHERENTE` : le fichier nommé `2026_03` couvre en réalité
`2025-11-03 → 2026-08-01`. Contrôle informatif du moteur, remonté tel quel.

## 6. Après création d'un worktree — séquence de démarrage

1. Vérifier les sources : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` et les masters de
   `02_TRAVAIL/` arrivent avec Git. Rien à faire.
2. Déposer le relevé bancaire dans `01_SOURCES_BRUTES/Banque/` (jamais versionné).
3. `python 02_TRAVAIL/lot8a_banque_import.py` — produit `BANQUE_LOT8_IMPORT.xlsx`.
4. `python 02_TRAVAIL/lot8b_banque_regles.py` — classification. **Écrit `REF_Setup.xlsm`** : à
   n'exécuter qu'en connaissance de cause.
5. `python 02_TRAVAIL/lot8c_rapprochement_banque.py` — onglets d'attente.
6. `python 02_TRAVAIL/lot13_export_powerbi.py` — produit les 13 CSV de `03_EXPORTS/PowerBI/`.
7. Lancer l'application.

Les étapes 3 à 6 exigent l'interpréteur portant pandas (`C:\Program Files\Python312\python.exe`),
pas celui de l'application.

Les lots 1 à 12 ne sont **pas** nécessaires à un démarrage : leurs sorties sont versionnées. Ils ne
se relancent que pour actualiser les données.

## 7. Orchestration — UI ou CLI, traitement par traitement

| Chaîne | Source | Périodicité | Déclenchement | Verdict |
|---|---|---|---|---|
| lot4quater → lot9 → lot10 → lot11 → lot12 → lot13 | masters amont | mensuelle | écran `/calculs` : prévisualisation, sélection de lots, run tracé, restauration | **A — UI** |
| lot3 (charges) | `SAISIE_Charges_Flux.xlsx` | au fil de l'eau | `/calculs`, chaîne « charges » | **A — UI** |
| lot6b → lot6f (ménages) | feuille Google M04 + factures externes | mensuelle | `/menages/recalculer`, **sur copies isolées uniquement** — l'accès réseau est remplacé par un stub | **A/B — UI sur copies, CLI pour le réel** |
| lot1 (Hostaway) | API Hostaway | quotidienne/hebdo | aucun écran | **B — CLI** |
| lot4bis / lot4ter | masters Hostaway + HH | mensuelle | aucun écran | **B — CLI** |
| lot5 (acomptes) | saisie Excel | ponctuelle | Power Query dans Excel | **B — Excel, ni CLI ni UI** |
| lot6a (comptage tâches) | Hostaway | mensuelle | aucun écran | **B — CLI** |
| lot7 (IK / avantages) | saisie | ponctuelle | aucun écran | **B — CLI** |
| lot8a / lot8b / lot8c (Banque) | relevé déposé | mensuelle | l'import applicatif `/banques-caisse/importer` existe, mais la chaîne moteur reste hors `/calculs` | **B — CLI** |

**Verdict d'orchestration : PARTIELLE.** La chaîne aval et les charges se pilotent depuis
l'application. Hostaway, réservations, Banque et acomptes restent en ligne de commande — ce sont
précisément les chaînes qui touchent une API externe, une source réelle ou Excel.

Le pilotage réel depuis `/calculs` reste par ailleurs verrouillé par
`CALCULS_REAL_RUN_ENABLED` (double verrou standard), non activé.

## 8. Imputation d'un règlement sur facture propriétaire — état exact

Classée `PARTIEL` dans la matrice `92`. Vérification faite, voici ce que recouvre « câblage
restant ».

**Ce qui fonctionne déjà :**

- `factures_proprietaires_service.solde()` calcule correctement total / imputé / reste et dérive
  4 statuts : `NON_REGLEE`, `PARTIELLEMENT_REGLEE`, `REGLEE`, `TROP_PERCU_A_CONTROLER` ;
- `/creances` affiche les colonnes Réglé / Compensé / Solde et le filtre par statut ;
- `/echeancier` ventile par tranche d'échéance ;
- `creances_dettes_service._imputations()` est le point d'accroche unique, explicitement documenté.

**Ce qui manque réellement :**

`_imputations()` retourne `0.0` en dur. Aucun objet du modèle ne peut porter une imputation :

- `banques_rapprochement_service.TYPES_OBJET` compte 9 types — `RESERVATION`,
  `PAYOUT_PLATEFORME`, `CHARGE_FOURNISSEUR`, `REGLEMENT_CHARGE`, `REVERSEMENT_PROPRIETAIRE`,
  `REMBOURSEMENT_ASSOCIE`, `REMBOURSEMENT_VOYAGEUR`, `MOUVEMENT_INTERNE`, `NON_IDENTIFIE` — et
  **aucun** ne désigne une facture propriétaire ;
- `proprietaires_tresorerie_service.NATURES` compte 7 natures et **aucune** ne correspond au
  règlement d'une facture émise ;
- la colonne « Compensé » est câblée à `0.0` : compenser une facture par le net propriétaire suppose
  une règle métier qui n'est pas arbitrée.

**Verdict : PARTIEL, et c'est une véritable brique à construire**, pas un bug local. Il faut
une table d'imputation append-only, un type d'objet dédié, une action utilisateur, et les règles de
refus (facture non émise, facture annulée, période clôturée, dépassement, désimputation).

Elle dépend en outre de `QUESTION_01` (nature des encaissements propriétaires), toujours sans
réponse, et de la décision déjà enregistrée : « à l'avenir, les règlements devront être différenciés
selon leur vraie nature au moment de la saisie ».

Cette mission ne la construit pas — elle la nomme.

## 9. Bug corrigé au passage — propriétaire absent de la liste des logements

**Symptôme** : `/logements` listait bien 17 logements mais affichait « Non renseigné » dans la
colonne Propriétaire sur **toutes** les lignes ; le filtre par propriétaire était vide.

**Cause** : le commit moteur `c8dea3c` « Supprime dates gestion dupliquees » a sorti
`proprietaire_id`, `date_entree_gestion` et `date_sortie_gestion` de
`PBI_Referentiel_Logements.csv` pour les porter dans `PBI_Referentiel_Gestion_Logements.csv`.
`logements_service` n'a pas suivi et cherchait toujours `proprietaire_id` dans le premier fichier.

**Pourquoi c'était invisible** : une colonne absente d'un CSV ne lève rien. Le service produisait
une chaîne vide, l'écran affichait le libellé de repli. La fiche détail, elle, lisait `REF_Setup`
directement et restait juste — l'incohérence entre liste et fiche était le seul signal.

**Effet de bord** : `commission_rows` était résolu depuis ce `proprietaire_id` vide. L'historique
des taux de commission était donc vide sur **toutes** les fiches logement.

**Correction** : lecture du second export du moteur. La sélection s'appuie sur `statut_gestion`,
champ **produit par le moteur** — aucune comparaison de dates n'est refaite dans l'application. Cinq
états explicites : `RESOLU`, `CLOS`, `A_CONTROLER`, `ABSENT`, `SOURCE_ABSENTE`. Un rattachement
ambigu n'est jamais tranché arbitrairement, et l'absence de l'export est désormais **affichée**
plutôt que silencieuse.

**Vérifié** : 17 logements, 12 propriétaires distincts, `LOG_0003` marqué « gestion terminée »,
filtre opérationnel. 14 rattachements portent bien `date_debut = 2025-01-01` (correction gestion
2025) et 3 conservent `2026-01-01`, conformes à leur historique propre.

8 tests de non-régression ajoutés.

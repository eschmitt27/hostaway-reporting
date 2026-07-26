# 35 — Audit ciblé : pipeline de calculs et clôture

Audit ciblé (≤45 min). Objectif : savoir quoi piloter, avec quel interpréteur, et **ne recréer
aucun orchestrateur qui existe déjà**.

## Correction d'une limite documentée à tort

Les documents précédents (`31`, `33`, `HANDOFF_CANONIQUE`) affirmaient que « les moteurs Lot9/Lot10
ne sont pas exécutables ici car `pandas` est absent ». **C'est faux et je le corrige** : `pandas`
est absent de l'interpréteur *de l'application*, pas de la machine.

| Interpréteur | Version | pandas | openpyxl | Rôle |
|---|---|:--:|:--:|---|
| `C:\Users\Ewan\miniconda3\python.exe` | 3.12.9 | ❌ | ✅ | application FastAPI + tests |
| `C:\Program Files\Python312\python.exe` | 3.12.3 | ✅ | ✅ | **moteurs (lots)** |

`app/config.py` connaissait déjà ce second interpréteur (`LOT4A_ENGINE_PYTHON`,
`MENAGES_ENGINE_PYTHON`) — le pilotage doit s'appuyer dessus.

**Preuve d'exécution réelle** : lancé sur le jeu de recette,
`lot9_construire_flux.py` s'exécute et retourne des contrôles bloquants explicites plutôt qu'une
erreur d'import :

```
BLOQUANT [CTR-9-001] Source manquante : MASTER_CALC_Reservations (…Lot4quater_SourceResolue\MASTER_CALC_Reservations_Resolues.xlsx)
BLOQUANT [CTR-9-001] Source manquante : MASTER_FACT_MEN_MenagesExternes (…Lot6c_MenagesExternes\MASTER_FACT_MEN_MenagesExternes.xlsx)
```

C'est le comportement idéal pour un vérificateur de prérequis : **chaque lot déclare lui-même ses
sources manquantes**. Tous les lots inspectés (3, 7, 8a, 9, 10, 11, 12, 13) émettent des contrôles
`BLOQUANT`.

## Orchestrateurs existants — à réutiliser, pas à réécrire

| Orchestrateur | Chaîne | Remarque |
|---|---|---|
| `02_TRAVAIL/run_regression_pipeline.py` | lot4quater → **lot9 → lot10 → lot11 → lot12 → lot13** | Chaîne AVAL de référence. Relance deux fois et compare un manifeste métier (hors colonnes techniques de date). Définit aussi les 9 tables de sortie à comparer. |
| `02_TRAVAIL/run_menages_pipeline.py` | lot6b → lot6d → lot6e → lot6f | Chaîne MÉNAGES. S'arrête au premier échec (`[BLOQUANT] … Pipeline arrêté`). Dit explicitement « lot9-12 NON relancés ». |
| `app/services/menages_chaine_service.py` + `app/runners/` | 6a→6f→11 sur copies | Recalcul ménages sur workspace isolé, déjà livré (APP-2b). |
| `app/services/controles_runner_service.py` | lot8c + lot11 sur copies | Déjà livré (APP-5B). |

Les deux runners `02_TRAVAIL/*.py` invoquent `subprocess.run([sys.executable, script])` : ils
héritent donc de l'interpréteur qui les lance — d'où l'importance de les lancer avec le bon.

## Cartographie des lots

| Lot | Entrées | Sorties | Dépend de | Bloquants | Durée |
|---|---|---|---|---|---:|
| lot3 charges | `SAISIE_Charges_Flux/Impacts`, REF_Setup | `Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx` | saisie | oui | non mesurée |
| lot6b/6d/6e/6f ménages | M04, Hostaway cleaning, ménages externes | `Lot6*/MASTER_*` | 6a (réseau) | oui | non mesurée |
| lot7 IK/avantages | `MASTER_FACT_MAN_IK_Avantages` | `Lot7_IK_Avantages/*` | saisie | oui | non mesurée |
| lot8a/8b/8c banque | brut CM ou `NORM_Banque` | `Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` | relevé | oui | non mesurée |
| lot4quater | réservations Hostaway + HH | `Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx` | lot1/lot4bis | oui | non mesurée |
| **lot9** flux unifié | réservations résolues, ménages externes, `NORM_Banque` (TYPE_FLUX_016 VALIDE), charges | `Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx` | lot4quater, lot6c, lot8, lot3 | **oui (CTR-9-001)** | non mesurée |
| **lot10** résultats | flux unifié, REF_Setup (taux, gestion) | `Lot10_Resultats/MASTER_CALC_{Commissions,Resultats,NetProprietaire}.xlsx` | lot9 | oui | non mesurée |
| lot11 contrôles | toutes sorties | `Lot11_Controles/MASTER_CTRL_Coherence.xlsx` | lot10 | oui | non mesurée |
| lot12 préfactures | résultats, net propriétaire | `Lot12_Factures/MASTER_FACT_Proprietaires.xlsx` | lot10 | oui | non mesurée |
| lot13 export PowerBI | toutes sorties | `03_EXPORTS/PowerBI/*.csv` | lot10/11/12 | oui | non mesurée |

Les durées ne sont pas renseignées : elles seront mesurées et journalisées par l'exécuteur
plutôt qu'estimées à la main.

## État de la recette pour un pipeline complet

Le jeu `data_recette` alimente aujourd'hui : REF_Setup, charges (SAISIE + MASTER de démarrage),
Lot7 (structure vide), `BANQUE_LOT8_IMPORT.xlsx`, PBI logements, base applicative
factures/fournisseurs. **Manquent** pour que lot9 aille au bout :
`Lot4quater_SourceResolue/MASTER_CALC_Reservations_Resolues.xlsx` et
`Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx`.

`recette/build_reservations_recette.py` existe et cible probablement le premier — à vérifier au
moment de construire la recette de nuit.

## Décisions pour la construction

1. **Aucun nouvel orchestrateur métier.** Le pilotage applicatif appellera les runners existants
   (`run_regression_pipeline.py`, `run_menages_pipeline.py`) ou les lots un par un, dans l'ordre
   qu'ils définissent déjà.
2. **Exécuteur contrôlé** (nouveau, côté application) : choisit l'interpréteur par lot, capture
   stdout/stderr/code retour, impose un timeout, mesure la durée, et ne conclut au succès que si
   `returncode == 0` **et** les sorties attendues existent.
3. **Prérequis** : s'appuyer sur les contrôles `BLOQUANT` que les lots émettent déjà, plus une
   vérification d'existence des fichiers d'entrée avant lancement.
4. **Mode réel désactivé par défaut**, comme partout ailleurs : `CALCULS_REAL_RUN_ENABLED` +
   confirmation, tous deux sous `RECETTE_MODE`.
5. **Transactionnel** : sauvegarde des sorties avant exécution, restauration si la chaîne devient
   incohérente, journal SQLite des runs et de chaque lot.

# 36 — Module Pilotage des calculs et clôture : état

Suite de l'audit `35_AUDIT_PIPELINE_CALCULS.md`.

## Ce qui est construit et prouvé

| Fonction | Statut | Preuve |
|---|:--:|---|
| Exécuteur contrôlé (interpréteur par lot, vérifié avant lancement) | ✅ | `test_calculs_executeur.py` (20) |
| Capture stdout / stderr / code retour | ✅ | `test_stderr_capture`, recette réelle (traceback lot4quater capturé) |
| Timeout imposé | ✅ | `test_timeout_respecte` |
| Durée mesurée et journalisée | ✅ | recette réelle : lot4quater 1,7 s |
| **Jamais de faux succès** | ✅ | `test_code_retour_zero_mais_sortie_absente_est_un_echec` |
| Environnement global jamais modifié | ✅ | `test_environnement_global_non_modifie` |
| Chemins sensibles masqués dans les logs | ✅ | `test_nettoyer_chemins_masque_racine_et_utilisateur` + recette (`<projet>`) |
| Ordre des lots repris des orchestrateurs existants | ✅ | 2 tests comparant à `run_regression_pipeline.py` / `run_menages_pipeline.py` |
| Prévisualisation scellée (token + empreinte) | ✅ | `test_previsualisation_ne_lance_rien` |
| Refus si entrées modifiées depuis la prévisualisation | ✅ | `test_entrees_modifiees_refusent_le_lancement` + HTTP |
| Refus si prérequis non satisfaits | ✅ | `test_prerequis_non_satisfaits_refusent_le_lancement` |
| Arrêt au premier échec, suivants IGNORE | ✅ | `test_arret_au_premier_echec…` + **recette réelle** |
| Pipeline partiel jamais annoncé réussi | ✅ | `test_pipeline_partiel_jamais_presente_comme_reussi` |
| Sauvegarde des sorties + rollback | ✅ | `test_sauvegarde_puis_restauration`, `test_rollback_apres_echec`, HTTP |
| Indicateurs relevés depuis les fichiers réels | ✅ | `test_indicateurs_releves_depuis_les_fichiers_reels` |
| Fichier absent = indicateur absent (jamais un 0 inventé) | ✅ | `test_indicateur_absent_si_fichier_absent` |
| Comparaison avant/après + écart anormal | ✅ | `test_comparaison_avant_apres` |
| Clôture : 7 statuts, transitions contrôlées | ✅ | `test_transition_interdite_refusee` |
| Clôture VALIDEE refusée si conditions non réunies | ✅ | `test_validation_refusee_si_conditions_non_reunies` + HTTP |
| Mode réel refusé par défaut | ✅ | `test_mode_reel_refuse_par_defaut` |
| Écrans `/calculs`, prévisualisation, suivi de run | ✅ | `test_calculs_routes.py` (14) + recette navigateur |

## Correction majeure d'une limite documentée à tort

Les tours précédents affirmaient que « les moteurs Lot9/Lot10 ne sont pas exécutables ici, pandas
étant absent ». **C'était faux.** pandas manque à l'interpréteur *de l'application* ; la machine
dispose de `C:\Program Files\Python312\python.exe` (3.12.3) qui le porte, et `app/config.py` le
connaissait déjà (`LOT4A_ENGINE_PYTHON`).

Conséquence : le pilotage exécute **réellement** les lots. Prouvé en recette navigateur — voir
ci-dessous.

Corollaire de conception trouvé pendant l'écriture des tests : le besoin de pandas est une propriété
**du lot**, pas du pilotage. Ma première version bloquait tout lot dès que l'interpréteur n'avait pas
pandas ; corrigé par `Lot.requiert_pandas`, avec deux tests symétriques.

## Recette navigateur réelle (données fictives, port 8030)

1. `/calculs` — interpréteur `C:\Program Files\Python312\python.exe`, version **3.12.3**, pandas
   **présent**, prérequis **satisfaits**, chaîne aval affichée
   (`lot4quater → lot9 → lot10 → lot11 → lot12 → lot13`) avec dépendances et sorties attendues.
2. **Prévisualisation** — fichiers lus (présents, horodatés), 6 sorties « non (création) », token et
   empreinte affichés. Aucune exécution.
3. **Lancement** — les lots ont **réellement tourné** :

| # | Lot | Statut | Code | Durée | Message |
|--:|---|---|--:|--:|---|
| 1 | lot4quater | **ECHEC** | 1 | 1,7 s | Code retour 1. |
| 2 | lot9 | IGNORE | — | 0,0 | Non lancé : un lot précédent a échoué. |
| 3 | lot10 | IGNORE | — | 0,0 | idem |
| 4 | lot11 | IGNORE | — | 0,0 | idem |
| 5 | lot12 | IGNORE | — | 0,0 | idem |
| 6 | lot13 | IGNORE | — | 0,0 | idem |

Run global **ECHEC** (0 réussi / 1 en échec) — jamais présenté comme un succès. stdout et stderr
réels capturés et consultables, avec la racine du projet masquée (`<projet>\02_TRAVAIL\…`).
Bouton de restauration proposé.

**L'échec est légitime et attendu** : `lot4quater` a besoin de sources de réservations que
`build_data_recette.py` ne produit pas (limite déjà identifiée à l'audit §« État de la recette »).
C'est exactement le comportement voulu : le pipeline s'arrête proprement, dit quel lot a échoué avec
son code retour et sa trace, et n'engage pas la suite.

## ⚠️ Ce document est dépassé sur trois points — voir `37`

Le Bloc 1 annoncé ci-dessous a été réalisé. Depuis `37_CHAINE_AVAL_RECETTE_ET_DEFAUT_LOT13.md` :

- **la chaîne `lot4quater → lot12` tourne réellement en SUCCES** (run `RUN-27CA69FD8D87`, 20,4 s) ;
- **la comparaison avant/après est alimentée** par deux runs réussis (tous écarts 0,00, ce qui
  prouve aussi l'idempotence des moteurs) ;
- **la clôture `VALIDEE` est atteinte** de bout en bout, et persiste après redémarrage.

Ce qui reste bloqué est **lot13**, pour un défaut du moteur lui-même (whitelist et filet
anti-sensible contradictoires), et non pour une limite du pilotage. Le tableau ci-dessous reste
valable pour tout le reste.

## Ce qui N'EST PAS fait (honnêteté)

| Attendu | État | Raison |
|---|---|---|
| Pipeline fictif complet allant jusqu'à Lot13 en SUCCÈS | ⛔ | Le jeu de recette ne produit pas `MASTER_CALC_Reservations_Resolues.xlsx` ni `MASTER_FACT_MEN_MenagesExternes.xlsx`. Il faut d'abord étendre `build_data_recette.py` (ou brancher `build_reservations_recette.py`) pour que la chaîne aval aille au bout. C'est le prochain bloc naturel. |
| Comparaison avant/après sur données réelles de calcul | ⚠️ | Mécanisme construit et testé (y compris détection d'écart anormal), mais jamais alimenté par deux runs réussis successifs, faute du point ci-dessus. |
| Clôture VALIDEE atteinte en recette | ⚠️ | Refusée à juste titre : aucun run réussi. La logique de refus est prouvée, la validation effective non. |
| « Rejouer un lot » isolé depuis l'UI | ⛔ | `executer_lot()` le permet côté service ; aucun bouton ne l'expose. |
| Durées de référence par lot | ⛔ | Mesurées et journalisées à chaque run, mais aucune estimation *a priori* n'est affichée (volontaire : pas d'estimation inventée). |
| Mode réel | ⛔ | Garde-fous construits, flags présents, **jamais activé** — conformément à la consigne. |
| Lots 3 / 6 / 7 / 8 pilotés en recette | ⚠️ | Déclarés dans les chaînes `charges` et `menages` et lançables, mais non exercés en recette ce tour. |

## Statut : **PARTIEL**

Le pilotage est réel et sûr : il exécute vraiment les lots avec le bon interpréteur, prévisualise,
scelle, sauvegarde, s'arrête proprement, journalise, restaure, et refuse une clôture non méritée.
Ce qui manque pour TERMINÉ est un **jeu de recette assez complet pour qu'une chaîne aille jusqu'au
bout en succès** — condition nécessaire pour exercer la comparaison avant/après et la validation de
clôture de bout en bout.

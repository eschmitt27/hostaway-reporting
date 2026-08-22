# Nettoyage legacy post-SQLite — 2026-08-22

Mission de nettoyage contrôlé exécutée après `ZERO EXCEL OPÉRATIONNEL = OUI` (voir `95` §20,
`97` §4). Objectif : réduire le code legacy Excel devenu mort par la migration HH/Charges/extras,
sans casser le runtime, sans perdre de règle métier, sans toucher aux migrations ni au moteur
pandas actif (Lot1-13, hors périmètre — ce sont les Lots ACTIFS, pas du legacy).

Périmètre : `05_APPLICATION/app/` et `05_APPLICATION/tests/` uniquement. Aucun fichier `02_TRAVAIL/`
(moteur), aucune migration, aucune donnée réelle touchée.

## 1. Inventaire audité

| Élément | Type | Appelants runtime avant | Remplacement SQLite | Règle unique ? | Action |
|---|---|---|---|---|---|
| `app/writers/saisie_hh_writer.py` | writer Excel | 0 (vérifié grep) | `reservations_hh_saisie_service` (0052/0053) | Non — mécanisme technique (temp/sha256/verrou) | **SUPPRIMÉ** |
| `app/writers/saisie_charges_writer.py` | writer Excel | 0 (vérifié grep) | `charges_saisie_service`/`charges_confirmation_service` | Non | **SUPPRIMÉ** |
| `app/services/saisie_charges_transaction_service.py` | orchestrateur transactionnel Excel | 1 réel (`_remplacer_fichier` par `calculs_pipeline_service.restaurer`) | idem ci-dessus | Non (schéma `charge_id` séquentiel = technique, pas économique) | **`_remplacer_fichier` déplacée** vers son seul appelant ; reste **SUPPRIMÉ** |
| `app/services/saisie_charges_journal_service.py` | journal d'audit Excel | 0 une fois transaction_service retiré | journalisation SQLite déjà en place ailleurs | Non | **SUPPRIMÉ** |
| `app/services/saisie_hh_schema_migration.py` | outil migration schéma (copies) | 1 constante réelle (`NEW_SAISIE_FIELDS`) | migration SQLite 0053 | Non | **Réduit à la constante** ; fonctions `migrate_*`/`_assert_copy_path` **SUPPRIMÉES** |
| `app/readers/saisie_charges_reader.py` (13 `read_ref_*`, `find_model_row`, `read_all_charge_ids`, `count_charges_with_prefix`) | lecteurs Excel referentiel/charge_id | 0 (vérifié grep app/ + tests/) | référentiels `ref_*` SQLite | Non | **SUPPRIMÉS** |
| `app/readers/saisie_charges_reader.py` (`reservation_id_exists`, `read_ref_categories_charges`, `read_ref_types_flux`, `MANUAL_COL_MAP`) | idem | 1 réel (`charges_preview_service`) + 1 test de parité réel (`test_profils_impact_schema.py`) | — | — | **CONSERVÉ** |
| `app/readers/saisie_hh_reader.py` (`FORMULA_COLS`, `_col_index`, `_col_letter`, `find_first_empty_data_row`) | lecteurs techniques writer HH | 0 une fois le writer retiré | — | Non | **SUPPRIMÉS** |
| `app/readers/saisie_hh_reader.py` (`read_ref_locale`, `read_existing_pks`, `SaisieHHReadError`) | lecteurs Excel HH | 0 appel réel, mais **80 tests actifs** (`test_saisie_hh_validation.py`) les monkeypatchent sur le namespace `saisie_hh_service` | `reservation_hh_overrides` (0053) | — | **CONSERVÉ** (import inerte documenté — retirer casserait 80 tests actifs pour aucun bénéfice) |
| `app/readers/saisie_hh_reader.py` (`generate_pk`, `MANUAL_COL_MAP`, `FORCED_VALUES`) | idem | réel (`saisie_hh_service`) | — | — | **CONSERVÉ** |
| `app/services/ref_assoc_mode_prepare_service.py` | outil bootstrap ponctuel gated | 0 appel réel, flag `REF_ASSOC_MODE_REAL_WRITE_ENABLED` jamais activé | migration 0029 (réalisée) | Possible réserve pour un futur mode réel non encore joué | **CONSERVÉ_ARCHIVE** (documenté comme résiduel dans `test_non_dependance_fichiers.py`, pas simplement mort) |
| `import openpyxl` (saisie_hh_service.py, ligne 14) | import mort | 0 | — | Non | **SUPPRIMÉ** |
| Adaptateurs SQLite→Excel moteur (`banque_adaptateur_moteur`, `reservations_adaptateur_moteur`, `hostaway_cleaning_tasks_adaptateur_moteur`, `calculs_executeur_service`, `calculs_pipeline_service`, `orchestrateur_moteur`) | pont vers le moteur pandas actif | Réels, appelés par l'orchestrateur | Aucun — le moteur Lot4quater/Lot6b/6c n'a pas de mode SQLite | N/A | **CONSERVÉ** (ce ne sont pas des adaptateurs de transition morts — ce sont les points de jonction ACTIFS avec le moteur legacy pandas, qui reste le moteur officiel pour ces lots) |

## 2. Tests supprimés (testaient exclusivement le code mort ci-dessus)

`test_saisie_hh_writer.py`, `test_saisie_charges_writer.py`,
`test_saisie_charges_transaction_service.py`, `test_saisie_charges_journal_service.py`,
`test_saisie_hh_schema_migration.py`, `test_path_guard.py` (ce dernier testait aussi
`test_tmp_path_hors_app_root`, déjà couvert par le garde `pytest_configure` de `conftest.py`).

`test_no_metier_calc.py::test_saisie_hh_writer_no_db_access` retiré (vérification structurelle
d'un fichier supprimé), remplacé par un commentaire explicatif — même pattern que la suppression
antérieure de `saisie_hh_orchestrator.py`, déjà documentée dans ce fichier.

## 3. Legacy conservé et pourquoi (inventaire final)

| Legacy restant | Pourquoi | Runtime ? | Condition de suppression future |
|---|---|---|---|
| `read_ref_locale`/`read_existing_pks`/`SaisieHHReadError` (saisie_hh_reader.py) | 80 tests actifs les monkeypatchent encore | Non (import inerte) | Réécrire les mocks de `test_saisie_hh_validation.py` pour ne plus patcher ces symboles, puis supprimer |
| `read_ref_categories_charges`/`read_ref_types_flux`/`MANUAL_COL_MAP` (saisie_charges_reader.py) | Preuve de parité réelle CHG_024 (`test_profils_impact_schema.py`) | Non (lecture seule pour un test) | Si ce test de parité historique est un jour jugé obsolète, supprimer avec lui |
| `ref_assoc_mode_prepare_service.py` | Outil de bootstrap ponctuel gated, jamais exécuté, documenté comme résiduel | Non | Si le mode réel REF_Assoc est définitivement abandonné, supprimer |
| Adaptateurs moteur pandas (Lot1/Lot4quater/Lot6b/Lot6c/Lot8/Lot10) | Le moteur pandas reste actif pour ces lots — ce ne sont pas des adaptateurs de transition mais l'architecture officielle actuelle | **Oui, actifs** | Uniquement si ces lots sont un jour migrés en SQLite natif (hors mandat de cette mission) |
| `MASTER_CALC_*.xlsx` lus par `proprietaires_reader`/`proprietaires_reglements_reader`/`comptabilite_analytique_service` | Sorties réelles du moteur Lot10/Lot12, aucun équivalent SQLite | **Oui, actifs** | Idem |

## 4. Mesure avant/après (05_APPLICATION/app/)

| Mesure | Avant | Après | Écart |
|---|---|---|---|
| Fichiers `.py` (app/) | 180 | 176 | -4 |
| Lignes `.py` (app/) | 44 144 | 41 719 | -2 425 |
| Fichiers de test | 176 | 170 | -6 |
| Références `MASTER_` (app/) | 209 | 203 | -6 |
| Références `SAISIE_` (app/) | 103 | 69 | -34 |
| Références `openpyxl` (app/) | 76 | 46 | -30 |

Aucun objectif chiffré arbitraire — mesure du gain réel uniquement.

## 5. Tests finaux (campagne complète après nettoyage)

- Moteur (`02_TRAVAIL`, racine du worktree) : **345 passed, 0 failed** (inchangé — moteur non touché).
- Application (`05_APPLICATION/tests/`, hors `test_recette_scenarios.py` déjà vérifié séparément
  ce jour) : **~2593 passed**, 4 échecs **pré-existants confirmés par `git stash`** avant cette
  mission (donc non introduits par le nettoyage) :
  - `test_banque_controle.py::test_07_08_rattacher_proprietaire_logement`
  - `test_banque_controle_finition.py::test_11_type_flux_valeur_technique_conservee`
  - `test_menages_chaine.py::test_chaine_e2e_reelle_sur_copies`
  - `test_ventes_lot12_adapter.py::test_generer_ecritures_du_mois` (+ pendant idempotent)

**0 nouvelle régression introduite par ce nettoyage.**

## 6. Gardes structurelles

`test_dag_ne_contient_aucun_noeud_fichier` (déjà existant, `test_orchestrateur.py`) prouve que le
DAG de l'orchestrateur ne raisonne jamais en fichiers (`MASTER`/`XLSX` absents des noms de noeud).
`test_bootstrap_zero_excel.py`/`test_clean_bootstrap.py` interceptent activement toute ouverture
`openpyxl.load_workbook` interne pendant un run complet de l'orchestrateur — garde dynamique plus
forte qu'un grep statique, jugée suffisante ; aucune garde statique supplémentaire ajoutée pour
éviter une redondance sans valeur ajoutée.

## 7. Mise à jour 2026-08-22 — clarification des adaptateurs SQLite→Excel + baseline 0 failed

Mission de fermeture technique post-legacy. Audit exhaustif des adaptateurs Lot1/4quater/6b/6c/8/10 :

| Adaptateur | Appelant réel | Runtime normal ? | Crée XLSX ? | Pourquoi | Action |
|---|---|---|---|---|---|
| `orchestrateur_moteur.py::executer_lot10` | `orchestrateur_dag.NOEUDS` (Lot10) | **OUI** | NON — `--source SQLITE --db <base>` | Lot10 rendu SQLite-natif | CONSERVÉ, conforme |
| `reservations_adaptateur_moteur.py` (`ecrire_resolues`/`ecrire_payouts`/`ecrire_tout`) | `menages_chaine_service.py` uniquement | NON (absent du DAG ; route recette `/menages/chaine/executer`, `MODE_COPIES` forcé, mode réel refusé en dur) | OUI, dans cette chaîne isolée uniquement | Reproduit Lot6b→Lot6c→Lot11 sur copies pour preuve de parité | LEGACY_PARITE (0 appelant runtime normal, prouvé) |
| `reservations_adaptateur_moteur._vue_flux` | `flux_unifie_service.py` (Lot9, DAG) | OUI mais fonction pure, 0 I/O | NON | Filtre réutilisé, pas dupliqué | CONSERVÉ |
| `banque_adaptateur_moteur.py` | `menages_chaine_service.py` uniquement | NON (même chaîne isolée) | OUI, isolé | idem | LEGACY_PARITE |
| `hostaway_cleaning_tasks_adaptateur_moteur.py` | `menages_chaine_service.py` uniquement | NON (même chaîne isolée) | OUI, isolé | idem | LEGACY_PARITE |
| `adaptateur_workspace.py` | les 3 adaptateurs ci-dessus | NON | Mécanique commune | idem | LEGACY_PARITE |
| `hostaway_adaptateurs.py`/`hostaway_cleaning_tasks_adaptateur.py` | 0 dans `app/` hors leur propre test | NON | NON (sens Excel→SQLite) | Reprise ponctuelle H6 | IMPORT_PONCTUEL, conservé |

**Verdict : ADAPTATEURS_XLSX_RUNTIME = 0/9. EXCEL_ENTRE_MOTEURS = 0/9. ZERO EXCEL OPÉRATIONNEL
reste OUI**, précisé : les classeurs que produit la chaîne `menages_chaine_service` existent
réellement mais ne sont jamais atteints par « Actualiser toute l'activité ».

Test bloquant renforcé (`test_bootstrap_zero_excel.py`) : interceptait seulement la lecture
(`openpyxl.load_workbook`) ; ajout de l'écriture (`openpyxl.Workbook.save`) sur les mêmes motifs
interdits — toujours vert.

**4 défauts pré-existants corrigés** (causes réelles, aucune règle économique touchée) :
1. `test_07_08_rattacher_proprietaire_logement` — `options_reference()`/`_ref()` ignoraient
   `db_path` (deux call-sites internes non filtrés) et `_REF_CACHE` n'était jamais vidé par
   `vider_cache()` — corrigés dans `banques_controle_service.py` ; référentiel manquant seedé
   dans le test.
2. `test_11_type_flux_valeur_technique_conservee` — même cause (référentiel non seedé), corrigé
   dans le test.
3. `test_chaine_e2e_reelle_sur_copies` — `cfg.SNAPSHOTS_DIR` non isolé (écrivait dans le vrai
   `data/snapshots/`), corrigé dans le test.
4. `test_generer_ecritures_du_mois(_idempotent)` — fixture `db` pointant vers une base différente
   de celle seedée, corrigé dans le test.

Campagne complète rejouée : moteur **345/345 passed**, application **~2599 passed** (8 shards).
**0 failed.** Détail : `JOURNAL_CONTROLES.md`, `JOURNAL_ANOMALIES.md`.

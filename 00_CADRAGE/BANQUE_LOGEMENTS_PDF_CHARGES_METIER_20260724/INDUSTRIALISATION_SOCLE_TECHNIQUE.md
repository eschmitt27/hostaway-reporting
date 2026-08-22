# Industrialisation socle technique — sauvegarde, rollback, activation contrôlée (2026-08-22)

Mission : sécuriser le système pour qu'aucune opération importante ne puisse dégrader les données
sans retour arrière possible, avant toute nouvelle automatisation métier. HEAD départ `c967e51`.

## 1. Audit initial — ce qui existait déjà (non dupliqué)

Le projet avait déjà, de sessions précédentes, une part substantielle de ce que cette mission
demande — vérifié fichier par fichier avant d'écrire une ligne de code :

| Mécanisme demandé | État avant cette mission |
|---|---|
| Sauvegarde de fichiers avant écriture | `snapshot_service.py` (0001) : `create_snapshot`/`verify_snapshot`/`restore_to_workspace`, manifeste sha256, table `snapshots`. Utilisé par `menages_chaine_service.py`. Ne sauvegarde que des FICHIERS Excel, jamais `app.db` elle-même. |
| Sauvegarde + rollback par run | `calculs_pipeline_service.py` (0018) : `preparer`/`sauvegarder`/`executer_lot`/`restaurer`, table `calculs_runs` (statuts PREPARE/EN_COURS/SUCCES/ECHEC/RESTAURE), table `calculs_sauvegardes` (sha256 avant/après). Complet pour les fichiers moteur, jamais pour `app.db`. |
| CURRENT/CANDIDATE dataset | `lot10_runs`/`lot12_runs` : colonne `actif` + index UNIQUE PARTIEL `WHERE actif=1` — un nouveau run est créé non-actif, promu seulement après validation. Déjà exactement le patron demandé. |
| Transaction atomique multi-table | `banque_classification_service.py` : `BEGIN IMMEDIATE` explicite. `factures_proprietaires_service.py` : connexion unique + commit unique en fin de fonction (rollback implicite sur exception). Déjà conforme. |
| Sauvegarde de `app.db` avant migration | **Absente.** `apply_migrations()` (`app/db/connection.py`) ne sauvegarde rien avant de jouer les migrations. |
| Historique centralisé multi-opérations | **Absent.** Chaque sous-système garde sa propre table de runs (`calculs_runs`, `lot10_runs`, `lot12_runs`, `hostaway_extractions`, `banque_imports`) — pas de vue d'ensemble. |
| Écran observabilité multi-runs | **Absent.** |

**Conclusion de l'audit** : ne pas reconstruire ce qui existe. Combler exactement les deux vrais
manques (sauvegarde de la base elle-même, vue centralisée des runs), en réutilisant au maximum
l'existant (mêmes conventions de nommage, mêmes patterns de service, migrations additives).

## 2. Architecture retenue (minimale)

```
Nouvelle opération critique (migration réelle, import massif…)
        │
        ▼
backup_service.sauvegarder()        ── copie app.db (checkpoint WAL d'abord), sha256, git_commit,
        │                              intégrité vérifiée immédiatement
        ▼
run_history_service.demarrer()      ── STARTED
        │
        ▼
Traitement (ex. apply_migrations)
        │
        ▼
Contrôle (integrity_check)
        │
   ┌────┴────┐
   OK        échec
   │          │
   ▼          ▼
SUCCESS   backup_service.restaurer(confirmer=True)  ── restaure la copie
                  │
                  ▼
            ROLLED_BACK (rejournalisé sur le fichier restauré, cf. §4)
```

## 3. Fichiers créés/modifiés

- `app/db/migrations/0057_industrialisation_sauvegarde_run_history.sql` (nouveau, additif) :
  tables `sauvegardes_base` et `run_history`.
- `app/config.py` : `BACKUPS_DIR` (nouveau, distinct de `SNAPSHOTS_DIR`).
- `app/services/backup_service.py` (nouveau) : `sauvegarder`, `verifier`, `lister`, `restaurer`,
  `purger`.
- `app/services/run_history_service.py` (nouveau) : `demarrer`, `marquer_validating`,
  `marquer_succes`, `marquer_echec`, `marquer_rollback`, `derniers`.
- `app/services/migration_service.py` (nouveau) : `migrer_avec_sauvegarde` — point d'entrée
  protégé pour une vraie migration de schéma (cf. `85_RUNBOOK_MIGRATION_APP_DB_REELLE.md`).
  `apply_migrations()` elle-même **non modifiée** : appelée par le fixture `tmp_db` de centaines
  de tests, y ajouter une sauvegarde automatique aurait ralenti toute la suite pour un risque nul
  sur une base de test jetable.
- `app/routes/observabilite.py` + `app/templates/observabilite_runs.html` (nouveaux) : écran
  `/observabilite/runs`, lecture seule.
- `app/main.py` : enregistrement du nouveau routeur.

## 4. Mécanisme de sauvegarde (Phase 2)

`backup_service.sauvegarder(operation, db_path=None)` :
1. `PRAGMA wal_checkpoint(FULL)` sur la source (le mode `journal_mode=WAL`, actif partout dans ce
   projet, laisse des écritures non répercutées dans un fichier `-wal` séparé — un `shutil.copy2`
   du seul `.db` risquerait de figer un état incohérent sans ce checkpoint préalable).
2. Copie horodatée sous `BACKUPS_DIR`.
3. `PRAGMA integrity_check` sur la COPIE (jamais sur la source active).
4. Enregistrement dans `sauvegardes_base` : `sauvegarde_id_opaque`, `operation`, `git_commit`
   (`git rev-parse HEAD`), `database_hash` (sha256 de la copie), `taille_octets`,
   `validation_status` (PENDING/VALIDE/CORROMPU).

`verifier()` revérifie hash + intégrité à tout moment (fichier disparu, corrompu après coup).
`purger(garder_n)` supprime les sauvegardes physiques au-delà des N plus récentes VALIDÉES — **non
automatique**, appelé uniquement à la demande (mission §2 : « aucune suppression automatique sans
règle claire »). Un test dédié (`test_purger_ne_supprime_jamais_automatiquement`) vérifie par
`git grep` que `purger()` n'est appelé nulle part dans `app/`.

## 5. Mécanisme de rollback (Phase 3)

`restaurer(sauvegarde_id, cible=None, confirmer=False)` : refuse tant que `confirmer` n'est pas
`True` (une restauration remplace tout le fichier cible). Revérifie l'intégrité de la sauvegarde
AVANT de l'appliquer — jamais restaurer un fichier connu corrompu.

`migration_service.migrer_avec_sauvegarde()` orchestre le flux complet Phase 2→3 : sauvegarde →
`STARTED` → migration → contrôle intégrité → `SUCCESS`, ou restauration automatique + `ROLLED_BACK`
si la migration lève une exception OU si la base résultante échoue au `integrity_check`.

**Point technique non trivial trouvé pendant les tests** : `restaurer()` remplace tout le fichier
cible, y compris la table `run_history` elle-même — la ligne du run en échec, écrite avant la
restauration, disparaît avec le fichier remplacé. Corrigé : l'issue (`ROLLED_BACK`) est
rejournalisée dans une entrée fraîche **après** la restauration, sur le fichier qui subsiste
réellement, en référençant le run d'origine dans le message d'erreur.

## 6. Versionnement des datasets (Phase 4)

**Déjà en place, non reconstruit** : `lot10_runs.actif`/`lot12_runs.actif` avec index UNIQUE
PARTIEL `WHERE actif=1` implémentent exactement CURRENT_VALID_DATASET (une seule ligne active) /
CANDIDATE_DATASET (un nouveau run créé non-actif, promu seulement si validation OK). Confirmé par
les tests négatifs déjà écrits lors de la mission durcissement (`test_double_dataset_lot10_actif_
toujours_refuse`, `2026-08-22`, mission précédente). Aucune modification nécessaire.

## 7. Séparation règles métier / stockage (Phase 5 — trajectoire, pas de refactor)

Audit rapide : les services de saisie (`charges_saisie_service.py`, `reservations_hh_saisie_
service.py`, `proprietaires_tresorerie_service.py`) séparent déjà clairement lecture SQL / règle
métier (`valider()`) / écriture — pattern homogène depuis les migrations Excel→SQLite. Le point le
moins séparé reste le pont vers le moteur pandas (`calculs_executeur_service.py`,
`orchestrateur_moteur.py`) : la logique de commission/résultat vit dans les scripts `02_TRAVAIL/`,
hors SQLite, par choix assumé (moteur non migré). Risque : si une règle de commission devait un
jour être dupliquée côté application avant la migration complète du moteur, elle divergerait de
l'original pandas. Trajectoire recommandée pour une prochaine mission : n'écrire aucune règle
économique nouvelle dans `05_APPLICATION/app/` tant que le Lot correspondant n'est pas migré —
seuls des contrôles de COHÉRENCE (déjà en place, ex. `controles_lot11_service.py`) sont légitimes
côté application avant migration complète.

## 8. Observabilité (Phase 6)

Écran `/observabilite/runs` (lecture seule) : derniers runs (`run_history`, tous types confondus
qui l'utilisent) et sauvegardes de la base (`sauvegardes_base`). Ne remplace pas l'écran Pilotage/
Actualisation existant (pilotage des lots) ; complète la vue avec les opérations protégées par ce
socle.

## 9. Tests (Phase 7)

- `test_backup_service.py` (10 tests) : création, intégrité, détection de corruption/fichier
  manquant, restauration (refus sans confirmation, contenu réellement restauré, refus si la
  sauvegarde est corrompue), purge, non-automaticité de la purge.
- `test_run_history_service.py` (6 tests) : transitions, statut inconnu refusé, run introuvable
  signalé, limite de pagination.
- `test_migration_service.py` (2 tests) : migration réussie journalisée, migration en échec
  restaurée automatiquement et rejournalisée.
- `test_observabilite_routes.py` (2 tests) : écran vide, écran peuplé.
- `test_sqlite_migrations.py` : `EXPECTED_TABLES` mis à jour (`sauvegardes_base`, `run_history`).

Aucune écriture réelle : `BACKUPS_DIR`/`db_path` systématiquement isolés sous `tmp_path` dans
chaque test (garde `garde_sources_reelles.py` active par ailleurs, autouse).

## 10. Limites

- `apply_migrations()` elle-même reste sans sauvegarde automatique (choix délibéré, §3) — seule
  `migrer_avec_sauvegarde()` (nouveau point d'entrée) protège une VRAIE migration.
- Les imports Hostaway/Banque ne journalisent pas encore dans `run_history` (chacun garde sa
  propre table existante — pas de retrofit dans cette mission, cf. mission §5 de la trajectoire).
- `purger()` existe mais n'est appelé nulle part automatiquement — politique de rétention à
  décider (fréquence, déclencheur) dans une prochaine session si le volume de sauvegardes le
  justifie.
- Écran observabilité limité aux deux nouvelles tables — pas encore une vue unifiée de
  `calculs_runs`/`lot10_runs`/`lot12_runs`/`hostaway_extractions`/`banque_imports`.

## 11. Prochaine étape recommandée

Câbler `run_history_service`/`backup_service` dans le point d'entrée réel d'un import massif
(Hostaway ou Banque) le jour où cette protection devient nécessaire en production — pas fait ici
par prudence (mission explicite : ne pas commencer l'orchestrateur ni le scheduler Hostaway).

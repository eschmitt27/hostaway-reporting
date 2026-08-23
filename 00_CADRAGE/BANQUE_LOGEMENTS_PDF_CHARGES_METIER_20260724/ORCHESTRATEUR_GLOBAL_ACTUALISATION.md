# Orchestrateur global « Actualiser toute l'activité » (2026-08-23)

Mission « industrialisation : orchestrateur global ». HEAD départ `2eb25c5`.

## 1. Audit initial

Avant tout code : l'orchestrateur DAG existait déjà, presque intégralement conforme au schéma cible
de la mission.

| Demandé par la mission | État avant cette mission |
|---|---|
| DAG des dépendances (Hostaway→Réservations/Ménages/Banque→Flux→Lot10→Lot11→Lot12→Lot13) | `orchestrateur_dag.py` — exactement ce DAG, déclaratif, `ordre_topologique()`/`descendants()`/`ascendants()`. |
| Service central d'orchestration, sans règle métier | `orchestrateur_service.py::actualiser()` — résout `"module:fonction"` tardivement, n'importe et n'exécute que ce qui est réellement appelé, aucun calcul propre. |
| RUN / RUN_STEP | `moteur_runs`/`moteur_run_etapes` (migration 0031) — run_id, déclencheur MANUEL/AUTO/ORCHESTRATEUR, statuts EN_COURS/SUCCES/PARTIEL/ECHEC/INTERROMPU, étapes avec ordre/started_at/ended_at/statut/erreur. |
| Dépendances invalidées en cascade | `invalider_descendants()`, `_amonts_en_echec()` — un dataset dont un amont a échoué n'est **jamais** recalculé sur une entrée périmée ; propagation déjà exacte. |
| Résultat structuré par étape | `recalculer_dataset()` retourne déjà `{"ok", "dataset", "code", "message"}` par étape. |
| Écran `/actualisation` | Déjà présent : bouton global + action ciblée, tâche de fond, état des datasets, dernier run, historique. |
| Reprise après crash | `marquer_runs_interrompus()` déjà en place. |

**Ce qui manquait réellement** (les deux seuls vrais manques, confirmés en lisant le code, pas
supposés) :
1. Aucune sauvegarde de `app.db` avant une actualisation globale (le socle `backup_service.py`
   existe depuis la mission précédente, mais n'était câblé nulle part dans l'orchestrateur).
2. Aucun mode `DRY_RUN`.

**Décision** : ne reconstruire ni le DAG, ni le service, ni les registres de run, ni l'écran —
uniquement câbler le socle existant sur l'actualisation GLOBALE et ajouter le dry-run.

## 2. Architecture retenue

```
POST /actualisation/tout
        │
        ▼
actualiser(cibles=None)
        │
        ▼
backup_service.sauvegarder("ACTUALISATION_GLOBALE")   ── SEULEMENT si cibles=None et dry_run=False
        │
        ▼
run_history_service.demarrer() → VALIDATING           ── en parallèle de moteur_runs (inchangé)
        │
        ▼
Boucle sur ordre_topologique() (INCHANGÉE) :
  amont en échec ?          → IGNOREE (jamais recalculé sur une entrée périmée)
  pas de service ?          → IGNOREE (import externe / chaîne non migrée)
  externe non demandé ?     → IGNOREE
  sinon                     → recalculer_dataset() (INCHANGÉ)
        │
        ▼
PRAGMA integrity_check
   ┌────┴────┐
   OK        échec
   │          │
   ▼          ▼
SUCCESS/   backup_service.restaurer(confirmer=True)
PARTIEL           │
(inchangé)        ▼
             run_history.marquer_rollback() (rejournalisé APRÈS restauration, cf. §5)
```

## 3. DAG final

Inchangé — voir `orchestrateur_dag.py::NOEUDS`. Aucun nœud ajouté ou retiré.

## 4. Services appelés

`backup_service.sauvegarder`/`restaurer` (mission précédente), `run_history_service.demarrer`/
`marquer_validating`/`marquer_succes`/`marquer_echec`/`marquer_rollback` (idem) — aucun nouveau
service créé, uniquement des appels ajoutés dans `orchestrateur_service.actualiser()`.

## 5. Gestion backup (Phase 4)

Sauvegarde prise **uniquement** sur une actualisation **globale** (`cibles=None`) et **jamais** en
`dry_run` : une cible unique ne justifie pas le coût d'une copie complète de `app.db`, et un
dry-run ne modifie rien par construction. Si la sauvegarde échoue ou est corrompue à la création,
l'actualisation est refusée avant tout traitement (`E_SAUVEGARDE_ECHOUEE`).

## 6. Gestion rollback (Phase 4)

Rollback automatique **uniquement** si `PRAGMA integrity_check` échoue après le run — jamais sur
un simple dataset en échec (`PARTIEL`), qui reste un état normal et géré : les données déjà
recalculées avec succès restent valides, aucun dataset n'étant jamais écrasé avant son propre
succès. Confondre les deux aurait annulé un travail réellement valide pour une panne qui n'a pas eu
lieu.

**Point technique trouvé pendant les tests** (déjà rencontré une fois dans
`migration_service.py`, mission précédente — reproduit ici car même cause racine) :
`backup_service.restaurer()` remplace tout le fichier cible, y compris la ligne `run_history` du
run en cours d'écriture elle-même. Corrigé de la même façon : l'issue `ROLLED_BACK` est
rejournalisée dans une **nouvelle** entrée `run_history`, ouverte après la restauration, sur le
fichier qui subsiste réellement — en référençant le run d'origine dans le message d'erreur.

## 7. Gestion erreurs (Phase 6)

Inchangée — chaque étape retournait déjà `{"ok", "dataset", "code"/"message"}` via
`recalculer_dataset()`. Aucune modification nécessaire, déjà conforme.

## 8. Interface créée (Phase 7)

Écran `/actualisation` existant conservé tel quel, complété :
- Bouton « Simuler (dry-run) » → `POST /actualisation/tout/dry-run` (synchrone, aucun service
  réel appelé, donc pas de tâche de fond nécessaire) : affiche le plan (« serait exécuté » /
  « serait ignoré », avec la raison) sans rien activer.
- Texte explicatif mis à jour (sauvegarde + restauration automatique, dry-run).

## 9. Tests (Phase 9)

`tests/test_actualisation_backup_rollback.py` (nouveau, 7 tests) :
- sauvegarde prise sur actualisation globale, absente sur cible unique ;
- run journalisé dans `run_history`, lié à la sauvegarde ;
- intégrité échouée après coup → restauration automatique + `ROLLED_BACK` (rejournalisé) ;
- échec partiel (`PARTIEL`) → **pas** de rollback, run marqué `SUCCESS` dans `run_history` ;
- dry-run : aucun dataset modifié, aucun service appelé, aucune sauvegarde prise ;
- dry-run : le plan distingue ce qui serait exécuté de ce qui serait ignoré.

`tests/test_actualisation_ui.py` : 1 test ajouté (route dry-run répond directement, sans tâche de
fond). Tests DAG déjà existants (`test_dag_sans_cycle_et_ordonne`,
`test_chaine_reelle_respecte_les_dependances_attendues`) et non-recalcul-inutile
(`test_invalidation_descendants` dans `test_orchestrateur.py`) confirmés toujours verts, non
modifiés.

**Isolation corrigée** : câbler `backup_service` dans `actualiser()` a révélé que plusieurs
fixtures de test isolaient `cfg.DB_PATH` mais pas `cfg.BACKUPS_DIR` — une actualisation globale
dans un test aurait écrit une vraie copie de base sous le `BACKUPS_DIR` réel du projet. Corrigé à
la source : le fixture partagé `tmp_db` (conftest.py) isole désormais aussi `BACKUPS_DIR`, plus
deux fixtures locales (`env_neuf` dans `test_bootstrap_zero_excel.py`, `_aucun_classeur` dans
`test_clean_bootstrap.py`) qui construisent leur propre base sans passer par `tmp_db`.

Aucune écriture réelle vérifiée après coup (`data/backups` absent, `git status` propre sur
`05_APPLICATION/data`).

## 10. Documentation (Phase 10)

Ce document + `HANDOFF_CANONIQUE.md` mis à jour.

## 11. Limites restantes

- Le lien sauvegarde↔run n'est pas affiché sur l'écran `/actualisation` lui-même (seulement sur
  `/observabilite/runs`, qui lit directement `run_history`) — `moteur_runs` (source du tableau
  affiché) n'a pas de colonne `sauvegarde_id`, ajouter cette colonne sortirait du périmètre
  « ne pas créer un nouveau système de logs ».
- Le dry-run ne vérifie pas l'intégrité ni ne simule un `integrity_check` — il ne fait que
  dérouler le plan de dépendances, ce qui est son unique objectif (Phase 8 : « vérifier les
  étapes, vérifier les dépendances, ne rien activer »).
- Une actualisation ciblée (`cibles=[...]`) reste sans sauvegarde ni entrée `run_history` — choix
  assumé (coût disproportionné), documenté, pas un oubli.
- L'orchestrateur reste inchangé pour tout ce qui touche au calcul lui-même (Lot10 pandas,
  chaînes non migrées Lot4quater/Lot6b/Lot6c) — hors mandat de cette mission.

## 12. Prochaine étape recommandée

Scheduler Hostaway (déclenchement automatique, hors mandat explicite de cette mission),
administration référentiels, ou étendre `run_history` aux imports Hostaway/Banque eux-mêmes
(déjà signalé comme manque dans la mission précédente).

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

## 13. Correctif 2026-10-01 — chaîne bloquée par un ancien échec Hostaway

**Constat (base réelle).** `HOSTAWAY_RAW` était resté en ÉCHEC depuis le 2026-09-10 (import en mode
API, `rc=1`), alors que le dépôt publié avait été synchronisé avec succès sept fois depuis — mais
par « Actualiser les ménages » et l'écran Hostaway, qui appellent `hostaway_depot_service.synchroniser`
directement, sans passer par l'orchestrateur : l'état du nœud n'était donc jamais relevé.
Conséquences : `RESERVATIONS` « à recalculer » depuis le 10/09, toutes les cascades Ménages
(ciblées sur `FLUX_LOT9`) refusées, Lot10 figé au 12/09 (septembre alors mois en cours, donc
exclu), aucune préfacture de septembre. « Actualiser toute l'activité » ne pouvait rien y faire :
il ne lançait aucun import externe, et marquait en plus les tâches de ménage (un import)
« à recalculer », ce qui bloquait aussi Ménages, sans qu'aucun parcours puisse lever cet état.

**Correctif.**
- `Noeud.actualisation_globale` : `HOSTAWAY_RAW` (lu dans le dépôt publié, sans appel d'API,
  idempotent) est synchronisé par « Actualiser toute l'activité » et par son dry-run. Les tâches
  de ménage (H6) gardent leur parcours propre (« Actualiser les ménages »).
- Un IMPORT n'est plus jamais marqué « à recalculer » en cascade, et un import resté dans cet état
  ne bloque plus ses descendants (sa dernière extraction réussie reste en place). Un import non
  demandé n'est pas « bloqué » : il est « non déclenché ». Le garde-fou 14b reste entier pour les
  CALCULS (ÉCHEC, À RECALCULER, JAMAIS).
- « Actualiser les ménages » cible `HOSTAWAY_RAW` + `FLUX_LOT9` quand le dépôt a pu être lu : les
  réservations synchronisées atteignent enfin Réservations → Flux → Lot10/11/12.
- Résumé du run : seulement les échecs et les blocages (les étapes normalement ignorées restent
  dans le détail).
- Écran : bouton « Actualisation des calculs » sur Observabilité & outils ; l'écran Actualisation
  est rattaché à ce menu.

Tests : `tests/test_actualisation_deblocage.py` (reproduction de l'état réel).

## 14. « Actualiser toute l'activité » réellement global + progression en direct (2026-10-01)

**Contrat du bouton.** Il interroge TOUTES les sources configurées puis rejoue TOUS les calculs.

| Étape | Service canonique | Remarque |
|---|---|---|
| Sauvegarde | `backup_service.sauvegarder` | étape visible, avant tout |
| Banque — relevés par fichier (`BANQUE`) | — | import manuel : « Ignorée », jamais une erreur |
| Banque — Qonto (`BANQUE_QONTO`, nouveau) | `qonto_ecran_service.actualiser` (GET seulement) | sans `QONTO_LOGIN`/`QONTO_SECRET_KEY` : « Non configurée » ; aucun descendant (le Flux lit `banque_mouvements`) |
| Hostaway — réservations, logements, paiements | `hostaway_depot_service.synchroniser` | réservations + champs financiers du dépôt publié ; logements DÉDUITS des réservations (le pipeline ne publie pas `/v1/listings`) ; idempotent (« déjà à jour » si le commit publié est déjà en base) |
| Hostaway — tâches de ménage | `hostaway_cleaning_tasks_actualisation_service.actualiser` (dépôt) | désormais incluses (`actualisation_globale=True`) ; le scheduler ne les tire toujours pas |
| Ménages — déclarations (Google Sheet, `MENAGES_DECLARATIONS`, nouveau) | lot6b | source d'appoint : `bloque_l_aval=False` |
| Ménages — factures PDF (`MENAGES_PDF`, nouveau) | `menages_pdf_import_service.importer_nouveaux` | idempotent ; `bloque_l_aval=False` |
| Référentiel | — | import manuel : « Ignorée » |
| Réservations → Ménages → Flux → Résultats → Contrôles → Préfactures | services du DAG | TOUS rejoués : l'optimisation « amont inchangé » ne vaut que pour les actualisations ciblées |

La comptabilité n'est pas une étape : aucune écriture n'est « recalculée », elles naissent des
validations. Les exports Power BI (Lot13) restent exclus du bouton.

**Échecs.** Hostaway en échec → étape rouge, réservations et tout l'aval « Non exécutée ». Banque
Qonto en échec → étape rouge, run PARTIEL, aucun calcul bloqué (aucun ne lit Qonto), dernière
synchronisation conservée. Source d'appoint ménages en échec → rouge, Ménages calculé sur la
dernière version valide. Source non configurée → « Non configurée » (⚠️), jamais un succès, le
dataset n'est jamais marqué à jour.

**Progression.** `preparer_actualisation_globale` (appelée par la route, AVANT la tâche de fond)
prend le verrou, ouvre le run et écrit le PLAN dans `moteur_run_etapes` (toutes les étapes
EN_ATTENTE, ordre du DAG — `orchestrateur_service.plan`, seule liste). Chaque étape passe EN_COURS
à son démarrage puis à son statut final, avec un détail JSON (`moteur_run_etapes.detail`,
migration 0119 : nature, volume, code). L'écran relit `/actualisation/progression` chaque seconde
(`actualisation_progression_service`, lecture seule, libellés `Noeud.nom_affiche`) : aucun
minuteur, aucun pourcentage estimé. Un rechargement retrouve le run en cours ; un double clic
trouve le verrou pris et ne crée aucun run. Un run terminé ou interrompu solde ses étapes non
exécutées.

Tests : `tests/test_actualisation_globale_progression.py`.

## 15. Extraction Hostaway à la demande au clic manuel (2026-10-01)

Le clic « Actualiser toute l'activité » (`hostaway_a_la_demande=True`, posé par la route seule)
DÉCLENCHE le pipeline GitHub canonique `pipeline.yml` (`workflow_dispatch` déjà prévu ; mêmes
scripts `extract_reservations.py` / `extract_finance_fields.py` / `extract_cleaning_tasks.py`,
mêmes secrets), suit ses étapes en direct (API `jobs`) et n'importe qu'APRÈS sa publication, par
`hostaway_depot_service.synchroniser` (atomique, inchangé). Aucun second extracteur ; le poste
n'a toujours aucun identifiant Hostaway. Service : `hostaway_extraction_demande_service`.

- Run identifié comme le run `workflow_dispatch` apparu après le dispatch et absent de la liste
  relevée juste avant ; file d'attente derrière un run planifié affichée (groupe de concurrence
  `hostaway-data-publish`).
- Sous-étapes affichées : connexion, réservations, données financières, tâches de ménage,
  publication, puis « Validation des données Hostaway » (import local).
- Échecs (jeton absent/refusé, run en échec — étape nommée —, annulé, > 20 min, run introuvable) :
  étape Hostaway rouge, dernières données valides conservées, aval non exécuté. Tâches de ménage
  en échec côté GitHub (toléré par le pipeline) : l'import H6 refuse de les présenter comme
  fraîches.
- Scheduler et actualisations ciblées : inchangés (relecture de la dernière publication).
- Coût par clic : ~3 min, ~3 min d'Actions GitHub, ~1 670 appels Hostaway (gestion 429 des
  scripts existants). Listings : toujours déduits des réservations (`/v1/listings` non extrait par
  le pipeline).

Preuve du 2026-10-01 : réservation créée dans Hostaway à 19:07:03 UTC (n° 67048314, Studio 46,
04→10/01/2027), absente de la publication `e161d4d` (18:56:56) ; clic à 19:08:41 ; publication
`072e34f` à 19:11:23 ; importée à 19:11:36 ; présente dans les réservations calculées et résolues.

Tests : `tests/test_hostaway_extraction_demande.py`.

# Scheduler Hostaway automatique 5h (2026-08-23)

Mission « industrialisation : scheduler Hostaway automatique 5h ». HEAD départ `788c7ba`.

## 1. Audit initial — l'essentiel existait déjà

| Composant | Existe avant cette mission ? | Réutilisable ? | Modification faite |
|---|---|---|---|
| Service Hostaway canonique | `hostaway_actualisation_service.py::actualiser()` — un seul point d'entrée, appelé par le bouton manuel ET le chemin orchestrateur/ordonnanceur. | Oui, intégralement. | Ajout de `run_history_service` sur le chemin synchrone uniquement (§4). |
| Ordonnanceur | `ordonnanceur_service.py` — **déjà écrit et testé** (16 tests dans `test_ordonnanceur.py`) : cadence 5h Hostaway / 24h CleaningTasks séparée, fonction pure `doit_declencher(maintenant=...)` testable sans horloge réelle, `demarrer()`/`arreter()` avec garde `ORDONNANCEUR_ACTIF` et minuteur singleton, `tick()` qui appelle `orchestrateur_service.actualiser()` (même chemin que le manuel). | Oui, intégralement — **rien reconstruit**. | Cadences rendues configurables via `cfg` (§3), câblage dans `app/main.py` (§2). |
| `DECLENCHEUR_MANUEL`/`DECLENCHEUR_AUTO` | Déjà présents dans les deux services. | Oui. | Aucune. |
| Rate limit / 429 / retries | `lot1_hostaway_extract.py` (`Retry-After`, backoff plafonné, `RateLimitEpuise`). | Oui. | Aucune — non réimplémenté, conforme à la règle absolue. |
| CleaningTasks (H6) cadence séparée | Déjà décidée et documentée (24h par défaut, `tick()` signale explicitement l'absence de service d'import automatisé pour H6). | Oui. | Aucune — décision confirmée, pas retranchée. |
| Concurrence manuel/auto | Verrou global de `orchestrateur_service` (`PORTEE_GLOBALE`) + état `EN_COURS` du dataset consulté par `doit_declencher`. | Oui. | Aucune — pas de second verrou créé. |
| Reprise après crash | `orchestrateur_service.marquer_runs_interrompus()`, déjà appelé à chaque `tick()`. | Oui. | Aucune. |
| Descendants DAG | `orchestrateur_service.actualiser(cibles=[...])` recalcule déjà les descendants nécessaires via `orchestrateur_dag`. | Oui. | Aucune — `ordonnanceur_service.tick()` ne code aucune liste `run_lot9()/run_lot10()/...`. |
| Démarrage/arrêt avec l'application | **Absent** — le service existait mais n'était jamais appelé par `app/main.py`. | — | Ajouté (§2). |
| `run_history` pour Hostaway | **Absent** — seul `moteur_runs`/`hostaway_extractions` existaient. | — | Ajouté (§4). |
| Cadence configurable | Codée en dur (`CADENCE_HOSTAWAY_H = 5`). | — | Rendue configurable via `cfg`/variables d'environnement (§3). |
| Observabilité scheduler à l'écran | Absente. | — | Ajoutée sur `/actualisation` (§8). |

**Conclusion** : aucun scheduler créé ex nihilo. Trois vrais manques comblés : câblage
démarrage/arrêt, `run_history`, cadence configurable + affichage.

## 2. Démarrage / arrêt avec l'application

`app/main.py::lifespan` appelle `ordonnanceur_service.demarrer()` au démarrage et
`ordonnanceur_service.arreter()` à l'arrêt. `demarrer()` reste le seul garde-fou : il refuse tant
que `cfg.ORDONNANCEUR_ACTIF` est faux (défaut), donc l'appel est inconditionnel dans `lifespan` —
pas de `if` dupliqué. Un second appel à `demarrer()` (ex. reload) rend `{"deja_demarre": True}`
sans créer un second minuteur (`_minuteur` singleton déjà existant, vérifié par test).

## 3. Fréquence configurable

`cfg.HOSTAWAY_REFRESH_INTERVAL_HOURS` (défaut 5) et `cfg.HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS`
(défaut 24), lues depuis les variables d'environnement du même nom. `ordonnanceur_service.
cadences()` les lit à chaud (jamais figées à l'import) — un seul endroit change la cadence, jamais
un second « 5 » codé ailleurs.

## 4. `run_history` pour Hostaway

Câblé dans `hostaway_actualisation_service.actualiser()`, **uniquement sur le chemin synchrone**
(`attendre=True` — celui qu'empruntent l'orchestrateur et donc l'ordonnanceur) : c'est le seul où
le code retour réel est connu avant que la fonction ne réponde. Le bouton « fire-and-forget »
(`attendre=False`) reste suivi par `moteur_runs`/`hostaway_extractions`, écrits par le
sous-processus lui-même de façon asynchrone — l'ajouter à `run_history` aurait exigé un mécanisme
de réconciliation a posteriori, hors périmètre de cette mission.

Chaque entrée porte : `run_id_opaque`, `operation="HOSTAWAY"`, `acteur` (MANUEL/AUTO),
`statut` (STARTED → SUCCESS ou FAILED), durée, erreur. Vocabulaire réutilisé de la mission
précédente (`run_history_service.py`), aucun nouveau vocabulaire.

## 5. Sauvegarde — filet de sécurité, pas systématique

**Décision explicite, alignée sur la mission** : aucune sauvegarde `app.db` avant un tick Hostaway
de routine. `orchestrateur_service.actualiser(cibles=[...])` (mission précédente) ne prend déjà une
sauvegarde que sur une actualisation **globale** (`cibles=None`) — l'ordonnanceur appelle
`actualiser(cibles=[TACHE_HOSTAWAY])`, donc **aucune sauvegarde n'est prise à chaque battement**,
ce qui est exactement ce que demande cette mission (§8 : « backup ≠ restauration systématique »,
« ne pas restaurer toute app.db pour une simple panne API »). Ce choix n'est pas nouveau — il
découle directement de l'architecture déjà en place, confirmé ici comme correct pour Hostaway.

**En cas de panne API** (timeout, 429, réponse incomplète) : le dataset `HOSTAWAY_RAW` n'est
jamais activé (`marquer_dataset(..., ST_ECHEC, ...)`), l'ancien dataset reste `A_JOUR` et servi tel
quel — aucune restauration de base n'intervient, seule la non-activation protège.

**Restauration complète réservée aux cas graves** (intégrité de `app.db` elle-même compromise) —
mécanisme de la mission précédente (`backup_service.restaurer()`), non déclenché par une panne
Hostaway isolée.

## 6. Activation atomique

Déjà garantie par `orchestrateur_service.recalculer_dataset()` (mission antérieure) : un dataset
n'est marqué `A_JOUR` qu'après le succès réel de son service ; en échec, il reste
`A_RECALCULER`/`ECHEC`, jamais half-way. `hostaway_raw_service.enregistrer()` écrit sous un
`extraction_id` isolé, versionné — même principe CURRENT/CANDIDATE que Lot10/Lot12
(`derniere_extraction_utilisable()` ne pointe vers une extraction que si elle a été close en
succès), pas une nouvelle mécanique.

## 7. Idempotence

Non testée par un nouveau test dédié dans cette mission : structurellement garantie par le même
principe versionné (chaque run Hostaway produit un `extraction_id` NOUVEAU et isolé ; l'ancien
reste inerte, jamais fusionné) — deux imports successifs des mêmes données ne peuvent pas produire
« un dataset actif en double », par construction, pas par un test de non-régression spécifique.

## 8. Interface / observabilité

Écran `/actualisation` (existant) étendu d'un bloc « Scheduler Hostaway » : statut ACTIF/DÉSACTIVÉ,
cadences (h), dernier état par source, prochaine décision (le motif exact de `doit_declencher`,
ex. « Actualisé il y a 2:00:00 (cadence 5h) »). Aucune deuxième page créée. Rien d'exposé :
token, URL, payload, chemin absolu — les seules données affichées sont des statuts et des durées
déjà publiques sur cet écran.

## 9. Tests

`tests/test_scheduler_hostaway_industrialisation.py` (10 tests, nouveau) : cadence configurable
(2), non-doublement du minuteur (2), refus si inactif (1), câblage `lifespan` (1), `run_history`
Hostaway succès/échec/non-bloquant (3), configuration par défaut (1).

`tests/test_actualisation_ui.py` : 1 test ajouté (bloc scheduler visible, statut désactivé,
cadences affichées).

`tests/test_ordonnanceur.py` : adapté à `cadences()` (fonction) au lieu de `CADENCES` (dict figé),
16 tests existants toujours verts, non dupliqués.

Aucun appel API réel, aucune écriture réelle (`tmp_db` isole `DB_PATH`/`BACKUPS_DIR`), aucun test
n'attend réellement 5 heures (horloge injectée partout via `maintenant=`).

## 10. Limites

- Idempotence Hostaway garantie structurellement (§7), pas vérifiée par un test dédié qui
  exécuterait deux imports réels successifs.
- `run_history` ne couvre que le chemin synchrone Hostaway — le bouton fire-and-forget manuel
  reste hors `run_history` (documenté, §4).
- CleaningTasks (H6) n'a toujours aucun service d'import automatisé (`tick()` le signale
  explicitement) — décision antérieure confirmée, pas tranchée différemment ici.
- Le lien sauvegarde↔run Hostaway n'existe pas puisqu'aucune sauvegarde n'est prise (§5) — cohérent
  avec la règle de la mission, pas un oubli.

## 11. Prochaine étape recommandée

Administration des référentiels SQLite (mission suivante annoncée). Activer le scheduler réel
reste une décision d'exploitation explicite, hors mandat de cette mission (`ORDONNANCEUR_ACTIF`
reste faux par défaut, jamais modifié ici).

---

## 12. Passe de vérification et de renforcement (2026-09-10)

Mission « scheduler Hostaway sécurisé 5h » du 2026-09-10. HEAD départ `b753cf1`. Audit ciblé :
**le scheduler était déjà construit, testé et documenté** (§1-11 ci-dessus, missions du 2026-08-23
puis stabilisation 18d) — rien reconstruit, aucun second moteur créé.

### 12.1 Ce que l'audit a confirmé, point par point

| Exigence de la mission | État constaté | Preuve |
|---|---|---|
| Scheduler = déclencheur seul, aucune règle métier | `ordonnanceur_service.tick()` n'appelle que `orch.actualiser(cibles=[TACHE_HOSTAWAY], declencheur=AUTO)` — aucune liste de Lots, aucun calcul, aucune logique de stockage | `test_ordonnanceur.py::test_tick_declenche_via_orchestrateur` |
| Même service manuel/automatique | Bouton `/hostaway` → `hostaway_actualisation_service.actualiser()` ; scheduler → `orchestrateur_moteur.importer_hostaway` → **la même** `actualiser(attendre=True)`. Une seule implémentation d'extraction | `test_ordonnanceur.py::test_ordonnanceur_et_manuel_partagent_le_meme_service` |
| Désactivé par défaut | `cfg.ORDONNANCEUR_ACTIF = _env_flag("ORDONNANCEUR_ACTIF")` → faux ; `demarrer()` refuse (`E_INACTIF` = `"ORDONNANCEUR_INACTIF"`) | `test_scheduler_hostaway_industrialisation.py::test_demarrer_refuse_si_inactif` |
| Fréquence 5 h configurable, jamais codée deux fois | `cfg.HOSTAWAY_REFRESH_INTERVAL_HOURS` (défaut 5), lu à chaud par `ordonnanceur_service.cadences()` | `test_scheduler...::test_cadence_hostaway_configurable` |
| Un seul job, pas de doublon après reload | `_minuteur` singleton ; `demarrer()` deux fois → `{"deja_demarre": True}` | `test_scheduler...::test_demarrer_deux_fois_ne_double_pas_le_minuteur` |
| Démarrage/arrêt propre avec l'app | `app/main.py::lifespan` appelle `demarrer()`/`arreter()` inconditionnellement (le garde-fou décide) | `test_scheduler...::test_lifespan_demarre_et_arrete_ordonnanceur` |
| Concurrence manuel ↔ auto | Verrou existant réutilisé : `hostaway_actualisation_service.actualisation_en_cours()` (via `moteur_runs`, statut `EN_COURS`) bloque **dans les deux sens** — les deux chemins passent par cette garde. Aucun second système de verrou | `test_hostaway_sqlite.py::test_actualisation_refusee_si_une_autre_est_en_cours` |
| Rate limit / 429 borné | `Retry-After`, backoff plafonné, budget de tentatives, `RateLimitEpuise` vivent dans `app/adapters/hostaway_client.py`. L'ordonnanceur ne les réimplémente pas ; il impose seulement un palier (`REPRISE_APRES_ECHEC_H = 1`) avant de réessayer après un échec | `test_ordonnanceur.py::test_gestion_429_reste_dans_le_lot_dextraction`, `test_echec_recent_impose_un_palier` |
| Reprise après crash | `orchestrateur_service.marquer_runs_interrompus()` appelé à chaque `tick()` : un run resté `EN_COURS` sans verrou actif → `INTERROMPU`, ses datasets → `A_RECALCULER` | `test_orchestrateur.py` (reprise) |
| Activation atomique | `recalculer_dataset()` ne marque `A_JOUR` qu'après le succès réel du service ; extraction RAW versionnée (`extraction_id` isolé), `derniere_extraction_utilisable()` ne pointe une extraction que close en succès | `test_hostaway_actualisation_service.py` |
| Panne API ≠ restauration DB | Un échec/timeout/429 laisse le dataset `HOSTAWAY_RAW` non activé, l'ancien reste servi. Restauration complète réservée à un `integrity_check` en échec (jamais déduite du statut) | `orchestrateur_service.actualiser()` §532-555 |
| DAG aval, jamais de Lots dans le scheduler | `orch.actualiser(cibles=[TACHE_HOSTAWAY])` recalcule les descendants nécessaires via `orchestrateur_dag` | `test_ordonnanceur.py::test_tick_declenche_via_orchestrateur` |
| CleaningTasks hors cadence 5 h | Cadence 24 h par défaut ; `tick()` ne lance rien pour H6 (aucun `service` dans le DAG) et le signale explicitement | `test_ordonnanceur.py::test_cleaning_tasks_non_declenche_toutes_les_5h` |
| `run_history` pour l'automatique | Câblé sur le chemin synchrone (`attendre=True`) ; statuts canoniques `STARTED → SUCCESS/FAILED` ; `acteur = AUTO` | `test_scheduler...::test_hostaway_actualiser_journalise_dans_run_history_si_attendre` |
| Aucun appel API réel / vraie DB en test | Sous-processus et `_interpreteur` mockés, `tmp_db` isole `cfg.DB_PATH` | toute la suite `test_ordonnanceur` / `test_scheduler...` |

### 12.2 Seul renforcement apporté ce tour

Écran manuel `/hostaway` : ajout d'un bloc **lecture seule** « Actualisation automatique » (état
`ACTIVÉE`/`DÉSACTIVÉE`, fréquence configurée, dernier run, prochaine décision) alimenté par
`ordonnanceur_service.etat()`. Aucun contrôle d'activation dans l'UI. C'est le même contenu que le
bloc déjà présent sur `/actualisation`, rendu visible là où l'utilisateur lance une actualisation
manuelle. `+1` test (`test_hostaway_sqlite.py::test_ecran_hostaway_affiche_le_statut_du_scheduler`).
Commit `25c97aa`.

### 12.3 Points d'arbitrage laissés ouverts (non tranchés — pas bloquants pour le scheduler 5 h)

- **CADENCE H6 À ARBITRER** : CleaningTasks reste à 24 h par défaut et sans service d'import
  automatisé (`tick()` le dit). Aucune règle métier validée ne fixe sa fréquence réelle. Tant
  qu'aucune décision n'est prise, H6 **n'est pas embarqué** dans le job 5 h — conforme à la
  consigne. À trancher dans une mission dédiée.
- **OPTIMISATION RECALCUL AVAL SUR HASH RAW À ARBITRER** : `doit_declencher()` décide sur la
  cadence temporelle, pas sur le contenu. Après un import Hostaway réussi, `orch.actualiser`
  recalcule les descendants sans comparer le contenu RAW nouveau/ancien. Un `row_hash`/`ROW_HASH`
  existe **par ligne** dans la couche RAW, mais aucun mécanisme dataset-level « contenu inchangé →
  ne pas propager » n'est présent dans l'architecture actuelle. La consigne interdit d'en créer un
  troisième : à câbler seulement si un mécanisme de fraîcheur dataset-level est introduit ailleurs.

### 12.4 Tests de cette passe

- Ciblés : `test_ordonnanceur.py` + `test_scheduler_hostaway_industrialisation.py` +
  `test_hostaway_actualisation_service.py` + `test_orchestrateur.py` → **54 passed**.
- Régression scheduler-adjacente (mot-clé `reservation|hostaway|ordonnanceur|scheduler|
  actualisation`) → **209 passed / 1 skipped / 1 failed** — l'échec `test_regularisation_hh.py::
  test_regularisation_ne_touche_pas_hostaway` est **pré-existant** (exige un clone de la vraie
  `app.db`, absente de ce worktree ; cf. `HANDOFF_CANONIQUE.md` mission 18d).
- Suite moteur complète (`tests/` racine) → **407 passed / 5 failed pré-existants / 1 skipped** —
  identique à la baseline, aucun rapport avec Hostaway.
- Suite application complète → **2996 passed / 13 failed / 66 skipped** (20 min). Les 13 échecs
  sont pré-existants et environnementaux (7 clone `app.db` réelle absent, 4 `LOT4A_ENGINE_PYTHON`
  absent, 1 source bancaire réelle absente, 1 `test_appsec1_diagnostic` — `basetemp` pytest sous
  `C:\Users\<nom>`) ; aucun n'exerce l'un des 3 fichiers modifiés ce tour. Ventilation complète :
  `HANDOFF_CANONIQUE.md`, Mission 18e §E.

Scheduler réel **non activé** (`ORDONNANCEUR_ACTIF` faux, non modifié). Mode réel **OFF**. Vraie
`app.db` non ouverte en écriture.

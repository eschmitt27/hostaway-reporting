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

---

## 13. Mission 28 (2026-09-13) — l'audit refait sur l'arbre réel

> **Les §1-12 décrivent l'état au 2026-09-10.** Plusieurs de leurs affirmations ne tenaient plus au
> 2026-09-13 ; elles sont corrigées ici, et c'est ce paragraphe qui fait foi.

### 13.1 Continuité

HEAD de départ `0d454bc`, et non `b753cf1` : `b753cf1` en est un ancêtre, 74 commits plus loin sur
la même branche, worktree propre. Migrations **0087**. Entre-temps, l'ingestion Hostaway est passée
au dépôt publié par le pipeline GitHub (`27411b4`, `9d03e05`) et le nœud H6 a reçu un service :
l'audit de 18e ne pouvait pas être repris tel quel.

### 13.2 Audit — composant, existant, réutilisation

| Composant | Existe | Réutilisé | Modification |
|---|---|---|---|
| Scheduler (`ordonnanceur_service`) | oui | oui — aucun second scheduler | cycle de vie durci (§13.3 D6), H6 retiré des tâches automatiques, observabilité |
| Démarrage/arrêt (`app/main.py::lifespan`) | oui | oui | aucune |
| Service Hostaway canonique | `hostaway_depot_service.synchroniser` (bouton, orchestrateur, ménages) | oui | verrou, `run_history` complet, code retour ≠ 0 = échec |
| Moteur d'extraction (`lot1 --source DEPOT_GITHUB`) | oui | oui | aucune |
| `HostawayClient`/`RateLimitEpuise`/`Retry-After` | `app/adapters/hostaway_client.py` | oui | aucune |
| Orchestrateur + DAG | oui | oui | périmètre des imports externes, conservation de l'aval inchangé, invalidation au changement, déclencheur transmis |
| Verrou/bail (`orchestrateur_verrous`) | oui | oui — portée `HOSTAWAY_RAW`, aucun second mécanisme | `prendre_verrou` rendu atomique |
| Reprise après crash | `marquer_runs_interrompus` (orchestrateur et lot1) | oui | clôture des entrées `run_history` orphelines |
| `run_history` | oui | oui — statuts `STARTED/SUCCESS/FAILED` existants | `dernier()`, `marquer_orphelins()` |
| Identité de version | `hostaway_extractions.source_ref` (0086) + journal `orchestrateur_dataset_evenements` | oui — aucun hash nouveau | lu pour décider « inchangé » |
| `backup_service` | oui | oui, inchangé | aucune — jamais appelé sur une actualisation ciblée |
| Écrans | `/hostaway`, `/actualisation`, `/observabilite/runs` | oui — aucun écran créé | blocs enrichis, colonne Déclencheur |
| Configuration | `ORDONNANCEUR_ACTIF`, `HOSTAWAY_REFRESH_INTERVAL_HOURS` | oui (le contrat `HOSTAWAY_AUTO_ENABLED` de la consigne = `ORDONNANCEUR_ACTIF`, nom déjà en place) | cadence bornée (≥ 1 h) |

### 13.3 Défauts réels trouvés et corrigés

| # | Défaut | Conséquence réelle | Correctif | Preuve |
|---|---|---|---|---|
| D1 | H6 (dépend de HOSTAWAY_RAW, porte un service) exécuté en descendant de chaque run 5 h, et par `tick()` toutes les 24 h | sans identifiants de tâches, ECHEC H6 → MENAGES bloqué → FLUX_LOT9, LOT10, LOT11, LOT12 `A_RECALCULER` après **chaque** run automatique | un import externe ne part que s'il est lui-même demandé ; la propagation ne le traverse pas ; `TACHES_AUTOMATIQUES = (HOSTAWAY_RAW,)` | `test_27_*` (×3) |
| D2 | `importer_hostaway` forçait `declencheur="AUTO"` | un run lancé depuis `/actualisation` tracé AUTO | l'orchestrateur transmet son déclencheur aux services qui le déclarent | `test_06_*` |
| D3 | garde de concurrence lue dans `moteur_runs`, écrit par le sous-processus après démarrage ; `prendre_verrou` lisait puis écrivait hors transaction | clic pendant un battement : deux imports concurrents | bail `HOSTAWAY_RAW` pris par `synchroniser` avant toute lecture ; `BEGIN IMMEDIATE` | `test_16_*`, `test_17_*`, `test_16_17_prendre_verrou_est_atomique_sous_concurrence` |
| D4 | `marquer_dataset` conservait `calcule_le` en le lisant dans un `SELECT statut` | chaque EN_COURS/ECHEC effaçait la date du dernier succès : **le palier après échec ne s'appliquait jamais**, relance à chaque battement | `SELECT statut, calcule_le` | `test_09_12_depot_injoignable_*` (2e battement), `test_obs_un_echec_conserve_*` |
| D5 | aval recalculé à chaque battement, même dépôt inchangé | chaîne complète recalculée ~5 fois/jour pour rien | décision par identité d'extraction (§13.7) | `test_14_*` |
| D6 | `arreter()` pendant un battement : le `finally` réarmait un minuteur | fil orphelin après arrêt ; deux schedulers après redémarrage | génération de cycle + verrou de cycle | `test_19_*`, `test_26_*` |
| D7 | `run_history` absent si dépôt illisible ou déjà synchronisé ; timeout → `str(TimeoutExpired)` (ligne de commande : 3 chemins absolus) ; message dépôt brut (URL pouvant porter un jeton) | trous d'historique, fuite de chemins/identifiants à l'écran et en base | entrée ouverte/close par `synchroniser` quelle que soit l'issue ; `HOSTAWAY_DELAI_DEPASSE` ; `sanitize_erreur_externe` | `test_06_07_08_*`, `test_09_12_*`, `test_22_*`, `test_obs_22_*` |
| D8 | code retour du moteur ≠ 0 rendait `ok=True` | le bouton annonçait « Synchronisation terminée » pour un import échoué | `ok=False`, `HOSTAWAY_IMPORT_ECHOUE` | `test_import_en_echec_n_est_jamais_annonce_termine` |
| D9 | deux tests `importer_hostaway` faisaient un vrai `git fetch` | appel réseau réel en test ; échec hors dépôt Git (constaté sur la copie de référence) | dépôt doublé | `test_hostaway_actualisation_service.py` |

### 13.4 Architecture finale

```
Scheduler (déclencheur seul : quand ?)
  └─ orchestrateur_service.actualiser(cibles=[HOSTAWAY_RAW], declencheur=AUTO)   ← même appel que
       │                                                                             /actualisation cible
       ├─ DAG → orchestrateur_moteur.importer_hostaway(declencheur)
       │     └─ hostaway_depot_service.synchroniser  ← SERVICE CANONIQUE (aussi : bouton /hostaway, ménages)
       │          ├─ bail orchestrateur_verrous[HOSTAWAY_RAW]   (refus immédiat si pris)
       │          ├─ run_history HOSTAWAY  STARTED → SUCCESS | FAILED
       │          ├─ lecture du dépôt publié (identité = commit)
       │          └─ lot1_hostaway_extract --source DEPOT_GITHUB → extraction RAW isolée
       │                → close SUCCES = ACTIVATION (sinon jamais utilisable)
       ├─ « inchangé ? » = extraction servie == extraction du dernier passage À JOUR
       └─ descendants du DAG : conservés si inchangés, invalidés puis recalculés sinon
```

Le bouton `/hostaway` appelle **le même service** : il synchronise la couche RAW et affiche le
résultat. Il ne recalcule pas l'aval (comportement antérieur conservé) ; la version importée n'est
jamais perdue pour autant — le run orchestré suivant, automatique ou « Actualiser », constate
qu'elle n'a pas été propagée et la propage (`test_15_import_fait_depuis_l_ecran_*`).

### 13.5 Planification, configuration, fuseau

`ORDONNANCEUR_ACTIF` (faux par défaut) · `HOSTAWAY_REFRESH_INTERVAL_HOURS` (5) ·
`HOSTAWAY_CLEANING_TASKS_INTERVAL_HOURS` (24, affichée, jamais déclenchée). Une valeur absente,
illisible ou < 1 retombe sur le défaut : 0 ne veut jamais dire « à chaque battement ». Battement
15 min (question « est-ce dû ? »), cadence métier lue à chaud. Toutes les dates en UTC,
`AAAA-MM-JJTHH:MM:SSZ`, horloge injectable (`maintenant=`), aucun datetime naïf.

### 13.6 Concurrence, crash, rate limit

- **Concurrence** : bail `HOSTAWAY_RAW` pris avant la lecture du dépôt, rendu à la fin ; toute autre
  synchronisation reçoit `HOSTAWAY_ACTUALISATION_EN_COURS` immédiatement. Le scheduler lit le bail
  (`verrou_actif`) et s'abstient sans rien lancer. Plusieurs processus sur la même base : chacun peut
  porter un scheduler, le bail garantit qu'un seul import tourne.
- **Crash** : run orchestrateur ouvert sans verrou → `INTERROMPU`, ses datasets EN_COURS →
  `A_RECALCULER` (existant) ; run lot1 → `INTERROMPU` par PID ou bail (existant) ; entrée
  `run_history` restée STARTED → `FAILED` « RUN_INTERROMPU », durée vide, dès que le bail prouve
  qu'aucune synchronisation ne tourne. Un changement de données invalide les enfants AVANT leur
  recalcul : un arrêt entre les deux ne laisse aucun aval « à jour » sur l'ancienne version.
- **Rate limit** : le chemin 5 h n'appelle plus l'API (dépôt). Pour les chemins API (H6),
  `hostaway_client` borne tentatives (3), attente unitaire (60 s) et budget (180 s), respecte
  `Retry-After`, lève `RateLimitEpuise`. Côté scheduler : un échec impose un palier d'1 h puis la
  cadence ; aucune boucle.

### 13.7 Données : activation atomique, panne ≠ restauration, aval

- Extraction écrite sous un `extraction_id` isolé ; utilisable seulement close SUCCES/PARTIEL. Un
  moteur tué laisse un fragment jamais servi : **aucun mélange** ancien/nouveau
  (`test_09_12_timeout_*`).
- Timeout, dépôt injoignable, code retour ≠ 0 : dataset précédent servi, **aucune restauration** de
  `app.db`, aucune sauvegarde prise (actualisation ciblée). La restauration reste réservée à un
  `integrity_check` en échec après une actualisation globale (inchangé).
- **Aval** : sur une actualisation ciblée, un descendant déjà À JOUR dont tous les amonts du run
  sont inchangés est conservé (étape IGNOREE, motif écrit). « Actualiser toute l'activité » reste un
  recalcul complet.
- **Limite assumée** (sémantique Mission 14b inchangée) : quand l'import échoue, les descendants du
  run sont marqués `A_RECALCULER` (« amont en échec ») bien que leurs données restent valides ; ils
  sont recalculés au succès suivant. À arbitrer.

### 13.8 CleaningTasks (H6)

**Hors scheduler 5 h — CADENCE H6 À ARBITRER.** Aucune règle validée ne fixe sa fréquence ; les
tâches ne sont pas publiées par le pipeline et ce poste n'a pas d'identifiants. Cadence 24 h
conservée en configuration et affichée, jamais déclenchée. « Actualiser les ménages » (qui désigne
H6 lui-même) reste le seul déclencheur.

### 13.9 Observabilité — où vit chaque information d'un run

| Information | Registre (schéma réel) |
|---|---|
| run_id | `run_history.run_id_opaque` (synchronisation) · `moteur_runs.run_id` (run orchestrateur) |
| opération | `run_history.operation = HOSTAWAY` |
| source Hostaway | `hostaway_extractions.mode = DEPOT_GITHUB`, `source_ref`, `source_horodatage` |
| déclencheur | `run_history.acteur` · `moteur_runs.declencheur` · `orchestrateur_datasets.declencheur` |
| début / fin / durée | `run_history.date_debut`, `date_fin`, `duree_s` |
| statut | `run_history.statut` (STARTED/SUCCESS/FAILED) · `moteur_runs.statut` |
| erreur sanitisée | `run_history.erreur` · `orchestrateur_datasets.erreur_message` |
| volumes | `hostaway_extractions.nb_reservations/nb_payouts/nb_listings` · `orchestrateur_datasets.nb_lignes` |
| datasets modifiés | `moteur_run_etapes` (SUCCES = recalculé, IGNOREE « inchangé » = conservé) |
| sauvegarde | `run_history.sauvegarde_id_opaque` — vide : aucune sauvegarde sur une actualisation ciblée |

Écrans : `/hostaway` (état, fréquence, dernier run avec statut/déclencheur/durée, dernier succès,
dernier échec, prochain run prévu) · `/actualisation` (mêmes informations + H6 « Automatique : Non »)
· `/observabilite/runs` (colonne Déclencheur ajoutée). Aucun jeton, endpoint, chemin ni payload.

### 13.10 Correspondance des 27 tests demandés

Fichier principal : `tests/test_scheduler_hostaway_securise.py` (préfixe = numéro).

| # | Exigence | Test(s) |
|---|---|---|
| 1 | désactivé par défaut | `test_01_*` · `test_ordonnanceur::test_flag_desactive_par_defaut_dans_la_configuration` |
| 2 | activation explicite | `test_02_04_*` |
| 3 | fréquence configurable | `test_03_*` (5 valeurs) · `test_scheduler_hostaway_industrialisation::test_cadence_hostaway_configurable` |
| 4 | un seul job | `test_02_04_*` |
| 5 | même service | `test_05_*` · `test_ordonnanceur::test_ordonnanceur_et_manuel_partagent_le_meme_service` |
| 6 | déclencheur AUTOMATIQUE tracé | `test_06_07_08_*` · `test_06_run_lance_depuis_l_ecran_*` |
| 7 | run history | `test_06_07_08_*` |
| 8 | succès | `test_06_07_08_*` · `test_13_*` |
| 9 | timeout | `test_09_12_timeout_*` |
| 10 | 429 + Retry-After | `tests/test_lot1_rate_limit.py::test_retry_after_est_respecte`, `::test_retry_after_plafonne`, `::test_retry_after_illisible_bascule_sur_le_backoff` (suite moteur) |
| 11 | budget épuisé | `tests/test_lot1_rate_limit.py::test_429_persistant_leve_une_erreur_explicite`, `::test_le_nombre_de_tentatives_est_borne`, `::test_budget_d_attente_borne` · palier : `test_09_12_depot_injoignable_*` |
| 12 | dernier dataset conservé | `test_09_12_*` (×2) |
| 13 | candidat activé | `test_13_*` |
| 14 | aucune modification → aucun recalcul | `test_14_*` |
| 15 | modification → DAG aval | `test_15_*` (×3, dont arrêt brutal) |
| 16 | manuel actif → auto refusé | `test_16_*` · `test_16_17_prendre_verrou_*` |
| 17 | auto actif → manuel refusé | `test_17_*` |
| 18 | crash → run interrompu | `test_18_*` |
| 19 | redémarrage propre | `test_19_*` (×2) |
| 20 | aucun appel API réel | `test_20_21_*` (requests, sockets et `git` interdits) |
| 21 | aucune vraie DB | `test_20_21_*` (toute connexion SQLite vise la base de test) |
| 22 | aucun secret loggé | `test_22_*` (×2) · `test_obs_22_*` · assertions de fuite de `test_09_12_*` |
| 23 | idempotence | `test_23_24_25_*` · `test_hostaway_depot_github::test_une_seconde_synchronisation_du_meme_etat_n_importe_rien` |
| 24 | pas de doublon réservation | `test_23_24_25_*` · `test_hostaway_sqlite::test_rejouer_la_meme_extraction_ne_duplique_rien` |
| 25 | pas de doublon payout | `test_23_24_25_*` |
| 26 | pas de double scheduler après reload | `test_26_*` · `test_19_26_*` |
| 27 | CleaningTasks non embarqué | `test_27_*` (×3) |

### 13.11 Points ouverts (non bloquants)

- **CADENCE H6 À ARBITRER** (§13.8).
- **Aval après un import en échec** : statut `A_RECALCULER` alors que les données restent valides
  (§13.7) — sémantique 14b, à arbitrer.
- **Bouton `/hostaway`** : synchronise sans recalculer l'aval ; propagation au run orchestré suivant
  (§13.4). Le rendre propagateur changerait un parcours recetté : décision produit.
- **Mode `attendre=False`** de `synchroniser` (aucun appelant en production) : le bail est rendu au
  lancement, la garde `moteur_runs` protège seule la suite.

### 13.12 Commits et tests

Voir `HANDOFF_CANONIQUE.md`, Mission 28 : SHA, fichiers, résultats ciblés et suites complètes.

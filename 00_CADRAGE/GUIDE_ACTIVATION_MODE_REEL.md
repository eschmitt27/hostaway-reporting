# Guide d'activation du mode réel

⚠️ **Le mode réel ne doit pas être activé sans décision explicite.** Ce guide décrit la procédure ;
il ne l'exécute pas.

État par défaut d'une installation : **tous les verrous d'écriture réelle sont faux**, y compris
`RECETTE_MODE` et `MODE_REEL_ECRITURES`. Prouvé par `tests/test_flags_inventaire.py`.

> **2026-09-10 — une instance de recette réelle a été mise en service** (mission « application
> réelle testable de bout en bout »), avec `MODE_REEL_ECRITURES=1` et les writers SQLite-seuls
> activés : Charges, Factures, Cycle Ménages, Comptabilité, Banque. Restent volontairement OFF :
> `CALCULS_REAL_RUN_*` et `MENAGES_REAL_RECALC_*` (tous deux réécrivent des fichiers Excel/CSV
> **réels suivis par Git**), et le scheduler Hostaway. Détail et procédure de relance :
> `RECETTE_MODE_REEL_20260910.md`. **Cela ne change pas le défaut d'installation** : sans variable
> d'environnement, tout reste faux.

## 1. Prérequis — checklist GO / NO GO

Chaque ligne doit être **GO** avant toute activation. Une seule NO GO arrête la procédure.

| # | Prérequis | Comment le vérifier | État au 2026-07-27 |
|--:|---|---|---|
| 1 | Suite complète verte hors flake connu | `pytest -q` en 4 tranches | ✅ 1967 passés / 1 flake pré-existant |
| 2 | Worktree propre, rien en attente | `git status --porcelain` vide | ✅ |
| 3 | Sauvegarde intégrale des sources réelles | copie horodatée hors du worktree, **vérifiée par sha256** | ⛔ **à faire** |
| 4 | Aucun fichier Excel ouvert | `detecter_excel_ouvert()` | à vérifier au moment T |
| 5 | Aucun verrou actif | `inspecter_verrou()` sur chaque verrou de `05_APPLICATION/data/` | à vérifier au moment T |
| 6 | Module concerné classé TERMINÉ | `MATRICE_ETAT_MODULES.md` | Charges, Banque, Factures, Calculs ✅ · **Ménages ⛔** |
| 7 | Arbitrages métier tranchés | handoff, section « ARBITRAGE EN ATTENTE » | ⛔ **pivot D101 en attente** |
| 8 | Recette sur copies contrôlées passée | `RAPPORT_RECETTE_GLOBALE.md` | ⛔ **non réalisée** |
| 9 | Rollback prouvé sur le module visé | run + restauration | ✅ Charges, Calculs · autres à faire |
| 10 | Responsable identifié et présent | — | à nommer |

**Au 2026-07-27 : NO GO.** Les lignes 3, 7 et 8 ne sont pas satisfaites, et Ménages n'est pas
terminé.

## 2. Fichiers concernés

L'écriture réelle touche, selon le module activé :

| Module | Fichiers écrits |
|---|---|
| Charges | `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx`, `SAISIE_Charges_Impacts.xlsx`, puis `02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx` |
| Banque | `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` |
| Factures | base SQLite applicative uniquement — aucun classeur métier |
| Calculs | toutes les sorties `02_TRAVAIL/Lot*/` et `03_EXPORTS/PowerBI/` |
| Ménages | `02_DONNEES_NORMALISEES/menages/M04_*`, `02_TRAVAIL/Lot6*/` |

## 3. Ordre d'activation

**Un module à la fois.** Jamais deux en même temps : en cas d'incident, l'origine doit être
immédiatement identifiable.

Ordre recommandé, du moins au plus engageant :

1. **Calculs** — ne produit que des sorties recalculables, entièrement régénérables.
2. **Banque** — import idempotent, dédoublonné par `ROW_HASH`.
3. **Factures / Règlements** — n'écrivent que dans SQLite.
4. **Charges** — écrit dans une source durable ; le plus sensible.
5. **Ménages** — **pas avant** que le module soit terminé.

## 4. Procédure d'activation

1. Rejouer la checklist GO / NO GO. Une seule NO GO : arrêter.
2. Prendre la sauvegarde (§1 ligne 3) et **vérifier son sha256**.
3. Poser les variables du module visé, jamais plus :

   ```
   RECETTE_MODE=0
   MODE_REEL_ECRITURES=1
   <MODULE>_REAL_WRITE_ENABLED=1
   <MODULE>_REAL_WRITE_CONFIRMATION_ENABLED=1
   ```

   ✅ **Mis à jour le 2026-09-10.** L'avertissement précédent (« passer en réel exige d'abord une
   modification de `config.py` ») **n'est plus vrai**. Les verrous NIVEAU B passent désormais par
   `_verrou_ecriture(nom)` = `(RECETTE_MODE or MODE_REEL_ECRITURES) and _env_flag(nom)` : un
   SECOND contexte d'activation existe à côté de la recette, et l'activation réelle est une simple
   configuration d'environnement.

   Le principe des **deux leviers simultanés** est conservé, ainsi que le **défaut faux partout** :
   ni `MODE_REEL_ECRITURES` seul, ni la variable du module seule n'activent quoi que ce soit
   (figé par `tests/test_flags_inventaire.py`).

   Les cinq gardes **gelées** (`HH_REAL_WRITE_*`, `REF_ASSOC_MODE_REAL_WRITE_ENABLED`,
   `CONTROLES_REAL_WRITE_*`) ne sont **pas** concernées : elles restent littéralement `False`, sans
   aucun chemin d'activation. `CONTROLES_REAL_WRITE_*` est même un interlock **inverse** — l'activer
   ferait REFUSER le recalcul des contrôles sur copie.
4. Redémarrer l'application. Vérifier le bandeau : il ne doit **plus** afficher « MODE RECETTE ».
5. **Prévisualiser** l'opération. Aucune écriture ne doit avoir eu lieu à ce stade.
6. Vérifier l'empreinte des entrées affichée par la prévisualisation.
7. **Confirmer** explicitement.
8. Vérifications d'après-écriture (§5).

## 5. Vérifications après écriture

- le fichier cible a bien changé (mtime, taille, sha256 différent de la sauvegarde) ;
- **aucun autre fichier n'a changé** — comparer les sha256 de toute la sauvegarde ;
- les contrôles bloquants du module sont à zéro ;
- pour Calculs : la comparaison avant/après ne montre aucun écart anormal ;
- le journal d'audit porte l'opération, l'acteur et l'horodatage.

## 6. Désactivation immédiate

Remettre les flags à `False` et redémarrer. **La désactivation ne demande aucune autorisation** :
en cas de doute, désactiver d'abord, analyser ensuite.

## 7. Procédure d'incident

1. **Désactiver** le mode réel immédiatement.
2. **Ne rien réécrire** : ne pas tenter de « corriger » par une seconde écriture.
3. Restaurer depuis la sauvegarde (§1 ligne 3), vérifier par sha256.
4. Si l'application propose une restauration de run (Calculs), l'utiliser : elle sauvegarde les
   sorties avant chaque exécution.
5. Consigner dans `JOURNAL_ANOMALIES.md` : ce qui a été écrit, ce qui a été restauré, ce qui reste
   incertain. **Ne jamais clore un incident sur une supposition.**
6. Ne réactiver qu'après une nouvelle checklist GO / NO GO complète.

## 8. Ce que ce guide ne couvre pas

- La **recette sur copies contrôlées des données réelles** (§16 du brief) n'a pas été réalisée :
  elle est un prérequis (ligne 8) et reste à faire.
- Aucune mesure de **performance** sur volumes réels n'a été prise.
- Les modules Propriétaires, Réservations et Contrôles n'ont pas été ré-exercés dans ce chantier.

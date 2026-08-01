# 64 — Orchestration Lot4quater ↔ Lot9 : audit du contrat, cause réelle de l'anomalie apparente

## Reproduction (audit ciblé, 45 min max)

| Étape | Entrée | Sortie | Grain attendu | Grain produit (run défaillant, mission précédente) | Statut |
|---|---|---|---|---|---|
| `/calculs` lancé, mois `2026-06`, chaîne aval | requête HTTP avec paramètre `mois` | run `RUN-...` | — | — | déclenché |
| lot4quater exécuté par l'app | `MASTER_CALC_Reservations.xlsx` (live), `HIST_Reservations_Cloturees.xlsx` (historique) — **absent à ce moment** | `MASTER_CALC_Reservations_Resolues.xlsx` | historique complet (tous mois) | mois ouverts seulement (repli sur live pour les 17 mois CLOTURE, faute de HIST) | **SUCCÈS applicatif, mais sortie réduite** |
| VUE_FLUX | — | 104 lignes | 1349 lignes | 104 lignes (mois 2026-06 et suivants seulement) | réduit, cohérent avec le repli |
| lot9 | `VUE_FLUX` | — | ≥ 1000 lignes (`CTR-9-003`) | 104 lignes | **BLOQUANT [CTR-9-003]** |

## Réponses explicites

1. **`/calculs` demande-t-il un calcul mensuel ou une reconstruction globale ?**
   Un calcul mensuel du point de vue de l'utilisateur (il choisit un mois de référence pour la
   comparaison affichée), mais **la chaîne aval elle-même ne connaît aucune notion de mois** —
   `lot4quater`/`lot9`/`lot10` ne reçoivent jamais ce paramètre, ils reconstruisent toujours
   l'intégralité de l'historique disponible. Le paramètre `mois` de `/calculs` ne sert qu'à choisir
   le mois affiché dans le tableau de comparaison après coup, pas à filtrer les entrées du pipeline.

2. **Lot4quater doit-il produire une vue mensuelle ou une vue globale ?**
   **Globale, toujours.** Confirmé par lecture du code (`02_TRAVAIL/lot4quater_resoudre_source_
   reservations.py`) : aucun argument CLI, aucune variable d'environnement mois, la boucle sur
   `live` ne filtre que sur l'état ouvert/clôturé, jamais sur un mois demandé.

3. **Lot9 attend-il toutes les périodes disponibles ou uniquement la période active ?**
   **Toutes les périodes disponibles** — `CTR-9-003` (volume ≥ 1000) est justement un garde-fou
   contre une régénération partielle, cohérent avec un contrat global.

4. **La sortie existante est-elle remplacée, filtrée ou fusionnée ?**
   **Remplacée intégralement** à chaque exécution (le fichier est reconstruit de zéro à partir de
   `live` + `HIST`, jamais fusionné avec l'ancienne sortie) — mais la construction elle-même reste
   globale par contrat ; ce n'est pas un remplacement partiel piloté par un mois.

5. **Pourquoi l'exécution moteur directe restait-elle verte ?**
   Parce qu'au moment de l'exécution directe (mission précédente, après correction du gap de
   copie), `HIST_Reservations_Cloturees.xlsx` **était déjà présent** dans l'environnement de
   copies — le repli documenté ne s'est donc jamais déclenché, et le contrat global s'est exécuté
   normalement des deux côtés (direct et applicatif), produisant les mêmes 1391/1349 lignes.

6. **Quel contrat est documenté dans l'architecture et les tests historiques ?**
   Le docstring du script lui-même (§ »Règle structurante«) : *« mois ouvert = live ; mois clôturé
   = HIST ; upsert sans suppression ; toute différence sur un mois clôturé ⇒ alerte, HIST prime »*.
   Aucune notion de « mois demandé » n'y figure — le contrat a toujours été global.

## Cause réelle établie

**Ce n'est pas un bug d'orchestration.** La différence entre le run défaillant (104 lignes) et le
run réussi (1349 lignes) tient entièrement à la présence ou l'absence de
`02_DONNEES_NORMALISEES/historique_reservations/HIST_Reservations_Cloturees.xlsx` dans
l'environnement de copies au moment de l'exécution — un écart de complétude de mon propre
environnement de recette (ce fichier a été copié dans une mission antérieure pour débloquer
`lot11`, mais **après** le run `/calculs` qui avait échoué). Lot4quater a appliqué son propre
mécanisme de repli déjà documenté (`CLOTURE_SANS_HIST` → `A_CONTROLER`, alerte explicite dans les
logs), jamais un crash silencieux ni une donnée fabriquée.

## Options évaluées (pour mémoire, aucune retenue car aucun défaut à corriger)

| Option | Évaluation |
|---|---|
| A. Lot4quater reconstruit toute `VUE_FLUX` à chaque exécution | **Déjà le comportement actuel** — confirmé correct |
| B. Résultat mensuel + fusion transactionnelle | Non pertinent : le contrat n'a jamais été mensuel |
| C. `/calculs` exécute Lot4quater pour toutes les périodes avant Lot9 | Déjà le cas — Lot4quater ignore le paramètre mois et traite tout |
| D. Lot9 consomme une collection de sorties mensuelles | Changerait un contrat global déjà correct et déjà testé par les consommateurs historiques — rejeté |

**Aucune correction de code appliquée.** Aucune règle métier modifiée. Aucun contournement de
`CTR-9-003`.

## Preuve de non-régression (répétée deux fois, via `/calculs`)

| Run | Lots | Statut | REEL global | Écart vs run précédent |
|---|---|---|---|---|
| 1 (HIST copié) | 6/6 | SUCCÈS | 291 852,76 € | — |
| 2 (relance, idempotence) | 6/6 | SUCCÈS | 291 852,76 € | 0,00 € |

Résultat **identique au centime près** à l'exécution moteur directe de la mission précédente.
Équivalence moteur direct ↔ application **confirmée**.

## Tests

`tests/test_lot4quater_resoudre_source_reservations.py` (nouveau, 6 tests, fixtures fictives,
script réel exécuté via `runpy.run_path`) fixant ce contrat : aucun filtre mois, HIST prime sur le
live pour les mois clôturés, repli documenté et signalé si HIST absent (jamais un crash, jamais de
données fabriquées), réinjection des réservations disparues du live, idempotence, filtre
`VUE_FLUX`. Suite complète `tests/` (moteur) : **268 passés, 0 échec**.

## Conclusion

`CTR-9-003` n'a jamais été désactivé, contourné ni affaibli. Le contrat de `lot4quater` est
confirmé correct et documenté par des tests. Le blocage observé était un artefact de
l'environnement de recette globale (fichier réel non encore copié), pas un défaut applicatif.

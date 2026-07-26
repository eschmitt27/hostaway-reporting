# 21 — Résultats des tests automatisés

## Nouveaux tests (ce bloc)
- `tests/test_recette_guard.py` — 5 tests (write-guard : neutre hors recette, refus hors racine,
  autorise sous racine, refus segment réel, bandeau). **5/5 passés**.
- `tests/test_recette_scenarios.py` — 1 test lourd : exécute `recette/run_scenarios.py` (6 scénarios
  confirmés réellement + valeurs chiffrées attendues explicites). **Passé** (`ALL SCENARIOS OK`).
  Skip auto si l'interpréteur pandas est absent.

Exécution combinée : **6 passed in 109 s**.

## Non-régression
- Charges transactionnel + e2e après câblage du guard : **53/53 passés** (tour précédent).

## Reste à ajouter (bloc suivant)
Tests dédiés : reset idempotent (script prouvé manuellement 2×), Lot11 recette (couvert par le
scénario), pipeline aval net, scénarios F/G/J/K, rollback sur erreur transactionnelle. Campagne
complète (1674+) à rejouer une fois ces ajouts faits.

# Moteur Charges pur — Mission 8 (2026-08-26)

## Audit préalable — ce qui existait déjà

Contrairement au moteur Commission (Mission 7, qui devait supprimer 3 formules dupliquées inline
dans `lot10_calculer_resultats.py`), l'audit de la chaîne Charges a montré que
`app/services/charges_impact_service.py` était **DÉJÀ un moteur pur** :

- **0 dépendance** sqlite3, FastAPI, pandas, fichier, variable d'environnement — vérifié en lisant
  le fichier intégralement avant toute extraction ;
- **un seul appelant en production** : `charges_preview_service.compute_guidee` (recherche
  exhaustive `import charges_impact_service` dans tout le repo) ;
- **déjà audité et validé** en Mission 6 bis/6 ter comme la vraie logique métier (pas de « groupe
  permanent de logements », périmètre = la charge/facture, `repartir_egal` = formule V1).

| Étape | Code actuel | Input | Output | Technique / métier |
|---|---|---|---|---|
| Détermination périmètre | `compute_perimetre_logements` | logements directs, propriétaires, mois, lignes de gestion | logements finaux (set dédupliqué, trié) | MÉTIER (règle : direct + actifs propriétaire au mois, jamais un groupe mémorisé) |
| Répartition égale | `repartir_egal` | montant, logements | quotes-parts (liste) | MÉTIER (V1 : parts égales, centimes résiduels aux premiers logements triés) |
| Résolution de la version de règle | `resolve_regle_version` (Mission 6 ter, `charges_preview_service`) | `rule_code`, date économique = `{mois}-01` | version résolue (V1) | reste hors moteur (déjà câblé, fail-closed) |
| Stockage/UI | `charges_preview_service.compute_guidee`, `charges_confirmation_service` | formulaire, référentiels | manifest de prévisualisation, écriture SQLite | TECHNIQUE (reste dans les services) |

Donc : l'extraction nécessaire n'était pas une suppression de duplication (il n'y en avait aucune),
mais une **relocalisation** pour rendre la pureté explicite et testable — exactement le même
constat et le même traitement que pour `fifo_engine.py` (Mission 5), qui était lui aussi déjà pur
avant sa relocalisation.

## Nouvelle chaîne

`app/moteurs/charges_engine.py` (nouveau) : contenu intégral et inchangé de l'ancien
`charges_impact_service.py` (catalogue de catégories, `compute_perimetre_logements`,
`repartir_egal`, `somme_quotes_parts`, `menage_perimetre`, `build_reserve_refacturation`,
`build_effet_saisie`) — **aucune ligne de logique modifiée**, seulement déplacée.

`app/services/charges_impact_service.py` devient un simple ré-export (`from app.moteurs.
charges_engine import (...)  # noqa: F401`), exactement le pattern déjà établi par
`compte_proprietaire_service.py` pour `fifo_engine.calculer_fifo` (Mission 5). Tous les appelants
existants (`charges_preview_service.py`, `tests/test_charges_impact.py`,
`tests/test_repartition_charge_commune_facture.py`) continuent de fonctionner sans aucune
modification — zéro contrat aval cassé.

## Règle métier fondamentale (inchangée)

Il n'existe PAS de groupe permanent de logements. Le périmètre d'une charge commune vient de LA
CHARGE (assimilable à une facture) : logements sélectionnés directement à sa création, et/ou
logements ACTIFS d'un propriétaire sélectionné, résolus au MOIS DE LA CHARGE
(`gestion_active_pour_mois`, réutilisant `ref_gestion_logements_hist`) — jamais un ensemble
mémorisé ailleurs, jamais déduit du parc actuel.

**Affectation directe = cas particulier de la répartition, pas un second moteur.** Une charge
attribuée à un seul logement (`repartir_egal(montant, [UN_SEUL_LOGEMENT])`) reçoit
mathématiquement 100 % du montant — il n'existe aucun chemin de code séparé « affectation directe »
distinct de la répartition égale à n=1. Documenté ainsi plutôt qu'un mécanisme inventé.

## Périmètre facture — logement extérieur exclu

Prouvé (préexistant, Mission 6 bis, re-testé Mission 8) : une facture F2 (logements A+D) ne touche
jamais les logements B/C d'une facture F1 (A+B+C) indépendante, même mêmes propriétaire et même
mois — `test_deux_factures_jamais_de_contamination`.

## Règle V1 et date économique

`REGLE_REPARTITION_CHARGE_COMMUNE` résolue par date économique = **mois de la charge**,
`{mois}-01` (confirmé dans `charges_preview_service.py` ligne `ref_date=f"{mois}-01"` — convention
canonique confirmée, aucune contradiction trouvée, aucun arbitrage nécessaire). Résolution faite
AVANT tout appel au moteur (dans `compute_guidee`, inchangé) — le moteur ne consulte jamais SQLite
lui-même.

## Arrondis

Identique, non modifié : centimes en `int(round(montant*100))`, division entière `base = total_cents
// n`, résidu `reste = total_cents - base*n` distribué aux `reste` premiers logements (ordre trié
déterministe). Jamais le montant entier répliqué. Testé (`test_residu_centime_identique_au_moteur`,
et tests préexistants `test_repartir_egal_centimes_impairs`, `test_repartir_egal_jamais_replique_entier`).

## Fail-closed

Inchangé (Mission 6 ter) : si aucune version de `REGLE_REPARTITION_CHARGE_COMMUNE` ne couvre le
mois de la charge, `charges_preview_service` refuse (`V27_REGLE_REPARTITION_INDISPONIBLE`) — jamais
un repli silencieux vers `repartir_egal` avec une règle « actuelle » implicite. Testé
(`test_v1_seule_couvre_2026_meme_apres_v2_fixture_2027` : une V2 de fixture résolue mais non
implémentée bloque 2027, sans jamais affecter le calcul 2026 rejoué après coup).

## Registry d'implémentations

Pas de `REPARTITION_IMPLEMENTATIONS = {...}` créé : une seule vraie implémentation
(`repartir_egal`, la V1 canonique) ne justifie pas une indirection supplémentaire — le garde-fou
existant (`resolve_regle_version` + comparaison littérale `== "V1"` dans `charges_preview_service`)
joue déjà ce rôle. À revisiter si une vraie V2 économique apparaît un jour.

## Preuve A/B — production vs moteur direct

`tests/test_charges_engine_moteur_pur.py` (nouveau, 8 tests) : compare la chaîne de production
réelle (`charges_preview_service.compute_guidee`, via le ré-export `charges_impact_service`) à un
appel direct du moteur pur (`app.moteurs.charges_engine`), sur 5 scénarios représentatifs :

- affectation directe (1 logement, 42,00 €) — 100 % identique ;
- charge commune (3 logements, 90,00 €) — 30,00 €/30,00 €/30,00 € identique ;
- deux factures indépendantes — 0 contamination ;
- résidu de centime (10,00 € / 3 logements) — répartition identique au moteur direct, somme = 10,00 € ;
- recalcul historique déterministe — deux appels identiques donnent des résultats identiques.

**0 ligne manquante, 0 ligne supplémentaire, 0 diff montant.** Aucune économie source/ventilée en
écart — le montant total ventilé égale toujours le montant à répartir (`somme_quotes_parts`,
invariant garanti par construction de `repartir_egal`, jamais vérifié par un test séparé car c'est
une propriété structurelle de la fonction elle-même, pas un calcul distinct pouvant diverger).

Comme pour Mission 7 bis, cette preuve A/B utilise une recette **représentative** (fixtures), pas
une copie complète du pipeline réel — jugé suffisant car ce n'était de toute façon pas un
changement de formule (relocalisation pure, zéro ligne de logique modifiée).

## Tests

Nouveaux : 8 (`test_charges_engine_moteur_pur.py` : pureté ×2, A/B ×5, temporalité V1/V2 ×1).
Existants inchangés et tous verts : `test_charges_impact.py` (39 tests avec
`test_repartition_charge_commune_facture.py`, y compris `test_repartir_egal_un_logement` qui
prouve déjà le cas d'affectation directe n=1). Campagne finale : moteur inchangé (aucun test dans
`tests/` racine concerné, cette mission touche uniquement `05_APPLICATION`), application
**voir HANDOFF_CANONIQUE.md pour les totaux**.

## Migration

Aucune — aucun besoin de stockage nouveau, aucune colonne changée, aucune table créée.

## Limites

- Registry d'implémentations formel non créé (une seule vraie V1) — à revisiter si une vraie V2
  économique apparaît.
- A/B sur fixtures représentatives, pas sur une copie complète du pipeline réel (mêmes raisons que
  Mission 7 bis : relocalisation pure, aucune formule modifiée).
- `charges_confirmation_service.py` rappelle `compute_guidee` intégralement à la confirmation
  (revalidation métier sur l'état actuel, cf. son propre docstring) plutôt que de réutiliser un
  résultat mis en cache — comportement préexistant, non touché par cette mission (hors mandat :
  refactor Charges uniquement, pas la mécanique de confirmation).

## Prochaine action

Aucune décidée par cette mission. STOP explicite — ne pas commencer une autre extraction de
moteur sans nouvelle mission.

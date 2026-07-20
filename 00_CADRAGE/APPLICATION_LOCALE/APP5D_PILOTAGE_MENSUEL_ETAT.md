# APP-5D — Pilotage mensuel : tableau de bord agrégé (PRÊT POUR INTÉGRATION)

**Statut : `APP5D_PRET_POUR_INTEGRATION`**

`/pilotage-mensuel` — agrégation **strictement en lecture seule** sur 7 modules existants
(réservations, banques/caisse, contrôles APP-5B, clôtures APP-5C, ménages, charges, règlements
propriétaires). **Aucune règle métier recalculée, aucune écriture, aucune logique dupliquée** :
chaque compteur provient d'un appel direct à la fonction de service déjà existante du module
concerné.

## Résilience

Chaque source est enveloppée individuellement (`_safe()` dans `pilotage_mensuel_service.py`) — une
panne d'un bloc met `None` sur ses seuls compteurs et l'ajoute à `indisponibles`, sans jamais
interrompre le calcul des autres blocs ni faire planter la page. Testé : 7 pannes isolées + pannes
simultanées + valeurs `None`/listes vides/schéma incomplet/date invalide (21 tests,
`test_pilotage_mensuel_resilience.py`).

## Drill-down

Chaque compteur (y compris `nb_exceptions`, corrigé — n'avait initialement aucun lien) renvoie vers
l'écran source filtré existant (mois + filtre métier réel, jamais une liste non filtrée), vérifié
fonctionnellement (contenu réellement filtré, pas seulement présence du lien HTML).

## Export

CSV sécurisé (injection formule/en-tête neutralisée, disclaimer explicite), séparateur `;` (Excel
FR), aucune donnée bancaire brute, aucun id SQLite.

## Défaut de mise en page trouvé et corrigé

`.main-area` (flexbox) sans `min-width: 0` empêchait `.table-container` de faire son
`overflow-x: auto` — `/pilotage-mensuel` débordait de 144px même à 1920px avant correction. Corrigé
dans `app.css`. Recette visuelle (Playwright, 4 largeurs) : 0px de débordement après correction.

## Preuves

Voir checkpoint `APP5CD_FINAL_INTEGRABLE_20260720_141530` et `APP5C_CLOTURES_MENSUELLES_ETAT.md`
pour les chiffres de campagne complète (partagée avec APP-5C dans la même exécution).

## Limites connues

Les compteurs « Charges » et « Réservations » du mois sont des décomptes bruts (pas de
catégorisation « à traiter » exposée par les services existants sans inventer une règle métier).

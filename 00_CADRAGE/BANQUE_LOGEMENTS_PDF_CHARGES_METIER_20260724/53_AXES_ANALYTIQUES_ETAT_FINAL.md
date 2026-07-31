# 53 — Axes analytiques : état final (Bloc 3-4, fermeture Analytique/Résultats)

Suite de `50`/`51`/`52`. Complète les axes demandés : fournisseur, catégorie, prestataire
(sources réelles), plateforme et activité (`NON_DISPONIBLE` assumé, raison explicite).

## Audit ciblé — sources trouvées

| Manque | Source disponible | Blocage | Correction |
|---|---|---|---|
| Axe fournisseur | `comptabilite_auxiliaires_service` (`49`), déjà complet | aucun — juste pas exposé comme « axe » dédié avec détail propre | `comptabilite_axes_service.fournisseurs/fournisseur_detail`, routes `/resultats/fournisseurs/{id}` |
| Axe catégorie | `charges_reader.read_charges()` → `categorie_charge_id` (MASTER Lot3), déjà utilisé par `/resultats/charges` | pas de détail par catégorie (liste des charges de la catégorie) | `comptabilite_axes_service.categories/categorie_detail`, routes `/resultats/categories(+/{id})` |
| Axe prestataire | `menages.fournisseur_id_opaque` + `cout_prevu`/`cout_reel` (migration `0019`) — même référentiel Fournisseurs que l'axe comptable, **vue opérationnelle différente** | aucun service ne l'agrégeait comme axe autonome | `comptabilite_axes_service.prestataires/prestataire_detail`, routes `/resultats/prestataires(+/{id})` |
| Axe plateforme | `canal_id` existe **uniquement** côté réservations « hors Hostaway » (`saisie_hh_reader.py`) ; les réservations Hostaway (l'écrasante majorité) n'exposent aucun canal résolu au niveau applicatif | source non représentative du portefeuille | **`NON_DISPONIBLE` assumé**, raison explicite affichée |
| Axe activité | `type_flux_id` (Lot9/Lot10) — classification technique fine (~20 codes), jamais organisée en taxonomie « hébergement/ménage/frais/services » dans `DECISIONS_METIER.md` | construire le regroupement serait inventer une taxonomie arbitraire | **`NON_DISPONIBLE` assumé**, raison explicite affichée |

## Fournisseur ≠ Prestataire — même tiers possible, jamais confondus

Un `fournisseur_id_opaque` peut porter les deux rôles : `/resultats/fournisseurs/{id}` montre la
vue **comptable** (dette, factures, règlements, solde 401000) ; `/resultats/prestataires/{id}`
montre la vue **opérationnelle ménage** (nombre de ménages, coût prévu/réel, écart, logements
couverts). Chaque fiche prestataire porte un lien explicite vers la fiche fournisseur du même
tiers — jamais un montant additionné entre les deux vues.

## Routes livrées (Bloc 4)

`/resultats/plateformes`, `/resultats/plateformes/{id}` (NON_DISPONIBLE, raison affichée),
`/resultats/fournisseurs/{id}` (nouveau — la liste existait déjà), `/resultats/prestataires`,
`/resultats/prestataires/{id}`, `/resultats/categories`, `/resultats/categories/{id}`,
`/resultats/activites`, `/resultats/activites/{id}` (NON_DISPONIBLE, raison affichée).

Toutes utilisent `comptabilite_axes_service.py` (nouveau) — aucun second calcul : fournisseur et
catégorie relisent des services/lecteurs déjà réels, prestataire agrège directement `menages`
(source déjà réelle, jamais un calcul inventé).

## Ce qui n'est pas fait, honnêtement

- Plateforme et activité restent `NON_DISPONIBLE` — pas un report, une décision assumée : les
  construire exigerait soit d'ignorer que la source est non représentative (plateforme), soit
  d'inventer une taxonomie (activité).
- Pagination/filtres avancés par axe : les listes actuelles ne paginent pas (volumes actuels
  faibles, cf. jeu de recette) — à ajouter si le volume réel le justifie.

## Drill-down (Bloc 5, 2026-07-31)

L'écriture comptable affichait son origine (`FACTURE`/`REGLEMENT`/`LOT12_PROPRIETAIRE_MOIS`/
`RAPPROCHEMENT`) en texte brut — la chaîne axe → mesure → écriture s'arrêtait là. Ajout de liens
ciblés par `origine_type` vers l'objet réel (facture, règlement, fiche propriétaire, rapprochement)
— aucun lien ajouté là où aucune route de détail n'existe (ex. `CHARGE`/`FACTURE_LIGNE` au niveau
ventilation, laissés en texte). Fiche prestataire : `menage_id_opaque` rendu cliquable vers
`/menages/cycle/{id}`. Aucune 404 sur objet existant ; 404 propre confirmée sur identifiant inconnu
(écriture, ménage). 6 tests dédiés (`test_resultats_drilldown.py`).

## Exports par axe (Bloc 6, 2026-07-31)

CSV livrés : dashboard (global par vision), logement (grain `PAR_MOIS_LOGEMENT`, existant),
propriétaire, fournisseur, prestataire, catégorie, réconciliation. Plateforme et activité n'ont pas
d'export — cohérent avec `NON_DISPONIBLE` : aucune donnée à exporter. Aucun chemin absolu, aucun
identifiant SQLite brut, aucune donnée bancaire dans ces exports (seuls les axes analytiques, pas le
rapprochement bancaire lui-même). 8 tests dédiés (`test_resultats_exports_axes.py`).

## Tests

`test_comptabilite_axes.py` (13), `test_resultats_axes_routes.py` (10),
`test_resultats_drilldown.py` (6), `test_resultats_exports_axes.py` (8) — 37 tests. Suite ciblée
Comptabilité+Résultats+Axes rejouée : 93 passés, 0 échec (dernier sous-ensemble contrôlé).

## Recette navigateur réelle (Bloc 7, 2026-07-31) — anomalie trouvée et corrigée

`build_data_recette.py` rejoué, serveur recette lancé (port 8020, `PROJECT_ROOT` **et**
`APP_DATA_DIR` sur `data_recette`, tous les flags d'écriture réelle activés), pipeline aval complet
lancé pour 2026-06 via `/calculs` (6/6 lots OK, 43,8 s) — jeu de données neuf, aucune sortie
préexistante.

**Anomalie réelle trouvée en navigateur, absente des tests unitaires** : la réconciliation
**B — Lot10 ↔ Analytique** comparait `Lot10 GLOBAL` (un total sur tout le jeu de données, sans
grain mensuel) à l'Analytique **filtrée sur le mois sélectionné** — un écart artificiel de
10 035,00 € apparaissait dès qu'un mois était choisi (`A_CONTROLER` alors que tout est cohérent).
Exactement le type d'erreur que le brief interdit : comparer des grains incompatibles. Les tests
unitaires ne l'avaient jamais vu car leur fixture ne portait qu'un seul mois (global = mensuel par
coïncidence). Corrigé dans `app/routes/resultats.py` (route HTML et export CSV) : B ignore
désormais le filtre mois — elle est, par construction, à l'échelle du jeu de données complet,
comme `H` (dont elle est l'alias). Note explicative ajoutée dans le template. Test de non-régression
avec fixture à deux mois (`test_reconciliation_b_reste_ok_quel_que_soit_le_mois_filtre`).

Vérifié en suite (navigateur réel, pas seulement TestClient) : dashboard, 3 visions, logements
(liste + détail + drill-down écritures), propriétaires (liste + détail, non-confusion résultat
conciergerie/net propriétaire), plateformes (`NON_DISPONIBLE`), fournisseurs, prestataires
(`NON_DISPONIBLE` — aucun ménage produit par ce pipeline), catégories (liste + détail réel),
activités (`NON_DISPONIBLE`), ménages, comptabilité (vide — aucune écriture générée par ce
pipeline, cohérent), 8 réconciliations, cumul (13 356,10 € sur 6 mois = GLOBAL exact), tous les
exports CSV (contenu réel, aucun chemin absolu). Redémarrage du serveur : persistance confirmée
(valeurs identiques). Pipeline relancé une seconde fois sur le même mois : sorties sauvegardées
avant écrasement, second run **idempotent** (tous les indicateurs de comparaison à écart 0,00),
totaux `/resultats` et export dashboard identiques après la seconde exécution — **aucun double
comptage**.

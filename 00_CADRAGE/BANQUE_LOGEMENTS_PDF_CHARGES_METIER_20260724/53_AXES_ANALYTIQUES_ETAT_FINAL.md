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
- Export CSV par axe : cf. `51`/`52` pour l'export dashboard existant ; les nouveaux axes n'ont pas
  reçu leur propre bouton d'export dédié ce tour (l'export générique reste disponible pour
  logement/mois/vision).

## Tests

`test_comptabilite_axes.py` (13), `test_resultats_axes_routes.py` (10) — 23 tests. Suite ciblée
Comptabilité+Résultats+Axes rejouée : 246 passés, 0 échec.

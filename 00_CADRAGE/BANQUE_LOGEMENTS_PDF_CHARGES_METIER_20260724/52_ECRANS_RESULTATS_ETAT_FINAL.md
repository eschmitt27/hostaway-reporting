# 52 — Écrans Résultats : état final (Phase 3)

Suite de `51` (Phase 2 — moteur analytique). Troisième et dernière phase de la mission
Analytique/Résultats.

## Routes livrées

| Route | Source | État |
|---|---|---|
| `/resultats` | `mesures_globales`, `mesures_par_logement`, `mesures_cumulees`, `comptabilite_periodes_service` | dashboard, filtres mois/vision, comparaison mois précédent + cumul |
| `/resultats/mensuel` | `mesures_par_logement`/`mesures_par_proprietaire` | tableau mensuel par logement et par propriétaire |
| `/resultats/cumule` | `mesures_cumulees` | cumul sur les mois réellement disponibles, `nb_mois_couverts` toujours affiché (jamais présenté comme un exercice complet à tort) |
| `/resultats/logements` | `mesures_par_logement` | liste, lien vers le détail |
| `/resultats/logements/{id}` | `fiche_logement` + `drill_down_logement` | toutes visions disponibles pour ce logement + écritures qui le portent (Phase 1/2) |
| `/resultats/proprietaires` | `mesures_par_proprietaire` | liste, lien vers le détail |
| `/resultats/proprietaires/{id}` | `fiche_proprietaire` + auxiliaire (`49`) | **distingue explicitement** résultat conciergerie (Lot10) et net propriétaire (auxiliaire 411000 / Lot12), jamais additionnés |
| `/resultats/plateformes` | — | **NON_DISPONIBLE assumé** : aucune dimension plateforme peuplée, ni Lot10 ni écritures ; construire l'axe demanderait d'arbitrer où la plateforme se rattache — non improvisé |
| `/resultats/fournisseurs` | `comptabilite_auxiliaires_service` (`49`) | réutilise l'auxiliaire fournisseur existant, aucun second calcul |
| `/resultats/charges` | `charges_reader.read_charges()` | agrégation par `categorie_charge_id`, lecture seule MASTER Lot3 |
| `/resultats/menages` | `menages_cycle_service.lister()` | comptage par type, lien vers le module Ménages existant pour le détail |
| `/resultats/comptabilite` | `comptabilite_ecritures_service` + `comptabilite_periodes_service` | totaux par journal, statut de la période |
| `/resultats/reconciliation` | `comptabilite_reconciliations_service` (`51`) | les 8 réconciliations, mois filtrable |
| `/resultats/lignes/{ecriture_id_opaque}` | — | redirige vers `/comptabilite/ecritures/{id}` — pas un second écran dupliqué, la fiche écriture porte déjà lignes + ventilation |
| `/resultats/export.csv` | `mesures_par_logement` | export CSV filtré mois/vision, aucune PII, aucun chemin absolu |

Aucun calendrier construit (instruction explicite) : le sélecteur de mois liste
`ana.mois_disponibles()` — les mois réellement présents dans Lot10, jamais une plage inventée.

## NON_DISPONIBLE plutôt que zéro

Chaque écran affiche `NON_DISPONIBLE` (jamais un tableau de zéros silencieux) quand :
- `MASTER_CALC_Resultats.xlsx` est absent/vide (dashboard, mensuel, cumulé, logements,
  propriétaires, réconciliations B/C/H) ;
- aucune donnée Lot12 n'existe pour le mois (réconciliation G) ;
- aucun ménage lié à une charge n'existe (réconciliation F) ;
- la dimension plateforme n'existe pas du tout (page dédiée).

Vérifié par recette navigateur réelle sur `data_recette` (qui ne construit pas de sorties Lot10
fictives) : le dashboard affiche `NON_DISPONIBLE` partout où c'est honnête, sans jamais fabriquer
un montant.

## Recette

**Automatisée** (`test_resultats_routes.py`, 18 tests, fixtures Excel isolées + fixtures SQLite) :
dashboard avec/sans source, mensuel, cumulé (avec/sans source), logements (liste + détail + détail
inconnu), propriétaires (liste + détail avec distinction résultat/net), plateformes
(NON_DISPONIBLE), fournisseurs (réel via écriture générée), charges, ménages, comptabilité (réel
via écriture ACHATS), réconciliation, redirection ligne→écriture, export CSV.

**Navigateur réel** (Chrome, port 8092, `data_recette` avec `PROJECT_ROOT` isolé, cf. leçon
`JOURNAL_ANOMALIES.md` 2026-07-29) :
1. Dashboard sur `data_recette` (sans sorties Lot10 fictives) → `NON_DISPONIBLE` partout, aucun zéro
   fabriqué.
2. Facture fictive `FAC-B66515E38CA7` (déjà seedée par `build_data_recette.py`) → génération réelle
   de l'écriture ACHATS (`ECR-3E463AAEFDF3`) → validation → `/resultats/fournisseurs` affiche
   désormais ce fournisseur avec sa dette → `/resultats/comptabilite` affiche le journal ACHATS.
3. `/resultats/charges` : 5 catégories réelles (fictives) agrégées depuis le MASTER Lot3 de
   `data_recette` (CHG_008 1050,00 € / 3 lignes, etc.).
4. `/resultats/reconciliation` : les 8 lignes rendues, D (Banque↔BANQUE) `OK` (0=0, aucun mouvement
   sur ce jeu), E (Factures↔auxiliaires) montre le montant gauche réel (864,00 €, total des soldes
   ouverts) avec droit `NON_DISPONIBLE` en mode agrégé (sans fournisseur précis) — comportement
   attendu, pas un bug.
5. **Persistance** : redémarrage du serveur (port 8093) → `GET /comptabilite/ecritures/ECR-
   3E463AAEFDF3` renvoie toujours `VALIDEE`.
6. Nettoyage : `data_recette/app_data/app.db` régénéré après la recette (idempotent,
   `build_data_recette.py`), aucune trace de l'écriture de test dans l'état livré.

## Ce qui n'est pas fait, honnêtement

- Comparaisons « avant/après recalcul » et « réel contre comptable » au niveau dashboard : la vision
  est sélectionnable (REEL/COMPTABLE/HORS_COMPTA) et le tableau GLOBAL les affiche côte à côte, mais
  aucun écran dédié de delta chiffré entre deux visions n'a été construit séparément — l'information
  est présente, pas doublement calculée dans un nouvel écran.
- Export XLSX et Power BI : seul le CSV a été construit (§20 du brief l'autorise explicitement en
  premier ; XLSX/Power BI non entrepris faute de besoin exprimé et de contrat Lot13 à respecter
  pour un nouvel export).
- Plateformes, réservations, prestataires comme axes : non peuplés (Phase 1/2), donc pas d'écran
  autre que le constat honnête `/resultats/plateformes`.

## Tests

18 tests (`test_resultats_routes.py`). Suite ciblée Comptabilité + Résultats rejouée : 184 passés,
0 échec.

## Critère de fin — Analytique et Résultats

Les deux critères de fin du brief sont couverts pour le périmètre livré : mappings branchés,
dimensions peuplées (logement/propriétaire), mesures, réconciliations (7/8, la 8ᵉ assumée non
disponible), drill-down jusqu'à l'écriture, écrans avec filtres/comparaisons/export/explicabilité,
recette navigateur avec persistance, aucune anomalie bloquante inexpliquée. Restent hors périmètre,
documentés : axes plateforme/fournisseur/prestataire/catégorie/activité non peuplés, réconciliation
Lot9↔Lot10 réelle non construite, export XLSX/Power BI non entrepris.

**Aucun pourcentage de projet supérieur à 85 % n'est annoncé.** Voir `HANDOFF_CANONIQUE.md` et
`48_ROADMAP_RESTANTE_PROJET.md` pour l'estimation à jour et la suite réelle du projet.

# 51 — Moteur analytique et réconciliations (Phase 2)

Suite de `50` (Phase 1 — mappings et ventilation). Deuxième phase de la mission Analytique/Résultats.

## Principe tenu : aucun recalcul, une couche de lecture

`comptabilite_analytique_service.py` ne fait QUE lire `MASTER_CALC_Resultats.xlsx`
(`PAR_MOIS_LOGEMENT`, `PAR_MOIS_PROPRIETAIRE`, `GLOBAL`) via deux nouvelles fonctions du reader
existant `proprietaires_reglements_reader.py` (`resultats_par_logement()`, `resultats_global()` —
additives, aucune fonction existante modifiée). Aucune donnée n'est dupliquée en base : pas de
nouvelle table SQLite "grain analytique" qui recopierait ce que Lot10 a déjà calculé — le risque de
dérive entre une copie et sa source aurait été pire que l'absence de copie. Le grain EST celui que
Lot10 fournit (mois × logement/propriétaire × vision), interrogé à la demande.

Côté Comptabilité, le grain existe déjà depuis la Phase 1 : `ecriture_lignes` (dimensions) +
`ecriture_ligne_ventilation` (méthode/statut/mapping). Aucune nouvelle table nécessaire non plus.

## Mesures et axes

`mesures_globales()` — une entrée par vision (REEL/COMPTABLE/HORS_COMPTA), incluant le
`commentaire_hc` où Lot10 a DÉJÀ vérifié REEL=COMPTABLE+HC (jamais revérifié ici, juste affiché).
`mesures_par_logement(mois, vision)` / `mesures_par_proprietaire(mois, vision)` — filtre sur le
grain Lot10 tel quel. `fiche_logement(id, mois)` / `fiche_proprietaire(id, mois)` — regroupe les
visions disponibles pour UNE dimension, base du drill-down. `drill_down_logement(id, mois)` —
descend jusqu'aux écritures dont la ventilation (Phase 1) porte ce logement, donc jusqu'à la fiche
écriture existante (`/comptabilite/ecritures/{id}`), donc jusqu'à la pièce.

Axes couverts : mois, logement, propriétaire, vision, journal/compte comptable (déjà filtrable côté
Comptabilité existante). **Non couverts, honnêtement** : plateforme, réservation, fournisseur,
prestataire, catégorie, activité — aucune de ces dimensions n'est peuplée sur les écritures
(seules `logement_id`/`proprietaire_id` le sont, Phase 1) ni présente dans les sorties Lot10
consultées ici ; les inventer serait improviser une ventilation non documentée.

## Réconciliations — `comptabilite_reconciliations_service.py`

8 fonctions, chacune renvoie `{statut, libelle_gauche, libelle_droit, montant_gauche, montant_droit,
ecart, tolerance, detail}` — jamais une exception, jamais un statut inventé hors de la liste
`OK|ECART_TOLERE|A_CONTROLER|BLOQUANT|NON_DISPONIBLE`.

| # | Réconciliation | État | Détail |
|---|---|---|---|
| A | Lot9 ↔ Lot10 | **NON_DISPONIBLE, assumé** | aucun lecteur Lot9 au niveau applicatif ; le contrôle existe déjà dans le moteur (`CTR-LOT10-*`) — le dupliquer recalculerait ce que Lot10 contrôle déjà |
| B | Lot10 ↔ Analytique | FAIT | par construction (l'Analytique lit Lot10 directement), vérifie que la somme par logement égale le total GLOBAL — défense en profondeur |
| C | Analytique ↔ Comptabilité | FAIT | compare le résultat Lot10 (vision COMPTABLE) à VENTES(crédit)−ACHATS(débit) déjà généré/validé ; un écart est **attendu** tant que toutes les factures n'ont pas leur écriture — classé `A_CONTROLER`, jamais `BLOQUANT` |
| D | Banque ↔ journal BANQUE | FAIT | rapprochements confirmés vs écritures BANQUE ; détecte les rapprochements confirmés SANS écriture générée (objets manquants listés) |
| E | Factures ↔ auxiliaires | FAIT | solde restant des factures ouvertes d'un fournisseur vs solde de son auxiliaire (401000) |
| F | Ménages ↔ charges | FAIT | ménages liés à une charge (`menages.charge_id`) vs charges effectivement retrouvées dans le MASTER Lot3 — détecte un lien vers une charge disparue/absente |
| G | Commissions ↔ VENTES Lot12 | FAIT | somme `montant_du_conciergerie` (Lot12, adaptateur Phase précédente) vs somme des écritures VENTES générées pour le mois |
| H | Total analytique ↔ résultat global | FAIT | alias explicite de B, demandé comme point de contrôle final distinct par le brief |

## Explicabilité

`résultat (Lot10 GLOBAL) → mesure (mesures_par_logement/proprietaire) → dimension (logement_id/
proprietaire_id, Phase 1) → ligne analytique (ecriture_lignes + ecriture_ligne_ventilation) →
écriture (fiche existante) → objet métier (facture/charge, lien déjà tracé)`. La chaîne
mouvement bancaire est atteinte via l'écriture BANQUE → rapprochement → mouvement (page
`/comptabilite/rapprochement`, déjà livrée en `49`). Aucun montant agrégé n'est affiché sans un
chemin vers son détail — `drill_down_logement` et les fiches propriétaire/auxiliaire existantes
couvrent ce chemin pour les dimensions peuplées.

## Ce qui n'est pas fait, honnêtement

- Réconciliation Lot9↔Lot10 réelle : nécessiterait un lecteur Lot9 dédié, non construit (le moteur
  le fait déjà, cf. `CTR-LOT10-*`).
- Axes plateforme/réservation/fournisseur/prestataire/catégorie/activité : aucune dimension peuplée
  pour ces axes à ce stade (Phase 1 n'a peuplé que logement/propriétaire).
- Aucune table de grain persistée : décision assumée (éviter une seconde source de vérité qui
  pourrait diverger de Lot10 ou des écritures).

## Tests

`test_comptabilite_analytique.py` (8), `test_comptabilite_reconciliations.py` (12) — 20 tests,
fixtures Excel isolées (même patron que `test_ventes_lot12_adapter.py`), aucune source réelle
modifiée. Suite ciblée Comptabilité + Propriétaires rejouée : 144 passés (1 échec = le flake
ordre-dépendant déjà documenté, reproduit à l'identique, sans lien avec ce tour).

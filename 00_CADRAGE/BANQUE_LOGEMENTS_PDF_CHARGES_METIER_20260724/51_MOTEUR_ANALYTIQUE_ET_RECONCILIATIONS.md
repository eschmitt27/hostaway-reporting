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
| A | Lot9 ↔ Lot10 | **FAIT** (2026-08-01, cf. suite ci-dessous) | lecteur `lot9_flux_reader.py` (`MASTER_CALC_Flux.xlsx`, lecture seule) ; compare la somme signée des flux Lot9 filtrés par `inclure_resultat_<vision>` à `PAR_MOIS_LOGEMENT` de Lot10, même règle de filtrage que `lot10_calculer_resultats.build_resultats` documente elle-même — un audit, pas un second moteur. `NON_DISPONIBLE` uniquement si `MASTER_CALC_Flux.xlsx` n'existe pas (Lot9 pas encore exécuté) |
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

## Suite (2026-08-01) — réconciliation A (Lot9 ↔ Lot10) fermée, 8/8

Nouveau lecteur `app/readers/lot9_flux_reader.py` (`MASTER_CALC_Flux.xlsx`, onglet `MASTER`,
lecture seule) + `cfg.MASTER_CALC_FLUX` (`Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx`, additif).

Grain de comparaison : `(mois, logement_id)`. Le côté Lot9 somme les flux signés par `sens`
(`PRODUIT` = +montant, `CHARGE` = −montant, `NEUTRALISATION` exclue — exactement la même
sémantique que documente `lot10_calculer_resultats.build_resultats`), filtrés par
`inclure_resultat_<vision>='OUI'` (même colonne, même nom que celle que Lot10 lit). Le côté Lot10
est `PAR_MOIS_LOGEMENT` pour cette vision. `logement_id` vide côté Lot9 est rattaché à
`GLOBAL_NON_AFFECTE` — **exactement** la sentinelle que `lot10_calculer_resultats.SENTINEL_GLOBAL`
utilise, pour comparer des grains réellement compatibles (une divergence de convention aurait
produit un faux écart, pas une vraie anomalie).

Détection : clés présentes côté Lot9 sans équivalent Lot10 (`cle_sans_lot10`), clés Lot10 sans
flux Lot9 correspondant (`cle_sans_lot9`), doublons de `ROW_HASH` côté Lot9 (`doublon_lot9` —
exclus de la somme mais toujours signalés, jamais masqués même si les totaux coïncident par
ailleurs). Tolérance 0,01 €, `NON_DISPONIBLE` uniquement si `MASTER_CALC_Flux.xlsx` n'existe pas
(Lot9 pas encore exécuté sur ce jeu).

Ne recalcule jamais Lot10 (aucune agrégation propre inventée — la même règle que Lot10 documente
lui-même) et ne modifie jamais Lot9 (lecture seule stricte).

11 tests dédiés (`test_comptabilite_reconciliation_lot9_lot10.py`) : égalité exacte, écart toléré,
écart supérieur, source absente, flux sans Lot10, résultat sans Lot9, doublon, NEUTRALISATION
exclue, mauvaise vision, mauvaise période, sentinelle logement vide.

**Les 8 réconciliations sont maintenant toutes implémentées** (aucune n'est plus assumée
`NON_DISPONIBLE` par construction — `NON_DISPONIBLE` ne signifie plus que la source réelle manque
sur le jeu consulté, jamais qu'elle n'a jamais été construite).

## Axes analytiques restants

Plateforme/fournisseur/prestataire/catégorie/activité : toujours non peuplés à ce stade (aucune
source fiable identifiée pour plateforme/prestataire/activité sans improviser ; fournisseur et
catégorie ont une source réelle mais ne portent pas encore de service/écran dédié — cf. suite dans
`53_AXES_ANALYTIQUES_ETAT_FINAL.md` si construits).

## Tests

`test_comptabilite_analytique.py` (8+5), `test_comptabilite_reconciliations.py` (12),
`test_comptabilite_reconciliation_lot9_lot10.py` (11) — 36 tests, fixtures Excel isolées (même
patron que `test_ventes_lot12_adapter.py`), aucune source réelle modifiée. Suite ciblée
Comptabilité + Résultats rejouée : 195 passés, 0 échec.

### Historique (avant fermeture de la réconciliation A)

Suite ciblée Comptabilité + Propriétaires rejouée : 144 passés (1 échec = le flake
ordre-dépendant déjà documenté, reproduit à l'identique, sans lien avec ce tour).

# 75 — Contrat trésorerie propriétaires (MOUVEMENT_TRESORERIE_PROPRIETAIRE)

Décision produit imposée le 2026-08-03. Objet distinct de Lot5 (acompte de réservation hors
Hostaway, inchangé, non renommé, jamais alimenté par ce nouvel objet).

## Ce que représente l'objet

Un mouvement financier entre la société et un propriétaire (acompte, remboursement,
régularisation, compensation, avance, restitution). Jamais une réservation, une charge, une
facture, une écriture bancaire, ou une écriture comptable.

## Modèle (migration `0025_tresorerie_proprietaires.sql`)

Deux tables, style identique aux journaux déclaratifs déjà en place (migrations 0014/0015/0023) :
`mouvements_tresorerie_proprietaires` (état courant, version optimiste, aucune suppression
physique) et `mouvements_tresorerie_proprietaires_evenements` (historique append-only).

Valeurs fermées : `sens` (`PROPRIETAIRE_VERS_SOCIETE`/`SOCIETE_VERS_PROPRIETAIRE`), `nature` (7
valeurs), `statut` (`BROUILLON`/`A_CONTROLER`/`VALIDE`/`ANNULE`). Un mouvement bancaire ne
détermine jamais automatiquement la nature.

## Service (`app/services/proprietaires_tresorerie_service.py`)

`creer` (BROUILLON), `previsualiser`, `modifier_brouillon` (BROUILLON uniquement),
`valider` (→ VALIDE), `annuler` (jamais une suppression), `charger`, `lister`, `historique`,
`solde` (par propriétaire, informatif), `montant_rapproche`/`reste_a_rapprocher` (délèguent à
`banques_rapprochement_service`, aucun calcul dupliqué), `objets_rapprochables` (VALIDE + reste >
0, jamais un BROUILLON ni un ANNULE).

20 tests (`tests/test_proprietaires_tresorerie_service.py`) : cycle de vie complet, natures/sens
fermés, propriétaire inconnu refusé, suppression d'un mouvement validé interdite, historique
append-only, solde, reste à rapprocher après un rapprochement partiel.

## Intégration au moteur de rapprochement existant

**Aucun second moteur créé.** `app/services/banques_candidats_service._reversements_proprietaires()`
génère des candidats `REVERSEMENT_PROPRIETAIRE` (type déjà déclaré migration 0015) à partir des
mouvements `VALIDE` avec un reste à rapprocher, filtrés par cohérence de sens
(`CREDIT` bancaire ↔ `PROPRIETAIRE_VERS_SOCIETE`, `DEBIT` bancaire ↔ `SOCIETE_VERS_PROPRIETAIRE`).
Le partiel et le multiple sont déjà gérés par `banques_rapprochement_service` (migration 0015,
somme des montants rapprochés actifs) — réutilisé tel quel, aucune duplication.

6 tests (`tests/test_banques_candidats_service.py`) : candidat créé après validation, brouillon
jamais candidat, filtre de sens des deux côtés, reste à rapprocher réduit après un rapprochement
partiel, disparition une fois soldé intégralement.

## Ce qui N'EST PAS fait par cette mission (scope explicite, pas un oubli)

- **Routes et écrans** (`GET/POST /proprietaires/{id}/tresorerie*`) : non créés. Le service est
  prêt à être branché, mais aucune route/template n'a été écrite ni testée ce tour.
- **Rapprochement groupé (section 9 du brief)** : `banques_rapprochement_service.proposer()` ne
  score qu'un objet à la fois (référence/montant/date), jamais une somme de plusieurs objets pour
  un seul mouvement bancaire. Un algorithme de recherche bornée (sous-ensemble, tolérance 0,01 €,
  fenêtre de date, limite combinatoire) reste à concevoir et tester — non construit ce tour, risque
  combinatoire réel si fait sans soin.
- **File A_ENVOYER_IA (83 mouvements)** : non traitée ce tour. Aucune route, aucun écran.
- **Statut "source Airbnb absente" affiché dans l'application** : non ajouté ce tour.
- **Suite complète (`2284 passés/75 ignorés/1 échec`) et pipeline Lot8a→Lot13** : non rejoués dans
  leur intégralité ce tour — seule la régression ciblée (`banque or proprietaire or migration`,
  544 passés/29 ignorés/0 échec) a été faite.
- **Recette navigateur** : non effectuée.

Ces éléments nécessitent un temps de conception/test supplémentaire que je n'ai pas voulu bâcler —
mieux vaut un socle testé et honnête qu'une UI non vérifiée présentée comme terminée.

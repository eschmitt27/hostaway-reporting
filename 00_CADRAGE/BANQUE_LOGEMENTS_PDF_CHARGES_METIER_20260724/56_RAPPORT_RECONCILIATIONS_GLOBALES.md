# 56 — Réconciliations globales sur copies de données réelles

Exécutées via `/resultats/reconciliation`, sur les copies (`SOURCES_COPIEES` + copie de l'`app.db`
réel), mois 2026-06, tolérance 0,01€ sauf mention contraire. Les mêmes 8 réconciliations que sur le
jeu fictif (cf. `51`, `53`) — aucun second moteur, relecture des services déjà livrés.

| Réconciliation | Gauche | Droit | Écart | Statut | Commentaire |
|---|---:|---:|---:|---|---|
| A — Lot9 ↔ Lot10 | 11 856,22 | 11 856,22 | 0,00 | **OK** | Confirme que les sorties Lot9/Lot10 réelles (2026-07-24) sont mutuellement cohérentes |
| B — Lot10 ↔ Analytique | 291 779,67 | 291 779,67 | 0,00 | **OK** | Sur 24 mois réels (2025-01→2027-02) — confirme en conditions réelles le correctif du tour précédent (grains incompatibles) |
| C — Analytique ↔ Comptabilité | 281 255,51 | 0,00 | 281 255,51 | A_CONTROLER | Attendu : 0 écriture VENTES/ACHATS générée en réel (module jamais exercé en réel) |
| D — Banque ↔ journal BANQUE | 0,00 | 0,00 | 0,00 | **OK** | Cohérent, aucun rapprochement réel n'existe |
| E — Factures ↔ auxiliaires | 0,00 | — | — | NON_DISPONIBLE | Aucune facture réelle enregistrée dans l'app |
| F — Ménages ↔ charges | — | — | — | NON_DISPONIBLE | Aucun ménage réel enregistré dans l'app |
| G — Commissions ↔ VENTES | 4 231,90 | 0,00 | 4 231,90 | A_CONTROLER | Détail : 10 propriétaires attendus (`PROP_0001`…`PROP_0012`, ids opaques uniquement) sans écriture VENTES générée — cohérent avec C |
| H — Total analytique ↔ résultat global | 291 779,67 | 291 779,67 | 0,00 | **OK** | Identique à B (alias), confirmé par construction |

## Invariant REEL = COMPTABLE + HORS_COMPTA

Vérifié sur les données réelles globales (feuille `GLOBAL`, tous mois confondus) :
`281 255,51 (COMPTABLE) + 10 524,16 (HORS_COMPTA) = 291 779,67 (REEL)`. Écart 0,00€. Commentaire
Lot10 natif : *« REEL=COMPTABLE+HC vérifié (écart=0.00 EUR) »* — confirmé, pas recalculé par
l'application.

## Constat global

4 réconciliations **OK** (A, B, D, H), 2 **A_CONTROLER** attendues (C, G — faute d'écritures
Comptabilité réelles, pas une anomalie), 2 **NON_DISPONIBLE** attendues (E, F — factures/ménages
jamais enregistrés en réel). Aucune réconciliation **BLOQUANTE**, aucun écart inattendu au-delà de
la tolérance sur les axes réellement exercés.

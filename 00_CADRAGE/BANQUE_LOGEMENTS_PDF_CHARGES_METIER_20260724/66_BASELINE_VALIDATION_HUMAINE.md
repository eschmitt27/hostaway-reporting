# 66 — Baseline technique figée pour la validation humaine

Snapshot logique de l'état technique validé au 2026-08-02. Aucune donnée sensible copiée dans Git
— uniquement des hashes, compteurs et totaux, déjà publics dans les rapports `54`-`65`.

## Git

| Élément | Valeur |
|---|---|
| HEAD | `c65f891` (après le commit de sécurité de ce tour) |
| Master | `8b47807` (intact, jamais touché) |
| Branche | `feature/banque-logements-pdf-charges-metier` |
| Commits de cette série (depuis `7ea5a81`) | `6d80612` (tests lot4quater), `e5248fd` (docs), `c65f891` (fix sécurité bandeau + tests) |

## Sources réelles (88 fichiers de données, intégrité)

Baseline complète dans `_RECETTES_GLOBALES/RECETTE_GLOBALE_20260801_004232/HASHES/
BASELINE_SOURCES_REELLES_SHA256.txt` (hors Git). Re-vérifiée identique à chaque tour depuis le
2026-08-01 : **88/88 fichiers de données inchangés**. Seule exception, intentionnelle et
committée : `02_TRAVAIL/lot8a_banque_import.py` (code, pas une donnée).

## Migrations

24 migrations SQL appliquées (`schema_migrations`, 24 lignes). Testées sur une copie de l'`app.db`
réel ET sur une base vierge : `integrity_check` = `ok`, 0 violation de clé étrangère, idempotentes.

## Sorties pipeline de référence (mois 2026-06, après cycle Banque complet)

| Sortie | Valeur |
|---|---|
| REEL | 291 722,75 € |
| COMPTABLE | 281 198,59 € |
| HORS_COMPTA | 10 524,16 € |
| Identité REEL = COMPTABLE + HORS_COMPTA | vérifiée, écart 0,00 € |
| Lot9 — flux totaux | 1385 (1349 RES + 12 MEN + 24 BNQ + 0 CHG + 24 GPM — MEN a varié 12↔20 selon la copie M04, sans impact sur l'identité ci-dessus) |
| Lot10 — lignes `PAR_MOIS_LOGEMENT` (REEL) | 248 |
| Lot10 — lignes `PAR_MOIS_PROPRIETAIRE` (REEL) | 202 |
| Réservations `A_CONTROLER` exclues | 612 |
| Lot12 — préfactures | 261 (3132 lignes), 0 facture finale (attente validation humaine) |

## Banque (référence, cycle Lot8a→8b→8c complet)

| Indicateur | Valeur |
|---|---|
| Mouvements (Lot8a) | 541 |
| Format détecté | `RELEVE_CONSOLIDE` |
| Total débit / crédit | 51 744,37 € / 52 148,21 € |
| Doublon détecté (non masqué) | 1 |
| Statut contrôle (Lot8b) | 24 `VALIDE` / 517 `A_CONTROLER` |
| Statut classification (Lot8b) | 236 `CLASSE` / 222 `RAPPROCHEMENT_REQUIS` / 83 `A_ENVOYER_IA` |
| Niveau de risque (Lot8b) | 74 `ELEVE` / 215 `MOYEN` / 252 `FAIBLE` |
| Rapprochements Airbnb proposés (Lot8c) | 166, tous `EN_ATTENTE_EXPORT_AIRBNB` |
| Rapprochements propriétaires proposés (Lot8c) | 56, tous `EN_ATTENTE_SAISIE_ACOMPTE` |
| Confirmations automatiques | **0** |

## Réconciliations (via l'application, sur les sorties de référence)

| Réconciliation | Écart | Statut |
|---|---:|---|
| A — Lot9 ↔ Lot10 | 0,00 € | OK |
| B — Lot10 ↔ Analytique | 0,00 € | OK |
| C — Analytique ↔ Comptabilité | 281 198,59 € | A_CONTROLER (0 écriture réelle) |
| D — Banque ↔ journal BANQUE | 0,00 € | OK |
| E — Factures ↔ auxiliaires | — | NON_DISPONIBLE |
| F — Ménages ↔ charges | — | NON_DISPONIBLE |
| G — Commissions ↔ VENTES | 4 231,90 € | A_CONTROLER |
| H — Total analytique ↔ résultat global | 0,00 € | OK |

## Tests

| Campagne | Résultat |
|---|---|
| `tests/` racine (moteur, 268 fichiers-tests confondus avec cas) | 268 passés, 0 échec |
| `05_APPLICATION/tests/` (132 fichiers, 6 shards, `TEST_SHARDS_RECETTE_GLOBALE.txt`) | 2284 passés / 75 ignorés / 1 échec pré-existant (`test_appsec1_diagnostic`) |

## Paramètres de recette utilisés

`PROJECT_ROOT`/`APP_DATA_DIR` = environnement de copies dédié (hors Git) ; tous les flags
d'écriture réelle activés **uniquement sur la copie** (write-guard applicatif borné à
`RECETTE_ROOT`) ; réseau non sollicité (aucun appel Hostaway/Google Sheet/bancaire réel) ; mode
réel désactivé partout (`RECETTE_MODE=1` sur toute la session).

## Ce que cette baseline NE certifie PAS

- Aucune validation métier des règles de classification Banque (seed générique).
- Aucune validation du plan de comptes (toujours PROVISOIRE).
- Aucune exécution en mode réel, à aucun moment.
- Les 517 mouvements `A_CONTROLER` et les 222 rapprochements proposés restent **non tranchés** —
  cf. `68_MATRICE_ARBITRAGES_BANQUE.md`.

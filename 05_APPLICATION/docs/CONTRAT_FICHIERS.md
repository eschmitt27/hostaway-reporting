# CONTRAT_FICHIERS.md — Règles d'accès fichiers

## Principe

L'application ne modifie **jamais** les sources brutes, référentiels, sorties calculées ou exports.
Seule écriture autorisée : `SAISIE_*.xlsx` via écriture atomique + snapshot préalable (activée à partir des lots de saisie APP-2/APP-3).

## Chemins READ-ONLY (l'app ne les modifie jamais)

| Chemin | Type | Raison |
|---|---|---|
| `01_SOURCES_BRUTES/REF_Setup.xlsm` | Référentiel | Source de vérité principale |
| `01_SOURCES_BRUTES/REF_Cloture_Mensuelle.xlsx` | Référentiel clôture | Déclencheur D097 — écriture uniquement via lot clôture validé |
| `01_SOURCES_BRUTES/REF_Banque_Regles.xlsx` | Référentiel banque | Read-only |
| `01_SOURCES_BRUTES/BANQUE_LOT8_IMPORT/` | Import bancaire | Source brute |
| `02_TRAVAIL/**` | Moteur Python | L'app déclenche les scripts, ne les modifie pas |
| `02_TRAVAIL/*/MASTER_*.xlsx` | Sorties calculées | Jamais modifiées par l'app |
| `03_EXPORTS/PowerBI/*.csv` | Exports | Générés par le moteur uniquement |

## Chemin SAISIE (écriture atomique — INACTIF au Lot APP-0)

| Chemin | Activé à partir de |
|---|---|
| `01_SOURCES_BRUTES/SAISIE_ReservationsHorsHostaway.xlsx` | APP-2 |
| `01_SOURCES_BRUTES/SAISIE_Charges_Flux.xlsx` | APP-3 |
| `01_SOURCES_BRUTES/SAISIE_Acomptes.xlsx` | APP-3 |
| `01_SOURCES_BRUTES/SAISIE_AirCover.xlsx` | APP-3 |
| `01_SOURCES_BRUTES/SAISIE_ImputationsAirbnb.xlsx` | APP-3 |
| `01_SOURCES_BRUTES/SAISIE_Ajustements_PostCloture.xlsx` | APP-3 |

Écriture atomique = fichier temporaire → validation schéma → snapshot préalable → remplacement atomique.
Cellules texte commençant par `=` : préfixées par `'` (règle anti-formule).

## Chemin APP_DATA (seul endroit où l'app écrit librement)

`05_APPLICATION/data/` — app.db + snapshots/ + restore_workspace/ — gitignored.

## Invariant vérifié au boot

`/health` vérifie : `is_writable(REF_Setup.xlsm) == False`.

# CARTE_FLUX_DONNEES.md — Flux de données APP-0

## Types de données

| Type | Direction | Exemples |
|---|---|---|
| IMPORTÉ (lecture) | Excel/CSV → App | MASTER_RUN_Log.xlsx, PBI_Flux.csv, MASTER_CTRL_Coherence.xlsx |
| GÉNÉRÉ (par moteur) | Scripts Python → MASTER_* | MASTER_CALC_Flux, MASTER_FACT_Proprietaires, etc. |
| SAISI (écriture atomique) | App → SAISIE_*.xlsx | SAISIE_Charges_Flux, SAISIE_Acomptes… (inactif APP-0) |
| TRACÉ (journal) | App → SQLite (app.db) | audit_events, pipeline_runs, snapshots, screen_states |

## Flux lecture (actifs au Lot APP-0)

```
PBI_Flux.csv            → home.py (KPI lignes flux)
PBI_Controles_Ouverts.csv → home.py (KPI contrôles ouverts)
PBI_Referentiel_Logements.csv → home.py (KPI logements)
PBI_Referentiel_Proprietaires.csv → home.py (KPI propriétaires)
MASTER_RUN_Log.xlsx     → run_log_reader.py → sources_calculs (onglet Journaux)
```

## Flux écriture (Lot APP-0)

```
App → app.db (SQLite journal uniquement)
  audit_events    : toutes les actions utilisateur
  pipeline_runs   : dry-run simulés
  snapshots       : registre des copies horodatées
  screen_states   : état des écrans
```

## Invariant central

`SQLite → Excel` : **INTERDIT** (synchronisation bidirectionnelle interdite).
`App → MASTER_*` : **INTERDIT**.
`App → REF_Setup.xlsm` : **INTERDIT**.
`App → sources brutes` : **INTERDIT**.

La vérité de calcul reste exclusivement dans le moteur Python + Excel.

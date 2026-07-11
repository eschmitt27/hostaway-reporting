# DEF-1 (cache/retry Google Sheet) + DEF-2 (runbook) — 2026-06-19

## Fichiers modifiés / créés (suivis Git)
| Fichier | Type | Rôle |
|---|---|---|
| 02_TRAVAIL/lib_sheet_source.py | CRÉÉ | fetch retry(3)/timeout20s/attentes5-10s, validation, cache atomique, last_resolution.json, blocage SOURCE_SHEET_INDISPONIBLE |
| 02_TRAVAIL/lot6b_m04_menages_internes.py | MODIFIÉ | utilise lib + log si CACHE |
| 02_TRAVAIL/lot6f_cout_complet_menages.py | MODIFIÉ | utilise lib + log si CACHE |
| 02_TRAVAIL/lot11_controles_coherence.py | MODIFIÉ | lit last_resolution.json -> contrôle SOURCE_SHEET_CACHE_UTILISE (A_CONTROLER) si resolution_source=CACHE, déterministe |
| .gitignore | MODIFIÉ | + cache _cache_google_sheet/ |
| 00_CADRAGE/RUNBOOK_OPERATEUR.md | CRÉÉ | runbook opérateur 14 sections |

## Non versionnés
- 02_DONNEES_NORMALISEES/menages/_cache_google_sheet/ (suivi_menage.csv, .meta.json, last_resolution.json)
- 04_LOGS/

## Tests (worktree isolé, scripts modifiés copiés, zéro churn prod)
| Test | Attendu | Résultat |
|---|---|---|
| S1 réseau OK + chaîne complète | baseline + provenance RESEAU | REEL 291779.67 / COMPTABLE 281255.51 / HC 10524.16 ; Lot11 7 A_CONTROLER ; RESEAU | OK |
| S2 réseau KO + cache <=72h | bascule CACHE | resolution_source=CACHE | OK |
| S3 réseau KO + cache >72h | BLOQUANT | SOURCE_SHEET_INDISPONIBLE | OK |
| S4 réseau KO + pas de cache | BLOQUANT | SOURCE_SHEET_INDISPONIBLE | OK |
| S5 téléchargement échoué | cache préservé | sha inchangé | OK |
| lot11 si CACHE | 8 A_CONTROLER / 20 INFO, contrôle unique | 8 / 20, SOURCE_SHEET_CACHE_UTILISE x1 | OK |
| lot11  x2 (déterminisme) | pas de doublon | 1 seule ligne | OK |

## Baseline (réseau OK) — inchangée
REEL 291779.67 = COMPTABLE 281255.51 + HORS_COMPTA 10524.16 ; Lot11 0 BLOQUANT / 7 A_CONTROLER / 20 INFO.
En bascule CACHE : Lot11 = 0 / 8 / 20 (8e = SOURCE_SHEET_CACHE_UTILISE).

## Limites restantes
- Empêchement de clôture = procédural (runbook) + contrôle A_CONTROLER visible ; pas de verrou code (aucun script de clôture métier n'existe encore).
- Métadonnées .git/worktrees résiduelles (lock OneDrive) — inertes, hors repo.
- DEF-3 (extraction lot1/lot8/lot6c non rejouable sans prérequis) = normal, documenté dans le runbook.

## Verdict : DEF-1 et DEF-2 traités. Autonomie renforcée (résilience réseau + runbook).

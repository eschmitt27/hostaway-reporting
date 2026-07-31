# 58 — Rapport sécurité et confidentialité (recette globale, copies de données réelles)

## Pages et exports scannés

`/`, `/health`, `/health/diagnostic`, `/resultats`, `/resultats/logements`, `/resultats/
proprietaires`, `/resultats/categories`, `/resultats/fournisseurs`, `/resultats/reconciliation`,
`/resultats/export.csv`, `/resultats/dashboard/export.csv` — capturés en fichiers HTML/CSV bruts,
scannés par motif.

## Motifs recherchés (0 occurrence sur tous les fichiers scannés)

| Motif | Résultat |
|---|---|
| Chemins absolus (`C:\Users`, `OneDrive`, `AppData`) | 0 |
| Nom d'utilisateur Windows (`Ewan`) | 0 |
| Email (regex `[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`) | 0 |
| Téléphone (regex FR `0[1-9]([-. ]?[0-9]{2}){4}`) | 0 |
| IBAN / motif compte bancaire | 0 |
| Token / secret / password / api_key | 0 |

## Point de vigilance identifié (non bloquant)

`/health/diagnostic` expose des chemins **logiques** (`<PROJECT_ROOT>`, `<APP_DATA_DIR>`) — jamais
de chemin réel — conforme à `APP-SEC-1` déjà en place. Ce diagnostic reste désactivé par défaut
(`DIAGNOSTIC_DETAILS_ENABLED`), activé volontairement ce tour pour l'audit, jamais exposé en
production. Comportement déjà correct, aucune correction nécessaire.

Les identifiants propriétaires apparaissant dans la réconciliation G (`PROP_0001`…`PROP_0012`)
sont des **identifiants opaques**, pas des noms réels — conforme à la convention du projet.

## Écriture réelle impossible depuis la recette

Vérifié : tous les flags d'écriture réelle activés pointaient exclusivement vers
`RECETTE_ROOT=SOURCES_COPIEES` (le write-guard applicatif borne toute écriture à ce chemin). Aucune
tentative d'écriture hors de ce périmètre n'a été observée ni n'était possible par construction.
Aucune connexion réseau (bancaire, Google Sheet, Hostaway, email) n'a été déclenchée — le pipeline
lu ce tour (`charges`, tentative `aval`) n'appelle aucun de ces services externes.

## Verdict sécurité

**0 fuite** sur les pages et exports contrôlés. Rien à corriger.

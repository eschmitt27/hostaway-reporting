# 55 — Matrice des écarts de contrats sur données réelles (recette globale, copies)

Audit mené sur les copies (`SOURCES_COPIEES`), jamais sur le réel. Aucune correction appliquée aux
sources réelles — les actions proposées ci-dessous sont des recommandations, pas des exécutions.

| Source | Contrat attendu | Contrat réel | Écart | Sévérité | Action proposée |
|---|---|---|---|---|---|
| `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` | présent (lu par `lot9_construire_flux.py`, contrôle `CTR-9-001`) | dossier `Lot8_Banque/` **inexistant** dans le réel | source obligatoire absente | **BLOQUANT** (pour toute ré-exécution du pipeline aval) | Exécuter l'import Banque réel (Lot8) au moins une fois pour produire ce fichier, ou documenter explicitement que la chaîne aval réelle reste figée aux sorties de 2026-07-24 tant que Banque n'est pas alimentée |
| `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` (onglet `SAISIE`) | lignes de charges saisies | **0 ligne** | VIDE VALIDE, pas un défaut de contrat (colonnes toutes présentes, `README`/`REF_LOCALE`/`CONTROLES_SAISIE` intacts) | INFO | aucune — état réel attendu tant que la saisie manuelle des charges n'a pas commencé côté réel |
| `03_EXPORTS/PowerBI/PBI_Referentiel_Logements.csv` | présent (généré par `lot13`) | **absent** | `lot13` jamais exécuté en réel | MINEUR | sans objet tant que la chaîne aval n'est pas rejouée (bloquée par la ligne Banque ci-dessus) |
| `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx` (`PAR_MOIS_LOGEMENT`) | mois consécutifs | **2026-11 et 2027-01 absents** de la série 2025-01→2027-02 (22 mois présents sur 26 attendus) | deux trous dans la série réelle | MINEUR | à vérifier auprès du moteur/opérateur métier — peut être des mois réellement sans activité ou un oubli d'exécution ; ne pas fabriquer les lignes manquantes |
| `02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx` | régénéré à chaque run du pipeline aval | **daté 2026-07-24**, dernière exécution connue, non régénérable ce tour (bloqué par la ligne Banque) | fraîcheur non garantie par rapport à l'état actuel de `01_SOURCES_BRUTES` | MINEUR (mais la réconciliation A confirme sa cohérence interne avec Lot10 : écart 0,00€) | régénérer dès que la source Banque est disponible |
| `05_APPLICATION/data/app.db` (comptabilité) | écritures/factures pour tester les réconciliations C/E/F/G | **0 écriture, 0 facture, 0 ménage** enregistrés en réel | module Comptabilité/Factures/Ménages jamais exercé en réel (seulement en `data_recette` fictif) | INFO — cohérent avec l'état de projet documenté (mode réel jamais activé) | aucune, attendu |

## Résumé

1 écart **BLOQUANT** (source Banque manquante — bloque la ré-exécution complète du pipeline aval
sur données réelles), 2 écarts **MINEUR** (trou d'un mois dans la série réelle ; fraîcheur non
garantie de Lot9-13 faute de pouvoir les régénérer), 2 constats **INFO** (charges et comptabilité
réelles vides — état de projet connu, pas une anomalie de contrat).

Aucun de ces écarts n'a été corrigé sur le réel (interdit par la mission). Aucun n'a nécessité de
correction de code (le comportement applicatif face à chacun est déjà correct : `SOURCE_ABSENTE`/
`NON_DISPONIBLE`/`BLOQUANT` explicites, jamais un zéro fabriqué ni un faux succès).

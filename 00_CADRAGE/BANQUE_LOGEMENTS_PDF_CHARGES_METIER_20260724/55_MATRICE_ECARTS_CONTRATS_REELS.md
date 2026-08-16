# 55 — Matrice des écarts de contrats sur données réelles (recette globale, copies)

Audit mené sur les copies (`SOURCES_COPIEES`), jamais sur le réel. Aucune correction appliquée aux
sources réelles — les actions proposées ci-dessous sont des recommandations, pas des exécutions.

| Source | Contrat attendu | Contrat réel | Écart | Sévérité | Action proposée |
|---|---|---|---|---|---|
| `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` | présent (lu par `lot9_construire_flux.py`, contrôle `CTR-9-001`) | dossier `Lot8_Banque/` **inexistant** dans le réel — confirmé : c'est une **sortie** de `lot8a_banque_import.py`, dont la source brute d'entrée (`01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx`) n'existe pas non plus sur disque | source obligatoire absente, **remontée à la cause racine** : aucune source bancaire brute jamais fournie côté réel | **BLOQUANT** (pour toute ré-exécution du pipeline aval) | Déposer l'export Crédit Mutuel réel sous `01_SOURCES_BRUTES/Banque/`, exécuter `lot8a` — décision et geste humains, détail complet : `61_CONTRAT_SOURCE_BANQUE_LOT8.md` |
| `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` (onglet `SAISIE`) | lignes de charges saisies | **0 ligne** | VIDE VALIDE, pas un défaut de contrat (colonnes toutes présentes, `README`/`REF_LOCALE`/`CONTROLES_SAISIE` intacts) | INFO | aucune — état réel attendu tant que la saisie manuelle des charges n'a pas commencé côté réel |
| `03_EXPORTS/PowerBI/PBI_Referentiel_Logements.csv` | présent (généré par `lot13`) | **absent** | `lot13` jamais exécuté en réel | MINEUR | sans objet tant que la chaîne aval n'est pas rejouée (bloquée par la ligne Banque ci-dessus) |
| `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx` (`PAR_MOIS_LOGEMENT`) | mois consécutifs | **2026-11 et 2027-01 absents** — **cause identifiée** : la seule réservation de chacun de ces mois (`RES-2026-11-HA-001`, `RES-2027-01-HA-001`) est un placeholder `A_CONTROLER`/`DIRECT_SANS_SAISIE_HH` à montant 0, exclu de `VUE_FLUX` (Lot4quater) ; ce sont les seules lignes de leur mois donc le mois entier disparaît en aval | filtrage upstream cohérent, pas un bug — détail complet : `62_RAPPORT_MOIS_LOT10_MANQUANTS.md` | **RÉSOLU (expliqué)** — statut **VIDE_VALIDE/NON_APPLICABLE** | aucune correction de code ; compléter la saisie Hors-Hostaway pour `LOG_0015`/`PROP_0011` si l'activité doit apparaître |
| `02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx` | régénéré à chaque run du pipeline aval | **daté 2026-07-24**, dernière exécution connue, non régénérable ce tour (bloqué par la source Banque) | fraîcheur non garantie par rapport à l'état actuel de `01_SOURCES_BRUTES` | MINEUR (mais la réconciliation A confirme sa cohérence interne avec Lot10 : écart 0,00€) | régénérer dès que la source Banque est disponible |
| `05_APPLICATION/data/app.db` (comptabilité) | écritures/factures pour tester les réconciliations C/E/F/G | **0 écriture, 0 facture, 0 ménage** enregistrés en réel | module Comptabilité/Factures/Ménages jamais exercé en réel (seulement en `data_recette` fictif) | INFO — cohérent avec l'état de projet documenté (mode réel jamais activé) | aucune, attendu |

## Résumé

1 écart **BLOQUANT** (source Banque manquante à la racine — bloque la ré-exécution complète du
pipeline aval sur données réelles, cause et action précisées dans `61`), 1 écart **RÉSOLU
(expliqué)** ce tour (deux mois Lot10 manquants — filtrage upstream cohérent, détail `62`), 1 écart
**MINEUR** restant (fraîcheur de Lot9-13 non garantie faute de pouvoir régénérer), 2 constats
**INFO** (charges et comptabilité réelles vides — état de projet connu, pas une anomalie de
contrat).

Aucun de ces écarts n'a été corrigé sur le réel (interdit par la mission). Aucun n'a nécessité de
correction de code (le comportement applicatif face à chacun est déjà correct : `SOURCE_ABSENTE`/
`NON_DISPONIBLE`/`BLOQUANT` explicites, jamais un zéro fabriqué ni un faux succès).

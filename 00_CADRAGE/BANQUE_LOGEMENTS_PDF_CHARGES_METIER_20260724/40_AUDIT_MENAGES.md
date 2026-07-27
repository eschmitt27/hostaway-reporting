# 40 — Audit ciblé du module Ménages

Phase 3.1. Audit borné, préalable à toute construction.

## Conclusion d'entrée : il ne s'agit pas d'une construction, mais d'une extension

Le brief demande de « construire un module Ménages complet ». **Un module Ménages existe déjà**, et
il est substantiel :

| Existant | Détail |
|---|---|
| Services | `menages_service.py`, `menages_recalcul_service.py`, `menages_chaine_service.py` |
| Routes | 13 routes sous `/menages` |
| Écrans | `menages_list`, `menages_detail`, `menages_controler`, `menages_diagnostic`, `menages_recalculer`, `menages_run`, `menages_chaine` |
| Runner | `runners/menages_recalcul_runner.py` + stubs sous `runners/stubs_menages/` |
| Tests | 6 fichiers (`test_menages*.py`) |
| Export Power BI | `PBI_Menages_Cout_Complet`, `PBI_Menages_Rapprochement` (déjà déclarés dans lot13) |

Rien de tout cela ne doit être réécrit. La consigne « ne pas recréer un second moteur » s'applique
aussi au module applicatif.

## Ce que fait l'existant

L'existant est un module de **rapprochement et de reporting**, pas de gestion opérationnelle :

    stub Hostaway → lot6b (déclarations internes) → lot6c (ménages externes, factures PDF)
                  → lot6d (rapprochement) → lot6e (gain/perte) → lot6f (coût complet)
                  → lot11 (contrôles)

`menages_chaine_service` orchestre cette chaîne **sur copies**, dans un workspace isolé, avec
vérification sha256 avant/après des fichiers réels. `MODE_REEL` est bloqué par
`MENAGES_REAL_RECALC_ENABLED`.

L'écran `/menages` compare, par (mois, logement, intervenant) : ménages Hostaway comptés, déclarés
internes (M04), déclarés externes (factures), écart, coût standard / direct / complet, anomalies.

## Cartographie

| Objet | Source de vérité | Producteur | Consommateur | Écran actuel | Manque |
|---|---|---|---|---|---|
| Ménage Hostaway (tâche réalisée) | `MASTER_FACT_HA_CleaningTasks_Discovery` | lot6a / stub | lot6c, lot6d | `/menages` (comptage) | pas d'objet ménage unitaire |
| Déclaration interne | Google Sheet copiée → `M04_MENAGES_PowerQuery` | lot6b | lot6d, lot6f | `/menages` (comptage) | pas de saisie applicative |
| Ménage externe | Factures PDF prestataires | lot6c (`lib_menages_externes_pdf`) | lot6d, lot6f | `/menages` (comptage) | pas de lien vers la facture SQLite |
| Rapprochement | `MASTER_CTRL_Rapprochement_Menages` | lot6d | lot11, PBI | `/menages`, `/menages/a-controler` | — |
| Coût complet | `MASTER_CALC_CoutComplet_Menages` | lot6f | lot10, PBI | `/menages/{...}` | — |
| Coût unitaire interne | `REF_Setup` (taux horaire, coût fixe) | `lib_menage_costs` | lot6b, lot6f | — | pas d'écran de tarif |
| Prestataire | `REF_Intervenants` (REF_Setup) | REF | tous | — | **pas relié au référentiel Fournisseurs SQLite** |
| Facture / règlement / banque | SQLite (modules livrés) | app | — | `/factures` | **aucun lien ménage ↔ facture** |

## Écarts réels entre l'existant et la cible du brief

| Attendu | État | Nature de l'écart |
|---|---|---|
| Liste, filtres mois/logement/prestataire/type/statut | ⚠️ partiel | filtres existants ; **pas de filtre par statut** faute de statut |
| Ménages attendus vs réalisés | ✅ | comptage Hostaway vs déclarations |
| Cycle de vie `PLANIFIE → … → REGLE / ANNULE / LITIGE` | ⛔ **absent** | l'existant n'a **aucun statut opérationnel** : c'est un module de comptage, pas de suivi unitaire |
| Création d'un ménage hors Hostaway | ⛔ absent | aucune écriture applicative de ménage |
| Affectation d'un prestataire | ⛔ absent | idem |
| Correction / validation / annulation / remplacement | ⚠️ | `outrepasser` existe (décision humaine sur une anomalie), pas un cycle de vie |
| Rattachement réservation / facture / charge | ⛔ absent | le lien ménage ↔ facture SQLite n'existe pas |
| Consultation règlement / mouvement bancaire | ⛔ absent | dépend du point précédent |
| Prestataires qualifiés (interne/externe, période de tarif, logements autorisés) | ⛔ absent | `REF_Intervenants` (Excel) et `fournisseurs_referentiel_service` (SQLite) **coexistent sans lien** |
| Contrôles de la liste du brief (20 codes) | ⚠️ partiel | lot6d/lot11 en couvrent une partie côté moteur ; aucun catalogue applicatif comme pour Banque/Factures |

## Règles métier — ce qui est codé, et ce qui manque

Codé dans `lib_menage_costs.py` :

- `PIVOT_FIXED_COST = 2026-06-01` : bascule entre coût **horaire historique** et coût **fixe** ;
- `resolve_hourly_rate()` : taux horaire historisé, jamais le tarif courant appliqué rétroactivement ;
- `resolve_fixed_internal_cost()` : coût fixe avec priorité de candidats (intervenant, logement,
  type de logement).

**Écart de règle à arbitrer — ne rien inventer.** Le brief énonce « avant mai 2026 : heures × taux
horaire historique ». Le moteur pivote au **1er juin 2026**, pas au 1er mai. L'un des deux est faux ;
je ne tranche pas. Tant que ce n'est pas arbitré, aucun écran ne doit afficher une règle de tarif
qui contredirait le moteur.

**Règles du brief non retrouvées dans le code** :
- « ménage externe = prix de facture **+ quote-part des courses classées ménage** » ;
- « ménage interne = quote-part des courses ménage interne **+ cave 50 €** ».

La quote-part de courses existe côté lot6f (pools courses/consommables, aujourd'hui **vides**). La
règle « cave 50 € » n'apparaît nulle part dans `lib_menage_costs`. À arbitrer avant construction :
soit elle est portée par `REF_Setup` (coût fixe par logement), soit elle n'est pas implémentée.

## Anomalie de configuration relevée au passage

`MENAGES_REAL_RECALC_ENABLED = False` est **codé en dur** dans `config.py`, alors que tous les
autres verrous d'écriture ont été migrés vers le double verrou
`RECETTE_MODE and _env_flag("…")`. L'incohérence n'est pas dangereuse (False est le plus sûr), mais
elle empêche d'activer ce mode par variable d'environnement comme les autres, et fait mentir la
règle « double verrou partout ». À aligner.

## Ce qui a été fait après l'audit (phase 3, bloc 1)

Trois corrections isolées, chacune trouvée en exerçant et non en relisant.

### 1. Dernier verrou codé en dur, aligné

`MENAGES_REAL_RECALC_ENABLED = False` était la seule garde d'écriture encore écrite en dur. Elle
fait mentir la règle « double verrou partout » et interdisait l'activation par variable
d'environnement. Alignée sur `RECETTE_MODE and _env_flag(…)`. **Reste False par défaut ; le mode
réel n'est pas activé.**

### 2. Trou dans la garantie « jamais de faux succès »

Les lots Ménages étaient déclarés `sorties=()`. Or `sorties_ok = all(...) if sorties else True` :
sans sortie déclarée, **la garantie ne s'appliquait pas à ces lots**. Un lot6* sortant en code 0
sans rien produire aurait été annoncé SUCCES.

Sorties déclarées d'après `menages_chaine_service.SORTIES_CHAINE`, seule cartographie auditée. Un
test (`test_chaque_lot_menages_declare_ses_sorties`) empêche la régression.

### 3. `lot6c` manquait dans la chaîne du pilotage

Le pilotage déclarait `lot6b → lot6d → lot6e → lot6f`. `lot6c` (ménages externes) est pourtant une
étape du runner existant, et lot6d **consomme** sa sortie. Ajouté, avec la dépendance
`lot6d ← (lot6b, lot6c)`.

Le test d'ordre comparait la chaîne à une liste recopiée à la main ; il la compare désormais à
`menages_chaine_service.STEPS_CHAINE`, de sorte qu'une divergence entre le pilotage et
l'orchestrateur casse le test au lieu de passer inaperçue.

## État réel de la chaîne Ménages en recette

| Lot | Statut | Détail |
|---|---|---|
| lot6b | ✅ **SUCCES** | 2,6 s — déclarations internes + M04 produits et vérifiés |
| lot6c | ✅ **SUCCES** | 1,7 s — ménages externes produits et vérifiés |
| lot6d | ⛔ **ECHEC** | voir ci-dessous |
| lot6e | — | non atteint |
| lot6f | — | non atteint |

Deux blocages successifs, le premier levé :

1. **Source Hostaway absente** — `IndexError` sur le glob de
   `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`. **Levé** : `build_menages_hostaway()` seede
   désormais cette source (4 tâches réalisées, une par logement du parc, onglets `data`,
   `MASTER_ENRICHI`, `VUE_COMPTAGE`).
2. **Clé `None` dans l'agrégation de lot6d** — `TypeError: '<' not supported between instances of
   'NoneType' and 'str'` (`lot6d_rapprochement_menages.py:220`, tri de `res_app`). Une des sources
   agrégées porte un `logement_id` ou `proprietaire_id` nul. **Non résolu** : identifier laquelle
   demande d'inspecter les sorties de lot6b/lot6c dans le workspace, travail non entamé.

C'est le point de reprise exact. Rien n'est déclaré fonctionnel au-delà de lot6c.

## Décision de conduite

Construire le cycle de vie opérationnel demandé (statuts, création hors Hostaway, affectation,
rattachements facture/charge/règlement/banque, catalogue de contrôles, prestataires qualifiés)
représente un module comparable à Factures — dont la livraison a demandé un tour complet.

Il ne sera **pas** déclaré fait sans preuve navigateur, tests, pipeline et documentation, comme le
brief l'exige explicitement. Le découpage retenu, dans cet ordre, est inscrit au handoff :

1. aligner `MENAGES_REAL_RECALC_ENABLED` sur le double verrou (petit, isolé) ;
2. arbitrer les trois règles métier ci-dessus ;
3. modèle SQLite du ménage unitaire + statuts, adossé aux comptages existants ;
4. qualification prestataires sur le référentiel Fournisseurs (jamais un second référentiel) ;
5. services, écrans, rattachements ;
6. catalogue de contrôles ;
7. jeu de recette, recette navigateur, pipeline.

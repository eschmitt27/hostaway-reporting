# Arbre des charges — conséquences métier (banc d'essai du 2026-09-11)

> Ce document répond à une seule question, pour chaque type de charge :
> **« si je crée cette charge, qu'est-ce qu'elle provoque partout dans l'application ? »**
>
> Tout ce qui suit a été **exécuté** sur une copie isolée de la vraie base, pas déduit du code.
> Version courte et lisible : [`ARBRE_CHARGES_LISIBLE.md`](ARBRE_CHARGES_LISIBLE.md).

---

## 1. Les axes RÉELS du modèle

Découverts dans le code (`charges_engine.CATEGORY_CATALOG`, `charges_preview_service`,
`ref_codes_impact`), pas supposés.

| Axe | Valeurs | Où c'est décidé |
|---|---|---|
| **Catégorie** | 15 catégories, 4 groupes | `CATEGORY_CATALOG` |
| ↳ sous-axe *ménage* | `FORCE` (3) · `CHOIX` (4) · `INTERDIT` (8) | idem |
| ↳ sous-axe *avantage* | autorisé (4) · interdit (11) | idem |
| **Périmètre** | 0 logement (GLOBAL) · 1 (LOGEMENT) · N (GLOBAL + périmètre) | `compute_perimetre_logements` |
| **Refacturable** | OUI (exige ≥ 1 logement final) · NON | V24 |
| **Code d'impact** | `IC` intra-comptable · `HC` extra-comptable | `STANDARD_CODES_IMPACT` |
| **Mode de paiement** | 6 modes, dont 2 interdits ; certains exigent associé ou carte | V07/V08/V09 |
| **Avantage associé** | OUI (catégorie compatible + associé) · NON | V25/V26 |
| **Montant récupéré** | OUI · NON | dérivation du type de flux |

`CHG_024` (catégorie personnalisée) a un **régime propre** : GLOBAL forcé, jamais refacturable,
libellé libre obligatoire.

**`HR` (hors résultat) existe au référentiel mais est exclu du formulaire standard.** C'est un code
de neutralisation, pas une charge ordinaire. Le modèle le supporte (`flux_unifie_service`), la
saisie non — et le test `test_hr_est_hors_du_formulaire_standard` verrouille cette frontière.

---

## 2. LA distinction structurante : deux axes, jamais confondus

| | VENTILATION ANALYTIQUE | REFACTURATION COMMERCIALE |
|---|---|---|
| Question | Combien cette dépense **coûte-t-elle** à ce logement ? | Combien je **récupère**, et sur quelle facture ? |
| Montant | montant ÷ nombre de logements | le montant **TOTAL** |
| Exemple (700 €, 2 logements) | **350 / 350** | **700 €** à récupérer |
| Porté par | `charges_perimetre_analytique` (0074) | `charges_refacturation_positions` |
| Contrainte | somme des parts = montant **exact** | cumul imputé ≤ montant source |

**Preuve exécutée** : charge de 700 € sur 2 logements → analytique 350/350, puis refacturation
**500 € sur une facture et 200 € sur l'autre**. La ventilation analytique n'a rien imposé.

---

## 3. Ce que le code d'impact décide (§12)

Trois sources concordent — `ref_codes_impact`, `flux_unifie_service._IMPACT_FLAGS`, et les données
réelles de `flux_unifies` :

| Code | Libellé | Résultat réel | Résultat comptable | Résultat extra | Écriture ? |
|---|---|---|---|---|---|
| **IC** | Intra-comptable | OUI | **OUI** | NON | via la facture |
| **HC** | Hors compta / extra-comptable | **OUI** | NON | **OUI** | non |
| `HR` | Hors résultat | NON | NON | NON | non |

**Constaté sur les 336 flux réels** : 284 en `IC` (OUI/OUI/NON), 52 en `HC` (OUI/NON/OUI). Aucune
divergence entre le référentiel, le moteur et les données.

À retenir : **`HC` n'est pas « sans effet »** — la dépense pèse bien sur le résultat économique
réel, elle n'entre simplement pas dans la comptabilité générale. Seul `HR` est neutre partout.

> **Colonnes `charges.impact_resultat_reel` / `impact_resultat_comptable` : toujours NULL.**
> Elles ne sont lues par personne. Le calcul de résultat passe par `flux_unifies`, dont lot9 dérive
> les trois drapeaux depuis `code_impact`. Ce sont des colonnes héritées, sans consommateur —
> documenté ici pour que leur vacuité ne soit pas prise pour une anomalie.

---

## 4. Matrice des feuilles VALIDES (18 scénarios exécutés)

Montant 700 € sauf mention. `A_CONTROLER` partout à la création : aucune branche ne naît conforme.

| ID | Scénario | Affectation | Nb log. | Analytique | Refac. | Position | Écriture |
|---|---|---|---|---|---|---|---|
| ARB_001 | GLOBAL non refacturable | GLOBAL | 0 | — | NON | non | non |
| ARB_002 | 1 logement, non refacturable | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_003 | 1 logement, **refacturable** | LOGEMENT | 1 | 700 | OUI | **700 dispo** | non |
| ARB_004 | **2 logements, refacturable** | GLOBAL | 2 | **350 / 350** | OUI | **700 dispo** | non |
| ARB_005 | 3 logements, 100 € | GLOBAL | 3 | **33,34 / 33,33 / 33,33** | OUI | 100 dispo | non |
| ARB_006 | 2 logements, non refacturable | GLOBAL | 2 | 350 / 350 | NON | non | non |
| ARB_007 | Propriétaire (élargit à ses logements) | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_010 | **HC** — extra-comptable | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_011 | **IC** — intra-comptable | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_012 | HC **+ refacturable** | LOGEMENT | 1 | 700 | OUI | 700 dispo | non |
| ARB_020 | Ménage par **logement** | GLOBAL | — | **ménage 350/350** | NON | non | non |
| ARB_021 | Ménage par **intervenant** | GLOBAL | — | **ménage 700** | NON | non | non |
| ARB_022 | Catégorie ménage, ménage non retenu | LOGEMENT | 1 | 700 | OUI | 700 dispo | non |
| ARB_030 | Paiement espèces | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_031 | Carte associée | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_032 | Avantage associé | GLOBAL | 0 | — | NON | non | non |
| ARB_033 | Payé avec montant récupéré | LOGEMENT | 1 | 700 | NON | non | non |
| ARB_040 | Catégorie personnalisée | GLOBAL | 0 | — | NON | non | non |

**Verdict : 18 / 18 CONFORMES.**

Deux constantes sur toutes les branches :
- **aucune charge ne produit d'écriture comptable** — la comptabilité naît de la **facture** ;
- **aucune charge ne naît conforme** — toutes arrivent `A_CONTROLER`.

---

## 5. Branches INVALIDES (18 testées) — refus propre exigé

| ID | Tentative | Code de refus |
|---|---|---|
| INV_001 | Refacturable sans logement final | `V24_REFAC_SANS_LOGEMENT` |
| INV_002/003 | Montant zéro / négatif | `V03_MONTANT_INVALIDE` |
| INV_004 | Montant non numérique | `V03_MONTANT_NON_NUMERIQUE` |
| INV_005 | Catégorie inconnue | `V04_CATEGORIE_INVALIDE` |
| INV_006 | `HR` depuis le formulaire standard | `V06_CODE_IMPACT_HORS_STANDARD` |
| INV_007 | Logement inconnu | `V23_LOGEMENT_INCONNU` |
| INV_008 | Charge ménage déclarée refacturable | `V22_MENAGE_NON_REFACTURABLE` |
| INV_009 | Parcours ménage sans périmètre | `V21_MENAGE_PERIMETRE_VIDE` |
| INV_010 | Impact ménage sur catégorie qui l'interdit | `V20_IMPACT_MENAGE_INTERDIT` |
| INV_011 | Avantage sur catégorie incompatible | `V25_AVANTAGE_INTERDIT` |
| INV_012 | Avantage sans associé | `V26_AVANTAGE_SANS_ASSOCIE` |
| INV_013/014 | Date manquante / mal formée | `V01_DATE_MANQUANTE` / `V01_DATE_INVALIDE` |
| INV_015 | Mode de paiement interdit | `V07_MODE_PAIEMENT_INTERDIT` |
| INV_016 | Catégorie personnalisée sans libellé | `V17_LIBELLE_PERSONNALISE_MANQUANT` |
| INV_017 | Catégorie personnalisée refacturable | `V18_PERSONNALISE_REFAC_INTERDITE` |
| INV_018 | Mode de paiement manquant | `V07_MODE_PAIEMENT_MANQUANT` |

**Verdict : 18 / 18 refusées avec le code attendu.**

Après 18 créations **et** 18 refus : `integrity_check` **ok**, `foreign_key_check` **0**,
**0 ligne partielle**, **0 périmètre orphelin**, **0 position orpheline**, **0 événement orphelin**.
La transaction est bien tout-ou-rien.

---

## 6. Chaîne aval, exécutée de bout en bout

```
charge 700 € / 2 logements / refacturable
   ANALYTIQUE      350 (LOG_0001·PROP_0001) + 350 (LOG_0002·PROP_0002) = 700,00
   POSITION        700,00 DISPONIBLE, éligible à PROP_0001 ET PROP_0002
   FACTURE 1       + 500,00  → disponible 200,00
   FACTURE 2       + 200,00  → disponible   0,00
   ÉMISSION        2026-06-001 (804,81 €) · 2026-06-002 (776,30 €)
   COMPTABILITÉ    411000 D = 706000 C, équilibrée, idempotente
   POSITION        imputé 700,00 / 700,00 → IMPUTEE
```

Une charge **non refacturable** n'est jamais proposée à une facture : vérifié.

---

## 7. Défauts trouvés et corrigés par ce banc

### D1 — `affectable_menage` calculé puis perdu *(corrigé, migration 0076)*
`charges_preview_service` calcule explicitement ce drapeau pour les charges ménage et le renvoie
dans `row_data`. Mais la colonne n'existait pas et `CHAMPS_SAISIE` ne la listait pas : la valeur
disparaissait en silence à l'INSERT. Or **`lot6f_cout_complet_menages` filtre précisément dessus**
pour constituer ses pools de coût. Une charge ménage saisie dans l'application ne pouvait donc
structurellement jamais rejoindre le coût complet ménage.

### D2 — ventilation ménage jamais persistée *(corrigé, migration 0076)*
Le périmètre ménage (intervenants ou logements) était résolu à la prévisualisation puis jeté —
**exactement le défaut corrigé en 0074 pour les charges non ménage**, qui survivait sur cette
branche. Nouvelle table `charges_perimetre_menage`, avec contrainte : un périmètre désigne un
intervenant **ou** un logement, jamais les deux, jamais aucun.

### D3 — refacturation partielle impossible à valider *(corrigé, migration 0077)*
`imputer()` exige une justification dès que le montant imputé diffère du solde. La règle est juste
— ne récupérer qu'une partie d'une dépense est une décision. Mais elle n'était contrôlée qu'à la
**validation**, où plus personne ne pouvait fournir ce motif : l'interface ne le demandait nulle
part. Une facture portant 500 € sur 700 se composait normalement puis **refusait indéfiniment de se
valider**. Le motif est désormais recueilli au moment de la décision et porté par la ligne
(colonne dédiée : un commentaire détourné aurait été indistinguable d'une note quelconque).

---

## 8. Arbitrages métier restants (§22)

### A1 — `lot6f` lit le classeur, pas SQLite
`lot6f_cout_complet_menages` lit `SAISIE_Charges_Flux.xlsx`, feuille SAISIE. Les charges saisies
dans l'application vivent, elles, dans SQLite. Persister `affectable_menage` (D1) **rend la bascule
possible** ; la décider est un arbitrage : soit le moteur ménage lit SQLite, soit les deux sources
coexistent avec une règle de priorité explicite. **Comportement actuel** : les charges ménage
saisies dans l'application n'alimentent aucun pool de coût ménage.

### A2 — `HR` inaccessible depuis l'interface
Le référentiel définit `HR` (neutre partout, « pour neutralisation ou suivi uniquement ») et le
moteur le gère. La saisie ne le propose pas. **Comportement actuel** : une charge à neutraliser ne
peut pas être créée depuis l'application. À trancher : l'exposer avec une justification obligatoire,
ou assumer qu'il reste réservé aux imports.

### A3 — colonnes d'impact héritées
`charges.impact_resultat_reel` / `impact_resultat_comptable` sont dans le schéma, dans
`CHAMPS_SAISIE`, et restent NULL. Aucun consommateur. À trancher : les remplir par cohérence, ou
les retirer pour supprimer une source d'ambiguïté.

---

## 9. Invariants verrouillés par des tests

`tests/test_arbre_charges_invariants.py` — **46 tests paramétrés** :

| Invariant | Couverture |
|---|---|
| somme des ventilations = montant exact | 11 combinaisons montant × nombre |
| centime résiduel déterministe et reproductible | 4 cas |
| refacturable = total, jamais la quote-part | 1 à 5 logements |
| charge non refacturable → aucune position, jamais proposée | 0 à 3 logements |
| cumul imputé ≤ montant source | 5 répartitions |
| dépassement refusé / position close | 2 cas distincts |
| imputation partielle sans justification refusée | 1 |
| `prise_en_compta` dérivée du code d'impact | IC et HC |
| `HR` hors du formulaire standard | 1 |
| **aucune charge ne crée d'écriture** | 4 combinaisons impact × refacturable |
| cycle de contrôle stable après resynchronisation | 4 combinaisons |
| périmètre ménage persisté et bouclé | 2 modes |
| contrainte SQL une seule dimension | 1 |

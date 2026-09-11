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
| ~~`HR`~~ | ~~Hors résultat~~ | — | — | — | **supprimé des DEUX axes** (0078 puis 0079) |

**Constaté sur les 336 flux réels** : 284 en `IC` (OUI/OUI/NON), 52 en `HC` (OUI/NON/OUI). Aucune
divergence entre le référentiel, le moteur et les données.

À retenir : **`HC` n'est pas « sans effet »** — la dépense pèse bien sur le résultat économique
réel, elle n'entre simplement pas dans la comptabilité générale.

> **`HR` N'EXISTE PLUS**, ni sur les charges (0078) ni sur les réservations (0079). Aucune charge
> réelle ne l'a jamais porté. Une charge qui le porterait encore ressort `A_CONTROLER` — visible,
> jamais neutralisée en silence. Sur les réservations, les 125 lignes qui le portaient restent
> exclues par `statut_controle` + `motif_exclusion`, à 0,00 € d'écart. Un réimport de `REF_Setup`
> ne le ressuscite pas : la ligne est écartée et signalée.

> **Colonnes `charges.impact_resultat_reel` / `impact_resultat_comptable` : SUPPRIMÉES**
> (migration 0078). Elles recopiaient ce que `code_impact` dit déjà, et divergeaient en pratique :
> 4 charges réelles sur 5 avaient un `code_impact` renseigné et ces colonnes à NULL — l'écran de
> détail affichait « — » pour un impact parfaitement connu. L'impact se lit désormais par
> `charges_engine.impact_charge(code_impact)`, source unique. Rien n'est perdu : la valeur reste
> recalculable, c'est même ce qui rend la suppression sûre.

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

## 8. Arbitrages métier — état après la mission « FIN DU LEGACY »

### A1 — `lot6f` lit le classeur, pas SQLite → **TRANCHÉ : SQLite, chemin Excel supprimé**
Le moteur ménage lit désormais `charges` (drapeau `affectable_menage`, persisté par 0076), les
référentiels `ref_*`, `facture_lignes_menage` et `menages_declarations_internes`. Le chemin Excel
n'est pas désactivé, il est **supprimé** — équivalence prouvée sur 2026-06, 2026-07 et 2026-08, à
photographie de données identique, tous indicateurs monétaires et de volume à l'écart 0. Les
charges ménage saisies dans l'application alimentent maintenant les pools de coût. Voir
`RESTANT_EXCEL_OPERATIONNEL.md`.

### A2 — `HR` inaccessible depuis l'interface → **TRANCHÉ : supprimé, pas exposé**
La question « l'exposer ou l'assumer réservé aux imports » est close par une troisième réponse :
une charge neutre partout n'est pas une charge. `HR` est retiré du vocabulaire des charges, refusé
à l'écriture par `charges_saisie_service`, et absent des deux écrans qui l'offraient encore.
**Reste ouvert sur l'axe réservations** — voir §8bis.

### A3 — colonnes d'impact héritées → **TRANCHÉ : supprimées (migration 0078)**
L'audit a nuancé le constat « aucun consommateur » : il y en avait un, l'écran de détail d'une
charge, qui les AFFICHAIT — et affichait « — » sur 4 charges réelles sur 5, faute de valeur. Aucune
DÉCISION ne s'y appuyait en revanche (les seules comparaisons `impact_resultat_reel == "OUI"` du
dépôt portent sur les réservations). L'affichage dérive maintenant de `code_impact` et montre la
bonne valeur pour toutes les charges. Une seule source de vérité, comme demandé.

---

## 8bis. Arbitrages métier — tous tranchés le 2026-09-11

### B1 — `HR` sur l'axe RÉSERVATIONS → **TRANCHÉ : supprimé, exclusion conservée**
`HR` a disparu des réservations (migration 0079). Les 125 lignes — toutes des séjours propriétaire —
restent exclues, mais par ce qui l'exprimait déjà : `statut_controle = EXCLU_RESULTAT`, complété
d'un `motif_exclusion` désormais requêtable (`OWNERSTAY`). Aucun code de remplacement n'a été
inventé : un code d'impact dit COMMENT une somme pèse, pas qu'une ligne est absente de l'économie.
**Équivalence prouvée à 0,00 €** sur CA, commissions, résultat, net propriétaire et flux
reconstruits. Le filtre économique était d'ailleurs déjà triple (`VALIDE` + `impact_reel=OUI` +
`montant≠0`) : les 125 lignes y échouaient aux trois, ce qui rendait `HR` redondant.

### B2 — le centime résiduel → **TRANCHÉ : aucun centime ne disparaît**
`lib_repartition.py` est désormais la seule répartition monétaire du dépôt : centimes entiers, part
entière, résidu aux parts que l'arrondi a le plus lésées, départage par clé triée. `somme(parts)`
vaut toujours le montant source. **100,00 € sur 3 logements = 33,34 / 33,33 / 33,33.**
Trois implémentations concurrentes ont été unifiées : `lot6f` (qui perdait le centime),
`lib_charges_menage` et `facture_ventilation_menage_service` (où le DERNIER absorbait tout le
résidu — une ligne pouvait s'écarter de plusieurs centimes de sa part réelle).

### B3 — charges `A_CONTROLER` dans les pools ménage → **TRANCHÉ : exclues, et signalées**
Seule une charge `VALIDE` entre dans les calculs. Une colonne vide vaut `A_CONTROLER` : l'absence
d'avis ne vaut pas accord. L'asymétrie avec les factures externes est levée.

La règle serait dangereuse sans son pendant : une charge écartée ne doit pas s'évaporer. Deux
traces l'en empêchent — le contrôle `CHARGE_MENAGE_NON_VALIDEE` émis par lot6f à chaque run, et le
contrôle **BLOQUANT** `CHARGE_NON_VALIDEE_HORS_CALCULS` qui empêche la clôture du mois. Sur la base
réelle, 4 charges (988 €) sont ainsi passées d'« intégrées en silence » à « exclues et bloquantes ».

Vocabulaire unifié au passage : le bouton « Valider la charge » de la fiche écrivait `CONFORME`
tandis que l'écran « Charges à contrôler » écrivait `VALIDE`, **dans la même colonne**. Or lot9
n'ingère que `VALIDE` : une charge validée depuis la fiche restait invisible à l'économie, sans
message. Un seul mot désormais, `VALIDE` (migration 0080).

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

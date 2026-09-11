# Fin du legacy — charges / ménages / SQLite source unique

**Mission** : suppression du chemin Excel de `lot6f`, suppression totale de `HR`, suppression des
colonnes d'impact mortes.
**Date** : 2026-09-11 · **Branche** : `resume/pilotage-conciergerie-20260909`
**Commits** : `b8987af`, `a0a50b4`, `80e41ce`, `b5c8218`

---

## §A — En une page

Les trois décisions ont été exécutées. **Deux entièrement, une partiellement** — et la partie non
exécutée ne l'est pas par prudence excessive : l'appliquer telle quelle rendrait le résultat
économique faux.

| Décision | Résultat |
|---|---|
| **1 — Zéro Excel opérationnel** (cas `lot6f`) | ✅ **FAIT.** `lot6f` ne lit plus aucun classeur ni la Google Sheet. Chemin Excel supprimé, pas désactivé. Équivalence prouvée sur 3 mois avant suppression. |
| **2 — Supprimer HR** | ⚠️ **FAIT sur l'axe CHARGES. BLOQUÉ sur l'axe RÉSERVATIONS** — 125 réservations réelles en dépendent, dont 80 séjours propriétaire. Arbitrage à rendre. |
| **3 — Supprimer les colonnes d'impact** | ✅ **FAIT** (migration 0078), après un audit qui a nuancé la prémisse : elles avaient un consommateur, l'écran de détail. Il affiche maintenant mieux qu'avant. |

**Ce qui n'a pas été touché, comme demandé** : la facture `F-11/0-000001`, les charges réelles, les
compteurs, les référentiels, l'historique Hostaway, les 125 réservations `HR`. Aucun reset.

---

## §B — DÉCISION 1 : ce que `lot6f` lisait, et ce qu'il lit maintenant

`lot6f` calculait le coût complet ménage à partir de **quatre sources Excel** :

| Source Excel | Remplacée par |
|---|---|
| `REF_Setup.xlsm` (7 feuilles de référentiels) | `ref_logements`, `ref_types_logements`, `ref_intervenants`, `ref_couts_standards_menage`, `ref_couts_menage_interne`, `ref_taux_heures_menage`, `ref_charges_recurrentes` |
| `MASTER_FACT_MEN_MenagesExternes.xlsx` | `facture_lignes_menage` (+ filtre factures comptables) |
| **Google Sheet M04** (appel réseau) | `menages_declarations_internes`, que `lot6b` alimente depuis cette même feuille |
| `SAISIE_Charges_Flux.xlsx` | `charges` (drapeau `affectable_menage`, persisté par 0076) |

Le moteur passe de 491 à 445 lignes. Ont disparu : `sh()`, `norm()`, `_m04_url()`, la table de
prénoms en dur `INTMAP = {"imene": "INT_0001", …}`, le mapping de libellés d'appartements, la table
des mois en français, et les imports `csv`/`io`/`glob`/`unicodedata`/`subprocess`.

**Un seul lecteur parle encore à la feuille : `lot6b`.** C'était l'essentiel — deux lecteurs, c'était
deux mappings de prénoms et de libellés qui finiraient par diverger.

---

## §C — La comparaison Excel vs SQLite, et les 15 écarts

C'est le point où cette mission a failli mentir. La première comparaison, sur `2026-07`, à partir
d'une copie de la sauvegarde `BCK-EFB0BCC41FD0`, a produit **15 écarts** :

```
cout_complet_total      : EXCEL=1470.00  SQLITE=1499.99  écart=+29.99
cout_direct_total       : EXCEL=1420.00  SQLITE=1450.00  écart=+30.00
cout_standard_total     : EXCEL=1529.00  SQLITE=1558.00  écart=+29.00
nb_menages              : EXCEL=41       SQLITE=42       écart=+1
proprietaire_id_distincts: EXCEL=0       SQLITE=5        écart=+5
quote_part_local        : EXCEL=50.00    SQLITE=49.99    écart=−0.01
… (+9 autres, tous le même motif)
```

Il aurait été facile de conclure « le nouveau chemin compte une ligne de trop ». **C'est faux.**

En descendant ligne à ligne, un seul couple diffère : `LOG_0005 / INT_0001`, 0 ménage côté Excel,
1 côté SQLite. En remontant aux deux sources :

- `menages_declarations_internes`, ligne `id=25` : `Imène`, `Studio - 96 (Florane)`, juillet 2026,
  `nb_menages = 1`, **extraite le 2026-09-02** ;
- la Google Sheet, **re-téléchargée le 2026-09-11** par le chemin Excel : la même case vaut **0**.

**La feuille a été modifiée entre les deux dates.** Les 14 écarts monétaires et de volume ne
mesuraient pas deux calculs différents, ils mesuraient **deux photographies différentes du même
calcul**. Le −0,01 € sur `quote_part_local` en découle mécaniquement : les mêmes 50 € de cave
répartis sur un poids total différent (1558 au lieu de 1529).

Après resynchronisation de la copie isolée par `lot6b` (9 lignes mises à jour, **0 conflit**), la
comparaison a été rejouée sur **trois mois** :

| Mois | Indicateurs comparés | Écart |
|---|---|---|
| 2026-06 | 32 | **0** partout sauf `proprietaire_id` |
| 2026-07 | 32 | **0** partout sauf `proprietaire_id` |
| 2026-08 | 32 | **0** partout sauf `proprietaire_id` |

**Le 15e écart est un enrichissement, pas une divergence.** `REF_Logements` ne porte pas de colonne
`proprietaire_id` : le chemin Excel laissait la colonne vide. Le chemin SQLite résout le
propriétaire **de la période** via `ref_gestion_logements_hist`. Et `proprietaire_id` n'entre dans
aucun calcul de `lot6f` — c'est une colonne de sortie (vérifié : 4 occurrences, toutes des
écritures ou une liste de colonnes).

> **Constat collatéral, qui vous concerne directement.** La base réelle porte toujours la
> photographie du 2026-09-02. Le coût complet ménage de juillet calculé aujourd'hui repose donc sur
> une déclaration que la feuille ne contient plus. **Je n'ai rien resynchronisé** — cela modifie de
> la donnée réelle. C'est une action à faire (`lot6b`), et le plan de reset a été amendé pour
> qu'elle ait lieu **avant** le reset, où l'écart serait sinon attribué à tort au reset lui-même.

---

## §D — Le test « sans Excel disponible »

Une racine de projet a été construite ne contenant **que les 51 moteurs `.py`** : pas de
`01_SOURCES_BRUTES`, pas un seul `.xlsx/.xlsm/.xls`. `lot6f` déduisant sa racine de `__file__`, il y
cherche donc des classeurs qui n'existent pas.

Résultat : `rc = 0`, et des chiffres **identiques** au run nominal (standard 1529,00 · complet
1470,00 · écart 59,00 · même ventilation par intervenant).

Ce test est désormais permanent : `test_lot6f_anti_excel` reconstruit cette racine à chaque
exécution.

---

## §E — La suppression, et le refus bruyant

Le chemin n'est pas désactivé derrière un drapeau : il est **supprimé**. Un drapeau qui le
réactiverait serait un second moteur en sommeil.

`--source` n'accepte plus qu'une valeur. Un appel résiduel échoue explicitement :

```
lot6f_cout_complet_menages.py: error: argument --source: invalid choice: 'EXCEL'
                               (choose from SQLITE)   → code 2
```

Le drapeau lui-même survit parce que des appelants le passent (`orchestrateur_moteur`,
`menages_chaine_service`, `run_menages_pipeline`) : c'est `choices` qui garantit qu'aucun chemin
Excel ne peut être ré-emprunté en silence.

**Le classeur de SORTIE reste écrit** : `lot11`, `lot13/PowerBI` et `menages_reader` le consomment
encore. La décision visait les LECTURES.

---

## §F — La charge ménage, prouvée de bout en bout

Sur une **copie isolée de la base réelle** (7 déclarations internes de juillet, poids inégaux), une
charge de 300 € a été saisie par le service canonique — jamais par un `INSERT` direct :

```
saisie      : CHG-06268b732017 · 300.00 € · CHG_004 · affectable_menage=OUI · ACTIVE
lot6f       : rc=0, depuis une racine sans aucun classeur
ventilation : 44.67 + 5.58 + 27.92 + 60.08 + 105.91 + 27.92 + 27.92
SOMME       : 300.00 €        ÉCART : 0.00 €
```

Les montants diffèrent par ligne parce que les poids diffèrent (`nb_menages × coût standard`, D103)
— c'est la règle, pas un défaut.

Le cas de référence de la mission est vérifié séparément, sur trois logements de poids égaux :
**300 € / 3 = 100 / 100 / 100**, somme ventilée = pool.

---

## §G — Les centimes : déterministes, mais un centime manque

**100 € sur 3 logements de poids égal donnent 33,33 × 3 = 99,99 €.** Un centime n'est attribué à
personne.

C'est **déterministe et reproductible** (deux exécutions consécutives rendent les mêmes centimes —
test dédié), et le résidu est borné à 0,01 €. Mais la somme ventilée n'égale pas toujours le pool.

**Je n'ai pas corrigé.** Ajouter un rattrapage change la règle de répartition : à quelle ligne
donner le centime ? La plus lourde ? La première par `logement_id` ? C'est un arbitrage métier —
inscrit en **B2** dans `ARBRE_CHARGES_CONSEQUENCES.md`. Le test le constate explicitement
(`résidu == 0.01`), de sorte qu'une dérive au-delà d'un centime deviendrait rouge.

---

## §H — Exclusions et anti-double-comptage

Vérifiés par exécution réelle, pas par lecture du code :

| Règle | Vérification |
|---|---|
| `affectable_menage = NON` → hors pools | 300 OUI + 999 NON ⇒ 300,00 € ventilés |
| `statut = ANNULEE` → n'est pas un coût | 0,00 € ventilé |
| charge d'un autre mois → ne fuit pas | charge de juin ⇒ 0,00 € en juillet |
| `mois` dérivé de `date_charge`, pas d'une colonne | `date=2026-07-15` + `mois='2026-06'` ⇒ rattachée à juillet |
| une charge alimente **un** pool, **une** fois | les 4 autres pools restent à 0,00 € |
| chaque catégorie va dans son pool | `CHG_002`→courses, `CHG_004`→consommables, `CHG_018`→autres |

**Aucune addition Excel + SQLite n'a jamais eu lieu** : les deux moteurs ont toujours été exécutés
séparément, comme deux lectures du même mois.

---

## §I — DÉCISION 2 : HR supprimé de l'axe CHARGES

Aucune charge réelle n'a jamais porté `HR` : **0** charge, **0** récurrente, **0** flux. Mais il
restait atteignable par trois portes, alors que le formulaire « Nouvelle charge » l'excluait déjà —
le vocabulaire était **incohérent selon le point d'entrée** :

1. le mode « Charge hors résultat et hors comptabilité » de l'édition de facture ;
2. le filtre `['IC','HC','HR']` codé en dur dans l'écran de contrôle ;
3. la table de dérivation de `lot3`, qui le traduisait silencieusement en `NON/NON`.

Les trois sont fermées. Le refus est posé dans `charges_saisie_service.valider()`, **seule porte
d'écriture** de la table `charges` (un unique `INSERT INTO charges` dans tout le dépôt) : le retirer
des écrans sans fermer le service aurait été cosmétique.

Effet voulu dans `lot3` : une charge portant encore `HR` ne devient plus « neutre » en silence, elle
tombe sur `IMPACT_INCONNU` et ressort `A_CONTROLER`. **Visible**, jamais convertie en `IC` ou `HC`.

---

## §J — DÉCISION 2 : pourquoi l'axe RÉSERVATIONS est bloqué

> **ARBITRAGE MÉTIER REQUIS — décision à rendre**

`HR` désigne deux choses différentes selon l'objet. Sur les réservations, **125 lignes réelles** le
portent :

- **80** `OWNERSTAY_EXCLU` — « Hostaway ownerStay — exclu résultat » : les **séjours du propriétaire
  dans son propre logement**. Ni voyageur, ni encaissement, ni commission ;
- **45** `EXCLU_LEGACY` — mois de bascule sans archive économique d'origine ;
- plus le **suivi associé** de `lot7` (`CODE_IMPACT_SUIVI = "HR"`, D012).

Toutes ont `impact_resultat_reel = NON` et un montant retenu de **0,00 €**.

Les trois chemins possibles sont tous fermés par vos propres consignes :

| Chemin | Conséquence |
|---|---|
| Reclasser en `IC`/`HC` | 80 séjours propriétaire deviennent du chiffre d'affaires — résultat **faux**. Interdit : « NE PAS les convertir silencieusement ». |
| Créer un code équivalent | Interdit : « NE PAS créer un remplaçant équivalent ». |
| Ne plus produire ces lignes | Les séjours propriétaire disparaîtraient, alors qu'ils occupent un logement et génèrent un ménage. |

« Hors compta + hors résultat n'a pas de pertinence métier » est **vrai pour une dépense** : une
charge qui ne pèse nulle part n'est pas une charge. Ce ne l'est **pas pour une occupation sans
vente**, qu'il faut enregistrer *et* exclure du résultat.

**Options, coût, recommandation** : `HR_SUPPRESSION_AUDIT.md`, §4. Ma recommandation est de
**renommer** le code sur cet axe (`EXCLU`) si le mot « hors résultat » doit disparaître du
vocabulaire — cela satisfait l'intention sans falsifier le résultat. Non exécuté : cela touche de la
donnée réelle.

---

## §K — DÉCISION 3 : l'audit a nuancé la prémisse

La mission posait une condition : *« si l'audit exhaustif confirme qu'elles n'ont réellement aucun
consommateur »*. **L'audit ne l'a pas confirmée telle quelle** — et je le dis plutôt que de
l'arrondir :

- **aucune DÉCISION** ne s'appuyait sur `charges.impact_resultat_*`. Les seules comparaisons
  `impact_resultat_reel == "OUI"` du dépôt portent sur les **réservations**
  (`reservations_adaptateur_moteur:70`, `lot4bis:965`, `lot4quater:468`) ;
- mais **un consommateur existait** : l'écran de détail d'une charge les **affichait**.

Or il les affichait mal. Sur la base réelle :

| charge | `code_impact` | `impact_resultat_reel` | ce que l'écran montrait |
|---|---|---|---|
| CHG-85c2345c5c07 | IC | `NULL` | **—** |
| CHG-a1db33663116 | IC | OUI | OUI |
| CHG-d6fa0520ef8f | IC | `NULL` | **—** |
| CHG-f755790fd169 | IC | `NULL` | **—** |
| CHG-fc93f74a63d1 | HC | `NULL` | **—** |

**4 charges sur 5 affichaient « — » pour un impact parfaitement connu.** C'est exactement le défaut
que dénonce la consigne « une seule source de vérité » : la copie dénormalisée avait déjà divergé.

L'affichage dérive maintenant de `code_impact`. Vérifié sur l'instance redémarrée :
`CHG-85c2345c5c07` → réel **OUI** / comptable **OUI** ; `CHG-fc93f74a63d1` → réel **OUI** /
comptable **NON**. **L'écran dit maintenant la vérité là où il se taisait.**

La suppression est sûre précisément parce que rien n'est perdu : la valeur reste recalculable depuis
`code_impact`.

---

## §L — Migration 0078 et ses tests

`ALTER TABLE charges DROP COLUMN impact_resultat_reel / impact_resultat_comptable`.

| Test | Résultat |
|---|---|
| base vierge | colonnes absentes, `code_impact` et `affectable_menage` présents |
| double application | idempotente, colonnes identiques |
| copie de la base réelle (0077 → 0078) | 5 charges conservées, `code_impact` intact, seules les 2 colonnes visées retirées |
| impact toujours dérivable | les 5 charges rendent leurs drapeaux |
| base réelle après redémarrage | `integrity_check = ok`, `foreign_key_check = 0`, **125 réservations HR intactes** |

Sauvegarde préalable **`BCK-1D3054E7650C`** (schéma 0077, `VALIDE`) : rollback disponible.
Aucune migration existante n'a été modifiée.

---

## §M — Ce que la bascule a emporté ailleurs

Un effet de bord qu'il aurait été facile de ne pas voir. `lot6f` ne lisant plus la feuille, il ne
peut plus attester une provenance réseau. Le laisser dans le contrôle `lot11` aurait rendu
`SOURCE_SHEET_PROVENANCE_INCOMPLETE` **rouge en permanence** — et un contrôle toujours rouge cesse
d'être un contrôle, il devient un bruit qu'on apprend à ignorer.

Corrigé dans le même mouvement, dans les **deux** implémentations (moteur et applicative) : la
provenance de `lot6f` **est** celle de `lot6b`, par construction. `lot6b` est désormais la seule
étape évaluée. Le risque couvert — feuille périmée — reste intégralement porté.

---

## §N — La règle générale Excel → SQLite

Écrite dans `RESTANT_EXCEL_OPERATIONNEL.md` §5, en 8 points, telle qu'elle a été suivie ici. Les
trois qui ont réellement fait la différence :

- **comparer sur la même photographie** — sans cela, cette mission aurait conclu à un bug qui
  n'existait pas ;
- **tester avec les classeurs matériellement absents** — un `--source SQLITE` qui lit encore un
  fichier ne se voit pas autrement ;
- **verrouiller par un test comportemental** — un test qui lit le source se fait berner par un
  import indirect ; un audit hook, non.

---

## §O — Excel opérationnel restant

Inventaire complet dans `RESTANT_EXCEL_OPERATIONNEL.md`, avec trois catégories à ne pas confondre :
source de **lecture runtime** (à supprimer), **sortie/export** (à migrer plus tard), **pièce
d'origine** (à conserver — et rien n'a été supprimé dans cette catégorie).

`lot6f` est le **premier moteur à zéro lecture de classeur**. Dans l'application, un seul lecteur
runtime subsiste : `run_log_reader` → `MASTER_RUN_Log.xlsx`, journal technique sans donnée métier.

Ordre suggéré pour la suite : `lot1`/`lot8b` → chaîne ménages (`lot6c`, `6a`, `6d`, `6e`) → `lot3`
→ `lot5`, `lot7` → **`lot4bis/ter/quater` en dernier**, c'est le cœur du résultat économique.

---

## §P — Tests

| Suite | Avant | Après |
|---|---|---|
| **Application** | 3235 ✅ · **5** ❌ · 52 ⏭ | **3275 ✅ · 1 ❌ · 52 ⏭** (24 min 38 s) |
| **Moteurs** | 407 ✅ · **5** ❌ · 1 ⏭ | 407 ✅ · **5** ❌ · 1 ⏭ — **les 5 mêmes**, mesurées sur un arbre `HEAD` propre |

> Les 5 derniers tests de `test_hr_supprime_axe_charges` (garde-fous de la migration 0078) ont été
> écrits après le lancement de cette exécution : elle en compte 14 sur 19. Le fichier a été exécuté
> séparément — **19 ✅**. Le total courant est donc 3280, mais le chiffre mesuré, 3275, est celui
> reporté ici.

**42 tests ajoutés**, en trois fichiers : 12 anti-Excel `lot6f`, 11 ventilation ménage, 19
suppression `HR` + garde-fous de la migration 0078.

**Et 4 tests réveillés.** Les quatre gardes comportementaux de `test_lot6b_anti_excel` ne
s'exécutaient **jamais** : `cfg.LOT4A_ENGINE_PYTHON` a pour défaut un chemin Windows codé en dur
(`C:\Program Files\Python312\python.exe`), absent de ce poste, qui les faisait échouer sur un
`FileNotFoundError [WinError 2]`. Quatre preuves anti-Excel dormantes depuis leur écriture, et
comptées parmi les échecs « connus ». Elles tournent et passent.

**L'unique échec restant** (`test_banque_lot8_present_les_tests_gardes_s_executent`) est
antérieur à cette mission : le classeur bancaire est absent de ce worktree. Vérifié en mesurant la
baseline sur un arbre propre.

Trois tests existants ont été **mis à jour, pas contournés** : ils affirmaient le comportement `HR`
que la décision supprime. Chacun explique dans sa docstring pourquoi le contrat a changé.

---

## §Q — Ce que je n'ai PAS fait, et pourquoi

| Non fait | Raison |
|---|---|
| Supprimer `HR` de l'axe réservations | Ferait entrer 125 réservations, dont 80 séjours propriétaire, dans le résultat. **Arbitrage §J.** |
| Supprimer la ligne `HR` de `ref_codes_impact` | Orphelinerait ces 125 lignes. |
| Modifier les 125 réservations `HR` | « NE PAS les convertir silencieusement. » |
| Resynchroniser `menages_declarations_internes` | Modifie de la donnée réelle. Inscrit dans le plan de reset (§C). |
| Corriger le centime résiduel | Change une règle de répartition. **Arbitrage B2.** |
| Supprimer le classeur de sortie de `lot6f` | Trois consommateurs actifs ; la décision visait les lectures. |
| Supprimer `REF_Setup.xlsm` / `SAISIE_Charges_Flux.xlsx` | Pièces d'origine, explicitement hors périmètre. |
| Migrer les autres moteurs | « Ne lance pas une refonte de 15 autres lots. » |
| Tout reset | « CE N'EST TOUJOURS PAS LE RESET FINAL. » |

**Un défaut trouvé, non corrigé car hors périmètre** : le lien « Observabilité » de la navigation
principale (`base.html`) pointe vers `/observabilite`, alors que seule `/observabilite/runs` existe
— **404 en production**. Antérieur à cette mission (mon diff ne touche pas `base.html`). Le
corriger demande de choisir entre rediriger `/observabilite` ou changer le lien : à vous.

---

## §R — Verdict

> ## LEGACY EXCEL SUPPRIMÉ SUR LOT6F, HR SUPPRIMÉ CÔTÉ CHARGES — UN ARBITRAGE MÉTIER BLOQUE LA SUITE

`lot6f` est le premier moteur à ne lire aucun classeur, avec une équivalence prouvée sur trois mois
et verrouillée par un test comportemental. `HR` et les colonnes d'impact dérivées ont disparu de
l'axe charges, derrière une source de vérité unique — et l'écran de détail dit désormais la vérité
là où il affichait « — » sur quatre charges sur cinq.

**Ce qui reste n'est pas du travail technique, c'est une décision.** `HR` sur l'axe réservations
porte 80 séjours propriétaire ; le supprimer sans le remplacer les transformerait en chiffre
d'affaires. Les trois options sont chiffrées dans `HR_SUPPRESSION_AUDIT.md` §4 — il vous revient de
trancher.

**Avant le reset final, une action vous incombe** : relancer `lot6b`. La base porte encore la
photographie du 2026-09-02 de la Google Sheet, modifiée depuis.

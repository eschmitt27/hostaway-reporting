# Excel opérationnel restant — inventaire au 2026-09-11 (mis à jour, mission « lot6c vers SQLite »)

> Mission « FIN DU LEGACY CHARGES / MÉNAGES », **DÉCISION 1** : « ZÉRO EXCEL OPÉRATIONNEL. »
>
> Cet inventaire dit **où en est réellement** cet objectif, moteur par moteur. Il ne promet rien :
> il constate. `lot6f` avait basculé en premier ; **`lot6c` vient de le rejoindre** (mission « lot6c
> vers SQLite ») — avec lui, l'export legacy de `lot6b` a disparu du CODE (pas d'un flag), et la
> recette de chaîne ménages (`/menages/chaine`) tourne désormais **lot6b → lot6c → lot6d → lot6e →
> lot6f → lot11 en SQLite pur, sans aucun classeur intermédiaire** — vérifié par un run réel sur les
> données du projet (`STATUT_SUCCES`, `reel_intact=True`). Le reste est listé avec son coût et son
> risque, pour que l'ordre des prochaines bascules soit un choix et non une surprise.

---

## 1. Les trois catégories — ne pas les confondre

La consigne vise un **chemin de LECTURE runtime** : un moteur qui, pour calculer, va chercher sa
donnée dans un classeur. C'est cela qui crée un second moteur, un second mapping, et à terme deux
résultats différents. Trois choses très différentes portent l'extension `.xlsx` :

| | Nature | Statut vis-à-vis de DÉCISION 1 |
|---|---|---|
| **A. Source de lecture runtime** | le moteur lit le classeur POUR CALCULER | **à supprimer** |
| **B. Sortie / export** | le moteur ÉCRIT un classeur consommé par un écran, PowerBI, ou un lot non migré | à migrer plus tard, sans urgence |
| **C. Pièce d'origine / archive** | fichier reçu, justificatif, source d'import ponctuel | **à conserver** — explicitement hors périmètre |

La mission le dit : « NE PAS supprimer un Excel brut servant encore de : pièce justificative ;
archive ; source d'import d'un autre module. »

---

## 2. État par moteur — catégorie A (lecture runtime)

| Moteur | Lit encore | Coût de bascule | Note |
|---|---|---|---|
| **`lot6f_cout_complet_menages`** | **RIEN** ✅ | *fait* | **Premier moteur à zéro lecture.** Équivalence prouvée sur 2026-06/07/08, chemin Excel supprimé. |
| `lot4a_compare_reservations_hh` | RIEN ✅ | — | outil de comparaison, déjà sans classeur |
| `lot7_pq_avantages` | RIEN ✅ | — | générateur de M-code, n'ouvre rien |
| `lot3_generateur_charges` | `SAISIE_Charges_Flux`, `REF_Setup` | **moyen** | La table `charges` EST déjà canonique pour l'application (un seul `INSERT INTO charges`, dans `charges_saisie_service`). lot3 travaille sur le circuit classeur historique, en parallèle. |
| **`lot6b_m04_menages_internes`** | **RIEN** ✅ | *fait* | Lit la Google Sheet (source externe légitime) et écrit `menages_declarations_internes`. **Écrit UNIQUEMENT du SQLite** : le bloc d'export legacy (`--export-legacy`, classeurs M04 + MASTER_NORM) a été **supprimé du code**, mission « lot6c vers SQLite » §9 — plus un flag qui le désactive, plus de code du tout. Équivalence prouvée avant suppression : 39 lignes, 188 ménages, bit-à-bit identique. |
| **`lot6c_menages_externes`** | RIEN en `--source SQLITE` ✅ | *fait* | **Mode SQLite ajouté** (mission « lot6c vers SQLite ») : recalcule `VUE_ECART_HOSTAWAY` depuis `facture_lignes_menage` (alimentée par `facture_menage_pdf_service`, déjà canonique) et `menages_taches_enrichies` — aucun classeur lu ni écrit. Règle de classification partagée avec `menages_ecarts_service` (applicatif) via `lib_db_moteur.classer_ecart_menage_externe`. Le mode `EXCEL` par défaut (sans `--source`) reste le code legacy, pour tout appelant qui ne serait pas encore migré ; la recette de chaîne, elle, n'y passe plus jamais. |
| `lot6d`, `lot6e` | RIEN en `--source SQLITE` ✅ (déjà présent avant cette mission) | *fait, désormais RÉELLEMENT exercé* | Leur mode SQLite existait déjà mais n'était jamais emprunté par la recette de chaîne (bloquée par lot6c ci-dessus, en Excel par défaut). Il l'est maintenant : `menages_chaine_service` leur passe `--source SQLITE --db <base jetable> --mois <mois déclaré>`. Leur mode `EXCEL` par défaut reste du code legacy non supprimé (out of scope de cette mission — aucun appelant restant identifié, cf. §3bis). |
| `lot6a` | classeur Hostaway déjà régénéré depuis SQLite par `hostaway_cleaning_tasks_adaptateur_moteur` en recette | fait côté recette | Le vrai `lot6a_cleaning_tasks_comptage.py` (API Hostaway) reste hors périmètre Claude (appel réseau) ; la recette le remplace par un stub qui ré-écrit le classeur jetable depuis `menages_taches_enrichies` (SQLite), sans jamais appeler l'API. |
| `lot1_hostaway_extract` | `REF_Setup` | faible | seulement le référentiel, déjà en SQLite (`ref_*`) |
| `lot4bis`, `lot4ter`, `lot4quater` | classeurs réservations | **élevé** | cœur du résultat économique ; 7 909 lignes réelles |
| `lot5_master_acomptes_proprietaires` | `REF_Setup`, masters | moyen | acomptes déjà en SQLite (`mouvements_tresorerie_proprietaires`) |
| `lot7_generateur_avantages`, `lot7_ik_avantages` | `SAISIE_Charges`, `REF_Setup` | moyen | axe avantages/IK |
| `lot8a/8b/8c` (banque) | `BANQUE_LOT8_IMPORT`, `REF_Setup` | faible | l'import bancaire est par nature un fichier reçu (cat. C pour la source brute) |
| `lot9_construire_flux` | `path` (générique) | **faible** | a DÉJÀ migré ses sources économiques : `menages_cout_complet`, `menages_externes_historique`, plus de `SRC_GPM` ni `SRC_MEN` |
| `lot10_calculer_resultats` | `path` (générique) | faible | lit déjà `charges` en SQLite (`charger_charges_sqlite`) |
| `lot11_controles_coherence` | `path` (générique) | faible | lit déjà les datasets SQLite pour la provenance |
| `lot12`, `lot13` | masters | faible | génération de factures et export PowerBI |

### Dans l'application (`05_APPLICATION`)

Un seul lecteur de classeur subsiste au runtime :

- **`run_log_reader`** → `MASTER_RUN_Log.xlsx`, affiché sur l'accueil et « Sources & calculs ».
  **Journal technique d'exécution des moteurs**, aucune donnée métier. Catégorie B.
- `ref_setup_hh_reader` n'est PAS un lecteur runtime : par défaut il lit `ref_*` en SQLite ; il
  n'ouvre un classeur que si un appelant lui passe EXPLICITEMENT le chemin d'une copie de travail
  (flux « écriture réelle sur COPIE »). Ce n'est pas le repli interdit — un référentiel vide se
  voit, il n'est jamais compensé en douce par un fichier.

---

## 3bis. La dernière dépendance de lot6b — résolue

**Fait, mission « lot6c vers SQLite » (§9).** `lot6b` n'écrivait déjà plus de classeur par défaut ;
son export legacy (`--export-legacy`, classeurs M04 + MASTER_NORM) survivait pour **un seul
appelant** :

> `menages_chaine_service` — la **recette de chaîne complète** de l'écran `/menages/chaine`, qui
> rejoue lot6b → lot6c → lot6d → lot6e → lot6f → lot11 dans un workspace isolé.

La raison était nommée : elle exécutait **`lot6c`**, qui n'avait ni `--source SQLITE` ni la moindre
écriture SQLite — le workspace de recette devait donc contenir des classeurs, et lot6b devait
pouvoir en produire un.

**lot6c a désormais un mode `--source SQLITE`** (recalcule `VUE_ECART_HOSTAWAY` depuis
`facture_lignes_menage`/`menages_taches_enrichies`, sans classeur). La recette de chaîne construit
maintenant une **base SQLite jetable** (référentiels en lecture seule copiés depuis la vraie base +
PDF factures ménage importés par le vrai `facture_menage_pdf_service`) et fait tourner **lot6b →
lot6c → lot6d → lot6e → lot6f → lot11 exclusivement en `--source SQLITE`**, `--db <base jetable>`.
Preuve : `menages_chaine_service.executer_chaine(mode=MODE_COPIES)` rend `statut="SUCCES"`,
`ok=True`, `reel_intact=True`, et toutes les vérifications de sortie (`verif_sorties`) passent —
run réel contre les données du projet, aucun fichier réel modifié.

**L'export legacy de lot6b a donc été supprimé du CODE** (`EXPORT_LEGACY`/`--export-legacy` et le
bloc M04/MASTER_NORM entier n'existent plus dans `lot6b_m04_menages_internes.py`) — pas désactivé
par un flag qui pourrait être réactivé. `--sans-excel` reste accepté en argument, sans effet : c'est
devenu l'unique comportement possible.

Ce qui reste, hors périmètre de cette mission : les modes `EXCEL` par défaut de `lot6d`/`lot6e`
(inutilisés par la recette désormais, et sans appelant identifié ailleurs) et deux lectures
résiduelles du classeur M04 dans `lot11_controles_coherence` (un contrôle de fraîcheur, une entrée
de dépendance) — classeurs gelés à leur dernier contenu, plus jamais régénérés. Ce sont des lectures
mortes, pas des risques de divergence : rien ne les réécrit plus.

Ce qui était déjà acquis avant cette mission : **le parcours opérationnel ne pouvait déjà plus
produire de classeur par inadvertance.** Le commentaire qui justifiait l'ancien défaut invoquait un
blocage — « lot11 lit encore le classeur M04 » — **déjà levé depuis** : les deux implémentations de
lot11 lisent `menages_declarations_internes`. Le blocage avait survécu à sa propre disparition (et,
vérification faite pendant cette mission, `lot9` ne lisait déjà plus M04 non plus — TYPE_FLUX_013
est « analytique seul, non injecté » depuis D105 révisée).

---

## 3. Catégorie B — sorties encore consommées

`MASTER_CALC_CoutComplet_Menages.xlsx` (écrit par lot6f) **reste produit**. Il a trois
consommateurs identifiés : `lot11_controles_coherence`, `lot13_export_powerbi`, et
`app/readers/menages_reader` (`SOURCE_COUTCOMPLET`). Le supprimer aujourd'hui casserait ces trois
chemins sans rien apporter à DÉCISION 1, qui vise les lectures. La vraie sortie métier de lot6f est
déjà le dataset SQLite `menages_cout_complet` — celui que lot9 consomme.

De même pour `M04` (lot6b) et les masters de la chaîne réservations.

---

## 4. Catégorie C — à conserver, sans discussion

| Fichier | Pourquoi il reste |
|---|---|
| `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm` | référentiel d'origine, source d'import de `ref_*`, et copie de travail des flux « sur COPIE » |
| `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` | saisie historique et pièce d'origine des charges antérieures à la migration 0052 |
| `01_SOURCES_BRUTES/**` (banque, AirCover, acomptes, imputations…) | fichiers REÇUS : relevés, justificatifs. Ce sont des pièces, pas des bases. |
| sauvegardes `.BAK_*` | archives de sécurité |

**Aucun fichier de cette catégorie n'a été supprimé, et aucun ne doit l'être.**

---

## 5. La règle générale, pour les prochaines bascules

C'est la procédure suivie pour lot6f, et elle est reproductible telle quelle :

1. **Écrire le chemin SQLite à côté de l'ancien**, sans rien retirer. Les deux coexistent le temps
   de la preuve, et **jamais en production simultanément** : ce sont deux LECTURES du même mois,
   jamais deux apports. Le circuit `Application → SQLite → génération Excel → moteur → réimport`
   est interdit — il recrée le problème sous un autre nom.
2. **Comparer sur la MÊME photographie de données.** C'est le point où lot6f a failli mentir : la
   première comparaison a produit 15 écarts qui ont disparu dès que les deux sources ont été
   alignées. Un écart « métier » peut n'être qu'un écart de FRAÎCHEUR — il faut le prouver, pas le
   supposer. Comparer indicateur par indicateur, sur plusieurs mois.
3. **Expliquer chaque écart restant, ligne à ligne.** Objectif : écart = 0, sauf différence
   explicitement justifiée par une correction de bug ou un enrichissement (pour lot6f :
   `proprietaire_id`, colonne de sortie qu'aucun calcul n'utilise, vide via Excel, renseignée via
   SQLite).
4. **Tester avec les classeurs matériellement absents**, depuis une racine qui n'en contient aucun.
   Un `--source SQLITE` qui lit encore un fichier ne se voit pas autrement.
5. **Supprimer le chemin Excel** — pas le désactiver. Un drapeau qui le réactive est un second
   moteur en sommeil, et un second moteur finit toujours par diverger.
6. **Faire échouer bruyamment ce qui l'invoquerait encore.** Pour lot6f : `--source` n'accepte plus
   que `SQLITE`, donc un `--source EXCEL` résiduel s'arrête avec un message clair (code 2) plutôt
   que de retomber silencieusement ailleurs.
7. **Verrouiller par un test comportemental**, pas seulement structurel. `test_lot6f_anti_excel`
   installe un audit hook CPython qui voit toutes les ouvertures de fichiers — y compris celles
   faites en C par `zipfile` sous `openpyxl` — et vérifie qu'aucun `.xlsx/.xlsm/.xls` n'est ouvert
   EN LECTURE. Un test qui lirait seulement le source se ferait berner par un import indirect.
8. **Vérifier ce que la suppression emporte ailleurs.** Retirer la lecture de la Google Sheet dans
   lot6f a rendu sa *provenance* impossible à attester : le contrôle lot11 correspondant serait
   resté rouge en permanence. Il a fallu le corriger dans le même mouvement — la provenance de
   lot6f est désormais, par construction, celle de lot6b.

---

## 6. Ordre suggéré pour la suite

**`lot6c` est fait** (mission « lot6c vers SQLite ») — c'était le seul verrou qui maintenait encore
un workspace Excel dans la recette ménages, et donc l'export legacy de `lot6b` ; les deux sont
résolus (cf. §3bis). La recette tourne désormais lot6b→lot6c→lot6d→lot6e→lot6f→lot11 en SQLite pur.

Prochaines étapes suggérées : `lot1`/`lot8b` (référentiel seul, déjà en SQLite) → nettoyage des
modes `EXCEL` par défaut de `lot6d`/`lot6e`/`lot6a` (désormais du code mort côté recette : plus
aucun appelant connu ne les emprunte, mais le code lui-même n'a pas été supprimé — seule leur
invocation a changé) → les deux lectures résiduelles du classeur M04 dans `lot11_controles_coherence`
→ `lot3` (charges ; la table est déjà canonique côté application) → `lot5`, `lot7` →
`lot4bis/ter/quater` **en dernier** : c'est le cœur du résultat économique, et c'est là que le coût
d'une erreur est maximal.

---

## 7. Mise à jour 2026-09-12 — recette utilisateur n°3, §17bis

**Aucun flux Excel opérationnel n'a été réintroduit, et un chemin résiduel a été fermé.**

| Élément audité | État | Preuve |
|---|---|---|
| `menages_cout_complet` | **SQLite, recalculé dans la vraie base** (29 → 50 lignes, 6 mois) | `JOURNAL_CONTROLES.md`, `CTR-RECALCUL-MENAGES-COUT-COMPLET-2026-09-12` |
| `lot6c` | **Gardé** par un test dédié : le parcours `--source SQLITE` n'ouvre AUCUN classeur, ni en lecture ni en écriture | `test_lot6c_anti_excel.py` (audit hook CPython, 5 tests) |
| `lot6f` | Déjà `--source` réduit à `SQLITE`, chemin Excel supprimé avant cette mission | `test_lot6f_anti_excel.py` |
| `lot6b` | Export legacy supprimé du code (mission précédente) | `test_lot6b_anti_excel.py` |
| `lot6d` / `lot6e` | Mode `EXCEL` toujours présent dans le code, **plus aucun appelant ne l'emprunte** | voir ci-dessous |
| `lot6a` | Chantier distinct, **non improvisé** | voir ci-dessous |

### Le chemin résiduel qui a été fermé

`run_menages_pipeline.py` — script autonome, cité tel quel comme « commande » sur l'écran de
diagnostic ménages — lançait `lot6d`/`lot6e`/`lot6f` **sans aucun argument**. Ils retombaient donc
sur leur défaut `--source EXCEL` : un chemin Excel réel, atteignable par un utilisateur qui suivait
l'instruction affichée par l'application elle-même. Les trois appels passent désormais
`--source SQLITE --sans-excel`, comme le font déjà `orchestrateur_moteur` et
`menages_recalcul_service`.

### `lot6d` / `lot6e` — pourquoi le mode EXCEL n'est pas supprimé aujourd'hui

| | |
|---|---|
| **Rôle** | rapprochement des volumes (6d), gain/perte vs coût standard (6e) |
| **Source Excel** | `MASTER_NORM_Declarations_Internes.xlsx`, `M04_MENAGES_PowerQuery.xlsx`, masters Lot6a/6c — **tous gelés**, plus personne ne les régénère |
| **Consommateur** | aucun : les trois appelants réels (`orchestrateur_moteur`, `menages_recalcul_service`, `menages_chaine_service`) passent tous `--source SQLITE`, et le dernier qui ne le faisait pas vient d'être corrigé |
| **Cible SQLite** | déjà en place, testée, et exercée à chaque recalcul |
| **Blocage exact** | aucun blocage technique : c'est du **code mort à retirer**, pas une migration à faire. Le retirer implique de réécrire ~700 lignes de branche EXCEL dans deux scripts et de reprendre leurs tests structurels — un chantier propre, à faire d'un bloc, pas au milieu d'une mission de recette |

### `lot6a` — chantier distinct, réellement bloqué

| | |
|---|---|
| **Rôle** | extraction Hostaway CleaningTasks (API) → comptage |
| **Source Excel** | lit `REF_Setup.xlsm` (3 lectures : référentiels) et ÉCRIT `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` |
| **Consommateur** | le classeur est régénéré depuis SQLite par `hostaway_cleaning_tasks_adaptateur_moteur` pour la recette ; la table canonique `menages_taches_enrichies` est alimentée par le chemin applicatif (`hostaway_actualisation_service`), pas par ce script |
| **Cible SQLite** | `menages_taches_enrichies` — déjà la source de tout l'aval depuis cette mission (cf. §15) |
| **Blocage exact** | le script exige des identifiants API Hostaway (aucun `.env` sur cette installation) : il ne peut être ni exécuté ni testé ici. Le migrer à l'aveugle reviendrait à réécrire un extracteur réseau sans jamais l'exercer |

**Règle projet inchangée** : les exports XLSX/CSV utilisateurs restent autorisés ; Excel comme
**base ou source opérationnelle du moteur** reste interdit.


---

## §19 (recette utilisateur n°3) — suppression effective des chemins Excel morts, 2026-09-12

La section §7 ci-dessus concluait, pour `lot6d` et `lot6e` : *« aucun blocage technique : c'est du
**code mort à retirer**, pas une migration à faire »*. C'est fait.

### Ce qui a été supprimé

| Script | Avant | Après | Branche EXCEL retirée |
|---|---:|---:|---|
| `lot6d_rapprochement_menages.py` | 408 lignes | **360** | lecture de `REF_Setup.xlsm` (4 onglets), `MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`, `MASTER_FACT_MEN_MenagesExternes.xlsx`, `MASTER_NORM_Declarations_Internes.xlsx`, `M04_MENAGES_PowerQuery.xlsx` |
| `lot6e_gainperte_menages.py` | 365 lignes | **337** | lecture de `REF_Setup.xlsm` (3 onglets), `MASTER_FACT_MEN_MenagesExternes.xlsx`, `MASTER_NORM_Declarations_Internes.xlsx` |

**`load_workbook` : 0 occurrence dans les deux scripts.** Les fonctions d'aide devenues inutiles
(`sh()`, les constantes `REF` / `NORM_DIR` / `DRY_M04`, l'import `glob`) sont parties avec.

`openpyxl` reste importé dans les deux : il **ÉCRIT** le classeur de sortie. C'est un export
utilisateur, explicitement autorisé par la règle projet — ce qui était interdit, c'est Excel comme
**source** du moteur, et cette lecture n'existe plus.

### Ce qui n'a PAS été supprimé, et pourquoi

L'option `--source` **survit**, avec une seule valeur possible :

```
_ap.add_argument("--source", choices=("SQLITE",), default="SQLITE")
```

Deux raisons. Les appelants existants passent tous `--source SQLITE` explicitement : retirer
l'option les aurait tous cassés pour un gain nul. Et demander `EXCEL` produit désormais un **refus
lisible** plutôt qu'un comportement silencieusement différent :

```
error: argument --source: invalid choice: 'EXCEL' (choose from SQLITE)
```

### Vérifié après suppression

| Contrôle | Résultat |
|---|---|
| `lot6d --source SQLITE --mois 2026-07` | **19 lignes** (`VALIDE` 11, `A_CONTROLER` 8) |
| `lot6e --source SQLITE --mois 2026-07` | **7 lignes** |
| Appel **sans** `--source` (défaut) | identique — 19 lignes |
| `--source EXCEL` | **refusé**, message explicite |
| `--sans-excel` | « classeur legacy non écrit » — inchangé |

### `lot6a` — toujours NON, et le blocage n'a pas bougé

Le script exige des identifiants API Hostaway, absents de cette installation. Il ne peut être ni
exécuté ni testé ici, et le migrer à l'aveugle reviendrait à réécrire un extracteur réseau sans
jamais l'exercer. Sa cible SQLite (`menages_taches_enrichies`) est pourtant **déjà la source de
tout l'aval** : le script n'alimente plus rien de vivant. C'est un chantier distinct, pas un reste
de celui-ci.

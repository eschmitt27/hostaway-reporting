# Excel opérationnel restant — inventaire au 2026-09-11

> Mission « FIN DU LEGACY CHARGES / MÉNAGES », **DÉCISION 1** : « ZÉRO EXCEL OPÉRATIONNEL. »
>
> Cet inventaire dit **où en est réellement** cet objectif, moteur par moteur. Il ne promet rien :
> il constate. `lot6f` vient de basculer ; le reste est listé avec son coût et son risque, pour que
> l'ordre des prochaines bascules soit un choix et non une surprise.

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
| **`lot6b_m04_menages_internes`** | **RIEN au runtime** ✅ | *fait* | Lit la Google Sheet (source externe légitime) et écrit `menages_declarations_internes`. **SQLite est le DÉFAUT** : aucun run ne peut plus produire un classeur sans le demander. `--export-legacy` subsiste pour un seul appelant (cf. §3bis). Équivalence prouvée : 39 lignes, 188 ménages, bit-à-bit identique. |
| **`lot6c_menages_externes`** | `LOT6A`, `REF_SRC` | **moyen — PROCHAIN SUR LA LISTE** | **Aucun mode SQLite, aucune écriture SQLite.** C'est lui, et lui seul, qui impose encore un workspace Excel à la recette de chaîne — donc l'export legacy de lot6b. Le migrer libère les deux. |
| `lot6a`, `lot6d`, `lot6e` | classeurs amont | moyen | chaîne ménages, migrable après lot6c |
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

## 3bis. La dernière dépendance de lot6b, nommée

`lot6b` n'écrit plus de classeur par défaut. Son export legacy (`--export-legacy`) subsiste pour
**un seul appelant**, et un test vérifie qu'il reste seul :

> `menages_chaine_service` — la **recette de chaîne complète** de l'écran `/menages/chaine`, qui
> rejoue lot6b → lot6c → lot6d → lot6e → lot6f → lot11 dans un workspace isolé.

Pourquoi elle reste Excel : elle exécute **`lot6c`**, qui n'a ni `--source SQLITE` ni la moindre
écriture SQLite. Tant que lot6c lit et écrit des classeurs, le workspace de recette doit en
contenir — donc lot6b doit pouvoir en produire.

La chaîne de dépendance est courte et nommée : **migrer lot6c libère la recette, qui libère
l'export de lot6b.** Aucune autre raison ne maintient ce code en vie.

Ce qui est déjà acquis sans attendre : **le parcours opérationnel ne peut plus produire de classeur
par inadvertance.** Il fallait auparavant penser à passer `--sans-excel` ; il faut désormais
demander explicitement le contraire. Le commentaire qui justifiait l'ancien défaut invoquait un
blocage — « lot11 lit encore le classeur M04 » — **déjà levé depuis** : les deux implémentations de
lot11 lisent `menages_declarations_internes`. Le blocage avait survécu à sa propre disparition.

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

**`lot6c` d'abord** — c'est le seul verrou qui maintienne encore un workspace Excel dans la recette
ménages, et donc l'export legacy de `lot6b` (cf. §3bis). Ensuite `lot1`/`lot8b` (référentiel seul,
déjà en SQLite) → `lot6a/6d/6e` (leurs modes EXCEL deviennent alors du code mort) → `lot3`
(charges ; la table est déjà canonique côté application) → `lot5`, `lot7` →
`lot4bis/ter/quater` **en dernier** : c'est le cœur du résultat économique, et c'est là que le coût
d'une erreur est maximal.

# Arbitrages du banc — HR, arrondis, A_CONTROLER, observabilité, lot6b

**Date** : 2026-09-11 · **Branche** : `resume/pilotage-conciergerie-20260909` · **HEAD de départ** : `a3f718d`
**Migrations ajoutées** : `0079` (exclusion explicite), `0080` (vocabulaire de contrôle unique)

---

## §A — Continuité

| | |
|---|---|
| Worktree | `C:\Users\Ewans\.devswarm\repos\1\9537f8ef\resume-pilotage-conciergerie-20260909` |
| Branche | `resume/pilotage-conciergerie-20260909` |
| HEAD au départ | `a3f718d6e4cafdfba0498859756eb74ecd7568d2` — conforme |
| Base | `05_APPLICATION/data/app.db`, schéma **0078 → 0080** |
| Intégrité au départ | `integrity_check = ok`, `foreign_key_check = 0` |
| Sauvegardes | `BCK-0AA3408E2436` (avant tout), `BCK-5C367656E6E8` (avant migrations + actualisation) — les deux **VALIDE** |

Numérotation des migrations : `0049` manquant, antérieur à cette mission ; le compte fichiers (77)
et le compte en table concordent, donc sans effet.

Toutes les expériences destructives ont tourné sur **copies isolées** de la base réelle.

---

## §B — `HR` sur les réservations : ce qu'il portait réellement

Les 125 lignes ne sont pas « 80 séjours propriétaire + 45 legacy » comme on pouvait le lire. Elles
sont **125 séjours propriétaire**, dont 45 tombent dans des mois de bascule :

| `statut_controle` | `source` | `canal` | Lignes | Montant | Payout | Assiette | Ménage | Nuits |
|---|---|---|---|---|---|---|---|---|
| `EXCLU_RESULTAT` | `OWNERSTAY_EXCLU` | OWNERSTAY | 80 | 0,00 € | 0,00 € | 0,00 € | 0,00 € | 305 |
| `EXCLU_LEGACY` | `OWNERSTAY_EXCLU` | OWNERSTAY | 45 | 0,00 € | 0,00 € | 0,00 € | 0,00 € | 80 |

Toutes sur **un seul logement** (`LOG_0004` / `PROP_0004`), réparties de 2026-03 à 2027-01,
190 voyageurs, 385 nuits, **0,00 € partout**.

**Le constat qui a décidé de la suite :** `HR` était **triplement redondant**. Le filtre économique
est le même partout depuis l'origine :

```
statut_controle = 'VALIDE' AND impact_resultat_reel = 'OUI' AND montant_retenu <> 0
```

Les 125 lignes y échouent aux **trois** conditions. Mieux : `lot4bis` forçait déjà l'impact à
`NON/NON` dès que `statut_controle = EXCLU_RESULTAT`, **quel que soit le code d'impact**. Le code
`HR` ne décidait donc rien — il répétait, dans un champ prévu pour autre chose, ce qu'un statut
disait déjà.

Un quatrième usage existait, sans rapport : `lot7` marquait ses lignes de **suivi associé** avec
`HR`, et un contrôle exigeait cette valeur. Un cinquième, dormant : `ref_types_flux.TYPE_FLUX_005`
(remboursement associé) avait `HR` pour défaut — utilisé par **0 charge et 0 flux**.

---

## §C — `EXCLU` après : la migration, et la preuve

### Le modèle retenu — aucun nouveau moteur, aucun nouveau code

Un **code d'impact** décrit COMMENT une somme pèse sur l'économie. Il ne sait pas dire qu'une ligne
en est **absente** : c'est le rôle d'un statut. `HR` confondait les deux.

L'exclusion existait déjà — `statut_controle = EXCLU_RESULTAT`, vocabulaire partagé par lot4bis,
lot5, lot7 et la saisie HH. Il ne manquait que le **motif**, jusque-là en texte libre dans
`commentaire` (« Hostaway ownerStay — exclu résultat »), donc non requêtable.

| Avant | Après |
|---|---|
| `code_impact = 'HR'` | `code_impact = NULL` — pas d'impact neutre : **pas d'impact** |
| motif dans `commentaire` | `motif_exclusion = 'OWNERSTAY'` — colonne indexée |
| exclusion dite deux fois | dite une fois, par `statut_controle` |

Vocabulaire canonique : `02_TRAVAIL/lib_db_moteur.py` (`STATUTS_EXCLUSION`, `MOTIFS_EXCLUSION`,
`est_exclue()`, `motif_exclusion_pour()`, `impacts_reservation()`).

**Effet collatéral utile** : la migration a posé un motif sur *toutes* les lignes exclues, pas
seulement les 125. **6 425 réservations** sont exclues — 6 300 `LEGACY_SANS_ARCHIVE_ORIGINE` en
plus des 125 `OWNERSTAY`. Ce volume était invisible avant, faute de champ pour le compter.

### La preuve — deux copies, même photographie, recalcul complet

Le flux unifié a été **reconstruit** (`flux_unifie_service.construire()`) des deux côtés. Comparer
les tables en l'état n'aurait rien prouvé : la migration ne les touche pas. C'est le **recalcul**
qui devait rendre la même chose.

| Indicateur | AVANT | APRÈS | Écart |
|---|---|---|---|
| Réservations totales | 7 909 | 7 909 | **0** |
| Retenues (vue flux) | 1 350 | 1 350 | **0** |
| Exclues par statut | 6 425 | 6 425 | **0** |
| CA retenu | 317 803,23 € | 317 803,23 € | **0,00 €** |
| CA total | 1 585 185,58 € | 1 585 185,58 € | **0,00 €** |
| Assiette commission | 257 640,31 € | 257 640,31 € | **0,00 €** |
| Payout | 316 687,31 € | 316 687,31 € | **0,00 €** |
| Ménage retenu | 59 047,00 € | 59 047,00 € | **0,00 €** |
| Nuits | 29 613 | 29 613 | **0** |
| **Flux reconstruits** | 336 / 73 632,83 € | 336 / 73 632,83 € | **0,00 €** |
| **Commission conciergerie** | 47 678,33 € | 47 678,33 € | **0,00 €** |
| **Résultat** | 650 888,68 € | 650 888,68 € | **0,00 €** |
| **Net propriétaire** | 220 528,97 € | 220 528,97 € | **0,00 €** |
| Total produits / charges | 667 299,52 € / 16 410,84 € | idem | **0,00 €** |
| CA par mois (12 mois) | — | — | **0 sur chaque mois** |

**Écart économique : 0,00 €.** Les seules différences sont la disparition de `HR` et l'apparition
des motifs — c'est-à-dire exactement ce qui était demandé.

---

## §D — `HR` a disparu du modèle actif

| Emplacement | Avant | Après |
|---|---|---|
| `reservations_resolues.code_impact` | 125 | **0** |
| `reservations_calculees.code_impact` | 125 | **0** |
| `ref_codes_impact` | ligne `HR` | **supprimée** (2 codes restants) |
| `ref_types_flux.code_impact_defaut` | `TYPE_FLUX_005 = HR` | **NULL** |
| `flux_unifies` | 0 | 0 |
| Moteurs (`lot4bis`, `lib_lot4a`, `lot4quater`, `lot9`, `lot7`, `lib_controles_avantages`, `lot7_ik`) | valeur produite | **aucune** |
| `flux_unifie_service._IMPACT_FLAGS` | entrée `HR` | **retirée** |
| UI saisie HH | proposait `HR` | plus proposé (l'UI lit `ref_codes_impact`) |

`TYPE_FLUX_005` passe à `NULL` plutôt qu'à `IC` ou `HC` : sa neutralisation n'a jamais dépendu de ce
code — elle vient de `sens_flux = REMBOURSEMENT`, que lot3 traduit en `sens = NEUTRALISATION`. Lui
inventer un impact aurait changé ce que fait un futur remboursement.

**Une base neuve ne recrée pas `HR`, et un réimport non plus.** Le classeur `REF_Setup.xlsm` en
contient toujours une ligne : elle est désormais **écartée et signalée** à l'import
(`ref_setup_import_service.LIGNES_RETIREES`), jamais corrigée en silence — corriger reviendrait à
décider à votre place ce que `HR` devient.

Le mot subsiste dans des **docstrings** qui expliquent la migration. C'est voulu : viser le texte
brut obligerait à effacer l'explication pour faire passer un test. Le garde-fou ignore donc
commentaires et docstrings, et n'accepte aucune occurrence dans le code.

---

## §E — Arrondis : aucun centime ne disparaît

### Le défaut

Le dépôt portait **quatre** répartitions monétaires et **trois** comportements :

| Implémentation | Règle du résidu | Conséquence |
|---|---|---|
| `charges_engine.repartir_egal` | aux premiers de l'ordre trié | correcte |
| `lib_charges_menage` | **le dernier absorbe tout** | somme exacte, mais la dernière ligne peut s'écarter de plusieurs centimes de sa part réelle |
| `facture_ventilation_menage_service` | idem | idem |
| **`lot6f`** | **aucun rattrapage** | `round(pool × poids / total, 2)` ligne à ligne : **100,00 € sur 3 ventilaient 99,99 €** |

### La règle, désormais unique

`02_TRAVAIL/lib_repartition.py`, module pur, sans dépendance :

1. convertir en **centimes entiers** — jamais de flottants intermédiaires ;
2. donner à chacun la **partie entière** de sa part exacte ;
3. distribuer les centimes restants aux parts dont le **reste fractionnaire est le plus grand** —
   celles que l'arrondi a le plus lésées ;
4. à reste égal, départager par la **clé triée**.

Conséquence du point 4 : à poids égaux, tous les restes sont égaux, donc les premières clés triées
reçoivent les centimes. **100,00 € / 3 = 33,34 / 33,33 / 33,33.**

`somme(parts) == montant`, au centime, **toujours** — y compris sur un avoir (montant négatif).
Le bénéficiaire du résidu ne dépend jamais de l'ordre d'un curseur SQL sans `ORDER BY`.

| Cas | Résultat | Somme |
|---|---|---|
| 0,01 € / 3 | 0,01 / 0,00 / 0,00 | 0,01 € |
| 0,10 € / 3 | 0,04 / 0,03 / 0,03 | 0,10 € |
| 1,00 € / 3 | 0,34 / 0,33 / 0,33 | 1,00 € |
| 100,00 € / 3 | **33,34 / 33,33 / 33,33** | 100,00 € |
| 100,01 € / 3 | 33,34 / 33,34 / 33,33 | 100,01 € |
| 700,00 € / 2 | 350,00 / 350,00 | 700,00 € |
| 100,00 € / 7 | 14,29 ×4, 14,28 ×3 | 100,00 € |
| 10,01 € / 4 | 2,51 / 2,50 / 2,50 / 2,50 | 10,01 € |
| −100,00 € / 3 | −33,34 / −33,33 / −33,33 | −100,00 € |
| 300 € pondérés 8/1/5 | 171,43 / 21,43 / 107,14 | 300,00 € |

Les quatre points d'appel utilisent la même règle. `charges_engine.repartir_egal` subsiste (le
moteur Charges est un module PUR, sans accès au dossier des moteurs) mais un test verrouille son
accord avec la règle canonique : la duplication est tolérée, **le désaccord ne l'est pas**.

---

## §F — `A_CONTROLER` : exclue du calcul, visible dans les contrôles

### La règle

**Seule `VALIDE` entre dans les calculs.** `A_CONTROLER`, `ANOMALIE`, `REJETE` et une colonne
**vide** en sont exclus — l'absence d'avis ne vaut pas accord. Appliqué au coût complet ménage,
donc au résultat analytique et aux flux qui en découlent.

L'asymétrie précédente était difficile à défendre : une **facture** externe non validée était déjà
exclue, alors qu'une **charge** non contrôlée entrait.

### Le défaut découvert en chemin

Deux parcours écrivaient la **même colonne** `charges.statut_controle` avec **deux mots** :

- « Charges à contrôler » → `VALIDE` ;
- « Valider la charge » sur la fiche → `CONFORME`.

Or la chaîne économique ne connaît que `VALIDE`. **Une charge validée depuis la fiche restait
invisible au résultat** — l'écran affichait « conforme », la dépense ne pesait nulle part, et rien
ne le signalait. Unifié sur `VALIDE` par la migration 0080 (0 ligne concernée sur la base réelle :
le vocabulaire est fixé avant que le cas ne se présente).

### Le pendant, sans lequel la règle serait dangereuse

Une charge écartée ne doit pas s'évaporer. Deux traces :

1. **`CHARGE_MENAGE_NON_VALIDEE`** — émis par lot6f à chaque run, avec le nombre, le montant et les
   identifiants. Les contrôles sont désormais affichés **quel que soit le mode de sortie** : ils
   étaient construits après le court-circuit `--sans-excel`, si bien qu'un run sans classeur n'en
   disait rien.
2. **`CHARGE_NON_VALIDEE_HORS_CALCULS`** — contrôle **BLOQUANT** de lot11, qui **empêche la clôture
   du mois**. Sur la base réelle : **4 charges, 988 €**, passées d'« intégrées en silence » à
   « exclues et bloquantes ».

Le cycle complet est testé : création → absente du résultat → contrôle présent → validation →
présente **une seule fois** → recalcul sans doublon.

---

## §G — Observabilité

| | Avant | Après |
|---|---|---|
| Lien du menu (`base.html`) | `/observabilite` | `/observabilite/runs` |
| `GET /observabilite` | **404** | **308** → `/observabilite/runs` |
| `GET /observabilite/runs` | 200 | 200 |
| Navigation principale | 15/16 | **16/16** |

Le lien du menu est corrigé **et** la redirection ajoutée : une adresse mise en favori ou collée
dans un message doit continuer de fonctionner. `308` (permanent) parce que la cible est définitive.

---

## §H — lot6b avant

| Élément | État constaté |
|---|---|
| Rôle métier | Déclarations de ménages internes : Google Sheet M04 → normalisation Python → SQLite |
| Source externe | Google Sheet « Suivi ménage », **légitime** ; URL lue dans `ref_sources_systeme` (SRC_011), jamais en dur |
| Sortie canonique | `menages_declarations_internes` (SQLite) |
| Sorties Excel | `MASTER_NORM_Declarations_Internes.xlsx`, `M04_MENAGES_PowerQuery.xlsx` — **écrites par défaut** |
| Lectures Excel runtime | aucune (le repli `--url-depuis-excel` est opt-in explicite) |
| Interpréteur | `cfg.LOT4A_ENGINE_PYTHON` = `C:\Program Files\Python312\python.exe` **codé en dur, absent du poste** |
| Consommateurs aval | lot6d/6e/6f lisent **SQLite** (`--source SQLITE`), lot11 aussi |

**Le blocage documenté n'existait plus.** Le code justifiait l'export par défaut ainsi : « lot11 lit
ENCORE le classeur M04 ». Faux : les deux implémentations de lot11 lisent
`menages_declarations_internes` — le moteur le dit lui-même (« M04 : plus de classeur au runtime »).
Le blocage avait survécu à sa propre levée.

---

## §I — lot6b après

**SQLite est le DÉFAUT.** Aucun run ne peut plus produire un classeur sans le demander : il fallait
penser à `--sans-excel`, il faut désormais demander explicitement `--export-legacy`.

### Fraîcheur (§13) — la cause, puis la correction

La base portait la photographie du **2026-09-02** alors que la feuille avait changé. Ce n'était ni
un oubli ni un bug de calcul :

> `MENAGES_ACTUALISATION` a **échoué trois fois**, chacune en **0,0 s**, sur
> `ECHEC: préflight HOSTAWAY credentials absentes`.

Le préflight Hostaway faisait un `return` **avant** le PDF et la Google Sheet — deux sources qui
n'ont aucune dépendance à Hostaway. Une source indisponible en gelait deux autres, sans que rien ne
le dise. Le mécanisme de résultat **PARTIEL** existait pourtant juste en dessous.

Corrigé : le préflight gate **son étape**, plus toute la chaîne. Le sous-processus Hostaway n'est
toujours pas lancé pour rien (l'intention d'origine est préservée), et un résultat partiel n'est
jamais présenté comme un succès.

### Actualisation réelle (§25), après sauvegarde `BCK-5C367656E6E8`

| | Avant | Après |
|---|---|---|
| `menages_declarations_internes` | 31 lignes | **39 lignes** |
| `date_extraction` | 2026-09-02T17:44:21 | **2026-09-11T17:22:03** |
| Mois couverts | 2026-03 → 2026-07 | 2026-03 → **2026-08** |
| Ménages par mois | 28 / 32 / 34 / 28 / 42 | 28 / 32 / 34 / 28 / **41** / **25** |
| Classeurs écrits | — | **aucun** |
| Conflits Sheet/Application | — | **0** |

Juillet passe de 42 à 41 : c'est la correction identifiée lors de la mission précédente
(`LOG_0005`/`INT_0001`, passée de 1 à 0 dans la feuille). Août apparaît (25 ménages).

**Reste à faire** : `menages_cout_complet` (29 lignes) reflète encore les déclarations d'avant. Un
recalcul ciblé par mois depuis `/menages` suffit ; il n'a pas été déclenché ici pour ne pas modifier
de résultat économique hors du périmètre de la mission.

### Interpréteur (§15)

`cfg.LOT4A_ENGINE_PYTHON` a pour défaut `sys.executable`, la variable d'environnement restant
prioritaire. Les deux interpréteurs existent et pointent sur le venv du projet.

---

## §J — Comparaison legacy / SQLite (lot6b)

Deux copies isolées de la base réelle, **même source**, même moment :

| Indicateur | LEGACY (avec classeurs) | SQLite seul | Écart |
|---|---|---|---|
| Lignes de déclarations | 39 | 39 | **0** |
| Lignes `menages_declarations_extra` | 39 | 39 | **0** |
| Total ménages | 188 | 188 | **0** |
| Contenu ligne à ligne | — | — | **identique** (mois, logement, intervenant, nb, heures, lavage, statuts) |
| Conflits détectés | 0 | 0 | **0** |

**Écart : 0.** L'écriture des classeurs n'apporte rien au résultat métier.

---

## §K — Excel supprimé

| Élément | État |
|---|---|
| `lot6f` | **0 lecture de classeur** (mission précédente) |
| `lot6b` — parcours opérationnel | **0 classeur touché**, prouvé par audit hook CPython |
| `lot6b` — défaut | inversé : SQLite par défaut, export sur demande explicite |
| Interpréteur codé en dur | supprimé |

**Ce qui n'a PAS été supprimé, et pourquoi** — la réponse est nette et tient en une chaîne :

> `--export-legacy` subsiste pour **un seul appelant** : `menages_chaine_service`, la recette de
> chaîne complète de l'écran `/menages/chaine`. Elle reste Excel parce qu'elle exécute **`lot6c`**,
> qui n'a **ni mode SQLite ni aucune écriture SQLite**.
>
> **Migrer `lot6c` libère la recette, qui libère l'export de `lot6b`.** Aucune autre raison ne
> maintient ce code en vie, et un test vérifie qu'aucun second appelant n'apparaît.

Supprimer l'export aujourd'hui aurait cassé une recette de vérification vivante (694 lignes + écran
+ tests) sans la remplacer — c'est-à-dire retiré un filet de sécurité, pas du legacy.

---

## §L — Excel restants

Inventaire complet et révisé : **`RESTANT_EXCEL_OPERATIONNEL.md`**, avec producteur, consommateur,
raison, cible SQLite et priorité pour chaque moteur.

- **Zéro lecture** : `lot6f`, `lot6b` (runtime), `lot4a_compare`, `lot7_pq_avantages` ;
- **Prochain sur la liste** : **`lot6c`** — seul verrou de la recette ménages ;
- puis `lot1`/`lot8b` → `6a`/`6d`/`6e` → `lot3` → `lot5`/`lot7` → `lot4bis/ter/quater` **en dernier**
  (cœur du résultat économique) ;
- **À conserver** : `REF_Setup.xlsm`, `SAISIE_Charges_Flux.xlsx`, relevés bancaires, PDF — pièces
  d'origine, explicitement hors périmètre. Aucune n'a été supprimée.

---

## §M — Tests

| Suite | Avant | Après |
|---|---|---|
| **Moteurs** | 407 ✅ · **5** ❌ · 1 ⏭ | **407 ✅ · 5 ❌ · 1 ⏭** — les **5 mêmes**, mesurées sur un arbre `HEAD` propre |
| **Application** | 3275 ✅ · **1** ❌ · 52 ⏭ | **3353 ✅ · 1 ❌ · 50 ⏭** (25 min 18 s) |

**+78 tests qui passent, et le même unique échec qu'avant.** Il est identifié précisément :
`test_banque_lot8_present_les_tests_gardes_s_executent`, qui affirme
`Path(cfg.MASTER_BANQUE).exists()` — le classeur bancaire est absent de ce worktree. Antérieur à
cette mission, sans rapport avec elle.

**Tests ajoutés** (4 fichiers) :

| Fichier | Objet |
|---|---|
| `test_hr_exclu_reservations.py` | 21 tests — vocabulaire, migration sans perte, base neuve sans `HR`, réimport neutralisé, aucun moteur producteur, UI HH |
| `test_repartition_monetaire.py` | 30 tests paramétrés — invariant de somme, déterminisme, indépendance à l'ordre, avoirs, poids nuls, accord entre les 4 points d'appel |
| `test_lot6b_anti_excel.py` (étendu) | défaut SQLite prouvé par audit hook, export explicite, appelant unique, interpréteur réel |
| `test_lot6f_charges_menage_ventilation.py` (étendu) | `A_CONTROLER`/`ANOMALIE`/`REJETE`/vide exclus, contrôle émis, cycle validation sans doublon, somme = pool sur 7 montants |
| `_espion_ouvertures.py` | helper partagé — **un seul** script d'audit dans le dépôt |

**Tests mis à jour, jamais contournés** — chacun documente dans sa docstring pourquoi le contrat a
changé : `test_menages_actualisation_robustesse` (préflight partiel), `test_controles_avantages`,
`test_lot7b_suivi_associes`, `test_lot11_avantages_integration` (suivi sans code d'impact),
`test_menages_cave_et_pools` (répartition en une fois), `test_lot6f_charges_menage_ventilation`
(le centime est attribué, plus perdu), `test_lot4bis_ref_hh_sqlite` (schéma aligné).

**Un vrai défaut corrigé au passage** : le rapport final de `lot4bis` triait `code_impact`
directement — avec des valeurs désormais `None`, il levait un `TypeError` **après** l'écriture,
faisant échouer le lot sur son propre affichage.

---

## §N — Intégrité

| Contrôle | Résultat |
|---|---|
| `integrity_check` | **ok** |
| `foreign_key_check` | **0 violation** |
| Schéma | **0080** |
| Migration 0079 — base vierge | colonnes présentes, 0 `HR`, idempotente (3 applications) |
| Migration 0079 — copie réelle | 7 909 lignes conservées, motifs posés, 0 `HR` |
| Migration 0080 — portée réelle | 0 ligne (vocabulaire fixé par anticipation) |
| Seconde exécution du runner | sans effet |
| Aucune migration ancienne modifiée | vérifié |

---

## §O — `F-11/0-000001`

**INTACTE.** Non annulée, non modifiée, non renumérotée, non supprimée. Les 26 factures
propriétaires sont inchangées, les 5 charges aussi (identifiants, montants, statuts).

---

## §P — Reset

**NON EXÉCUTÉ.** Aucune donnée de recette supprimée, aucun compteur remis à zéro, aucune base
septembre reconstruite. `PLAN_RESET_PRODUCTION_APRES_RECETTE.md` est mis à jour — et seulement
mis à jour : migrations 0079/0080 à ne pas contredire, point de fraîcheur désormais **résolu**,
nouveau prérequis (recalculer `menages_cout_complet`, aucun contrôle bloquant ouvert).

Septembre 2026 reste le premier mois opérationnel final, et le premier éligible à clôture une fois
terminé.

---

## §Q — Instance

| | |
|---|---|
| URL | `http://127.0.0.1:8000` |
| Instance | **une seule** en écoute sur 8000 |
| Redémarrage | effectué après migrations 0079/0080 |
| Pages exigées | **10/10** — `/health`, `/logements`, `/fournisseurs`, `/menages`, `/factures-proprietaires`, `/creances`, `/comptabilite`, `/clotures`, `/observabilite` (308), `/observabilite/runs` |
| Navigation complète | **16/16**, aucun lien mort |
| Scheduler | **OFF** — `ORDONNANCEUR_ACTIF = False`, `demarrer()` refuse explicitement |
| CleaningTasks auto | **OFF** — aucun déclenchement automatique |

---

## §R — Verdict

> ## HR SUPPRIMÉ, LOT6B SQLITE PARTIEL — LIMITATION IDENTIFIÉE ET NOMMÉE

**`HR` n'existe plus comme code métier**, sur aucun axe. L'effet d'exclusion des réservations est
conservé explicitement, sous un statut et un motif requêtable, à **0,00 € d'écart** sur le CA, les
commissions, le résultat, le net propriétaire et les flux reconstruits. Une base neuve ne le recrée
pas ; un réimport du classeur ne le ressuscite pas.

**Aucun centime ne disparaît** : une seule règle de répartition dans tout le dépôt, invariant de
somme vérifié sur des cas paramétrés, résidu attribué de façon déterministe et documentée.

**Une charge non validée n'affecte plus aucun résultat** — et ne disparaît pas pour autant : elle
bloque la clôture tant qu'elle n'est pas tranchée. Le vocabulaire de validation, qui existait en
deux mots dans la même colonne, n'en a plus qu'un.

**`lot6b` est SQLite par défaut** : le parcours opérationnel ne peut plus produire un classeur, la
preuve est comportementale, l'équivalence est à écart 0, et la source a été réellement actualisée
après huit jours de blocage silencieux.

**Ce qui reste, et pourquoi c'est « PARTIEL »** : le code d'export de lot6b existe toujours, pour un
unique appelant — la recette de chaîne `/menages/chaine`, qui reste Excel parce que **`lot6c` n'a
aucun mode SQLite**. Le supprimer aujourd'hui aurait retiré un filet de sécurité, pas du legacy.
La dépendance est courte, nommée, testée, et la prochaine étape est évidente : **migrer `lot6c`**.

# 97 — Inventaire Excel du runtime applicatif

Inventaire **mesuré** (grep sur `05_APPLICATION/app/`), pas déclaré. Il répond à une seule question :
**reste-t-il un endroit où l'application LIT un master CALCULÉ comme source de données ?**

Les cinq catégories sont celles de la mission :

| Catégorie | Définition | Légitime ? |
|---|---|---|
| `SOURCE_EXTERNE` | classeur fourni par l'utilisateur ou un tiers (référentiel, saisie, relevé) | oui |
| `EXPORT_OPTIONNEL` | classeur/CSV ÉCRIT, que personne ne relit dans le système | oui |
| `LEGACY_PARITE` | ouvert pour comparer ou vérifier une empreinte, jamais comme source | oui |
| `ADAPTATEUR_MOTEUR` | classeur jetable FABRIQUÉ depuis SQLite pour un lot pandas non migré | transitoire |
| `BUG_RUNTIME` | l'application lit un master CALCULÉ comme source | **doit être 0** |

---

## 1. Résultat

**BUG_RUNTIME = 0.**

Vérification :

```
grep -rn "MASTER_CALC_\|MASTER_CTRL_\|MASTER_FACT_\|MASTER_NORM_" app/readers app/services app/routes \
  | grep -E "load_workbook|read_sheet|list_sheets"
→ aucune ligne
```

Deux `BUG_RUNTIME` ont été trouvés et corrigés pendant cette mission — tous deux invisibles, parce
que le fichier existait toujours et se lisait sans erreur :

| Module | Lisait | Conséquence réelle | Corrigé en |
|---|---|---|---|
| `readers/controles_cloture_reader.py` | `MASTER_CTRL_Coherence.xlsx` | les écrans servaient les contrôles du **dernier calcul legacy**, pas du calcul courant | `controles_lot11_constats`/`_champs`/`dashboard_mois` |
| `readers/proprietaires_reader.py` | `MASTER_FACT_Proprietaires.xlsx` | les préfactures — dont celles proposées à la **facturation propriétaire émise** — venaient du dernier calcul legacy | `lot12_prefactures_entete`/`_lignes` |

---

## 2. Ce qui reste, par catégorie

### 2.1 `SOURCE_EXTERNE` — légitime

`REF_Setup.xlsm` (référentiel) et les saisies humaines : `SAISIE_Charges_Flux`,
`SAISIE_Charges_Impacts`, `SAISIE_AirCover`, `SAISIE_ImputationsAirbnb`,
`SAISIE_Ajustements_PostCloture`, `SAISIE_AcomptesProprietaires`, `SAISIE_ReservationsHorsHostaway`,
plus le relevé bancaire.

Modules concernés : `ref_setup_reader`, `ref_setup_hh_reader`, `ref_setup_import_service`,
`saisie_charges_*`, `saisie_hh_*`, `proprietaires_extras_reader`, `charges_*`,
`logements_creation_service`, `logements_gestion_service`, `banques_import_service`.

Ce sont des **entrées**, pas des intermédiaires de calcul. Elles restent Excel tant que la saisie
n'est pas dans l'application — chantier distinct, hors périmètre de cette mission.

### 2.2 Chaînes NON MIGRÉES — dépendance réelle, assumée

| Module | Ouvre | Pourquoi c'est encore Excel |
|---|---|---|
| `readers/charges_reader.py` | `MASTER_FACT_MAN_Charges` (Lot3) | Charges n'est pas migré. Frontière documentée, également citée par `flux_unifie_service` (module CHG). |
| `readers/controles_detail_reader.py` | `MASTER_FACT_MEN_MenagesExternes`, onglet `VUE_ECART_HOSTAWAY` (Lot6c) | `lot6c` n'a pas de mode SQLite. Sert le DÉTAIL des écarts ménages ; le CONSTAT, lui, est calculé en SQLite par Lot11. |
| `readers/reservations_hh_reader.py` | `MASTER_FACT_MAN_ReservationsHorsHostaway` (Lot4) | réservations hors Hostaway non migrées. |
| `readers/menages_reader.py` | classeurs Lot6 restants + `REF_Setup` (noms propriétaires) | 10/10 sources principales sont SQLite ; le reliquat suit la migration de `lot6b`/`lot6c`. |
| `services/calculs_pipeline_service.py` | sorties de lots | orchestration de la chaîne legacy, distincte de l'orchestrateur de datasets. |

Ces lectures ne portent PAS sur une chaîne migrée : elles portent sur des chaînes dont le moteur
lui-même est encore pandas/Excel. Les migrer suppose de migrer `lot3`, `lot4quater`, `lot6b`,
`lot6c` — explicitement hors périmètre.

### 2.3 `ADAPTATEUR_MOTEUR` — transitoire, et RÉDUIT

`adaptateur_workspace`, `banque_adaptateur_moteur`, `reservations_adaptateur_moteur`,
`hostaway_cleaning_tasks_adaptateur_moteur` fabriquent un classeur JETABLE depuis SQLite pour un lot
pandas qui ne sait pas lire la base.

**Ce qui a disparu :** la boucle `SQLite → XLSX → Lot11 → SQLite`. Lot11 étant SQLite natif,
`controles_runner_service` recalcule directement en base sur une copie ; `controles_lot11_adapter.py`
est supprimé.

**Ce qui reste :** ces adaptateurs sont encore appelés par `menages_chaine_service`, qui orchestre la
chaîne Ménages — laquelle contient `lot6b` et `lot6c`, sans mode SQLite. Ce n'est donc **pas** une
boucle artificielle : c'est la seule façon d'alimenter un moteur qui ne lit que des classeurs. Elle
disparaîtra avec la migration de ces deux lots.

`reservations_adaptateur_moteur._vue_flux` est aussi utilisé par `flux_unifie_service` — mais comme
**fonction pure** (le filtre du sous-ensemble VUE_FLUX), sans écriture de classeur.

### 2.4 `EXPORT_OPTIONNEL`

`services/lot13_export_service.py` — écrit les 13 CSV Power BI + le dictionnaire. N'en relit aucun.
L'application ne lit rien de `03_EXPORTS/` : les seules références restantes sont la configuration,
un diagnostic d'existence dans `health`, et la déclaration de sortie du pipeline.

### 2.5 `LEGACY_PARITE`

`hostaway_adaptateurs`, `hostaway_cleaning_tasks_adaptateur` — reprises en lecture seule d'un master
vers SQLite, utilisées pour amorcer ou comparer. `controles_runner_service` et
`menages_chaine_service` nomment encore des chemins de masters pour en **vérifier l'empreinte**
(« le réel n'a pas bougé ») : nommer un chemin n'est pas le lire.

---

## 3. Réponses aux questions de la mission

**« INTERMÉDIAIRE EXCEL OBLIGATOIRE = 0 ? »** — **Oui pour les chaînes migrées.** Hostaway →
Réservations → Lot9 → Lot10 → Lot11 → Lot12 → Lot13 se calcule intégralement de SQLite à SQLite,
sans qu'aucun classeur ne soit nécessaire à un moment quelconque. **Non pour les chaînes non
migrées** : `lot6b`/`lot6c` exigent encore un classeur, produit par un adaptateur jetable.

**« BUG_RUNTIME = 0 ? »** — **Oui**, vérifié par grep et verrouillé par cinq tests bloquants
(`test_lot9_sans_master_calc_flux`, `test_lot10_sans_masters`, `test_lot11_sans_masters`,
`test_lot12_sans_masters`, `test_master_ctrl_coherence_zero_runtime`, `test_menages_sans_excel`).

**« ZERO EXCEL OPÉRATIONNEL ? »** — **Pas encore, et il serait faux de l'écrire.** Excel n'est plus
un intermédiaire obligatoire du cœur économique, et plus aucun écran ne lit un master calculé. Mais
Excel reste opérationnellement nécessaire pour : les SAISIES (aucune n'est dans l'application), le
RÉFÉRENTIEL (`REF_Setup.xlsm` reste la source d'import), et les chaînes Lot3 / Lot4quater /
Lot6b / Lot6c.

La formule exacte qui décrit l'état réel est :

> **Zéro master calculé lu par l'application. Zéro intermédiaire Excel obligatoire dans le cœur
> économique migré. Excel subsiste comme source externe (saisies, référentiel) et pour les lots
> non encore migrés.**

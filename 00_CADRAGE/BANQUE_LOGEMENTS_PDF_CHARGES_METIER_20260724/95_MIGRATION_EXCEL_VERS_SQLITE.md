# 95 — Migration Excel → SQLite : inventaire et état

Inventaire **mesuré**, pas déclaré : produit en croisant les fichiers présents, les scripts qui les
écrivent, et les modules applicatifs qui les nomment.

**54 fichiers** Excel/CSV hors archives, données de recette et artefacts runtime.

---

## 1. Synthèse

| Classe | Nombre | Cible d'architecture |
|---|---|---|
| `SOURCE_EXTERNE` | 1 | **Reste** — import ponctuel autorisé |
| `TABLE_SQLITE_SOURCE` | 1 | Migré en base (0029), **encore lu en Excel** |
| `SAISIE_INTERNE` | 7 | À migrer — saisie doit passer par l'application |
| `CALCUL_INTERMEDIAIRE` | 29 | À migrer — tables dérivées SQLite |
| `EXPORT_OPTIONNEL` | 14 | Doivent devenir terminaux |
| `A_CLASSER` | 2 | À qualifier |

---

## 2. Le constat qui compte

La migration du référentiel est faite **au niveau du stockage** et pas encore **au niveau de la
consommation**.

`REF_Setup.xlsm` est importable dans les 28 tables `ref_*` depuis la migration 0029. Il reste
pourtant lu par **18 modules applicatifs**. Le repository `ref_setup_repo`, lui, n'a **qu'un seul**
consommateur : l'écran d'import qui l'a créé.

Autrement dit : la base contient le référentiel, personne ne s'en sert.

Même écart côté exports. `03_EXPORTS/PowerBI/` est censé être terminal (§19). Cinq de ces CSV sont
lus par l'application :

| Export | Modules applicatifs qui le lisent |
|---|---|
| `PBI_Referentiel_Logements.csv` | 4 |
| `PBI_Referentiel_Gestion_Logements.csv` | 2 |
| `PBI_Referentiel_Proprietaires.csv` | 1 |
| `PBI_Controles_Ouverts.csv` | 1 |
| `PBI_Flux.csv` | 1 |

C'est la boucle interdite par le §39 : **SQLite → Excel/CSV → application**. Elle est transitoire et
documentée depuis le correctif `/logements`, mais elle existe toujours.

---

## 3. `SOURCE_EXTERNE` — conforme à la cible

| Fichier | Producteur | Git |
|---|---|---|
| `01_SOURCES_BRUTES/Banque/2026_03_BRUT_Banque_CreditMutuel.xlsx` | tiers (banque) | non |

Seul fichier légitimement Excel dans l'architecture cible. Aucun module applicatif ne le lit
directement : il passe par `lot8a`.

---

## 4. `SAISIE_INTERNE` — 7 fichiers, tous à migrer

| Fichier | Lu par l'app |
|---|---|
| `SAISIE_Charges_Flux.xlsx` | 10 modules |
| `SAISIE_Charges_Impacts.xlsx` | 5 |
| `SAISIE_AirCover.xlsx` | 4 |
| `SAISIE_Ajustements_PostCloture.xlsx` | 4 |
| `SAISIE_ImputationsAirbnb.xlsx` | 4 |
| `SAISIE_AcomptesProprietaires.xlsx` | 2 |
| `SAISIE_ReservationsHorsHostaway.xlsx` | 2 |

Ce sont des **interfaces de saisie Excel**, explicitement interdites par la règle d'architecture.
`SAISIE_AcomptesProprietaires` est le cas le plus lourd : son passage vers le master se fait par
**Power Query**, actualisation manuelle dans Excel, 192 lignes de code M.

---

## 5. `CALCUL_INTERMEDIAIRE` — 29 fichiers

Les masters. Tous **suivis par Git** sauf `BANQUE_LOT8_IMPORT.xlsx`.

Les plus consommés, et donc les plus structurants à migrer :

| Master | Lu par l'app | Lu par des lots |
|---|---|---|
| `BANQUE_LOT8_IMPORT.xlsx` | 9 | 0 |
| `MASTER_CALC_Flux.xlsx` (Lot9) | 7 | 2 |
| `MASTER_CALC_Resultats.xlsx` (Lot10) | 7 | 2 |
| `MASTER_CALC_NetProprietaire.xlsx` (Lot10) | 7 | 2 |
| `MASTER_CTRL_Coherence.xlsx` (Lot11) | 7 | 2 |
| `MASTER_CALC_Commissions.xlsx` (Lot10) | 6 | 2 |
| `MASTER_NORM_Declarations_Internes.xlsx` | 6 | 0 |
| `MASTER_FACT_MEN_MenagesExternes.xlsx` | 6 | 0 |
| `MASTER_FACT_Proprietaires.xlsx` (Lot12) | 5 | 2 |

La colonne « lu par des lots » est décisive : un master lu par un autre lot est une **dépendance
inter-traitements**, exactement ce que la règle d'architecture interdit. Neuf masters sont dans ce
cas.

---

## 6. `A_CLASSER`

| Fichier | Nature probable |
|---|---|
| `00_CADRAGE/APPLICATION_LOCALE/Catalogue_operations_navigation_conciergerie.xlsx` | document de cadrage, aucun consommateur — sans effet |
| `01_SOURCES_BRUTES/VRBO/IMPORT_UNIQUE_Revenus_*.csv` | import historique ponctuel — conforme au §1 alinéa 1 |

Aucun des deux n'est lu par un module applicatif ni par un lot. Ni l'un ni l'autre n'est une
dépendance opérationnelle.

---

## 7. État par lot

| Lot | Entrée | Sortie | État migration |
|---|---|---|---|
| lot8a | Excel externe | Excel | **non migré** |
| lot8b | **SQLite** (règles) + Excel | Excel | **partiellement migré** |
| lot8c | Excel | Excel | non migré |
| lot1 Hostaway | API | Excel | non migré |
| lot3 charges | Excel | Excel | non migré |
| lot4bis/4ter/4quater | Excel | Excel | non migré |
| lot5 acomptes | Excel + **Power Query** | Excel | non migré |
| lot6a→6f ménages | Excel + Google | Excel | non migré |
| lot9 flux | Excel | Excel | non migré |
| lot10 résultats | Excel | Excel | non migré |
| lot11 contrôles | Excel | Excel | non migré |
| lot12 relevés | Excel | Excel | non migré |
| lot13 export | Excel | CSV | **conforme** (export terminal) — mais l'app lit ses CSV |

**1 lot sur 13 partiellement migré** : `lot8b`, qui lit ses règles de classification en SQLite
depuis cette mission et n'écrit plus dans le référentiel.

---

## 8. Ce qui a été fait dans cette mission

| Point | État |
|---|---|
| PII réelle dans `SEED_RULES` | **corrigé** — seed 100 % synthétique, HEAD nettoyé, historique Git intact |
| `FAMILLE_UZON` dans le code | **corrigé** — disparaît avec le seed |
| `FAMILLE_UZON` dans la **donnée** réelle | **subsiste** — `REF_Setup.xlsm` → `ref_banque_regles`, règle `R_074` |
| Chargement des règles `lot8b` | **fail-closed** — sans référentiel SQLite, le lot refuse de tourner |
| Isolation des snapshots | **corrigé** — chemins lus à l'appel, plus figés à l'import |
| Inventaire Excel complet | **fait** — ce document |

---

## 9. Ce qui n'a pas été fait

Les phases §43 B à N n'ont pas été entreprises : orchestrateur, registre de runs, fraîcheur,
administration du référentiel, chaîne Banque complète en SQLite, Lot5 hors Power Query,
réservations, ménages, Lot9, Lot10, Lot11, Lot12, détachement de l'application vis-à-vis des CSV
Power BI, retrait des masters.

Aucun test de parité n'a été mené, puisqu'aucun chemin SQLite alternatif n'existe pour ces lots.

---

## 10. Ordre recommandé pour la suite

Il découle de l'inventaire, pas d'une préférence :

1. **Détacher l'application des CSV Power BI** — 5 fichiers, 9 points de lecture. C'est le plus
   petit chantier et il supprime la boucle interdite `SQLite → CSV → app`. Les données sont déjà
   en base (`ref_logements`, `ref_gestion_logements_hist`, `ref_proprietaires`).
2. **Faire consommer `ref_setup_repo` par les 18 modules** qui lisent encore `REF_Setup.xlsm`.
3. **Orchestrateur + registre de runs + fraîcheur**, sans quoi rien n'est pilotable.
4. Puis les chaînes, dans l'ordre de leur couplage : Banque, Lot5, réservations, ménages, Lot9,
   Lot10, Lot11, Lot12.

Le critère du §44 s'applique à chaque retrait : producteur SQLite, consommateur SQLite, parité
vérifiée, tests verts, UI fonctionnelle, redémarrage, environnement neuf.

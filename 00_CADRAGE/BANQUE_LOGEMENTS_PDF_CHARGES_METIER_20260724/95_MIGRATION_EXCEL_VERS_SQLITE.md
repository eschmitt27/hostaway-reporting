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
| `01_SOURCES_BRUTES/Banque/BANQUE_ACTUELLE_HISTORIQUE_2025-11-03_2026-08-01.xlsx` | tiers (banque) | non |

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


---

## 11. Mise à jour — l'application est détachée des exports Power BI

### 11.1 Ce qui a changé

| Consommateur | Lisait | Lit désormais |
|---|---|---|
| `logements_service` (liste) | `PBI_Referentiel_Logements.csv` | `ref_logements` |
| `logements_service` (propriétaire) | `PBI_Referentiel_Gestion_Logements.csv` | `ref_gestion_logements_hist` |
| `logements_service` (fiche) | `REF_Setup.xlsm` | `ref_logements`, `ref_types_logements`, `ref_taux_commission`, `ref_couts_*` |
| `home` — logements | `PBI_Referentiel_Logements.csv` | `ref_logements` (parc, hors lignes techniques) |
| `home` — propriétaires | `PBI_Referentiel_Proprietaires.csv` | `ref_proprietaires` |
| `home` — réservations | `PBI_Flux.csv` | `MASTER_CALC_Flux` via `flux_unifie_reader` |
| `home` — contrôles | `PBI_Controles_Ouverts.csv` | `MASTER_CTRL_Coherence` via `controles_cloture_reader` |
| `proprietaires_reader` | `REF_Setup.xlsm` / `REF_Proprietaires` | `ref_proprietaires` |

**Lectures d'export Power BI par l'application : 9 → 0.**

Les deux compteurs du tableau de bord adossés à des masters ne sont pas un recul : ils quittent un
**export** pour une **sortie de moteur**. Ils basculeront quand le moteur écrira en SQLite. La
différence est explicite dans le code (`_compteur_referentiel` / `_compteur_moteur`).

### 11.2 Nouvelle couche

`referentiel_service` s'intercale entre les services d'écran et `ref_setup_repo` :

    route → service métier → referentiel_service → ref_setup_repo → SQLite

Chaque fonction reproduit la sémantique du lecteur Excel qu'elle remplace, y compris l'absence de
tri des taux et la notion volontairement absente de taux « actuel ».

### 11.3 Fail-closed

Sans référentiel importé, les écrans affichent `REFERENTIEL_NON_INITIALISE` et renvoient vers
l'écran d'import. Aucun repli sur le classeur. « Référentiel absent » et « référentiel vide »
restent deux messages distincts.

### 11.4 Parité vérifiée

| Contrôle | Ancien chemin | Nouveau chemin |
|---|---|---|
| Propriétaires réels | 12 (Excel) | 12 (SQLite) |
| Logements du parc | 17 | 17 |
| Rattachements de gestion | 17 | 17 |
| Compteur logements du tableau de bord | **19** | **17** |

Le dernier écart est **assumé** : l'ancien compteur incluait les deux lignes techniques
(`APPARTEMENT_DIVERS`, `LOGEMENT_DIVERS`) que l'écran Logements a toujours exclues. Les deux
affichages disaient des chiffres différents pour la même chose.

Les tests de `test_proprietaires.py` importent le **vrai** référentiel dans une base isolée : si
l'import perdait ou déformait une ligne, leurs compteurs tomberaient.

### 11.5 Consommateurs de `REF_Setup.xlsm` restants

18 modules au début de la mission, **16 à la fin** (`logements_service` et `proprietaires_reader`
migrés). La liste est figée dans `test_non_dependance_fichiers.py` : elle doit décroître, jamais
croître.

Ils se répartissent en deux familles, à traiter par deux missions distinctes :

- **Administration du référentiel** — services qui ÉCRIVENT dans le classeur
  (`logements_creation_service`, `logements_gestion_service`, `ref_assoc_mode_prepare_service`,
  la famille `saisie_hh_*`).
- **Moteur** — services qui pilotent ou contrôlent des lots
  (`calculs_executeur_service`, `calculs_pipeline_service`, `controles_runner_service`,
  `menages_chaine_service`, `menages_recalcul_service`, `file_registry`,
  `charges_preview_service`, `charges_controles_integrite_service`,
  `controles_cloture_reader`, `proprietaires_reglements_reader`).

### 11.6 Services lisant encore un MASTER du moteur (§27)

Frontière d'entrée de la mission suivante :

`flux_unifie_reader`, `controles_cloture_reader`, `controles_detail_reader`, `menages_reader`,
`banques_reader`, `charges_reader`, `proprietaires_reader` (parties `MASTER_*`),
`proprietaires_reglements_reader`, `rapprochement_bancaire_reader`, `reservations_hh_reader`,
`run_log_reader`.

Ces lectures sont **hors périmètre** de la présente mission : elles ne concernent ni un export ni
le référentiel, mais des résultats de calcul.

### 11.7 Bugs trouvés et corrigés

1. **Fiche logement en erreur 500** — le template supposait `etat.fiche` toujours défini. Un
   logement présent en base mais absent du classeur produisait une `UndefinedError`. Corrigé, avec
   la garde placée au niveau du bloc : un `set` Jinja déclaré dans une branche `{% if %}` n'existe
   pas dans les autres, ce qui avait provoqué un second échec.
2. **Liaisons figées à l'import** — `routes/proprietaires_tresorerie.py` et
   `proprietaires_tresorerie_service.py` faisaient `from ... import find_proprietaire`. Le nom
   étant lié au chargement, toute redirection du référentiel restait sans effet. Même famille que
   le défaut `snapshot_service` corrigé précédemment.

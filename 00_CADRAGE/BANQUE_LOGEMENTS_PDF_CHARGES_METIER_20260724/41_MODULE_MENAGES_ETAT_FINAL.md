# 41 — Module Ménages : état

Suite de `40_AUDIT_MENAGES.md` et `41b_AUDIT_MENAGES_CYCLE_DE_VIE.md`. Ce document dit ce qui est
prouvé et ce qui ne l'est pas.

## Statut : **PARTIEL** (cycle de vie construit et prouvé ; module non fermé)

La chaîne de calcul est **verte de bout en bout**. Le **cycle de vie opérationnel a été construit ce
tour** : modèle SQLite, service, contrôles, routes, écrans, recette navigateur réelle. Reste ouvert :
alimentation des pools de courses en recette, rattachement charge exercé en réel (facture exercée),
et le module n'a pas eu de seconde passe de durcissement — voir §7.

## 1. La chaîne passe (correction d'un diagnostic erroné)

`lot6b → lot6c → lot6d → lot6e → lot6f → lot11`, plus l'étape Hostaway : **7/7 OK**, statut
`SUCCES`, `reel_intact = True` (sources réelles vérifiées inchangées par sha256).

Le rapport précédent annonçait « lot6d ECHEC » avec un `TypeError` de comparaison `NoneType`/`str`,
et attribuait la cause au jeu de recette. **C'était faux sur les deux points.** Voir ci-dessous.

## 2. Défaut de confidentialité trouvé et corrigé (ANO-2026-07-27-01)

En ajoutant la chaîne Ménages aux chaînes lançables depuis `/calculs`, le tour précédent avait créé
un chemin qui exécute chaque lot par **appel direct du script**. Or `lot6b` résout une URL depuis
`REF_Sources_Systeme` et **interroge réellement la feuille Google** des déclarations internes :

```
[lot6b] URL REF OK (SRC_011) | CSV 40 lignes | normalisées 24 | M04 MASTER 24 lignes
[lot6b] mai 2026 par intervenant : {('INT_0002','Kheira'), ('INT_0001','Imène')}
```

Des prénoms réels d'intervenantes se sont retrouvés dans `data_recette`. Aucune donnée réelle n'a
été **écrite** (les sources n'ont été que lues) et `data_recette` est gitignoré : rien n'a été
committé, l'exposition est restée locale.

**Cause du faux diagnostic lot6d** : le mapping des noms d'appartements réels vers le parc *fictif*
échouait (24 lignes `A_CONTROLER`), donc M04 sortait avec `logement_id = None` sur toutes ses
lignes. C'est ce M04 pollué qui cassait le tri de lot6d. **lot6d n'a aucun défaut, et
`build_data_recette` non plus** : le défaut était le chemin d'exécution.

**Correction** : `Lot.exige_workspace_controle` marque toute la chaîne ; `executer_lot` la refuse
**avant tout lancement** (aucun processus démarré) en renvoyant vers `/menages/chaine` ; la chaîne
n'est plus proposée dans `/calculs`. 4 tests, dont un qui vérifie que le garde-fou ne déborde pas
sur les chaînes aval et charges.

`menages_chaine_service` existait précisément pour cela : workspace isolé, sources copiées,
`stub_lib_sheet_source.py` à la place de l'accès réseau. Même leçon que lot3, sur un risque plus
grave.

## 3. Verrou périmé : reprise atomique

Un run interrompu laissait son verrou et bloquait **définitivement** les suivants, avec un symptôme
trompeur (`KeyError: 'reel_intact'`).

La suppression automatique était refusée au motif qu'un PID est recyclable. L'argument porte en
fait sur le sens inverse : un PID recyclé rend le verrou **actif**, donc protégé. Le vrai risque est
la course entre deux repreneurs — écarté par un **renommage atomique**.

`recuperer_verrou_perime()` n'écarte que l'état `POTENTIELLEMENT_PERIME`, refuse un processus vivant,
un verrou illisible ou d'une autre machine, conserve le fichier écarté pour inspection, calcule
l'âge, signale un verrou neuf dont le PID est déjà mort, et journalise **sans le nom de machine**.
Ce nom est aussi retiré du message d'erreur qui remonte à l'interface.

14 tests couvrant les huit cas demandés. Prouvé en conditions réelles : verrou périmé posé à la
main, chaîne relancée, 7/7 OK.

## 4. Règles métier — deux corrections de mes propres affirmations

### 4.1 Pivot historique : le moteur était conforme, pas en dérive

`DECISIONS_METIER.md` → **D101**, VALIDÉ le 2026-06-18 :

> Pivot **2026-06**. ≤ 2026-05 : `INTERNE_HEURES_M04` = nb_heures × taux horaire (PARAM_004).
> ≥ 2026-06 : `INTERNE_STANDARD_PARAMETRE` = nb_menages × forfait `REF_Couts_Menage_Interne`.

`lib_menage_costs.PIVOT_FIXED_COST = 2026-06-01` applique exactement D101.

La consigne du 2026-07-27 demandait de basculer au 1er mai 2026 en supposant une dérive du moteur.
**Le pivot n'a pas été modifié** : l'avancer recalculerait mai 2026 — le mois qui porte les données
réelles — avec l'autre méthode, ce que la consigne interdit elle-même. Arbitrage inscrit au handoff.

9 tests ancrent le comportement, dont les 4 frontières demandées (30/04, 01/05, 31/05, 01/06) et
trois garde-fous : date absente, heures absentes et taux hors période ne produisent **jamais** un
coût de 0,00 € inventé.

### 4.2 « Cave 50 € » : je l'avais déclarée introuvable — elle existe

Affirmation erronée du rapport précédent, due à une recherche limitée à `lib_menage_costs`. La règle
est documentée **et** implémentée :

- `REC_002 — Forfait local cave` (CHG_023, TYPE_FLUX_010, HC) vit dans `REF_Charges_Recurrentes` ;
- **D103** révise D045 : clé de répartition = `COUT_STANDARD_MENAGES_MOIS`, pas `NOMBRE_MENAGES` ;
- ventilée **uniquement sur les ménages internes**, date-aware, contrôle
  `REC_002_LOCAL_CAVE_INTERNE_ONLY` ;
- implémentée dans `lot6f_cout_complet_menages.py`.

Ce n'est donc **pas** « + 50 € par ménage » : c'est un forfait mensuel ventilé au prorata du poids
(coût standard) sur les seules lignes internes. 6 tests figent ces invariants — montant jamais codé
en dur, réservé aux internes, clé D103, ventilé une seule fois, pools déclarés explicitement.

**Aucun arbitrage n'est nécessaire sur la cave** : le grain est documenté.

## 5. Cycle de vie construit ce tour (audit `41b`)

Modèle SQLite (migration `0019`) : table `menages` (grain = occurrence, `MEN-xxxx`), 11 statuts du
brief (`PREVU → A_AFFECTER → A_REALISER → REALISE → A_CONTROLER → VALIDE → FACTURE → REGLE`,
`ANNULE`, `REMPLACE`, `LITIGE`), `menage_evenements` (historique append-only),
`fournisseur_menage_qualification` (extension du référentiel Fournisseurs — **pas** une table
concurrente).

`menages_cycle_service.py` — patron identique à `factures_service.py` :
- `creer()` résout le propriétaire depuis `logements_service.load_detail` (aucun second référentiel),
  bloque doublon/logement inconnu/propriétaire non résolu/type/date invalides ;
- `affecter()`/`remplacer()` vérifient la qualification (type + période + logements autorisés),
  refusent un prestataire archivé ; l'ancien prestataire reste dans l'historique, jamais réécrit ;
- `realiser()` exige une justification au-delà d'un seuil d'écart coût prévu/réel ;
- `changer_statut()` applique les transitions, dont la réouverture contrôlée depuis LITIGE (jamais
  un saut direct vers FACTURE/REGLE) ; refuse VALIDE sans coût ;
- `lier_facture()`/`lier_charge()` ne créent **jamais** l'objet lié, refusent la double liaison ;
- `contexte_facture_charge_reglement_banque()` délègue en lecture seule aux services existants.

`menages_controles_service.py` — même patron que `factures_controles_service.py` : 11 codes
(sans réservation, doublon probable, prestataire archivé, externe sans facture, durée négative,
validé sans coût, annulé encore facturé, charge dupliquée, écart non justifié, facture réglée sans
règlement retrouvé), ne lève jamais.

11 routes sous `/menages/cycle` + 4 écrans (liste/filtres, création, fiche détaillée avec actions
contextuelles selon les transitions autorisées, contrôles).

## 6. Recette navigateur réelle — parcours complet prouvé

Serveur de recette, port 8070, `RECETTE_MODE=1` + double verrou `MENAGES_CYCLE_REAL_WRITE_*`.

1. **Création** `MEN-36ACD6E735B8` (LOG_A1, EXTERNE, 2026-06-20) → propriétaire `PROP_A` résolu
   automatiquement, statut `PREVU`.
2. **Affectation** du prestataire fictif → `A_REALISER`, historique `AFFECTATION` horodaté.
3. **Réalisation** (coût réel 46 € vs prévu 45 €, écart sous le seuil) → `REALISE`.
4. Transitions `A_CONTROLER` → `VALIDE`.
5. **Contrôle en conditions réelles** : `/menages/cycle/controles` a immédiatement signalé
   `CTRL_MEN_EXTERNE_SANS_FACTURE` (CRITIQUE) et `CTRL_MEN_SANS_RESERVATION` (INFO) — les contrôles
   fonctionnent sur un cas réel, pas seulement en test unitaire.
6. **Rattachement** d'une facture existante (`FAC-477C2F7A50BB`, REGLEE, solde 0 €) → la fiche
   affiche facture, statut, solde, règlement `REG-1ED3A40AA329` (35 €), et « aucun mouvement
   bancaire rapproché » (exact : ce règlement n'était pas rapproché dans le jeu de recette).
7. Transitions `FACTURE` → `REGLE`, historique intégral (7 événements, tous horodatés).
8. **Second ménage** créé (LOG_B1, INTERNE) puis **annulé** → `ANNULE`, aucune action restante
   (état terminal), jamais supprimé.
9. **Redémarrage du serveur** → `GET /menages/cycle/MEN-36ACD6E735B8` renvoie toujours `REGLE`.
   **Persistance prouvée.**

## 7. Ce qui reste ouvert

| Sujet | État | Raison |
|---|---|---|
| Pools de courses en recette | ⛔ **bloqué, cause identifiée** | Les charges `affectable_menage=OUI` sont désormais seedées (G1 courses 80 €, G2 consommables 40 €, G3 hors mois 25 €) et Lot3 les traite correctement. **Mais la chaîne ménages ne peut pas tourner sur le parc fictif** : `menages_chaine_service` copie les sources depuis `PROJECT_ROOT`, et la source des déclarations internes — même remplacée par le stub — porte des noms d'appartements **réels** qui ne se mappent sur aucun `logement_id` du parc fictif. lot6d échoue alors sur `nom_app(lg) → None` (`TypeError: NoneType < str`), exactement le symptôme déjà rencontré. Voir §7bis. |
| Rattachement de charge exercé en réel | ⚠️ | `lier_charge()` testé unitairement (12 tests) ; non exercé en recette navigateur (aucune charge de recette disponible à lier lors de ce parcours). |
| Import PDF de facture ménage → lien direct depuis la fiche ménage | ⛔ | Le rattachement facture existe (`lier_facture`) ; aucun formulaire UI ne l'expose encore (fait par script dans cette recette). |
| Chaîne lot6 exercée avec un ménage du cycle en entrée | ⛔ | Le cycle de vie et la chaîne de comptage (lot6b→lot11) restent deux couches parallèles, non connectées par un flux de données. |

Ce qui a été fait est **prouvé** (modèle, service, contrôles, routes, écrans, recette navigateur,
persistance). Ce qui manque est cité sans arrondi.

## 7bis. Pools de courses : ce qui est fait, et le blocage exact

**Fait** — le jeu de recette porte désormais trois charges `affectable_menage=OUI` couvrant les cas
demandés :

| Scénario | Montant | Catégorie | Attendu |
|---|--:|---|---|
| `CHG_G1_COURSES` | 80,00 € | `CHG_003` | alimente le pool **COURSES** |
| `CHG_G2_CONSO` | 40,00 € | `CHG_004` | alimente le pool **CONSOMMABLES** |
| `CHG_G3_HORS_MOIS` | 25,00 € | `CHG_003`, avril | **exclue** des pools, comptée dans `nb_affectables` |

Lot3 les traite correctement (13 lignes MASTER, 8 en VUE_MENAGE) et la réconciliation aval reste
juste : REEL 703,90 = 558,90 + 145,00, invariant `REEL = COMPTABLE + HORS_COMPTA` vérifié
(638,90 + 65,00). Les 19 tests de `test_charges_pipeline.py` passent.

**Bloqué** — la ventilation elle-même n'est pas exercée, pour une raison structurelle :

- `menages_chaine_service` copie les sources depuis `PROJECT_ROOT` puis exécute la chaîne dans un
  workspace isolé, en remplaçant l'accès réseau par `stub_lib_sheet_source` ;
- avec `PROJECT_ROOT` = arbre **réel**, la chaîne passe **7/7** — mais elle lit alors la SAISIE
  réelle, qui ne contient pas mes charges fictives (et il est hors de question d'y écrire) ;
- avec `PROJECT_ROOT` = `data_recette`, la chaîne lit bien ma SAISIE fictive, mais la source des
  **déclarations internes** porte des noms d'appartements réels qui ne se mappent sur aucun
  `logement_id` du parc fictif → lot6d échoue sur `nom_app(lg) → None`.

**Ce n'est ni un défaut moteur ni un défaut de `build_data_recette`** : c'est une lacune du jeu de
recette, qui n'a pas de source de déclarations internes fictive alignée sur le parc fictif.

**Tentative de déblocage et second obstacle trouvé.** Fabriquer la source fictive supposait deux
mappings :

1. **Logements** — `lmap` se construit depuis `REF_Mapping_Logements.valeur_source`. Le REF fictif
   ne contient aujourd'hui que des lignes `listingMapId` (900001…), pas de lignes portant un **nom
   d'appartement**. Ajoutable sans difficulté.
2. **Intervenants** — bloquant. `lot6b_m04_menages_internes.py` porte un mapping **codé en dur dans
   le source du moteur** :

   ```python
   INTMAP = {"imene": ("INT_0001","Imène"), "kira": ("INT_0002","Kheira"), "kheira": (...)}
   ```

   Un prénom fictif donnerait `intervenant_id = None`. Or lot6d agrège aussi **par intervenant**
   (`res_int`), avec le même `sorted()` que pour les appartements : on retomberait exactement sur le
   `TypeError: NoneType < str`, en ayant seulement déplacé le problème.

   Utiliser les prénoms réels pour contourner **réinjecterait de la PII réelle dans le jeu de
   recette** — précisément le défaut corrigé au tour précédent (`ANO-2026-07-27-01`). Écarté.

C'est le **même motif que `lot6c`**, qui porte lui aussi des données réelles en dur (références de
factures `FAC-2026-05-AISSATA-001`, noms de prestataires). Deux moteurs de la chaîne ménages
embarquent des données réelles dans leur code source.

**Décision : ne pas bricoler.** Trois issues possibles, toutes des décisions à prendre :

- **A** — externaliser `INTMAP` (et les données en dur de `lot6c`) vers `REF_Intervenants` /
  `REF_Setup`, ce qui rendrait les moteurs pilotables par référentiel. Modification de moteur, à
  arbitrer.
- **B** — accepter que la chaîne ménages ne soit exerçable que sur l'arbre **réel** (en copies,
  7/7 OK, `reel_intact=True`), et renoncer à l'exercer sur données fictives. La ventilation des
  pools serait alors validée sur données réelles en lecture seule, jamais en recette isolée.
- **C** — enrichir le jeu de recette d'intervenants dont les identifiants correspondent aux clés
  d'`INTMAP` **sans en reprendre les prénoms réels** — impossible en l'état, `INTMAP` est indexé
  par prénom normalisé, pas par identifiant.

Tant que ce n'est pas tranché, la ventilation des pools et de REC_002 reste non exercée.

## 8. Tests ajoutés ce tour

| Fichier | Nb | Objet |
|---|--:|---|
| `test_calculs_executeur.py` (ajouts) | 4 | garde-fou workspace contrôlé |
| `test_menages_pivot_historique.py` | 9 | pivot D101 et frontières |
| `test_verrou_perime.py` | 14 | reprise de verrou, huit cas |
| `test_menages_cave_et_pools.py` | 6 | cave REC_002 et pools |
| `test_menages_cycle.py` | 28 | modèle SQLite + service (dont double verrou) |
| `test_menages_controles.py` | 8 | catalogue de contrôles |
| `test_menages_cycle_routes.py` | 11 | couche HTTP, cycle complet, persistance |
| `test_flags_inventaire.py` (ajouts) | 2 | flag `MENAGES_CYCLE_REAL_WRITE_*` |
| **Total** | **82** | |

Campagne ciblée `-k "menage"` : 230 passés / 14 skipés.

Campagnes ciblées : `-k "menage or calculs"` → 229 passés ;
`-k "verrou or lock or menage or charges_confirmation or saisie_charges"` → 340 passés.

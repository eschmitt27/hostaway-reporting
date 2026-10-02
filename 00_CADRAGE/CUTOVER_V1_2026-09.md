# CUTOVER V1 — comptabilité applicative au 1er septembre 2026

> **Exécuté le 2026-10-02** (transaction engagée à 18:44:21 UTC) sur la base réelle
> `C:\Users\Ewans\PilotageConciergerie\data\app.db`, sur ordre explicite de l'utilisateur.
> Décision **D-CUTOVER-V1-01** (`DECISIONS_METIER.md`), règles **V1-1 → V1-6** (`REGLES_METIER.md` §14),
> architecture §1.3 (`ARCHITECTURE_DONNEES.md`). Ce cutover supersède `PLAN_RESET_PRODUCTION_APRES_RECETTE.md`.
>
> **Verdict : RÉUSSI.** Factures 0, créances 0, charges héritées non rapprochées 0, banque, Hostaway,
> réservations, référentiels et fiche société identiques à la sauvegarde, facturation antérieure à
> septembre 2026 refusée par le serveur et par la base.
>
> **FINALISÉ le 2026-10-03 (§19, D-CUTOVER-V1-02)** : plus aucune comptabilité propriétaire
> antérieure dans aucun run, plus aucune facture ni dette fournisseur antérieure, calcul et import
> pré-V1 bloqués.

## 1. Objectif

Faire démarrer proprement la comptabilité applicative V1 au 01/09/2026 :

- sans ancienne facture propriétaire ;
- sans ancienne créance ni ancien acompte transformé en crédit ;
- sans charge héritée non rapprochée ;

tout en conservant intégralement :

- les sources fiables : banque, Hostaway, réservations, référentiels, fiche société ;
- les rapprochements banque ↔ charges déjà validés.

C'est une remise à zéro **contrôlée** de certaines données, pas une recréation de base. Sur 233 tables
analysées, 938 lignes ont été supprimées, toutes justifiées une à une (annexe A). Aucune suppression ne
repose sur la seule date. Les deux charges de septembre purgées et l'écriture VALIDÉE de septembre purgée
le montrent.

Décision de l'utilisateur, inscrite telle quelle :

> « La comptabilité applicative V1 de Chouette Patrimoine démarre au 1er septembre 2026. Les factures
> propriétaires antérieures ne sont pas reprises et aucune facture ne peut être créée ou éditée pour une
> période antérieure à septembre 2026. Les créances antérieures sont purgées. Les charges héritées ne sont
> conservées que lorsqu'elles disposent déjà d'un rapprochement bancaire validé. Les mouvements bancaires,
> les rapprochements banque ↔ charges correspondants, les données Hostaway, les réservations, les
> référentiels et les données société sont conservés. »

## 2. Date de cutover

| | |
|---|---|
| Paramètre | `V1_ACCOUNTING_START_DATE = 2026-09-01` |
| Stockage | Une ligne de `parametres_societe_facturation` (et non `REF_Parametres_Generaux`, que tout réimport de `REF_Setup` réécrit). Elle est historisée dans `parametres_societe_facturation_historique` (ligne 25, acteur « cutover-v1 (ordre utilisateur du 2026-10-02) »). |
| Lecture | `05_APPLICATION/app/services/perimetre_v1_service.py`, seul module qui connaît la date. Elle est relue en base à chaque appel, sans cache. |
| Immuabilité | Migration `0120_perimetre_v1_garde_fous.sql`, déclencheurs `trg_parametre_v1_immuable_update` et `trg_parametre_v1_immuable_delete`. Le paramètre n'est pas dans l'écran « Paramètres société ». |
| Premier mois comptable V1 | **2026-09** |
| Ce qu'elle ne date PAS | L'historique Hostaway, l'historique bancaire, la création des logements et des propriétaires, ni la validité des référentiels |

Tant que le paramètre est absent (installation neuve, bases de test), rien n'est restreint : la règle
naît avec le cutover.

## 3. Procédure exécutée

L'outil `05_APPLICATION/tools/cutover_v1.py` exécute une étape par commande. Chaque étape écrit son
rapport JSON dans `C:\Users\Ewans\PilotageConciergerie\data\backups\cutover_v1_20261002T184338Z\`.

| # | Étape | Rapport | Résultat |
|---|---|---|---|
| 0 | Répétition intégrale sur une **copie** de la base réelle : simulation ×2, exécution, reconstruction interne, actualisation canonique globale, vérification, comparaison Lot10 | copie temporaire (hors base réelle) | Tout est OK, voir §13 |
| 1 | `sauvegarder`, application arrêtée (le port 8000 est contrôlé par l'outil) | `01_etat_et_sauvegarde.json` | Sauvegarde lisible, même nombre de tables et de lignes |
| 2 | `simuler` deux fois, en lecture seule (`mode=ro`) | `02_simulation_1.json`, `02_simulation_2.json` | Deux rapports identiques (empreinte `6d6d1c48f6443561574b1548`), 0 anomalie, 0 table non classée |
| 3 | `executer --confirmer --acteur …` : migration 0120, puis UNE transaction `BEGIN IMMEDIATE` | `03_execution.json` | 24 vérifications OK, puis COMMIT |
| 4 | `reconstruire` : orchestrateur sans import externe, déclencheur `CUTOVER_V1` | `05_reconstruction.json` | SUCCES, run `ORCH-20261002184429-9e513f` |
| 5 | `verifier` | `04_verification_184436.json` | ok |
| 6 | Redémarrage n°1, 13 contrôles HTTP, arrêt, puis `verifier` | `04_verification_184713.json` | ok ; empreinte du fichier inchangée par le démarrage et les contrôles |
| 7 | Redémarrage n°2, 13 contrôles HTTP et 7 pages d'historique | — | ok ; l'application reste lancée |

Ordre de la transaction (`cutover_v1_service.executer`) :

1. identifiants à conserver ;
2. dépendances KEEP_BY_DEPENDENCY ;
3. suppression des enfants PURGE ;
4. suppression des parents PURGE ;
5. pose du paramètre et de son historique ;
6. `foreign_key_check` ;
7. `integrity_check` ;
8. invariants métier A → Z ;
9. **COMMIT seulement si tout est conforme**, sinon ROLLBACK intégral.

Le service refuse :

- sans `--confirmer` ;
- sans acteur ;
- en présence d'une anomalie de plan ;
- si le cutover est déjà appliqué.

Les PDF des factures purgées sont copiés AVANT la transaction. Ils ne sont retirés du dossier applicatif
qu'APRÈS le COMMIT. En cas de ROLLBACK, la copie est effacée.

## 4. État AVANT (application arrêtée)

| Mesure | Valeur |
|---|---|
| Code | HEAD au démarrage de la mission : `5fb7994`. Code du cutover : `4b08e98` (HEAD au moment de l'exécution) |
| Migration | **0119** |
| SHA256 | `719c23c8511db096dd8ec3f58d5e0f193c5497b9f85a86a63711d0a0751a2863` |
| Taille / mtime / WAL | 68 378 624 octets / 2026-10-02T16:52:29Z / 0 |
| `integrity_check` | ok |
| `foreign_key_check` | 0 erreur |
| Tables / lignes | 233 / 260 560 |
| Facturation | 27 factures propriétaires, dont 3 émises ; 3 créances |
| Charges | 12 (7 rapprochées, 5 non rapprochées) |
| Banque | 13 rapprochements bancaires (dont 8 banque ↔ charge validés) ; 14 mouvements bancaires |
| Comptabilité | 12 écritures |
| Réservations | 21 126 Hostaway ; 4 hors Hostaway |
| Référentiels | 12 propriétaires ; 19 logements ; 5 intervenants / fournisseurs |

## 5. Matrice KEEP / PURGE / KEEP_BY_DEPENDENCY / REBUILD

Les 233 tables sont classées, aucune n'est `NON_CLASSEE`. La matrice table par table, avec le nombre de
lignes avant et après, est en **annexe A**.

| État | Lignes (simulation) | Tables |
|---|---:|---|
| KEEP | 207 295 | 146 tables entièrement, plus la part conservée de 23 tables mixtes |
| PURGE | 938 | 18 tables entièrement (facturation propriétaire et créances), plus une part de 37 tables mixtes |
| KEEP_BY_DEPENDENCY | 120 | 2 tables entièrement (justificatifs), plus une part de 20 tables mixtes |
| REBUILD | 52 207 | 29 tables dérivées, plus la ligne du paramètre V1 |

L'exécution a compté **207 296** lignes KEEP. La ligne de plus est celle de la migration 0120 dans
`schema_migrations`, appliquée entre la simulation et l'exécution. Le nombre de lignes PURGE est
identique (938).

| Rôle | Tables | Lignes avant | État | Justification |
|---|---:|---:|---|---|
| Source Hostaway | 9 | 204 578 | KEEP | Donnée source réelle (réservations, finances, payouts, listings, tâches), toutes périodes |
| Archive des réservations | 8 | 17 | KEEP | Archive et classification historiques des réservations |
| Réservations hors Hostaway | 4 | 14 | KEEP | Source métier indépendante saisie dans l'application |
| Banque | 13 | 52 | KEEP | Mouvements bancaires et leur journal, jamais supprimés, quelle que soit la date |
| Référentiels | 55 | 742 | KEEP | Nécessaires au fonctionnement futur et à l'interprétation de l'historique (périodes de validité comprises) |
| Fiche société et paramètres de facturation | 1 | 20 | KEEP, plus 1 ligne posée | Fiche canonique de Chouette Patrimoine conservée à l'identique ; `V1_ACCOUNTING_START_DATE` ajouté |
| Factures fournisseurs reçues | 13 | 580 | KEEP | Source des coûts ménages externes (historique métier). Aucune écriture n'en est issue. Le prochain import PDF les recréerait. |
| Ménages (sources et décisions) | 12 | 100 | KEEP | Déclarations, historiques, arbitrages humains |
| Observabilité / technique | 32 | 1 170 | KEEP | Runs, sauvegardes, migrations, verrous, suivi des contrôles (diagnostic) |
| Données dérivées | 29 | 52 207 | REBUILD | Reconstruites par l'orchestrateur depuis les sources conservées |
| Facturation propriétaire et créances | 18 | 880 | PURGE | La V1 repart avec 0 facture et 0 créance |
| Justificatifs | 2 | 15 | KEEP_BY_DEPENDENCY | Pièces des charges conservées (la base interdit leur suppression) |
| Tables mixtes (banque, charges, écritures, lettrages, clôtures, cycles de règlement…) | 37 | 185 | Ligne par ligne | Voir §6 et §7 et l'annexe A |

## 6. Données purgées (938 lignes, journal exact de la transaction)

### Facturation propriétaire

| Table | Lignes |
|---|---:|
| `factures_proprietaires` | 27 |
| `factures_proprietaires_lignes` | 83 |
| `factures_proprietaires_reservations` | 144 |
| `factures_proprietaires_meta` | 26 |
| `factures_proprietaires_evenements` | 42 |
| `factures_proprietaires_conformite` | 3 |
| `factures_proprietaires_sequence` | 2 |
| `factures_proprietaires_lignes_charge` | 1 |

### Créances

| Table | Lignes |
|---|---:|
| `imputations_airbnb` | 2 |
| `proprietaire_allocations` | 1 |
| `proprietaire_recalculs` | 549 |
| `mouvements_tresorerie_proprietaires` (+ événements) | 1 (+ 2) |
| `proprietaires_releves` (+ événements) | 2 (+ 2) |
| `proprietaires_releve_cycle` | 1 |
| `proprietaires_paiement` | 1 |

### Charges

| Table | Lignes |
|---|---:|
| `charges` | 5 |
| `charge_evenements` | 10 |
| `charges_refacturation_positions` (+ événements) | 1 (+ 1) |

### Banque (données de recette)

| Table | Lignes |
|---|---:|
| `banque_rapprochements` (+ événements) | 3 (+ 4) |
| `banque_suggestion_decisions` | 2 |
| `banque_imports` | 2 |

### Comptabilité

| Table | Lignes |
|---|---:|
| `ecritures` | 3 |
| `ecriture_lignes` | 6 |
| `ecriture_evenements` | 4 |
| `clotures_mensuelles` | 2 |
| `cloture_evenements` | 6 |

### Détail métier

- **Factures propriétaires : 27 → 0.** Toutes portaient sur des prestations antérieures à septembre 2026.
  - 3 émises :
    - `F-11/0-000001` (465,88 €) ;
    - `2026-08-001` (823,65 €) ;
    - `2026-08-002` (288,95 €).
  - 1 VALIDÉE (287,57 €, août).
  - 22 BROUILLON (13 de juin, 9 d'août).
  - 1 ANNULÉE (140,86 €, août).
  - Les PDF des 3 factures émises sont archivés dans le dossier de cutover, sous
    `documents_archives\factures_proprietaires\` : `2026-08-001.pdf`, `2026-08-002.pdf`,
    `F-11-0-000001.pdf`. Aucun document source externe n'est touché.
- **Créances : 3 → 0** (les 3 factures émises). Partent avec elles :
  - l'acompte `MTP-E00AEA98D2EF` (12 €, PROP_0002, saisie manuelle liée à la facture 2026-08-001, sans
    mouvement bancaire). Il n'a **pas** été transformé en crédit V1 ;
  - 2 imputations Airbnb (`IMPA-6AD043EEEFFB`, `IMPA-862AECBFE514`) ;
  - 1 allocation FIFO et ses 549 lignes de journal de recalcul ;
  - les 2 relevés de règlement d'avant la V1 (`REG-93d9a3c8df`, `REG-e716cf6a6d`), avec leur cycle et
    leur paiement.
- **Charges : 12 → 7.** Les 5 charges sans rapprochement bancaire validé sont purgées, quels que soient
  leur date, leur statut ou leur montant :

  | Charge | Date | Montant | Statut avant |
  |---|---|---:|---|
  | `CHG-a1db33663116` | 2026-08-15 | 12,34 € | ANNULÉE |
  | `CHG-f755790fd169` | 2026-08-29 | 100,00 € | ACTIVE |
  | `CHG-fc93f74a63d1` | 2026-08-09 | 700,00 € | ACTIVE |
  | `CHG-85c2345c5c07` | 2026-09-09 | 146,00 € | ACTIVE |
  | `CHG-d6fa0520ef8f` | 2026-09-10 | 42,00 € | ACTIVE |

  La charge de 700 € était laissée en arbitrage B par les missions 30 à 33. La règle explicite du cutover
  la tranche (D-V1-4).
- **Écritures : 12 → 9.** Les 3 écritures VENTES issues des factures purgées sont supprimées :
  - `ECR-F3D2501E56ED` : F-11/0-000001, VALIDÉE, période 2026-09 ;
  - `ECR-FE82EE75FC01` : 2026-08-001, PROPOSÉE, période 2026-09 ;
  - `ECR-EC485FCC82B9` : 2026-08-002, PROPOSÉE, période 2026-08.

  Les deux premières tombaient en septembre. Elles auraient introduit un ancien produit dans le résultat
  V1 de septembre (assertion V).
- **Rapprochements : 13 → 10.** Les 3 lignes purgées sont des données de recette **prouvées** : elles
  pointent vers des mouvements bancaires **inexistants** et vers des objets de jeu d'essai, documentés
  comme fixtures en mission 14d :
  - `BRP-27391F26175A` : RESERVATION, CONFIRME, mouvement `MVT-a12f862291a5`, objet `RES_HOSTAWAY_2026_06_05` ;
  - `BRP-362217532E30` : CHARGE_FOURNISSEUR, **PROPOSE**, objet `CHG_MENAGE_B_2026_06` ;
  - `BRP-21D7A4681BEC` : CHARGE_FOURNISSEUR, **PROPOSE**, objet `CHG_SEED_003`.

  Aucun de ces trois rapprochements n'était un rapprochement banque ↔ charge validé. Avec eux partent les
  2 imports `releve_recette.csv` (2026-07-26) et les 2 décisions de suggestion de recette.
- **Clôtures** de l'ancien modèle purgées :
  - `CLO-19c1217854` : 2025-01, VALIDÉE ;
  - `CLO-1ab8711165` : 2025-02, EN_PREPARATION.

  Les deux ont été créées le 2026-08-30, avec l'ancien modèle.

## 7. Dépendances conservées (KEEP_BY_DEPENDENCY)

Pour chacune des 7 charges conservées, **tous les maillons existants de sa chaîne** restent en place.
Selon la charge, ce sont :

- mouvement bancaire ;
- rapprochement CONFIRMÉ ;
- lettrage VALIDÉ ;
- écriture de règlement ;
- justificatif ;
- périmètres analytique et ménage ;
- position de refacturation.

| Élément | Lignes conservées |
|---|---:|
| Rapprochements banque ↔ charge | 8 (+ 10 événements) |
| Lettrages | 7 (+ 15 lignes, + 7 événements) |
| Écritures BANQUE de règlement | 7 (+ 15 lignes, + 14 événements) |
| Justificatifs | 5 (+ 10 événements) |
| Événements de charge | 14 |
| Périmètres analytique / ménage | 3 / 3 |
| Position de refacturation | 1 (+ 1 événement) |

| Charge conservée | Date | Montant | Rapprochement(s) |
|---|---|---:|---|
| `CHG-28333720fcc6` | 2026-09-21 | 2,40 € | `BRP-362BC3E70463` |
| `CHG-16d11b7c940f` | 2026-09-21 | 12,00 € | `BRP-853AA1DDAA56` |
| `CHG-1d8e83f63d2f` | 2026-09-23 | 5,89 € | `BRP-ACA0FA5FE2EC` |
| `CHG-acdbb2a1124c` | 2026-09-24 | 48,39 € | `BRP-A4D9160A72BC` |
| `CHG-d0ff3f64c35f` | 2026-09-26 | 20,25 € | `BRP-7885CFBF27BC` + `BRP-85632D2AE840` |
| `CHG-5dfd31538114` | 2026-09-26 | 59,82 € | `BRP-C52387EBFC73` |
| `CHG-f46f1ae78219` | 2026-09-26 | 4,90 € | `BRP-B25B8246629B` |

Sont également conservés (KEEP), parce qu'il s'agit d'opérations bancaires réelles de septembre, avec
leurs écritures :

- `BRP-7AC561519BC9` : apport en compte courant d'associé, 200 €, écriture `ECR-C0412F1AE5F1` ;
- `BRP-6C9106A072ED` : transfert banque → caisse, 20 €, écriture `ECR-66F07BAF8CE3`.

La clôture `CLO-c0e0ac152c` (2026-09, EN_PREPARATION) est conservée : elle porte sur la période V1.

**Après le cutover, les 9 écritures restantes sont toutes de la période 2026-09.**

## 8. Règles de facturation (en place)

- **Aucune facture propriétaire** ne peut être créée, éditée, validée, émise ou déplacée sur une période
  de prestation antérieure à 2026-09.
- **Message** : « La facturation V1 débute en septembre 2026. » Pas de traceback, pas de création
  silencieuse.
- **Service** (`factures_proprietaires_service`) : `FacturationAvantV1` (code `FACTURATION_AVANT_V1`)
  est levée dans `creer`, `creer_exceptionnelle`, `creer_avoir`, `valider`, `emettre` et
  `_exiger_brouillon`. Le contrôle porte sur `mois` ET sur `periode_debut`, et sur toute édition d'un
  brouillon.
- **Période libre** (`factures_proprietaires_periode_service.previsualiser`) : refusée si elle commence
  avant le 2026-09-01.
- **Propositions du mois** (`factures_proprietaires_source.propositions_du_mois`) : vide avant la V1.
- **HTTP** :
  - `GET /factures-proprietaires/proposer?mois=2026-08` affiche le refus ;
  - un `POST /factures-proprietaires/generer` forgé pour août ne crée rien ;
  - `POST /factures-proprietaires/periode-libre` est refusé.
- **Base** : les déclencheurs `trg_fpr_periode_v1_insert` et `trg_fpr_periode_v1_update` refusent même
  un INSERT ou UPDATE SQL direct (`FACTURATION_AVANT_V1`).
- **Écrans** : les sélecteurs de mois et de dates sont bornés (`min="2026-09"` et `min="2026-09-01"`), et
  la mention V1 est affichée.
- **Numérotation** : la première facture de septembre recevra **`2026-09-001`**.
  `factures_proprietaires_service.prochain_numero` lit la série sans la consommer, et la séquence est vide.

## 9. Règle créances

**0 créance** après cutover. C'est le contrôle `creances_0` : il lit le hub Créances & Dettes
(`creances_dettes_service.creances()`), ce n'est pas un simple comptage de tables. Aucun ancien solde
client, aucun ancien acompte et aucune ancienne allocation n'est repris. L'assertion S vérifie
l'absence de reliquat (allocations FIFO, journal des recalculs, crédits). Les mouvements bancaires
bruts ne sont pas touchés.

## 10. Règle charges

Une charge héritée n'est conservée que si elle a un rapprochement **bancaire validé** :

- un `banque_rapprochements` au statut CONFIRMÉ, de type `CHARGE_FOURNISSEUR` ou `CHARGE`,
- qui pointe vers un mouvement **existant** (`qonto_transactions_statut_local` ou `banque_mouvements`),
- avec un lettrage éventuel VALIDÉ ;
- ou bien un lettrage VALIDÉ qui relie une ligne BANQUE à la charge.

La catégorisation d'un mouvement n'est pas un rapprochement. Résultat :

- 7 charges conservées, chacune avec son rapprochement (assertion D) ;
- 0 charge non rapprochée (assertion C) ;
- les 8 rapprochements banque ↔ charge d'avant le cutover sont tous présents (assertion E) ;
- aucun double comptage : le flux unifié ne contient aucun flux de charge, et le test
  `test_pas_de_double_comptage_des_charges_conservees` le vérifie.

## 11. Banque

- **Mouvements bancaires : 14 → 14.** Les 13 tables du rôle Banque (52 lignes) sont **identiques ligne à
  ligne** à la sauvegarde : identifiants stables, comptes, dates, montants, libellés, catégories,
  empreintes (assertions F et G). La comparaison de l'annexe A a été refaite sur l'état final, après la
  reconstruction et le redémarrage n°1, application arrêtée.
- **Rapprochements banque ↔ charges : 8 → 8**, identiques ligne à ligne à la sauvegarde.
- **Autres rapprochements réels : 2 → 2.**
- Les seules lignes bancaires retirées sont les données de recette prouvées du §6 : 2 imports
  `releve_recette.csv`, 3 rapprochements vers des mouvements inexistants, 2 décisions de recette.
- Aucun appel Qonto n'a été fait pendant le cutover : la reconstruction a été lancée sans import externe.

## 12. Hostaway, réservations et référentiels

- **Hostaway : 21 126 réservations → 21 126.** Les 9 tables sources (204 578 lignes) sont identiques ligne
  à ligne (assertion H).
- **Archive des réservations** : 8 tables, 17 lignes, identiques (assertion I).
- **Réservations hors Hostaway** : 4 tables, 14 lignes, identiques ; 4 réservations conservées
  (assertion J).
- **Historique métier consultable** : les pages Réservations de juillet et d'août 2026 répondent
  toujours (200, sans traceback), de même que le Lot10 (performance, toutes périodes).
- **Référentiels : 55 tables, 742 lignes.** 54 tables sont identiques. La 55e,
  `parametres_societe_facturation_historique`, ne gagne qu'une ligne : la pose du paramètre V1.
  Cela couvre :
  - les propriétaires (12 → 12) et les logements (19 → 19), archivés et historiques de gestion compris ;
  - les intervenants et fournisseurs (5 → 5) ;
  - les taux de commission historiques et les coûts ménage historiques ;
  - le plan comptable et les mappings ;
  - les clôtures techniques de `ref_cloture_mensuelle`.

  Assertions K à R.
- **Fiche société** : les 20 lignes de `parametres_societe_facturation` sont identiques à la sauvegarde.
  La seule différence est la ligne `V1_ACCOUNTING_START_DATE` ajoutée (assertion P).

## 13. Résultats

### Vérifications de la transaction

Les 24 vérifications sont OK avant le COMMIT :

| Code | Vérification |
|---|---|
| A | Factures propriétaires existantes = 0 |
| B | Créances = 0 |
| C | Charges héritées non rapprochées = 0 |
| D | Chaque charge conservée a son rapprochement bancaire validé |
| E | Tous les rapprochements banque ↔ charges validés d'avant le cutover sont présents |
| F, G | Mouvements bancaires et identifiants stables inchangés |
| H | Réservations Hostaway inchangées |
| I | Archive des réservations intacte |
| J | Réservations hors Hostaway conservées |
| K à O | Propriétaires, logements (archivés et historique compris), fournisseurs et référentiels conservés |
| P | Fiche société conservée |
| Q, R | Taux et coûts ménage historiques conservés |
| S | Aucune créance résiduelle |
| V | Aucune écriture antérieure à la V1 |
| KEEP | Chaînes conservées intactes |
| PARAM | Paramètre posé |
| Z | `foreign_key_check` = 0 |
| Y | `integrity_check` = ok |

### Après le COMMIT, la reconstruction et les redémarrages

`tools/cutover_v1.py verifier` a tourné deux fois, la seconde après un arrêt de l'application. Les deux
passages renvoient `ok: true` :

| Contrôle | Résultat |
|---|---|
| Cutover appliqué | oui |
| Factures émises / factures | 0 / 0 |
| Créances | 0 |
| Charges non rapprochées | 0 |
| Écritures avant V1 | 0 |
| Préfactures actives avant V1 | 0 |
| Facture d'août | refusée |
| Facture de septembre | autorisée |
| Prochain numéro de septembre | `2026-09-001` |
| `integrity_check` | ok |
| `foreign_key_check` | 0 |

### Contrôles HTTP (13)

Ils ont été joués sur l'instance réelle après **chacun** des deux redémarrages. Les 13 sont OK :

1. `/health` ;
2. la liste des factures est vide et son sélecteur est borné ;
3. la proposition d'août est refusée avec le message ;
4. la proposition de septembre est disponible ;
5. `generer` pour août est refusé ;
6. la période libre commençant en août est refusée ;
7. le démarrage d'une clôture d'août est refusé ;
8. les clôtures ne proposent que 2026-09 et 2026-10 ;
9. aucun mois antérieur à la V1 n'est proposé à la clôture ;
10. la page Créances répond ;
11. la page Charges répond ;
12. la page Comptabilité répond ;
13. la page Résultats répond.

Les deux POST de refus n'ont rien écrit : l'empreinte SHA256 du fichier est la même avant le démarrage
et après l'arrêt.

### Reconstruction

`ORCH-20261002184429-9e513f` : RESERVATIONS, MENAGES, FLUX_LOT9, LOT10, LOT11 et LOT12 en SUCCES, en
7 s. Une sauvegarde automatique a été faite avant le run : `BCK-FB65FEF34ECD`.

- Seules des préfactures **2026-09** sont actives (8).
- Les runs antérieurs des tables dérivées restent stockés, inactifs. Aucun écran ni export ne les lit :
  les lecteurs Lot12 et l'export Lot13 sont filtrés sur le run actif.

### Actualisation canonique globale (sur la copie)

L'actualisation canonique globale a été jouée **sur la copie** (environnement contrôlé), avec les
imports :

- BANQUE_QONTO, HOSTAWAY_RAW, MENAGES_DECLARATIONS, MENAGES_PDF et HOSTAWAY_CLEANING_TASKS en SUCCES,
  puis toute la chaîne de calcul, en 18 s ;
- `verifier` ok ;
- 0 facture, 0 créance, 0 charge non rapprochée recréée ; préfactures uniquement en 2026-09 (8).

Aucun moteur ne « reconstruit la comptabilité avant septembre ».

### Comparaison Lot10 (copie : avant / après cutover et actualisation)

- 2026-06, 2026-07 et 2026-09 sont **identiques**.
- 2026-08 ne diffère que par `autres_acomptes_recus` et `credit_a_traiter` (12 → 0) : c'est l'acompte
  purgé.
- Résultats identiques.

## 14. Rollback

**Dans la transaction.** Tout est engagé d'un bloc ou annulé d'un bloc :

- une vérification KO, une exception ou une anomalie de plan entraîne un ROLLBACK intégral ;
- les copies d'archives PDF sont alors effacées ;
- la base n'est jamais laissée partiellement purgée.

C'est prouvé par `test_rollback_si_une_verification_echoue` et `test_rollback_sur_erreur_inattendue`.

**Après le COMMIT** (retour à l'état d'avant), sur instruction explicite seulement :

1. Arrêter l'application.
2. Remplacer `data\app.db` par `backups\app_avant_cutover_v1_20261002T184338Z.db`, après avoir mis de
   côté l'actuelle et supprimé les `app.db-wal` / `app.db-shm`.
3. Restaurer `data\factures_proprietaires\` depuis `cutover_v1_20261002T184338Z\factures_proprietaires_avant\`.
4. Redémarrer.

La migration 0120 est réappliquée au démarrage. Ses déclencheurs restent **inactifs** puisque la base
restaurée ne contient pas le paramètre. La restauration de cette sauvegarde a été **testée** : l'empreinte
logique de la base restaurée, `fc8f461f36b6d1ed8122c56c` (schéma `b0a64f48d477cab9`), est identique à
celle de la base d'origine, sans aucune table différente.

## 15. Tests (sur bases isolées, `PILOTAGE_IGNORE_ENV_FILE=1` ; jamais sur la vraie base)

### `tests/test_cutover_v1.py` : 20 passed

| Domaine | Tests |
|---|---|
| Simulation | Dry-run sans modification, deux simulations identiques ; chaque table classée |
| Purge | Purge et conservation (factures, créances, charges non rapprochées, charge rapprochée, rapprochements, factures fournisseurs, mouvements, réservations, archive, référentiels, fiche société, FK) |
| Rollback et refus | Rollback sur vérification KO ; rollback sur erreur ; refus sans confirmation et second passage ; anomalie « argent réel » bloquante |
| Facturation | Facture d'août refusée, septembre et octobre autorisés ; période libre débutant en août refusée ; édition ou déplacement vers août refusé, même en SQL direct ; route proposer et sélecteur borné ; numérotation `2026-09-001` sans consommation |
| Comptabilité et clôtures | Écriture avant V1 refusée par le service et par la base ; aucune période antérieure clôturable ; paramètre immuable et ancien reset 14d refusé |
| Calculs et redémarrage | Lot12 V1 seulement ; rien ne réapparaît après redémarrage et recalculs ; pas de double comptage ; première période V1 = 2026-09 ; sans cutover, rien n'est restreint |

### Autres suites

- **Suites liées** : facturation propriétaire, clôtures, comptabilité, écritures, Lot12, cutover,
  migrations SQLite, périodes, paramètres société, flux financiers, crédits clients, hub créances,
  circuit banque, actualisation, navigation, templates. Résultat : **924 passed**.
- **Suite complète** (4 684 tests) : **4 647 passed / 37 skipped / 0 failed**. Elle a tourné en trois
  passes à cause de la limite de durée d'une tâche :
  1. 2 816 tests avant la coupure (2 799 passed, 17 skipped, 0 échec) ;
  2. les 126 fichiers restants, en deux lots : 936 passed / 7 skipped, puis 931 passed / 13 skipped.

  Les 19 tests rejoués deux fois ne sont comptés qu'une fois.
- **Migrations** : 0120 est couverte par la suite. Elle est appliquée à la base réelle par l'étape
  `executer`, et le schéma passe de 0119 à 0120.
- **Redémarrage** : deux redémarrages réels (§13).
- **Actualisation** : reconstruction réelle et actualisation globale sur copie (§13).

## 16. Empreintes SHA256

| Moment | SHA256 | Taille | Tables / lignes | Schéma | integrity | FK |
|---|---|---:|---|---|---|---:|
| Avant | `719c23c8511db096dd8ec3f58d5e0f193c5497b9f85a86a63711d0a0751a2863` | 68 378 624 | 233 / 260 560 | 0119 | ok | 0 |
| Juste après le COMMIT | `3c5993bc279d11f0ef4e0cf851badc174edc2ffbd59a1ffb9af898be89c85de1` | 68 382 720 | 233 / 259 625 | 0120 | ok | 0 |
| État final : après reconstruction, redémarrage n°1 et arrêt | `c7d489e78eaa1a149876e70b1ca5e504a9396dbc3f4c60d8e0363f2151789817` | 70 410 240 | 233 / 263 972 | 0120 | ok | 0 |

Arithmétique des lignes :

- 260 560 − 938 supprimées + 3 ajoutées = 259 625. Les 3 lignes ajoutées sont la migration 0120, le
  paramètre V1 et son historique.
- La reconstruction ajoute ensuite 4 347 lignes : un nouveau run actif des tables dérivées, plus le
  journal des runs et la sauvegarde.

L'empreinte finale est identique avant le démarrage n°1 et après l'arrêt qui le suit. L'application a
ensuite été relancée (redémarrage n°2) et reste en service.

## 17. Sauvegarde

| | |
|---|---|
| Base | `C:\Users\Ewans\PilotageConciergerie\data\backups\app_avant_cutover_v1_20261002T184338Z.db` |
| SHA256 du fichier de sauvegarde | `be77901b85b52eb2fd02e401b4eaf5bf11c08fd7103a1555238576ec9b16e718`. Il diffère de celui de la base source : l'API `backup` de SQLite réécrit les pages, le contenu reste identique. |
| Contrôle à la création | `integrity_check` ok, `foreign_key_check` 0, 233 tables et 260 560 lignes (identique à la source), schéma 0119 |
| Restauration | Testée : empreinte logique identique (§14) |
| PDF d'avant | `cutover_v1_20261002T184338Z\factures_proprietaires_avant\` (copie intégrale du dossier) |
| PDF des factures purgées | `cutover_v1_20261002T184338Z\documents_archives\factures_proprietaires\` (3 fichiers) |
| Sauvegarde automatique avant reconstruction | `backups\20261002_204429_ACTUALISATION_GLOBALE_BCK-FB65FEF34ECD.db` (état juste après le cutover) |

## 18. Limites

1. *(Résolu le 2026-10-03, §19 : les 15 factures ont été purgées.)* **Les 15 factures fournisseurs**
   des prestataires ménage (février → août) étaient conservées et restaient
   visibles « À contrôler » dans les dettes. Elles sont la source des coûts de ménage externes, et le
   prochain import PDF les recréerait (D-V1-6). Aucune écriture ne peut plus être passée sur leur
   période. Leur traitement éventuel en dettes V1 reste une décision utilisateur.
2. *(Résolu le 2026-10-03, §19 : aucune comptabilité n'est plus affichée avant la V1.)* **Le relevé
   propriétaire d'un mois antérieur** (`/proprietaires/{id}/2026-08`, par exemple) affichait
   toujours le bloc « Règlement » calculé par le Lot10 : montant dû, reste à payer, statut
   « À contrôler ».
   - C'est l'historique de performance (D-LOT-PROD-01), annoncé « Source : SQLite (Lot10) … aucun
     recalcul ».
   - Ce n'est **pas** une créance : il n'est pas compté dans le hub Créances & Dettes, il n'est pas
     reporté sur septembre, et la préfacture d'août n'existe plus (404).
   - Signaler ce bloc « hors comptabilité V1 » sur les mois antérieurs serait une évolution d'écran,
     non faite ici (aucun nouveau module pendant la mission).
3. **Lot10, août 2026** : `autres_acomptes_recus` et `credit_a_traiter` passent de 12 € à 0, suite à la
   purge de l'acompte. C'est voulu.
4. *(Résolu le 2026-10-03 pour la comptabilité, §19.)* **Les runs antérieurs des tables dérivées**
   (Lot10, Lot11, Lot12, jeux de réservations résolues)
   restent stockés, inactifs. Rien ne les lit ; ils seraient purgeables plus tard sans conséquence.
5. **Charges purgées qui correspondaient à une dépense réelle** : 146 € (linge, 09/09) et 42 € (petit
   équipement, 10/09). Si ces dépenses sont réelles, elles devront être ressaisies en V1 et
   **rapprochées** quand le paiement bancaire sera identifié. Celles d'août (700 €, 100 €) relèvent
   d'une période où aucune écriture n'est plus possible.
6. **Hors cutover** : `SOLDE_INITIAL_BANQUE` (solde d'ouverture) n'a pas été fourni, et n'a donc pas été
   créé. `STATUT_PERIODE` se déduit de la date V1 (ARCHITECTURE_DONNEES §1.3).

## 19. Finition du 2026-10-03 — plus aucune comptabilité antérieure (D-CUTOVER-V1-02)

> **Exécutée le 2026-10-03** sur la base réelle, application arrêtée, sur ordre explicite de l'utilisateur. **Verdict : CUTOVER V1 FINALISÉ.**
> Avant septembre 2026, il ne reste plus aucune donnée comptable propriétaire, dans aucun run, ni aucune facture ou dette fournisseur. Les sources, les référentiels et les chaînes de dépenses rapprochées sont identiques à la sauvegarde.

### 19.1 Décisions de l'utilisateur (verbatim, D-CUTOVER-V1-02)

> « La comptabilité propriétaire antérieure au 1er septembre 2026 n'est pas conservée dans l'application, y compris sous forme d'historique consultable. Les relevés, soldes, restes à payer, règlements et données calculées correspondantes doivent être supprimés et ne doivent pas pouvoir être reconstruits. Les seules données antérieures conservées sont les sources métier et référentiels explicitement nécessaires, notamment Hostaway, les réservations, la banque et les chaînes de dépenses déjà rapprochées. »

> « Une facture fournisseur antérieure au 1er septembre 2026 ne peut subsister que lorsqu'elle constitue une dépendance nécessaire d'une charge déjà rapprochée avec la banque. Dans le cutover V1 réalisé le 02/10/2026, les 15 factures fournisseurs de février à août identifiées n'avaient aucune dépendance de ce type et ont donc toutes été purgées. »

### 19.2 Comptabilité propriétaire antérieure — tous runs (actif, inactifs, anciens)

| Table | Contenu | Avant (total) | Supprimé | Après (total) | Après, mois < 2026-09 |
|---|---|---:|---:|---:|---:|
| `lot10_commissions` | Commissions par réservation (Lot10) | 2 825 | 2 484 | 341 | 0 |
| `lot10_net_exploitation` | Net d'exploitation par réservation (Lot10) | 2 825 | 2 484 | 341 | 0 |
| `lot10_net_reglement` | Règlements, soldes, restes à payer (Lot10) | 599 | 455 | 144 | 0 |
| `lot10_net_vue_mois` | Net propriétaire par mois (Lot10) | 443 | 323 | 120 | 0 |
| `lot10_resultats` | Résultats par mois / logement / vision (Lot10) | 1 285 | 1 046 | 239 | 0 |
| `lot10_run_mois_provenance` | Provenance des mois calculés (Lot10) | 129 | 48 | 81 | 0 |
| `lot10_commissions_a_controler` | Anomalies de commission des réservations antérieures (Lot10) | 1 139 | 794 | 345 | 0 |
| `lot12_prefactures_entete` | Préfactures, en-têtes (Lot12) | 563 | 419 | 144 | 0 |
| `lot12_prefactures_lignes` | Préfactures, lignes (Lot12) | 6 895 | 5 127 | 1 768 | 0 |
| `lot12_prefactures_id_legacy` | Préfactures, identifiants (Lot12) | 563 | 419 | 144 | 0 |
| `lot12_controle_mensuel` | Contrôle mensuel de facturation (Lot12) | 563 | 419 | 144 | 0 |
| `lot12_dashboard_facturation` | Tableau de bord de facturation (Lot12) | 418 | 298 | 120 | 0 |
| `lot12_a_controler` | Contrôles de facturation (Lot12) | 1 144 | 799 | 345 | 0 |
| `controles_lot11_constats_champs` | Champs de ce constat | 30 | 1 | 29 | 0 |
| `controles_lot11_constats` | Constat Lot11 sur la comptabilité d'août | 31 | 1 | 30 | 0 |
| `controles_lot11_dashboard_mois` | Statuts mensuels Lot11 des mois antérieurs | 21 | 19 | 2 | 0 |
| `controles_suivi` | Décision humaine attachée au constat | 1 | 1 | 0 | 0 |
| `controles_suivi_historique` | Historique de cette décision | 1 | 1 | 0 | 0 |
| **Total comptabilité propriétaire** | | | **15 138** | | **0** |

Avec les factures fournisseurs (§19.3 : 15 factures et 547 lignes propres, soit 562 lignes), la
transaction a supprimé **15 700 lignes** et posé 15 verdicts `ANTERIEURE_V1`.

Les colonnes « après » des tables Lot10/Lot12 ne comptent plus que la période V1 (septembre 2026), dans chaque run. Le run Lot10 recalculé après la purge ne contient que 2026-09 : chaque mois antérieur y est tracé `EXCLU_PERIMETRE_ANTERIEUR_V1` (§19.6). Les anomalies de commission ont été attribuées à leur mois par la réservation. Les anomalies « canapé », sans réservation, l'ont été par les lignes de commission du run qui les a produites : 0 ligne n'était attribuable à aucun mois.

Les tables d'état propriétaire déjà purgées le 2026-10-02 restent à 0 : relevés de règlement, allocations, acomptes, imputations, factures propriétaires. Les 3 lignes techniques `N/A` (« HC_ZERO_SOURCES_VIDES », 0 €, sans période) de 3 anciens runs ne sont pas de la comptabilité : elles restent.

### 19.3 Factures fournisseurs — 15 analysées, 15 purgées, 0 conservée par dépendance

| # | Facture | Pièce | Date | Prestataire | Montant TTC | Logements | Charge | Règlement / mouvement / rapprochement | Décision |
|---:|---|---|---|---|---:|---|---|---|---|
| 1 | `FAC-E97FFBA024F5` (2025-370) | 02-26-Aissata.pdf | 2026-02-28 | Aissata | 1 702,00 € | LOG_0003, LOG_0006, LOG_0009, LOG_0010, LOG_0011, LOG_0012, LOG_0013, LOG_0014 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 2 | `FAC-F5575F303A1D` (2025-016) | 02-26-Imrane.pdf | 2026-03-02 | PrivaDom (`INT_PRIVADOM`), pièce « Imrane » | 205,00 € | — | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 3 | `FAC-B979B5D35156` (2025-369) | 03-26-Aissata.pdf | 2026-03-31 | Aissata | 2 234,00 € | LOG_0001, LOG_0003, LOG_0006, LOG_0007, LOG_0008, LOG_0010, LOG_0011, LOG_0012, LOG_0013, LOG_0014, LOG_0016 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 4 | `FAC-7FAF8F813780` (0001) | 03-26-Mounir.pdf | 2026-03-31 | Mounir | 218,00 € | — | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 5 | `FAC-06D7B900C667` (2025-017) | 03-26-Imrane.pdf | 2026-04-21 | PrivaDom (`INT_PRIVADOM`), pièce « Imrane » | 228,00 € | — | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 6 | `FAC-E470067ABA38` (2026-36) | 04-26-Aissata_1.pdf | 2026-04-30 | Aissata | 2 313,00 € | LOG_0002, LOG_0003, LOG_0006, LOG_0007, LOG_0008, LOG_0010, LOG_0011, LOG_0012, LOG_0013, LOG_0014 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 7 | `FAC-067D2C5E73E9` (2026-37) | 04-26-Aissata_2.pdf | 2026-04-30 | Aissata | 15,00 € | — | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 8 | `FAC-5C2C34363676` (0002) | 04-26-Mounir.pdf | 2026-04-30 | Mounir | 491,00 € | LOG_0002, LOG_0003 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 9 | `FAC-43D8EA6C6817` (2026-37) | 05-26-Aissata.pdf | 2026-05-31 | Aissata | 1 439,00 € | LOG_0006, LOG_0007, LOG_0009, LOG_0010, LOG_0011, LOG_0012, LOG_0014, LOG_0016 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 10 | `FAC-D4390C34EC41` (0003) | 05-26-Mounir.pdf | 2026-05-31 | Mounir | 942,00 € | LOG_0002, LOG_0003, LOG_0006, LOG_0013 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 11 | `FAC-503A5C96C0BC` (2026-38) | 06-26-Aissata.pdf | 2026-06-30 | Aissata | 1 016,00 € | LOG_0006, LOG_0007, LOG_0010, LOG_0011, LOG_0012, LOG_0014, LOG_0016 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 12 | `FAC-41EAA7AC16DC` (0004) | 06-26-Mounir.pdf | 2026-06-30 | Mounir | 741,00 € | LOG_0002, LOG_0003, LOG_0006, LOG_0013 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 13 | `FAC-739CD2FB2199` (0005) | 07-26-Mounir.pdf | 2026-07-27 | Mounir | 520,00 € | LOG_0002, LOG_0003, LOG_0006, LOG_0011, LOG_0013 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 14 | `FAC-B4D0521FA674` (2026-40) | 07-26-Aissata.pdf | 2026-07-31 | Aissata | 1 056,00 € | LOG_0002, LOG_0006, LOG_0008, LOG_0009, LOG_0010, LOG_0011, LOG_0012, LOG_0013 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| 15 | `FAC-6F7396BA6F1A` (2026-41) | 08-26-Aissata.pdf | 2026-08-31 | Aissata | 2 790,00 € | LOG_0001, LOG_0002, LOG_0006, LOG_0007, LOG_0008, LOG_0009, LOG_0011, LOG_0012, LOG_0013, LOG_0016 | aucune | aucun / aucun / aucun (validé : non) | **PURGE** |
| | **Total** | | | | **15 910,00 €** | | | | **15 PURGE, 0 KEEP_BY_DEPENDENCY** |

**Justification, identique pour les 15 factures.** Hors de ses propres tables, aucune ne figure nulle part : chacune des 233 tables a été balayée, colonne par colonne. Il n'y a donc ni charge, ni règlement, ni mouvement bancaire, ni rapprochement, ni lettrage, ni écriture, ni justificatif. Aucune ne soutient une dépense rapprochée : c'est le cas B de D-V1-FIN-2.

| Table propre | Avant | Supprimé | Après |
|---|---:|---:|---:|
| `facture_lignes_menage_detail` | 127 | 127 | 0 |
| `facture_lignes_menage_pdf` | 127 | 127 | 0 |
| `facture_lignes_menage` | 127 | 127 | 0 |
| `facture_lignes` | 0 | 0 | 0 |
| `facture_ventilation_parts` | 99 | 99 | 0 |
| `facture_ventilations` | 19 | 19 | 0 |
| `facture_classification` | 15 | 15 | 0 |
| `facture_pdf_diagnostics` | 18 | 15 | 3 + 15 verdicts `ANTERIEURE_V1` |
| `facture_evenements` | 16 | 16 | 0 |
| `facture_interpretations` | 2 | 2 | 0 |
| `factures` | 15 | 15 | 0 |

- **Dettes fournisseurs actives antérieures : 15 (15 910,00 €) → 0.**
- **Factures fournisseurs antérieures : 15 → 0.**
- Les 15 PDF restent dans `01_SOURCES_BRUTES/MenagesExternes`.
- Pour chacune de leurs pièces, un verdict `ANTERIEURE_V1` est posé dans `facture_pdf_diagnostics` : nom, empreinte, numéro et montant imprimés, mention `CUTOVER_V1_FINITION`. L'import les reconnaît sans les relire.
- `menages_pdf_fichiers_hash` (15 lignes) est conservé : un PDF dont le contenu changerait serait réexaminé, et refusé à nouveau s'il reste antérieur.
- Les 3 diagnostics d'import sans facture (03-26-Imrane, 07-26-Aissata, 07-26-Mounir) sont conservés : c'est de l'observabilité.

### 19.4 Charges et rapprochements — 8 rapprochements pour 7 charges : cohérent, rien modifié

| Charge | Montant charge | Mouvement(s) bancaire(s) | Montant rapproché | Nb rapprochements | Statut |
|---|---:|---|---:|---:|---|
| `CHG-16d11b7c940f` | 12,00 € | QMV-07a2c46aa598 | 12,00 € | 1 | CONFIRME |
| `CHG-1d8e83f63d2f` | 5,89 € | QMV-f5f069b75623 | 5,89 € | 1 | CONFIRME |
| `CHG-28333720fcc6` | 2,40 € | QMV-6ddc58890a7f | 2,40 € | 1 | CONFIRME |
| `CHG-5dfd31538114` | 59,82 € | QMV-bca1eacdd6f4 | 59,82 € | 1 | CONFIRME |
| `CHG-acdbb2a1124c` | 48,39 € | QMV-fd24fe48af19 | 48,39 € | 1 | CONFIRME |
| `CHG-d0ff3f64c35f` | 20,25 € | QMV-39e3d2d845b2, QMV-34346b87d080 | 20,25 € | 2 | CONFIRME |
| `CHG-f46f1ae78219` | 4,90 € | QMV-d8e00892153a | 4,90 € | 1 | CONFIRME |
| **Total** | **153,65 €** | | **153,65 €** | **8** | |

La différence de 8 à 7 vient de `CHG-d0ff3f64c35f` (20,25 €), réglée par deux paiements GiFi de 5,00 € et 15,25 €. Les autres contrôles :

- 7 charges, toutes avec au moins un rapprochement confirmé vers un mouvement existant ;
- aucune charge orpheline (aucune) ;
- aucun rapprochement fantôme (aucun) ;
- aucun écart charge ↔ rapproché (aucun) ;
- aucun double comptage : le flux unifié ne contient aucun flux de charge.

### 19.5 Garde-fous ajoutés — rien ne peut revenir

| Où | Ce qui est refusé |
|---|---|
| Lot10, `classifier_mois_lot10` | Tout mois < 2026-09 est classé `ANTERIEUR_V1`, avant toute autre règle. Il n'est écrit dans aucune des tables et sa provenance le dit. Les anomalies de commission de ces réservations sont retirées (`exclure_anomalies_anterieures_v1`). |
| Paramètre | La date est relue en base (`lib_db_moteur.debut_v1`, `perimetre_v1_service`), jamais recopiée. Le test `test_cle_du_parametre_v1_identique_cote_moteur_et_application` verrouille la clé. |
| Écrans propriétaire | Le relevé mensuel, la préfacture, le relevé propriétaire, Propriétaires & règlements (liste, fiche, à contrôler) répondent `HORS_V1` : « Aucune comptabilité disponible pour cette période. ». Aucun sélecteur ne propose un mois antérieur. |
| Création côté propriétaire | Relevé de suivi (`proprietaires_suivi_service`), acompte, compensation ou régularisation (`proprietaires_tresorerie_service`) : « La comptabilité V1 débute en septembre 2026. » |
| Lot12 | Il ne lit que la période V1 (déjà en place) et ne reçoit plus d'anomalie antérieure du Lot10. |
| Lot11 | Aucun statut mensuel de clôture ou de facturation pour un mois antérieur. Les écarts ménages externes ↔ Hostaway ne sont calculés que sur la V1, côté application et côté moteur. |
| Factures fournisseurs | `factures_service.creer`, `valider`, `changer_statut` refusent une date antérieure. Migration **0121** : déclencheurs `trg_factures_fournisseurs_v1_insert` et `_update`. |
| Import PDF | Verdict `ANTERIEURE_V1`, idempotent, définitif tant que le fichier ne change pas : aucune facture, aucune dette, aucune ligne, aucune ventilation. |

### 19.6 Procédure et preuves

Rapports dans `C:\Users\Ewans\PilotageConciergerie\data\backups\finition_cutover_v1_20261002T231529Z`.

| # | Étape | Rapport | Résultat |
|---|---|---|---|
| A | Code et tests ciblés | `tests/test_finition_cutover_v1.py` | `test_finition_cutover_v1.py` 26 passed + `test_cutover_v1.py` 20 passed (46) |
| B | `finition-simuler` ×2, lecture seule | `02_simulation_1.json`, `02_simulation_2.json` | identiques (fbda9d2a545db6749427fe6a = fbda9d2a545db6749427fe6a), 0 anomalie, 15 700 lignes à supprimer |
| C | `finition-sauvegarder`, application arrêtée | `01_etat_et_sauvegarde.json` | Sauvegarde lisible ; restauration isolée identique (§19.8). Une première tentative (`…20261002T231409Z`) s'est interrompue au nettoyage de la copie de contrôle : des fichiers WAL annexes étaient restés. Sa sauvegarde, valide, est restée sur disque. L'outil a été corrigé (sauvegardes autonomes, `journal_mode=DELETE`) et c'est la seconde sauvegarde qui fait foi. |
| D | `finition-executer` : migration 0121, puis UNE transaction | `03_execution.json` | 22/22 invariants OK avant COMMIT |
| E | `finition-reconstruire` : LOT10, puis LOT11 et LOT12, sans import | `05_reconstruction.json` | SUCCES (ORCH-20261002231623-2ada2b) : LOT10 SUCCES, LOT11 SUCCES, LOT12 SUCCES |
| F | `finition-verifier`, après reconstruction puis après chaque redémarrage | `04_verification_*.json` (4) | 4 passages. Le premier, juste après la reconstruction, signalait 6 lignes de provenance : c'étaient les traces `EXCLU_PERIMETRE_ANTERIEUR_V1` du nouveau run, comptées à tort comme reliquat. La règle de comptage a été corrigée et testée (`test_la_trace_d_exclusion_lot10_n_est_ni_un_reliquat_ni_purgee`). Les 3 passages suivants : ok. |
| G | Deux redémarrages de l'application, contrôles HTTP | `06_controles_http.json` | 20 contrôles OK après chacun des deux redémarrages (août : message « Aucune comptabilité disponible pour cette période. », septembre normal, refus d'août, aucune ancienne facture ni dette) |
| H | Copie de la base réelle après purge : import PDF ×2 et actualisation depuis MENAGES_PDF (PDF présents) | §19.7 | 0 recréation |

**Invariants vérifiés avant le COMMIT :**

| Code | Vérification | Résultat |
|---|---|---|
| A | Factures propriétaires = 0 | OK |
| B | Créances = 0 (aucune facture émise, aucune allocation) | OK |
| C | Relevés propriétaires antérieurs = 0 (relevés et net par mois, tous runs) | OK |
| D | Soldes propriétaires antérieurs = 0 (règlements Lot10, tous runs) | OK |
| E | Restes à payer antérieurs = 0 (Lot10 et Lot12, tous runs) | OK |
| F | Acomptes hérités = 0 | OK |
| G | Compensations anciennes = 0 | OK |
| H | Dette fournisseur active antérieure = 0 ; factures fournisseurs antérieures = 0 | OK |
| I | Charges non rapprochées héritées = 0 | OK |
| J | Charges rapprochées inchangées | OK — 7 |
| K | Rapprochements banque ↔ charges inchangés, montants égaux | OK — 8 → 7 charges ; 153.65 € ↔ 153.65 € |
| L | Mouvements bancaires inchangés | OK |
| M | Hostaway inchangé | OK |
| N | Archive des réservations et réservations hors Hostaway inchangées | OK |
| O | Référentiels inchangés | OK |
| P | Fiche société inchangée | OK |
| Q | Paramètre V1 en place (facturation antérieure interdite) | OK |
| R | Comptabilité propriétaire antérieure : aucune donnée, tous runs confondus | OK |
| S | Comptabilité propriétaire de la période V1 intacte | OK — 8 |
| PROTEGEES | Toutes les tables protégées intactes (empreintes) | OK |
| U | PRAGMA foreign_key_check : 0 erreur | OK — 0 |
| T | PRAGMA integrity_check : ok | OK — ok |

**Vérification après coup (`finition-verifier`, dernier passage) :**

- `cutover_applique` : True
- `comptabilite_anterieure_0` : True
- `creances_0` : True
- `dettes_fournisseurs_anterieures_0` : True
- `charges_rapprochees` : 7
- `rapprochements_banque_charges` : 8
- `rapprochements_montants_egaux` : True
- `charges_non_rapprochees_0` : True
- `lot10_mois_actifs` : ['2026-09']
- `lot10_aucun_mois_anterieur` : True
- `lot10_provenance_anterieure_exclue` : True
- `releve_mois_anterieur` : HORS_V1
- `releve_mois_anterieur_hors_v1` : True
- `facture_proprietaire_mois_anterieur_refusee` : True
- `facture_proprietaire_v1_autorisee` : True
- `facture_fournisseur_anterieure_refusee` : True
- `facture_fournisseur_v1_autorisee` : True
- `integrity_ok` : True
- `foreign_key_check_0` : True
- `ok` : True

### 19.7 Copie de la base après purge — l'import ne recrée rien

La copie contient 15 PDF dans le dossier source.

- Import n°1 : 15 détectés, **0 importé**, 15 reconnus antérieurs à la V1, 0 remplacé, 0 échec.
- Import n°2 : 15 détectés, **0 importé**, 15 reconnus antérieurs à la V1, 0 remplacé, 0 échec.
- Actualisation depuis MENAGES_PDF : SUCCES — MENAGES_PDF SUCCES, MENAGES SUCCES, FLUX_LOT9 SUCCES, LOT10 SUCCES, LOT11 SUCCES, LOT12 SUCCES.
- Avant → après :
  - factures fournisseurs : 0 → 0 (antérieures 0 → 0) ;
  - dettes antérieures : 0 → 0 ;
  - charges : 7 → 7 ;
  - créances : 0 → 0 ;
  - comptabilité antérieure : 0 → 0.
- `verifier` sur la copie : ok = True.
- Créations, sur la copie uniquement :
  - facture propriétaire d'août : REFUSEE : La facturation V1 débute en septembre 2026. ;
  - facture propriétaire de septembre : AUTORISEE : BROUILLON ;
  - facture fournisseur d'août : REFUSEE : FACTURE_FOURNISSEUR_AVANT_V1 — La comptabilité V1 débute en septembre 2026. ;
  - facture fournisseur de septembre : AUTORISEE.

### 19.8 Empreintes et sauvegarde

| Moment | SHA256 | Taille | Tables / lignes | Schéma | integrity | FK |
|---|---|---:|---|---|---|---:|
| Avant la finition | `c7d489e78eaa1a149876e70b1ca5e504a9396dbc3f4c60d8e0363f2151789817` | 70 410 240 | 233 / 263 972 | 0120 | ok | 0 |
| Juste après le COMMIT | `118e8aa95e8848f30f6dfa77c20d4311b2a50024ca06d17928358f935598ed47` | 70 410 240 | 233 / 248 288 | 0121 | ok | 0 |
| État final : après reconstruction et redémarrages, application arrêtée | `a1cacb957a525121ba5b85b82d099b7c164da40371b35204e51b5bf52c8936f9` | 70 410 240 | 233 / 248 620 | 0121 | ok | 0 |

| | |
|---|---|
| Sauvegarde | `C:\Users\Ewans\PilotageConciergerie\data\backups\app_avant_finition_cutover_v1_20261002T231529Z.db` |
| SHA256 de la sauvegarde | `3cbe9ae37b61cfdde038adc8c89d5eb1bf3b403140d9636e42c202cb9db233e3` |
| Contrôle | integrity ok, FK 0, 233 tables, 263 972 lignes (identique à la source) |
| Restauration isolée | empreinte logique source `fc39dfca170dbace4147f9cb` = restaurée `fc39dfca170dbace4147f9cb` (schéma `0c2102e299184554`), integrity ok, FK 0, 233 tables, 263 972 lignes : **identique** |

**Preuve d'identité des sources à l'état final.** La comparaison est faite table par table, sur le
contenu trié : la sauvegarde d'avant la finition face à la base après reconstruction et redémarrages
(`08_preuve_identite_sources.json`).

| Groupe | Tables | Lignes avant → après | Tables différentes |
|---|---:|---|---|
| Banque (14 mouvements Qonto) | 13 | 52 → 52 | aucune |
| Hostaway (21 126 réservations) | 9 | 204 578 → 204 578 | aucune |
| Réservations hors Hostaway | 4 | 14 → 14 | aucune |
| Archive des réservations | 8 | 17 → 17 | aucune |
| Référentiels | 55 | 743 → 743 | aucune |
| Ménages (sources) | 12 | 100 → 100 | aucune |
| Chaînes des charges rapprochées (charges, rapprochements, lettrages, écritures, justificatifs) | 16 | 139 → 139 | aucune |
| Fiche société / paramètres (dont `V1_ACCOUNTING_START_DATE`) | 1 | 21 → 21 | aucune |

### 19.9 Tests

- Ciblés : `test_finition_cutover_v1.py` 26 passed + `test_cutover_v1.py` 20 passed (46).
- Modules concernés (159 fichiers) : 159 fichiers, 2 441 tests : 2 431 passed / 9 skipped / 1 failed au premier passage. L'échec, `test_menages_recalcul_mensuel_cible.py::test_08`, venait d'une clé `mois_impacte` ajoutée à tort sur le chemin de refus de l'import ; elle est retirée. Les 15 fichiers qui touchent l'import PDF ont été rejoués : 202 passed.
- Suite complète : **4 710 tests (4 684 de référence + 26 nouveaux) : 4 673 passed / 37 skipped / 0 failed**, sur le code final, en 4 lots parallèles (1 173 + 1 175 + 1 176 + 1 149 passed).

### 19.10 Limites

1. **Agrégats ménage de juin à août.** Ils ont été calculés avant la purge et ne sont pas recalculés (aucune reconstruction avant 2026-09). Ils n'alimentent plus aucune comptabilité, le Lot10 excluant ces mois. Un recalcul ménage ciblé de ces mois, s'il était lancé, les referait sans les factures purgées. C'est accepté (D-V1-FIN-6).
2. **Calculs de normalisation des sources.** Les réservations résolues et le flux unifié restent produits sur toutes les périodes, comme l'historique des réservations. Ils ne sont pas de la comptabilité propriétaire (D-V1-FIN-3).
3. **Métadonnées des runs inactifs.** `lot10_runs` et `lot12_runs` gardent leurs compteurs d'origine (nombre de lignes produites à l'époque) : c'est l'observabilité de l'exécution. Les lignes antérieures, elles, n'existent plus.
4. **Facture fournisseur antérieure nécessaire à une charge rapprochée.** Le cas n'existe pas. S'il se présentait, la finition s'arrêterait (anomalie) au lieu d'inventer une neutralisation.

---

## Annexe A — Matrice table par table (avant = sauvegarde, après = état final du §16)

Colonnes « KEEP / PURGE / KBD / REBUILD » : lignes de chaque état au moment de la simulation (KBD =
KEEP_BY_DEPENDENCY). La colonne « Après » donne l'état final, après la reconstruction ; « = » signifie
que le contenu est identique ligne à ligne à la sauvegarde.

| Table | Rôle | Avant | KEEP | PURGE | KBD | REBUILD | Après | Justification |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `hostaway_anomalies` | Source Hostaway | 484 | 484 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_cleaning_tasks` | Source Hostaway | 10 170 | 10 170 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_cleaning_tasks_extractions` | Source Hostaway | 13 | 13 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_extractions` | Source Hostaway | 13 | 13 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_listings` | Source Hostaway | 231 | 231 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_payouts` | Source Hostaway | 20 709 | 20 709 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_reservation_fees` | Source Hostaway | 2 172 | 2 172 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_reservation_finance_fields` | Source Hostaway | 149 660 | 149 660 | 0 | 0 | 0 | = | voir rôle (§5) |
| `hostaway_reservations` | Source Hostaway | 21 126 | 21 126 | 0 | 0 | 0 | = | voir rôle (§5) |
| `aircover` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ajustements_post_cloture` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `assiette_corrections_manuelles` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mois_archive_reglement` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mois_classification_legacy` | Archive des réservations | 17 | 17 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reservations_archives` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reservations_historique_cloture` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reservations_historique_corrections` | Archive des réservations | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reservation_hh_evenements` | Réservations hors Hostaway | 6 | 6 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reservation_hh_overrides` | Réservations hors Hostaway | 4 | 4 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reservations_hors_hostaway` | Réservations hors Hostaway | 4 | 4 | 0 | 0 | 0 | = | voir rôle (§5) |
| `saisie_hh_writes` | Réservations hors Hostaway | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_classement_decisions` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_classification_signaux` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_classifications` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_controle_runs` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_controles` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_import_source` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_mouvements` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_overrides` | Banque | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `caisse_transferts_banque` | Banque | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `qonto_accounts` | Banque | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `qonto_sync_runs` | Banque | 22 | 22 | 0 | 0 | 0 | = | voir rôle (§5) |
| `qonto_transactions_raw` | Banque | 14 | 14 | 0 | 0 | 0 | = | voir rôle (§5) |
| `qonto_transactions_statut_local` | Banque | 14 | 14 | 0 | 0 | 0 | = | voir rôle (§5) |
| `charges_justificatif_sequence` | Référentiel | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `fournisseur_details` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `fournisseur_evenements` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `fournisseur_menage_qualification` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `fournisseur_rattachement_evenements` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `fournisseur_rattachements` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `fournisseurs` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ik_bareme_tranches` | Référentiel | 42 | 42 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ik_baremes` | Référentiel | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ik_vehicules` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `justificatifs_sequence` | Référentiel | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mapping_categorie_compte` | Référentiel | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mapping_comptable_regles` | Référentiel | 33 | 33 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mapping_produits_facture` | Référentiel | 10 | 10 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mapping_regle_evenements` | Référentiel | 32 | 32 | 0 | 0 | 0 | = | voir rôle (§5) |
| `parametres_societe_facturation_historique` | Référentiel | 24 | 24 | 0 | 0 | 0 | 25 | voir rôle (§5) |
| `plan_comptable` | Référentiel | 43 | 43 | 0 | 0 | 0 | = | voir rôle (§5) |
| `plan_comptable_evenements` | Référentiel | 35 | 35 | 0 | 0 | 0 | = | voir rôle (§5) |
| `proprietaires_facturation` | Référentiel | 12 | 12 | 0 | 0 | 0 | = | voir rôle (§5) |
| `proprietaires_facturation_evenements` | Référentiel | 13 | 13 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_abonnements_logiciels` | Référentiel | 3 | 3 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_admin_evenements` | Référentiel | 49 | 49 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_assoc_mode` | Référentiel | 7 | 7 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_associes` | Référentiel | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_banque_regles` | Référentiel | 30 | 30 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_bareme_ik` | Référentiel | 27 | 27 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_bareme_ik_annees` | Référentiel | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_canape_parametres` | Référentiel | 4 | 4 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_canaux_reservation` | Référentiel | 5 | 5 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_cartes_paiement` | Référentiel | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_categories_charges` | Référentiel | 43 | 43 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_charges_recurrentes` | Référentiel | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_cloture_mensuelle` | Référentiel | 18 | 18 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_codes_impact` | Référentiel | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_couts_menage_interne` | Référentiel | 3 | 3 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_couts_standards_menage` | Référentiel | 10 | 10 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_gestion_logements_hist` | Référentiel | 17 | 17 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_intervenants` | Référentiel | 5 | 5 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_logements` | Référentiel | 19 | 19 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_mapping_logements` | Référentiel | 86 | 86 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_modes_paiement` | Référentiel | 6 | 6 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_parametres_generaux` | Référentiel | 7 | 7 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_proprietaires` | Référentiel | 12 | 12 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_regles_versions` | Référentiel | 3 | 3 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_setup_import_feuilles` | Référentiel | 28 | 28 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_setup_imports` | Référentiel | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_sources_systeme` | Référentiel | 11 | 11 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_statuts` | Référentiel | 29 | 29 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_statuts_payout` | Référentiel | 6 | 6 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_taux_commission` | Référentiel | 19 | 19 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_taux_heures_menage` | Référentiel | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_types_affectation` | Référentiel | 4 | 4 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_types_flux` | Référentiel | 20 | 20 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_types_lignes_menage` | Référentiel | 6 | 6 | 0 | 0 | 0 | = | voir rôle (§5) |
| `ref_types_logements` | Référentiel | 5 | 5 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_classification` | Factures fournisseurs reçues | 15 | 15 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_evenements` | Factures fournisseurs reçues | 16 | 16 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_interpretations` | Factures fournisseurs reçues | 2 | 2 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_lignes` | Factures fournisseurs reçues | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_lignes_menage` | Factures fournisseurs reçues | 127 | 127 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_lignes_menage_detail` | Factures fournisseurs reçues | 127 | 127 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_lignes_menage_pdf` | Factures fournisseurs reçues | 127 | 127 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_pdf_diagnostics` | Factures fournisseurs reçues | 18 | 18 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_ventilation_parts` | Factures fournisseurs reçues | 99 | 99 | 0 | 0 | 0 | = | voir rôle (§5) |
| `facture_ventilations` | Factures fournisseurs reçues | 19 | 19 | 0 | 0 | 0 | = | voir rôle (§5) |
| `factures` | Factures fournisseurs reçues | 15 | 15 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_pdf_fichiers_hash` | Factures fournisseurs reçues | 15 | 15 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reglements_fournisseurs` | Factures fournisseurs reçues | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `intervenant_menage_allocations` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `intervenant_menage_dettes` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `intervenant_menage_paiements` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `intervenant_menage_recalculs` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menage_evenements` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menage_overrides` | Ménages (sources et décisions) | 8 | 8 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_declarations_conflits` | Ménages (sources et décisions) | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_declarations_extra` | Ménages (sources et décisions) | 39 | 39 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_declarations_historique` | Ménages (sources et décisions) | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_declarations_internes` | Ménages (sources et décisions) | 39 | 39 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_externes_historique` | Ménages (sources et décisions) | 13 | 13 | 0 | 0 | 0 | = | voir rôle (§5) |
| `audit_events` | Observabilité / technique | 74 | 74 | 0 | 0 | 0 | = | voir rôle (§5) |
| `calculs_indicateurs` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `calculs_run_lots` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `calculs_runs` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `calculs_sauvegardes` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `cloture_statut_evenements` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `cloture_statuts` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `controles_runs` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `controles_suivi` | Observabilité / technique | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `controles_suivi_historique` | Observabilité / technique | 1 | 1 | 0 | 0 | 0 | = | voir rôle (§5) |
| `drafts` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_actualisations_hostaway` | Observabilité / technique | 3 | 3 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_changements_mois_clotures` | Observabilité / technique | 21 | 21 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_recalcul_runs` | Observabilité / technique | 12 | 12 | 0 | 0 | 0 | = | voir rôle (§5) |
| `menages_runs_cibles` | Observabilité / technique | 100 | 100 | 0 | 0 | 0 | = | voir rôle (§5) |
| `mois_reouvertures` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `moteur_run_etapes` | Observabilité / technique | 287 | 287 | 0 | 0 | 0 | 301 | voir rôle (§5) |
| `moteur_runs` | Observabilité / technique | 40 | 40 | 0 | 0 | 0 | 41 | voir rôle (§5) |
| `orchestrateur_dataset_evenements` | Observabilité / technique | 349 | 349 | 0 | 0 | 0 | 370 | voir rôle (§5) |
| `orchestrateur_datasets` | Observabilité / technique | 11 | 11 | 0 | 0 | 0 | 11 | voir rôle (§5) |
| `orchestrateur_verrous` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `periode_evenements` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `periodes_comptables` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `periods` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `pipeline_runs` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `run_history` | Observabilité / technique | 57 | 57 | 0 | 0 | 0 | 58 | voir rôle (§5) |
| `saisie_charges_writes` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `sauvegardes_base` | Observabilité / technique | 47 | 47 | 0 | 0 | 0 | 48 | voir rôle (§5) |
| `sauvegardes_base_tracabilite` | Observabilité / technique | 47 | 47 | 0 | 0 | 0 | 48 | voir rôle (§5) |
| `schema_migrations` | Observabilité / technique | 108 | 108 | 0 | 0 | 0 | 109 | voir rôle (§5) |
| `screen_states` | Observabilité / technique | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `snapshots` | Observabilité / technique | 12 | 12 | 0 | 0 | 0 | = | voir rôle (§5) |
| `banque_imports` | Mixte | 2 | 0 | 2 | 0 | 0 | 0 | PURGE 2 : imports de recette — Fichier « releve_recette.csv », aucun mouvement en base (fixture 14d). · KEEP 0 : autres imports — Journal bancaire réel. |
| `banque_rapprochement_evenements` | Mixte | 14 | 0 | 4 | 10 | 0 | 10 | PURGE 4 : événements des rapprochements de recette — Suivent leur rapprochement. · KEEP_BY_DEPENDENCY 10 : événements des rapprochements conservés — Suivent leur rapprochement. |
| `banque_rapprochements` | Mixte | 13 | 2 | 3 | 8 | 0 | 10 | KEEP_BY_DEPENDENCY 8 : banque ↔ charge validés — Règle absolue : un rapprochement validé mouvement ↔ charge n'est jamais supprimé. · KEEP 2 : autres rapprochements réels — Apport en compte courant, retrait d'espèces… : opérations bancaires réelles. · PURGE 3 : données de recette / orphelines — Mouvement bancaire inexistant et objet de recette (CHG_SEED_*, RES_HOSTAWAY_2026_06_05…), documentés comme fixtures (mission 14d). |
| `banque_suggestion_decisions` | Mixte | 2 | 0 | 2 | 0 | 0 | 0 | PURGE 2 : décisions de recette — Visent un mouvement inexistant et des charges CHG_SEED_* (fixtures). · KEEP 0 : autres décisions — Mémoire réelle. |
| `charge_evenements` | Mixte | 24 | 0 | 10 | 14 | 0 | 14 | PURGE 10 : rattachées à une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 14 : rattachées à une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `charges` | Mixte | 12 | 7 | 5 | 0 | 0 | 7 | KEEP 7 : charges rapprochées — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. · PURGE 5 : charges non rapprochées — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. |
| `charges_affectation_evenements` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : affectation d'une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 0 : affectation d'une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `charges_affectations` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : rattachées à une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 0 : rattachées à une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `charges_perimetre_analytique` | Mixte | 3 | 0 | 0 | 3 | 0 | = | PURGE 0 : rattachées à une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 3 : rattachées à une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `charges_perimetre_menage` | Mixte | 3 | 0 | 0 | 3 | 0 | = | PURGE 0 : rattachées à une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 3 : rattachées à une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `charges_refacturation_evenements` | Mixte | 2 | 0 | 1 | 1 | 0 | 1 | PURGE 1 : position d'une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 1 : position d'une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `charges_refacturation_positions` | Mixte | 2 | 0 | 1 | 1 | 0 | 1 | PURGE 1 : position d'une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 1 : position d'une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `cloture_documents` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : clôtures d'avant la V1 — Clôtures de l'ancien modèle (mois antérieurs à la V1). · KEEP 0 : clôtures de la période V1 — Premier mois V1 : 2026-09. |
| `cloture_elements` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : clôtures d'avant la V1 — Clôtures de l'ancien modèle (mois antérieurs à la V1). · KEEP 0 : clôtures de la période V1 — Premier mois V1 : 2026-09. |
| `cloture_evenements` | Mixte | 8 | 2 | 6 | 0 | 0 | 2 | PURGE 6 : clôtures d'avant la V1 — Clôtures de l'ancien modèle (mois antérieurs à la V1). · KEEP 2 : clôtures de la période V1 — Premier mois V1 : 2026-09. |
| `clotures_mensuelles` | Mixte | 3 | 1 | 2 | 0 | 0 | 1 | PURGE 2 : clôtures d'avant la V1 — Clôtures de l'ancien modèle (mois antérieurs à la V1). · KEEP 1 : clôtures de la période V1 — Premier mois V1 : 2026-09. |
| `ecriture_evenements` | Mixte | 22 | 4 | 4 | 14 | 0 | 18 | PURGE 4 : écritures des factures/objets purgés — Purement dérivées de la facturation propriétaire purgée. · KEEP_BY_DEPENDENCY 14 : écritures banque ↔ charges conservées — Règlement d'une charge conservée : la chaîne reste lisible. · KEEP 4 : autres écritures V1 — Opérations bancaires réelles de septembre (apport, retrait). |
| `ecriture_ligne_ventilation` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : écritures des factures/objets purgés — Purement dérivées de la facturation propriétaire purgée. · KEEP_BY_DEPENDENCY 0 : écritures banque ↔ charges conservées — Règlement d'une charge conservée : la chaîne reste lisible. · KEEP 0 : autres écritures V1 — Opérations bancaires réelles de septembre (apport, retrait). |
| `ecriture_lignes` | Mixte | 25 | 4 | 6 | 15 | 0 | 19 | PURGE 6 : écritures des factures/objets purgés — Purement dérivées de la facturation propriétaire purgée. · KEEP_BY_DEPENDENCY 15 : écritures banque ↔ charges conservées — Règlement d'une charge conservée : la chaîne reste lisible. · KEEP 4 : autres écritures V1 — Opérations bancaires réelles de septembre (apport, retrait). |
| `ecritures` | Mixte | 12 | 2 | 3 | 7 | 0 | 9 | PURGE 3 : écritures des factures/objets purgés — Purement dérivées de la facturation propriétaire purgée. · KEEP_BY_DEPENDENCY 7 : écritures banque ↔ charges conservées — Règlement d'une charge conservée : la chaîne reste lisible. · KEEP 2 : autres écritures V1 — Opérations bancaires réelles de septembre (apport, retrait). |
| `flux_lettrage_evenements` | Mixte | 7 | 0 | 0 | 7 | 0 | = | PURGE 0 : lettrage d'un objet purgé — Objet purgé. · KEEP_BY_DEPENDENCY 7 : lettrages banque ↔ charge conservés — Lettrage validé d'une charge conservée : la chaîne banque ↔ charge reste lisible. |
| `flux_lettrage_lignes` | Mixte | 15 | 0 | 0 | 15 | 0 | = | PURGE 0 : lettrage d'un objet purgé — Objet purgé. · KEEP_BY_DEPENDENCY 15 : lettrages banque ↔ charge conservés — Lettrage validé d'une charge conservée : la chaîne banque ↔ charge reste lisible. |
| `flux_lettrages` | Mixte | 7 | 0 | 0 | 7 | 0 | = | PURGE 0 : lettrage d'un objet purgé — Objet purgé. · KEEP_BY_DEPENDENCY 7 : lettrages banque ↔ charge conservés — Lettrage validé d'une charge conservée : la chaîne banque ↔ charge reste lisible. |
| `flux_propositions_refusees` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : refus visant un objet purgé — N'a plus d'objet. · KEEP 0 : autres refus — Mémoire des refus de rapprochement. |
| `ik` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : rattachées à une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 0 : rattachées à une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `ik_depenses_activite` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : IK d'une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 0 : IK d'une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `ik_evenements` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : IK d'une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 0 : IK d'une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `ik_trajets` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : IK d'une charge purgée — Charge héritée sans rapprochement bancaire validé : purgée, quels que soient sa date, son statut ou son montant. · KEEP_BY_DEPENDENCY 0 : IK d'une charge conservée — Charge héritée disposant d'un rapprochement bancaire validé : conservée avec toute sa chaîne. |
| `justificatif_evenements` | Mixte | 10 | 0 | 0 | 10 | 0 | = | KEEP_BY_DEPENDENCY 10 : événements de justificatifs conservés — Historique d'une pièce conservée. |
| `justificatifs` | Mixte | 5 | 0 | 0 | 5 | 0 | = | KEEP_BY_DEPENDENCY 5 : justificatifs de charges conservées — Pièce d'une charge conservée (la base interdit toute suppression). |
| `mouvements_tresorerie_proprietaires` | Mixte | 1 | 0 | 1 | 0 | 0 | 0 | PURGE 1 : acomptes/reversements sans lien bancaire — Ancien acompte lié à une facture purgée : jamais transformé en crédit V1. · KEEP 0 : liés à la banque — Argent réel. |
| `mouvements_tresorerie_proprietaires_evenements` | Mixte | 2 | 0 | 2 | 0 | 0 | 0 | PURGE 2 : événements des mouvements purgés — Suivent leur mouvement. · KEEP 0 : autres — Suivent leur mouvement. |
| `od_lignes` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : OD de l'ancienne période — Antérieure à la V1. · KEEP 0 : OD de la période V1 — Période V1. |
| `operations_caisse` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : caisse de l'ancienne période — Antérieure à la V1. · KEEP 0 : caisse de la période V1 — Période V1. |
| `operations_diverses` | Mixte | 0 | 0 | 0 | 0 | 0 | = | PURGE 0 : OD de l'ancienne période — Antérieure à la V1. · KEEP 0 : OD de la période V1 — Période V1. |
| `parametres_societe_facturation` | Mixte | 20 | 20 | 0 | 0 | 0 | 21 | KEEP 20 : fiche société et facturation — Fiche canonique de Chouette Patrimoine : conservée à l'identique. · REBUILD 0 : paramètre de cutover V1 — V1_ACCOUNTING_START_DATE posé par le cutover (une ligne ajoutée). |
| `proprietaires_paiement` | Mixte | 1 | 0 | 1 | 0 | 0 | 0 | PURGE 1 : cycle de règlement d'avant la V1 — État de recouvrement de l'ancien environnement. · KEEP 0 : cycle de la période V1 — Période V1. |
| `proprietaires_releve_cycle` | Mixte | 1 | 0 | 1 | 0 | 0 | 0 | PURGE 1 : cycle de règlement d'avant la V1 — État de recouvrement de l'ancien environnement. · KEEP 0 : cycle de la période V1 — Période V1. |
| `proprietaires_releve_evenements` | Mixte | 2 | 0 | 2 | 0 | 0 | 0 | PURGE 2 : cycle de règlement d'avant la V1 — État de recouvrement de l'ancien environnement. · KEEP 0 : cycle de la période V1 — Période V1. |
| `proprietaires_releves` | Mixte | 2 | 0 | 2 | 0 | 0 | 0 | PURGE 2 : cycle de règlement d'avant la V1 — État de recouvrement de l'ancien environnement. · KEEP 0 : cycle de la période V1 — Période V1. |
| `credit_client_evenements` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `credits_clients` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `factures_proprietaires` | Facturation propriétaire et créances | 27 | 0 | 27 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_conformite` | Facturation propriétaire et créances | 3 | 0 | 3 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_evenements` | Facturation propriétaire et créances | 42 | 0 | 42 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_lignes` | Facturation propriétaire et créances | 83 | 0 | 83 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_lignes_charge` | Facturation propriétaire et créances | 1 | 0 | 1 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_lignes_detail` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `factures_proprietaires_lignes_provenance` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `factures_proprietaires_meta` | Facturation propriétaire et créances | 26 | 0 | 26 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_reservations` | Facturation propriétaire et créances | 144 | 0 | 144 | 0 | 0 | 0 | voir rôle (§5) |
| `factures_proprietaires_sequence` | Facturation propriétaire et créances | 2 | 0 | 2 | 0 | 0 | 0 | voir rôle (§5) |
| `imputations_airbnb` | Facturation propriétaire et créances | 2 | 0 | 2 | 0 | 0 | 0 | voir rôle (§5) |
| `proprietaire_allocations` | Facturation propriétaire et créances | 1 | 0 | 1 | 0 | 0 | 0 | voir rôle (§5) |
| `proprietaire_recalculs` | Facturation propriétaire et créances | 549 | 0 | 549 | 0 | 0 | 0 | voir rôle (§5) |
| `rapprochement_evenements` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `rapprochements_reglements` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `reglement_repartitions` | Facturation propriétaire et créances | 0 | 0 | 0 | 0 | 0 | = | voir rôle (§5) |
| `controles_lot11_constats` | Données dérivées | 27 | 0 | 0 | 0 | 27 | 31 | voir rôle (§5) |
| `controles_lot11_constats_champs` | Données dérivées | 26 | 0 | 0 | 0 | 26 | 30 | voir rôle (§5) |
| `controles_lot11_dashboard_mois` | Données dérivées | 21 | 0 | 0 | 0 | 21 | 21 | voir rôle (§5) |
| `controles_lot11_runs` | Données dérivées | 11 | 0 | 0 | 0 | 11 | 12 | voir rôle (§5) |
| `flux_unifies` | Données dérivées | 363 | 0 | 0 | 0 | 363 | 359 | voir rôle (§5) |
| `flux_unifies_runs` | Données dérivées | 13 | 0 | 0 | 0 | 13 | 14 | voir rôle (§5) |
| `lot10_commissions` | Données dérivées | 2 590 | 0 | 0 | 0 | 2 590 | 2 825 | voir rôle (§5) |
| `lot10_commissions_a_controler` | Données dérivées | 1 041 | 0 | 0 | 0 | 1 041 | 1 139 | voir rôle (§5) |
| `lot10_net_exploitation` | Données dérivées | 2 590 | 0 | 0 | 0 | 2 590 | 2 825 | voir rôle (§5) |
| `lot10_net_reglement` | Données dérivées | 555 | 0 | 0 | 0 | 555 | 599 | voir rôle (§5) |
| `lot10_net_vue_mois` | Données dérivées | 411 | 0 | 0 | 0 | 411 | 443 | voir rôle (§5) |
| `lot10_resultats` | Données dérivées | 1 179 | 0 | 0 | 0 | 1 179 | 1 285 | voir rôle (§5) |
| `lot10_run_mois_provenance` | Données dérivées | 114 | 0 | 0 | 0 | 114 | 129 | voir rôle (§5) |
| `lot10_runs` | Données dérivées | 11 | 0 | 0 | 0 | 11 | 12 | voir rôle (§5) |
| `lot12_a_controler` | Données dérivées | 1 046 | 0 | 0 | 0 | 1 046 | 1 144 | voir rôle (§5) |
| `lot12_controle_mensuel` | Données dérivées | 555 | 0 | 0 | 0 | 555 | 563 | voir rôle (§5) |
| `lot12_dashboard_facturation` | Données dérivées | 411 | 0 | 0 | 0 | 411 | 418 | voir rôle (§5) |
| `lot12_prefactures_entete` | Données dérivées | 555 | 0 | 0 | 0 | 555 | 563 | voir rôle (§5) |
| `lot12_prefactures_id_legacy` | Données dérivées | 555 | 0 | 0 | 0 | 555 | 563 | voir rôle (§5) |
| `lot12_prefactures_lignes` | Données dérivées | 6 797 | 0 | 0 | 0 | 6 797 | 6 895 | voir rôle (§5) |
| `lot12_runs` | Données dérivées | 11 | 0 | 0 | 0 | 11 | 12 | voir rôle (§5) |
| `menages_cout_complet` | Données dérivées | 39 | 0 | 0 | 0 | 39 | = | voir rôle (§5) |
| `menages_cout_complet_provenance` | Données dérivées | 16 | 0 | 0 | 0 | 16 | = | voir rôle (§5) |
| `menages_gainperte` | Données dérivées | 39 | 0 | 0 | 0 | 39 | = | voir rôle (§5) |
| `menages_rapprochement` | Données dérivées | 124 | 0 | 0 | 0 | 124 | = | voir rôle (§5) |
| `menages_taches_enrichies` | Données dérivées | 825 | 0 | 0 | 0 | 825 | = | voir rôle (§5) |
| `reservations_calculees` | Données dérivées | 16 131 | 0 | 0 | 0 | 16 131 | 17 784 | voir rôle (§5) |
| `reservations_datasets` | Données dérivées | 20 | 0 | 0 | 0 | 20 | 22 | voir rôle (§5) |
| `reservations_resolues` | Données dérivées | 16 131 | 0 | 0 | 0 | 16 131 | 17 784 | voir rôle (§5) |
| **Total (233 tables)** | | **260 560** | **207 295** | **938** | **120** | **52 207** | **263 972** | |

### Justification par rôle

| Rôle | Justification |
|---|---|
| Source Hostaway | Donnée source réelle (réservations, finances, payouts, listings, tâches) : historique métier conservé intégralement, toutes périodes. |
| Archive des réservations | Archive et classification historiques des réservations : conservées intactes. |
| Réservations hors Hostaway | Source métier indépendante saisie dans l'application : conservée. |
| Banque | Mouvements bancaires et leur journal : jamais supprimés, quelle que soit la date. |
| Référentiel | Référentiel nécessaire au fonctionnement futur et à l'interprétation de l'historique (périodes de validité comprises) : jamais remis à zéro. |
| Factures fournisseurs reçues | Factures des prestataires (PDF importés) : source des coûts ménages externes, donc historique métier ; aucune écriture comptable n'en est issue. Les purger serait défait par le prochain import PDF, qui les recréerait depuis les documents sources. |
| Ménages (sources et décisions) | Déclarations, historiques et arbitrages humains des ménages : historique métier. |
| Observabilité / technique | Journal des runs, sauvegardes, migrations, verrous, suivi des contrôles : conservés pour le diagnostic. |
| Facturation propriétaire et créances | Ancienne facturation propriétaire et tout ce qui n'existe que pour elle : la V1 repart avec 0 facture et 0 créance. |
| Données dérivées | Reconstruites par l'orchestrateur depuis les sources conservées, après le cutover (les préfactures ne portent plus que sur la période V1). |

## Annexe B — Reprendre la procédure

```
cd 05_APPLICATION
../.venv/Scripts/python.exe tools/cutover_v1.py etat           # empreinte + contrôles, lecture seule
../.venv/Scripts/python.exe tools/cutover_v1.py verifier       # contrôles post-cutover (sans écriture)
```

`sauvegarder`, `simuler`, `executer` et `reconstruire` ont été joués une fois, le 2026-10-02.
`executer` refuse désormais tout second passage : le cutover est appliqué et le paramètre est immuable.

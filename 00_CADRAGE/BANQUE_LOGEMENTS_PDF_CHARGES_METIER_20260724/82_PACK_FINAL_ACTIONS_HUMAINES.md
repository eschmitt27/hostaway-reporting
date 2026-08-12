# 82 — Pack final des actions humaines restantes (2026-08-12)

Document unique, prêt à l'emploi. Remplace la lecture brute « 222 + 83 = 305 décisions Banque » :
après ventilation exacte, le vrai total est **138 décisions Banque** (166 lignes `PAYOUT_PLATEFORME`
Airbnb exclues — 0 décision requise, jamais de rapprochement réservation).

## 0. Continuité

HEAD `b54dda4`, branche `feature/banque-logements-pdf-charges-metier`, master `8b47807` intact,
worktree propre au début de cette mission. Port 8000 : constaté libre (PID 21136 déjà absent
depuis la mission précédente, non lié à cette mission, non manipulé). Mode réel : OFF, inchangé.

## 1. Baseline clôture (rappel)

| Niveau | Nombre |
|---|---:|
| BLOQUANT | 0 |
| A_CONTROLER | 14 |
| INFO | 10 |
| **TOTAL** | **24** |

**Clôture technique : GO.** Aucune correction déterministe supplémentaire trouvée cette mission
(0 bug — voir §6).

## 2. BLOC 1 — RÉSERVATIONS (42 saisies humaines)

Audit terminé, **aucune nouvelle recherche de preuve** effectuée (déjà épuisée, 0 nouvelle preuve
API Hostaway). Répartition vérifiée sur baseline fraîche :

| Canal | Nombre |
|---|---:|
| Direct | 38 |
| VRBO | 4 |
| **TOTAL** | **42** |

### Mécanisme existant (audité, non modifié)

**Direct** — écran `/reservations/nouvelle` (route `app/routes/reservations.py`), classeur
`SAISIE` (`app/readers/saisie_hh_reader.py`, `MANUAL_COL_MAP`), écrit par
`app/writers/saisie_hh_writer.py` dans `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx`.
Champ économique clé : **`total_percu`** (colonne L, montant total perçu de la réservation),
lié via `reservation_id_hostaway` (colonne H). Statut forcé `A_CONTROLER` à la saisie puis
recalculé par Lot10 (branche `df_hh_norm`).

**VRBO** — backfill CSV historique `01_SOURCES_BRUTES/VRBO/IMPORT_UNIQUE_Revenus_*.csv`, colonnes
`N° de réservation` (référence VRBO) et **`Montant du paiement`** (payout net), repris par
`lot4ter` dans `HIST_Reservations_Cloturees.xlsx` (branche `df_vrbo_norm` de Lot10). Aucun écran
dédié : ajout d'une ligne au CSV existant avec la référence de réservation VRBO et le montant net
réellement encaissé.

**Aucune fonctionnalité nouvelle nécessaire dans les deux cas.**

### Preuve sur fixtures (0 vraie réservation touchée)

Suite de tests existante exerçant intégralement le parcours Direct (saisie → validation →
recalcul → sortie de A_CONTROLER → commission/net propriétaire → historique) :
`test_saisie_hh_app2e.py`, `test_reservations_hh.py`, `test_saisie_hh_schema_migration.py`,
`test_saisie_hh_no_real_file.py` — **58/58 verts**. Le parcours est prouvé fonctionnel de bout en
bout ; aucune des 42 réservations réelles n'a été modifiée.

### Pack opérateur — DIRECT (38 cas, regroupés par logement×propriétaire)

| Logement | Propriétaire | Nb | Mois concernés | reservation_id (opaques, Hostaway) |
|---|---|---:|---|---|
| LOG_0001 | PROP_0001 | 1 | 2026-06 | 60075660 |
| LOG_0004 | PROP_0004 | 2 | 2026-07, 2026-10 | 61255258, 54480315 |
| LOG_0005 | PROP_0005 | 11 | 2026-01, 2026-02 | 57761021, 57761096, 57761118, 57761166, 57761240, 57761278, 56221047, 56221132, 56221180, 56221220, 56221250 |
| LOG_0010 | PROP_0005 | 1 | 2026-02 | 55123451 |
| LOG_0012 | PROP_0009 | 8 | 2026-08, 09, 11, 12, 2027-01, 02 | 61736936, 62255041, 61737110, 62255165, 61737520, 61737635, 61737735, 61737924 |
| LOG_0013 | PROP_0009 | 1 | 2026-05 | 57495107 |
| LOG_0014 | PROP_0010 | 3 | 2026-06, 2026-07 | 61044223, 62473289, 63644250 |
| LOG_0015 | PROP_0011 | 11 | 2026-02, 05-12, 2027-01, 02 | 54246504, 60046173, 60047436, 60047591, 60047651, 60047710, 60047754, 60047787, 60047842, 60047897, 60047948 |

Champ à fournir pour chacune : **montant total réellement perçu** (`total_percu`), saisi via
`/reservations/nouvelle` en liant le `reservation_id_hostaway`. Note : LOG_0005 (11 cas,
jan-fév 2026) et LOG_0015 (11 cas, récurrents sur 12 mois) sont des blocs probablement liés à un
même type de contrat récurrent — à vérifier au moment de la saisie, peut accélérer le travail si un
seul montant/règle s'applique à tout le bloc.

### Pack opérateur — VRBO (4 cas)

| Logement | Propriétaire | Mois | reservation_id |
|---|---|---|---|
| LOG_0008 | PROP_0008 | 2026-06 | 56388919 |
| LOG_0008 | PROP_0008 | 2026-08 | 57780060 |
| LOG_0008 | PROP_0008 | 2026-10 | 59855296 |
| LOG_0017 | PROP_0008 | 2026-11 | 62163639 |

Champ à fournir : **montant du paiement VRBO** (référence de réservation VRBO + montant net),
ajouté au CSV backfill existant, puis relance `lot4ter` (sur copie d'abord).

**Aucune valeur n'a été devinée ni proposée. 0 PII dans ce document.**

## 3. BLOC 2 — BANQUE : la vraie file humaine (138, pas 305)

### 3.1 Ventilation opérationnelle des 541 mouvements

| Catégorie | Lignes physiques | Mouvements économiques distincts |
|---|---:|---:|
| A. DÉJÀ CLASSÉ DÉTERMINISTEMENT (`CLASSE`) | 236 | 236 |
| B. PAYOUT_PLATEFORME (Airbnb, 0 décision) | 166 | 166 |
| C. TRÉSORERIE PROPRIÉTAIRE / candidat légitime | 56 | 56 |
| D. A_ENVOYER_IA | 83 | **82** (1 doublon `mouvement_id`, 2 lignes sources distinctes conservées, cf. §3.4) |
| E. AUTRE DÉCISION HUMAINE | 0 | 0 |
| F. INFO / aucun traitement | 0 (déjà compté en A/B) | — |
| **TOTAL** | **541** | — |

### 3.2 PAYOUT_PLATEFORME (166) — exclues du travail humain, catégorie confirmée correcte

**Vérification code (§12 du cadrage)** : la catégorie moteur (`categorie=PAYOUT_PLATEFORME`,
règle `R_001`, `lot8b_banque_regles.py`) est **déjà correcte**. L'application
(`app/services/banques_candidats_service.py`, commentaire explicite) **exclut déjà** ces
mouvements de tout candidat de rapprochement réservation — comportement conforme au commit
`78877da`, non réintroduit.

**Constat = A (libellé de reporting obsolète dans nos propres docs de mission), pas B (bug moteur).**
Les documents `81_BASELINE_CLOTURE_APRES_NETTOYAGE.md` et `HANDOFF_CANONIQUE.md` des missions
précédentes désignaient globalement les 222 lignes `RAPPROCHEMENT_REQUIS` comme
« rapprochement humain / rapprochement métier légitime » sans les décomposer — imprécis. **Corrigé
dans cette mission** (§9 ci-dessous). **Aucune correction de code.**

Le statut `RAPPROCHEMENT_REQUIS` porté par ces 166 lignes reste correct fonctionnellement : il
signifie « en attente de l'export Airbnb détaillé pour vérification globale du total déclaré »,
**jamais** « en attente d'une décision de rapprochement à une réservation ». Aucun produit
économique n'est créé pour ces lignes (`CTRL_RAPPROCHEMENT_8C` : `TOTAL_AIRBNB_INFORMATIF`,
« INFORMATIF UNIQUEMENT »). **0 décision humaine réelle. 1 prérequis externe optionnel** :
obtenir l'export Airbnb détaillé (priorité 3, non bloquant clôture).

### 3.3 TRÉSORERIE PROPRIÉTAIRE (56)

Aucune nature économique déduite (pas d'acompte/restitution/avance/remboursement inventé,
conformément à la règle absolue). Prérequis unique documenté par le moteur lui-même
(`CTRL_RAPPROCHEMENT_8C`, code `LOT5_PREREQUIS_MANQUANT`) : `SAISIE_AcomptesProprietaires.xlsx`
(Lot 5) est vide. Tant qu'il n'est pas alimenté, le rapprochement complet par mouvement reste
`EN_ATTENTE_SAISIE_ACOMPTE` pour les 56.

| Mouvement (opaque) | Date | Sens | Montant | Catégorie moteur | Décision nécessaire |
|---|---|---|---:|---|---|
| 56 mouvements, `mouvement_id` interne, `proprietaire_id` opaque déjà identifié | divers 2025-2026 | CREDIT | total 27 069,18 € | `VIREMENT_PROPRIETAIRE_A_RAPPROCHER` | Alimenter Lot 5 (acomptes propriétaires), puis classer chaque mouvement individuellement |

Détail nominatif (mouvement_id, dates, montants) volontairement non recopié ici — disponible dans
`BANQUE_LOT8_IMPORT.xlsx` onglet `RAPPROCH_PROPRIETAIRES_ATTENTE`, aucune donnée manquante côté
moteur. **1 action bloquante immédiate (alimenter Lot 5) → débloque ensuite jusqu'à 56
classifications individuelles.**

### 3.4 A_ENVOYER_IA (83 physiques → 82 décisions distinctes)

Vérifié programmatiquement : un seul couple date/montant/libellé (`PAIEMENT CB 1401 27370
SAINT P`, 120,00 €, 2026-01-15) génère le même `mouvement_id` sur **2 lignes sources distinctes**
(`ligne_source` 149 et 151 du relevé), la seconde portant déjà `codes_anomalie =
DOUBLON_BANCAIRE_POTENTIEL`. Conception conforme à l'exigence : **une seule décision humaine par
`mouvement_id`**, **aucune double comptabilisation**, **lignes sources conservées pour
traçabilité** (les 2 lignes physiques restent visibles dans `NORM_Banque`/`BRUT_Banque`). Rien à
corriger. **Chiffre réel actuel : 82 décisions distinctes.**

### 3.5 Vrai compteur Banque humain (remplace le brut 222+83)

| | Nombre |
|---|---:|
| BANQUE TOTAL | 541 |
| DÉTERMINISTE / AUCUNE ACTION | 236 |
| PAYOUT_PLATEFORME (0 décision) | 166 |
| HUMAIN PROPRIÉTAIRE | 56 |
| A_ENVOYER_IA DISTINCT | 82 |
| AUTRE HUMAIN | 0 |
| **TOTAL RÉEL DE DÉCISIONS HUMAINES BANQUE** | **138** |

C'est ce chiffre (138), et non 222+83=305, qui devient la baseline opérationnelle Banque.

## 4. BLOC 3 — AUTRES (résiduels + comptabilité)

### 4.1 A_CONTROLER résiduels — décomposition exacte des 14

| Code | Lignes | Représente | Déjà compté ailleurs ? |
|---|---:|---|---|
| `RESERVATION_A_CONTROLER_SANS_COMMISSION` | 1 (42 résa) | Bloc 1 | Oui — Pack Réservations |
| `VRBO_MONTANT_NON_RENSEIGNE` | 1 (4 résa) | Bloc 1 (sous-ensemble) | Oui — Pack Réservations |
| `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` | 9 (1/mois) | Bloc 2 (222 lignes RAPPROCHEMENT_REQUIS réparties sur 9 mois ouverts) | Oui — Pack Banque |
| `MENAGE_EXTERNE_ECART_HOSTAWAY` | 1 (4 logements) | **Nouveau résiduel réel** | Non |
| `MENAGE_EXTERNE_LOGEMENT_HORS_HA` | 1 (2 logements) | **Nouveau résiduel réel** | Non |
| `SOURCE_SHEET_PROVENANCE_INCOMPLETE` | 1 | **Nouveau résiduel réel (info, non prioritaire)** | Non |

**Sur les 14 A_CONTROLER, seuls 3 sont un travail réellement nouveau** (Ménages/provenance) — les
11 autres ne font que reformuler, en agrégat, les Blocs 1 et 2 déjà couverts ci-dessus.

Détail des 3 résiduels réels : audit confirmé 0 bug technique (session précédente, `81_BASELINE`
§4, reconfirmé cette mission — aucune source amont n'a changé). `MENAGE_EXTERNE_ECART_HOSTAWAY`
(4 logements, écart volume facture vs comptage Hostaway) et `MENAGE_EXTERNE_LOGEMENT_HORS_HA`
(2 logements, facture sans comptage Hostaway correspondant) nécessitent une revue humaine
ménages/factures, hors périmètre technique de cette mission (Réservations/Banque).
`SOURCE_SHEET_PROVENANCE_INCOMPLETE` est un garde-fou informatif (lot6b/lot6f) — comportement
voulu, pas un blocage.

### 4.2 INFO (10) — légitimes, non transformés en travail

Sources non alimentées par construction à ce stade du projet (Aircover, Imputations Airbnb,
Ajustements post-clôture, Suivi associé, Charges/Acomptes/IK vides) : `HC_ZERO_SOURCES_VIDES` (4),
sources absentes (4), `MENAGE_HA_SANS_FACTURE_EXTERNE` + `MENAGE_EXTERNE_RAPPROCHE_HOSTAWAY` (2).
**0 action.**

### 4.3 Mappings comptables — bloquent-ils techniquement ?

Audit non refait (état déjà connu, `46_PLAN_COMPTABLE_ET_MAPPINGS.md`). Réponse à la question du
cadrage (§19) : **non-blocage technique confirmé.** `606000` sert de compte générique unique pour
toute charge, avec un contrôle `CTRL_CPT_MAPPING_CATEGORIE_NON_ARBITRE` qui signale chaque ligne
`A_CONTROLER` sans empêcher la génération d'écritures équilibrées. **Réserve acceptable avec
écriture PROPOSÉE/A_CONTROLER**, pas un blocage de préparation du mode réel. Idem frais bancaires,
trésorerie propriétaire, associés/IK : mappings provisoires mais non structurellement bloquants.
TVA : hors périmètre, décision utilisateur déjà actée (« pas de TVA pour le moment »).

## 5. Bugs trouvés cette mission

**0.** Un point mérite mention sans être un bug : le doublon `mouvement_id` §3.4 est un
comportement de conception voulu et déjà correctement géré (déduplication par `mouvement_id`,
traçabilité par `ligne_source` conservée) — vérifié, pas corrigé, car rien à corriger.

## 6. Tests

0 code modifié cette mission → pas de campagne large. Ciblés exécutés :
- `test_saisie_hh_app2e.py`, `test_reservations_hh.py`, `test_saisie_hh_schema_migration.py`,
  `test_saisie_hh_no_real_file.py` : **58/58 verts** (parcours 42 Direct/VRBO prouvé sur
  fixtures).
- Régression Lot10/contrôles/clôture/confidentialité : héritée de la mission précédente
  (`b54dda4`, 24/24 verts), aucune source amont n'a changé depuis. Non rejouée en double —
  proportionnalité (§26 du cadrage).

**0 nouvel échec.**

## 7. Intégrité

`git status --short` : propre au début et à la fin (aucune source réelle modifiée — mission
docs-only). Baseline 950 fichiers : 3 diffs, strictement ceux déjà committés lors des missions
précédentes (`REF_Setup.xlsm`, `MASTER_FACT_HA_Reservations.xlsx`,
`HIST_Reservations_Cloturees.xlsx`). **0 nouveau diff métier réel.** Port 8000 : constaté libre en
début de mission, non manipulé, non requis (aucune instance de recette lancée — analyse faite sur
copies scratchpad hors port applicatif). Mode réel : OFF, inchangé.

## 8. Priorisation pour l'utilisateur

**PRIORITÉ 1 — nécessaire avant relevés propriétaires / commissions définitives**
1. 42 saisies Direct/VRBO (Bloc 1) — impact direct sur commission et net propriétaire des
   logements concernés.
2. 56 mouvements propriétaires Banque (Bloc 2, §3.3) — impact direct trésorerie propriétaire ;
   démarre par l'alimentation de Lot 5 (acomptes).

**PRIORITÉ 2 — nécessaire avant exploitation réelle quotidienne**
3. 82 décisions `A_ENVOYER_IA` (Bloc 2, §3.4) — classification de dépenses courantes (CB,
   prestataires, abonnements) déjà correctement isolées, pas de blocage clôture mais nécessaire
   pour une comptabilité courante propre.
4. `MENAGE_EXTERNE_ECART_HOSTAWAY` (4 logements) et `MENAGE_EXTERNE_LOGEMENT_HORS_HA`
   (2 logements) — réconciliation ménages/factures.

**PRIORITÉ 3 — réserve temporaire, non bloquante**
5. Export Airbnb détaillé (166 lignes `PAYOUT_PLATEFORME`) — purement informatif, aucune décision,
   à obtenir quand disponible.
6. `SOURCE_SHEET_PROVENANCE_INCOMPLETE` — garde-fou informatif, comportement voulu.
7. Arbitrage comptable fin (`606000` → mapping par catégorie) — réserve acceptable, non bloquant.

## 9. Correction de reporting appliquée (documentation uniquement)

`81_BASELINE_CLOTURE_APRES_NETTOYAGE.md` et `HANDOFF_CANONIQUE.md` sont mis à jour pour ne plus
présenter les 222 lignes `RAPPROCHEMENT_REQUIS` comme un bloc homogène de « décisions humaines » :
désormais décomposées 166 `PAYOUT_PLATEFORME` (0 décision) + 56 `VIREMENT_PROPRIETAIRE_A_RAPPROCHER`
(décisions réelles, conditionnées à Lot 5). Aucune donnée, aucun calcul, aucun statut moteur
modifié — texte de reporting seulement.

## 10. Préparation mode réel — réévaluation

| Axe | État |
|---|---|
| APPLICATION | VALIDÉE (pipeline complet rejouable, 0 bug résiduel) |
| CLÔTURE TECHNIQUE | GO |
| DONNÉES PROPRIÉTAIRES | VALIDE_AVEC_RESERVE (56 mouvements en attente Lot 5 + 42 saisies) |
| BANQUE OPÉRATIONNELLE | VALIDE_AVEC_RESERVE (138 décisions réelles restantes, ventilées) |
| COMPTABILITÉ | VALIDE_AVEC_RESERVE (mappings fins non arbitrés, non bloquants) |
| WRITERS | VALIDE — toujours fail-closed, `CALCULS_REAL_RUN_ENABLED` OFF |
| BACKUP | VALIDE — pattern backup+SHA256 systématique, exercé à chaque mission |
| ROLLBACK | A_FAIRE_HUMAIN — jamais exercé hors recette en conditions réelles |
| DRY-RUN | VALIDE — toutes les corrections de cette lignée de missions ont été testées sur copies avant réel |

## 11. Checklist GO/NO GO — 3 niveaux

| Niveau | Statut |
|---|---|
| A. TECHNIQUE | VALIDE (0 bloquant, 0 bug résiduel, pipeline complet rejouable) |
| B. DONNÉES HUMAINES | A_FAIRE_HUMAIN (138 décisions Banque + 42 saisies Réservations + 3 résiduels Ménages) |
| C. MODE RÉEL | BLOQUE — NO GO tant que B n'est pas traité, rollback jamais exercé hors recette |

## 12. Verdict

- **APPLICATION : VALIDÉE.**
- **CLÔTURE TECHNIQUE : GO.**
- **BLOQUANTS : 0.**
- **SAISIES HUMAINES RÉSERVATIONS : 42** (38 Direct + 4 VRBO).
- **DÉCISIONS HUMAINES BANQUE : 138** (56 propriétaires + 82 A_ENVOYER_IA ; 166 PAYOUT_PLATEFORME
  exclues, 0 décision).
- **A_CONTROLER NON BLOQUANTS : 14** (dont seulement 3 réellement nouveaux : Ménages/provenance ;
  11 sont des reformulations agrégées des Blocs 1 et 2).
- **COMPTABILITÉ : PRÊTE AVEC RÉSERVES.**
- **PRÉPARATION MODE RÉEL : NO GO** (données humaines B non traitées, rollback non exercé).
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

## 13. Prochaine action exacte

Traiter en parallèle, par priorité :
1. Alimenter Lot 5 (`SAISIE_AcomptesProprietaires.xlsx`) — débloque à lui seul 56 décisions Banque.
2. Saisir les 42 réservations Direct/VRBO via le mécanisme existant (`/reservations/nouvelle` +
   CSV backfill VRBO).
3. Classifier les 82 mouvements `A_ENVOYER_IA` restants.
Aucune action technique supplémentaire n'est nécessaire avant ces trois décisions humaines.

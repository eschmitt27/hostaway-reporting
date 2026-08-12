# 82 — Pack final des actions humaines restantes

Document unique, prêt à l'emploi. Mis à jour le 2026-08-13 après l'audit Lot 5
(`83_AUDIT_LOT5_RAPPROCHEMENT_PROPRIETAIRES.md`).

## Compteur global — contrôles ≠ objets ≠ décisions

Une même décision utilisateur peut fermer plusieurs lignes de contrôle. Seul le dernier chiffre
compte pour estimer le travail restant.

| Niveau | Nombre |
|---|---:|
| Lignes de contrôle ouvertes (Lot 11) | **24** (0 BLOQUANT, 14 A_CONTROLER, 10 INFO) |
| Objets économiques concernés | **183** (42 réservations + 56 mouvements propriétaires + 82 mouvements A_ENVOYER_IA + 3 objets Ménages/provenance) |
| **Décisions humaines réelles** | **entre 34 et 183** selon uniformité (détail par section ci-dessous) |

Les 166 `PAYOUT_PLATEFORME` sont exclues de tous ces compteurs : catégorie moteur déjà correcte,
0 décision, jamais rapprochées d'une réservation.

---

## 1. PROPRIÉTAIRES / LOT 5 — 56 mouvements

**Résultat de l'audit Lot 5 : 0 mouvement résoluble par preuve existante.** La source métier est
totalement vide (`SAISIE_AcomptesProprietaires.xlsx` : 0 ligne ; sortie Lot 5 : 0 ligne ; table
`mouvements_tresorerie_proprietaires` : 0 ligne). Détail complet et preuves : document `83`.

| Statut de preuve | Nombre |
|---|---:|
| PREUVE_A | 0 |
| PREUVE_B | 0 |
| AMBIGU | 0 |
| **ABSENT** | **56** |

Caractéristiques mesurées : 100 % CREDIT (entrants), 27 069,18 € au total, 2025-11 → 2026-07,
**aucun montant répété** (donc aucune série exploitable), `nature_presumee` moteur =
`ENCAISSEMENT_PROPRIETAIRE_A_VENTILER` (libellé d'attente, pas une nature établie).

### Tableau opérateur

| Identité (candidate) | Nb mvts | Cumul | Période | Statut identité | Objet Lot 5 candidat | Nature prouvée | Rapprochement | Action attendue |
|---|---:|---:|---|---|---|---|---|---|
| PROP_0009 | 16 | 8 839,74 € | 2025-11→2026-07 | CANDIDATE (règle libellé) | aucun | aucune | AUCUN | Confirmer identité + nature, créer objets |
| PROP_0002 | 9 | 5 702,02 € | 2025-11→2026-07 | CANDIDATE | aucun | aucune | AUCUN | idem |
| PROP_0006 | 9 | 4 040,24 € | 2025-11→2026-07 | CANDIDATE | aucun | aucune | AUCUN | idem |
| PROP_0008 | 8 | 4 577,88 € | 2025-11→2026-07 | CANDIDATE | aucun | aucune | AUCUN | idem |
| `FAMILLE_UZON_A_CONTROLER` | 7 | 1 161,69 € | 2025-11→2026-05 | **NON RÉSOLUE** (plusieurs personnes possibles) | aucun | aucune | AUCUN | **Trancher l'identité d'abord** |
| PROP_0010 | 5 | 1 547,61 € | 2025-11→2026-06 | CANDIDATE | aucun | aucune | AUCUN | Confirmer identité + nature, créer objets |
| PROP_0005 | 2 | 1 200,00 € | 2026-04→2026-05 | CANDIDATE | aucun | aucune | AUCUN | idem |

**Identifiants de mouvement opaques disponibles dans `BANQUE_LOT8_IMPORT.xlsx` onglet
`RAPPROCH_PROPRIETAIRES_ATTENTE` — non recopiés ici (0 PII dans Git).**

### Décisions à prendre

**7 décisions minimum** (une par identité), **56 maximum** (si chaque mouvement porte une nature
propre). La compression dépend entièrement d'une réponse utilisateur : « pour un propriétaire donné,
tous ces encaissements ont-ils la même nature ? » — non déduite ici.

Où agir : `SAISIE_AcomptesProprietaires.xlsx` (Lot 5) ou écran trésorerie propriétaires, puis
relancer Lot 8c pour le rapprochement automatique.

---

## 2. BANQUE / A_ENVOYER_IA — 82 mouvements

83 lignes physiques → **82 mouvements économiques distincts** (1 doublon `mouvement_id` vérifié :
2 lignes sources conservées pour traçabilité, 1 seule décision possible). Reconfirmé sur sortie
fraîche.

Caractéristiques : **79 DEBIT / 3 CREDIT**, total **3 204,79 €** — enjeu économique faible, mais
volume de clics élevé. Aucune règle des 30 règles Banque existantes ne les couvre (ils tombent tous
sur le catch-all `R_099`).

**Recherche de correspondance déterministe avec les référentiels existants** (propriétaires,
associés, intervenants/fournisseurs) : **0 correspondance**. Aucune identité ne peut être établie
sans l'utilisateur.

### Groupes à fort levier — RÈGLES CANDIDATES

Regroupement par motif de libellé normalisé. **Ce sont des aides opérateur, pas des validations.**
Aucune catégorie économique n'est proposée : seule l'utilisateur connaît la nature.

| Règle candidate | Motif (opaque) | Nb mvts | Cumul | Question utilisateur unique |
|---|---|---:|---:|---|
| RÈGLE_CANDIDATE_01 | `PAIEMENT CB ... VILNIUS` | 14 | 210,44 € | Ces 14 paiements chez le même marchand ont-ils tous la même nature ? Laquelle ? |
| RÈGLE_CANDIDATE_02 | `PAIEMENT PSC ... LA BREDE` | 9 | 199,21 € | idem |
| RÈGLE_CANDIDATE_03 | `PAIEMENT CB ... EUR` (motif générique) | 8 | 307,21 € | idem — motif peu discriminant, à examiner individuellement si besoin |
| RÈGLE_CANDIDATE_04 | `PAIEMENT PSC ... CADAUJAC` | 5 | 76,44 € | idem |
| RÈGLE_CANDIDATE_05 | `PAIEMENT PSC ... ST SELVE` | 5 | 16,60 € | idem |
| RÈGLE_CANDIDATE_06 | `PAIEMENT PSC ... BEGLES` | 3 | 64,62 € | idem |
| RÈGLE_CANDIDATE_07 | `PAIEMENT PSC ... VILLENAVE D'O` | 3 | 57,29 € | idem |
| RÈGLE_CANDIDATE_08 | `RETRAIT DAB ... LA BREDE` | 2 | 420,00 € | Retraits espèces — usage professionnel ou personnel ? |
| RÈGLE_CANDIDATE_09 | `... PAIEMENTS ... VILNIUS` | 2 | 30,48 € | Même marchand que RC_01 ? |
| RÈGLE_CANDIDATE_10 | `PAIEMENT CB ... LABREDE` | 2 | 119,41 € | Variante d'écriture de RC_02 ? |
| RÈGLE_CANDIDATE_11 | `PAIEMENT CB ... PAYLI/` | 2 | 28,89 € | Prestataire de paiement intermédiaire — marchand réel inconnu |
| RÈGLE_CANDIDATE_12 | `PAIEMENT PSC ... LABREDE/` | 2 | 19,06 € | Variante d'écriture de RC_02 ? |

**Couverture : 57 mouvements sur 82 (70 %) via 12 questions.** Si l'utilisateur répond par règle,
le travail passe de 82 clics à 12 réponses + 25 cas isolés.

### Cas individuels (25)

Les 25 restants n'ont aucun motif partagé. Les 2 plus significatifs, tous deux **CREDIT** :

| Date | Sens | Montant | Motif (opaque) | Candidat | Preuve | Décision demandée |
|---|---|---:|---|---|---|---|
| 2025-12-12 | CREDIT | 480,05 € | virement d'un tiers nommé | identité personne physique, **0 correspondance référentiel** | aucune | Qui est ce tiers, et à quel titre ? |
| 2026-05-26 | CREDIT | 396,33 € | même tiers nommé | idem | aucune | idem (probable même règle que ci-dessus, à confirmer) |

Les 23 autres sont des débits CB/PSC isolés entre 1,78 € et 120,00 € (cumul ≈ 750 €). Liste
complète : `BANQUE_LOT8_IMPORT.xlsx` onglet `IA_Classification`.

### Décisions à prendre

**12 réponses de règle + 25 cas isolés = 37 décisions**, au lieu de 82 clics — **si** l'utilisateur
accepte de répondre par groupe. Sinon 82. Aucune règle ne sera créée sans confirmation explicite.

---

## 3. RÉSERVATIONS DIRECT / VRBO — 42 saisies

Audit clos, aucune nouvelle preuve disponible (API Hostaway : `paymentStatus = Unknown` sur les 42).
Mécanisme de saisie existant audité et prouvé sur fixtures (58/58 tests verts, 0 vraie réservation
touchée).

| Canal | Nombre | Où saisir | Champ exact |
|---|---:|---|---|
| Direct | 38 | écran `/reservations/nouvelle` | `total_percu` (montant total perçu), lié par `reservation_id_hostaway` |
| VRBO | 4 | CSV backfill `01_SOURCES_BRUTES/VRBO/IMPORT_UNIQUE_Revenus_*.csv` | `Montant du paiement` + référence de réservation VRBO, puis relancer `lot4ter` |
| **TOTAL** | **42** | | |

### Groupes Direct (38)

| Logement | Propriétaire | Nb | Mois |
|---|---|---:|---|
| LOG_0005 | PROP_0005 | 11 | 2026-01, 2026-02 |
| LOG_0015 | PROP_0011 | 11 | 2026-02, 05→12, 2027-01, 02 |
| LOG_0012 | PROP_0009 | 8 | 2026-08, 09, 11, 12, 2027-01, 02 |
| LOG_0014 | PROP_0010 | 3 | 2026-06, 2026-07 |
| LOG_0004 | PROP_0004 | 2 | 2026-07, 2026-10 |
| LOG_0001 | PROP_0001 | 1 | 2026-06 |
| LOG_0010 | PROP_0005 | 1 | 2026-02 |
| LOG_0013 | PROP_0009 | 1 | 2026-05 |

### Groupes VRBO (4)

| Logement | Propriétaire | Nb | Mois |
|---|---|---:|---|
| LOG_0008 | PROP_0008 | 3 | 2026-06, 08, 10 |
| LOG_0017 | PROP_0008 | 1 | 2026-11 |

Identifiants `reservation_id` détaillés : voir historique de ce document (version antérieure) et
`80_AUDIT_RESERVATIONS_VRBO_DIRECT_A_CONTROLER.md`. **Décisions : 42** (un montant réel par
réservation, non compressible — chaque séjour a son propre montant).

---

## 4. TROIS AUTRES A_CONTROLER

Les 14 A_CONTROLER de la baseline contiennent **11 reformulations agrégées** des sections 1-3
ci-dessus (9 lignes `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` = section 1+2 ;
`RESERVATION_A_CONTROLER_SANS_COMMISSION` et `VRBO_MONTANT_NON_RENSEIGNE` = section 3) et
**3 objets réellement distincts** :

| Code | Objet | Raison | Action humaine | Bloque mode réel ? |
|---|---|---|---|---|
| `MENAGE_EXTERNE_ECART_HOSTAWAY` | LOG_0010, LOG_0011, LOG_0013, LOG_0014 | Écart de volume facturé vs comptage Hostaway | Valider l'écart (interne / hors HA / décalage de mois / saisie) — le moteur précise explicitement que ce n'est **pas** une accusation prestataire | Réserve acceptable |
| `MENAGE_EXTERNE_LOGEMENT_HORS_HA` | LOG_0009, LOG_0016 | Logements facturés absents du comptage Hostaway | Confirmer statut (archivé / hors HA / problème de mapping) | Réserve acceptable |
| `SOURCE_SHEET_PROVENANCE_INCOMPLETE` | étapes `lot6b`, `lot6f` | Provenance de source Ménages non prouvable (marqueur `PENDING` résiduel) | **Action technique, pas une décision métier** : relancer `lot6b`/`lot6f` avec accès réseau au Google Sheet M04 pour régénérer la provenance | Réserve acceptable, mais **empêche une clôture propre** tant qu'ouvert |

**Décisions : 2 métier + 1 action technique.**

---

## Priorisation

**PRIORITÉ 1 — avant relevés propriétaires / commissions définitives**
1. Section 3 — 42 saisies Direct/VRBO (impact direct commission et net propriétaire).
2. Section 1 — identités + natures propriétaires (7 réponses débloquent 56 mouvements, 27 069 €).

**PRIORITÉ 2 — avant exploitation réelle quotidienne**
3. Section 2 — 12 règles candidates + 25 cas isolés (3 205 €, faible enjeu mais nécessaire pour une
   comptabilité courante propre).
4. Section 4 — les 2 arbitrages Ménages.

**PRIORITÉ 3 — réserve temporaire**
5. Relance `lot6b`/`lot6f` avec réseau (provenance).
6. Export Airbnb détaillé (166 `PAYOUT_PLATEFORME`, purement informatif, 0 décision).
7. Arbitrage comptable fin (`606000` générique → mapping par catégorie ; trésorerie propriétaires ;
   associés/IK). Non bloquant : les écritures restent équilibrées, les mappings provisoires sont
   signalés par contrôle.

---

## Questions utilisateur (par levier décroissant)

| # | Question | Débloque |
|---|---|---:|
| QUESTION_01 | Pour chaque propriétaire (PROP_0009, 0002, 0006, 0008, 0010, 0005), tous les encaissements listés §1 ont-ils la même nature ? Si oui laquelle (acompte / remboursement / avance / autre) ? | 49 mouvements, 25 907 € |
| QUESTION_02 | Qui est le propriétaire réel derrière `FAMILLE_UZON_A_CONTROLER` ? | 7 mouvements, 1 162 € |
| QUESTION_03 | Les 12 règles candidates §2 sont-elles valides, et quelle nature pour chacune ? | 57 mouvements, 1 549 € |
| QUESTION_04 | Qui est le tiers des 2 virements CREDIT isolés §2, et à quel titre ? | 2 mouvements, 876 € |
| QUESTION_05 | Montants réellement perçus pour les 42 réservations §3 ? | 42 réservations |
| QUESTION_06 | Les écarts Ménages §4 (4 + 2 logements) sont-ils acceptés ou à corriger ? | 6 logements |

---

## Préparation mode réel

| Axe | État |
|---|---|
| APPLICATION | VALIDÉE (0 bug résiduel, pipeline complet rejouable, idempotent) |
| CLÔTURE TECHNIQUE | GO (0 BLOQUANT) |
| LOT 5 | FONCTIONNEL mais NON ALIMENTÉ (0 ligne) |
| DONNÉES PROPRIÉTAIRES | A_FAIRE_HUMAIN (56 mouvements, 0 preuve existante) |
| BANQUE OPÉRATIONNELLE | A_FAIRE_HUMAIN (82 mouvements) |
| COMPTABILITÉ | VALIDE_AVEC_RESERVE (mappings fins non arbitrés, non bloquants ; les 56 propriétaires ne créent **aucune** question comptable nouvelle) |
| MIGRATIONS BASE RÉELLE | **NON_TESTE** — base réelle en migration `0016`, la trésorerie propriétaires nécessite `0025`. À appliquer avant toute exploitation réelle (opération standard, gatée, non entreprise) |
| WRITERS | VALIDE — fail-closed, `CALCULS_REAL_RUN_ENABLED` OFF |
| BACKUP | VALIDE — pattern backup + SHA256 systématique |
| ROLLBACK | NON_TESTE — jamais exercé hors recette |
| DRY-RUN | VALIDE |

### Checklist GO/NO GO

| Niveau | Statut |
|---|---|
| A. TECHNIQUE | VALIDE |
| B. DONNÉES HUMAINES | A_FAIRE_HUMAIN (6 questions ci-dessus) |
| C. MODE RÉEL | BLOQUE — NO GO |

---

## Verdict

- **APPLICATION : VALIDÉE.**
- **CLÔTURE TECHNIQUE : GO. BLOQUANTS : 0.**
- **LOT 5 : FONCTIONNEL, NON ALIMENTÉ.**
- **MOUVEMENTS PROPRIÉTAIRES : 56 → 56 décisions humaines** (0 résoluble par preuve ; compressible
  à 7 si nature uniforme par propriétaire).
- **A_ENVOYER_IA : 82 → 82 décisions humaines** (compressible à 37 via 12 règles candidates ;
  aucune réduction par validation inventée).
- **DIRECT / VRBO : 42.**
- **AUTRES A_CONTROLER : 3.**
- **TOTAL DÉCISIONS HUMAINES : 183 au maximum, 89 au minimum** (7 + 37 + 42 + 3) selon les réponses
  aux questions de compression.
- **COMPTABILITÉ : PRÊTE AVEC RÉSERVES.**
- **PRÉPARATION MODE RÉEL : NO GO.**
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

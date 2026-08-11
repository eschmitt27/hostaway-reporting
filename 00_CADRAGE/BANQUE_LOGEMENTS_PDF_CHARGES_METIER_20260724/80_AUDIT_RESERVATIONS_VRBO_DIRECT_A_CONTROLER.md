# 80 — Audit des 70 RESERVATION_EXCLUE_A_CONTROLER (VRBO/Direct) (2026-08-11/12)

**Aucun payout inventé. Aucune donnée Banque consultée. Aucun mois clôturé réouvert.** Correction
de code déterministe (bug de double-comptage), committée. 38+4=42 réservations restent
`A_CONTROLER`, causes toutes identifiées et honnêtes.

## 1. Les 70 — identification

| Mesure | Nombre |
|---|---:|
| Lignes A_CONTROLER (avant) | 70 |
| Réservations distinctes | 70 (0 doublon) |
| VRBO | 31 |
| Direct | 39 |
| Booking / Airbnb | 0 |
| Hostaway (source de la réservation) | 70 / 70 (100 %) |
| Hors Hostaway (HH pure) | 0 |

Toutes les 70 sont des réservations **connues de Hostaway** (`reservation_id`/`listingMapId`
présents) mais dont le **payout plateforme** n'a jamais été calculable dès l'extraction Lot1 —
c'est une propriété du canal, pas un défaut d'extraction.

## 2. Code cause exact (Lot1, `PayoutCalculator`)

```
Direct  → return None, "DIRECT_HORS_HOSTAWAY", "A_CONTROLER", 0.0, ...   (docstring : "H3: Direct/
           VRBO-Unknown → None, jamais valorisé depuis Hostaway" — comportement voulu, documenté)
VRBO    → si paymentStatus in ("unknown", "") : "VRBO_UNKNOWN", "A_CONTROLER"
           sinon : payout via totalPriceFromChannel/totalPrice
```

| Code cause exact | Nombre |
|---|---:|
| `DIRECT_HORS_HOSTAWAY` (Direct, aucun payout plateforme par design) | 39 |
| `VRBO_UNKNOWN` (VRBO, `paymentStatus` Hostaway = Unknown/absent) | 31 |
| **Total** | **70** |

## 3. Le bug réel — double-comptage, PAS une règle métier manquante

`lot10_calculer_resultats.py` construisait `df_ac` (l'onglet A_CONTROLER) **directement depuis le
statut brut Lot1** (`df_payout["statut_calcul_payout"] == "A_CONTROLER"`), **sans vérifier** si la
réservation avait déjà été résolue ailleurs dans le pipeline via un mécanisme légitime déjà
existant :

- **VRBO** : un backfill CSV historique (`01_SOURCES_BRUTES/VRBO/IMPORT_UNIQUE_Revenus_*.csv`,
  mécanisme déjà en place depuis début août 2026, cf. `JOURNAL_ANOMALIES.md`) fournit le vrai
  montant pour certaines réservations VRBO, capturé dans `HIST_Reservations_Cloturees.xlsx` par
  `lot4ter` (branche `df_vrbo_norm` de Lot10). **27 des 31 VRBO** sont dans ce cas — payout réel
  connu, déjà intégré à `COMMISSIONS`, mais listées une seconde fois par erreur en A_CONTROLER.
- **Direct** : une saisie manuelle existante dans `MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx`
  (mécanisme HH, décision D054 déjà validée le 2026-06-15) fournit le montant pour les
  réservations directes liées. **1 des 39 Direct** (`60559486`, LOG_0009/PROP_0003,
  `RESHH-2026-05-001`, montant 2 343,48 €) est dans ce cas.

**Preuve vérifiée programmatiquement** (pas une supposition) : les 28 `reservation_id` supprimées
de A_CONTROLER après correctif sont **100 % présentes dans `COMMISSIONS`** avec des montants
réels (0 suppression injustifiée). Aucun effet sur les totaux agrégés (`commission_conciergerie`,
`net_proprietaire`, REEL/COMPTABLE/HORS_COMPTA) : ces 28 réservations étaient **déjà comptées**
dans ces totaux avant le correctif — le bug était purement un défaut d'affichage/exclusion
(double-listage), jamais une perte ou un gain financier.

## 4. Classification métier des 70

| Cause racine | Nombre | Type |
|---|---:|---|
| A. VRBO_PAYOUT_ABSENT (réellement absent, aucun backfill) | 4 | DONNEE_ABSENTE |
| B. DIRECT_PAYOUT_ABSENT (aucune saisie HH) | 38 | REGLE_METIER_DEJA_DEFINIE, saisie manquante |
| D. PAYOUT_PRESENT_MAIS_NON_RECONNU (bug dedup) | 28 (1 Direct + 27 VRBO) | BUG_TECHNIQUE — CORRIGÉ |
| Autres (C/E/F/G/H/I) | 0 | — |

| Type | Nombre |
|---|---:|
| BUG_TECHNIQUE | 28 |
| DONNEE_ABSENTE | 42 (38 Direct + 4 VRBO) |
| REGLE_METIER_MANQUANTE | 0 (la règle D054 existe déjà et fonctionne) |
| HISTORIQUE_GELE | 0 |
| AMBIGU | 0 |

## 5. Preuves

| Niveau | Nombre |
|---|---:|
| PREUVE_A (corrigé automatiquement) | 28 |
| PREUVE_B | 0 |
| ABSENT | 42 |
| AMBIGU | 0 |

## 6. Correction — test rouge, fix minimal, test vert

`tests/test_lot10_reservation_exclue_dedup.py` (2 cas) : (1) reproduit le bug (réservation
résolue via HH listée aussi en A_CONTROLER) — rouge avant fix ; (2) prouve qu'une réservation
réellement non résolue reste A_CONTROLER — déjà vert (non-régression). Fix : `lot10_calculer_
resultats.py`, avant construction de `df_ac`, exclusion des `reservation_id` déjà présents dans
`df_comm.reservation_id_hostaway` (5 lignes ajoutées, aucune règle des autres canaux modifiée).
Test rouge→vert. Régression complète (root `tests/`) : 274 passed, 0 nouvel échec.

## 7. Simulation (copie fidèle, jamais sur le réel)

| Cause | Avant A_CONTROLER | Résolubles déterministement | Restent |
|---|---:|---:|---:|
| VRBO backfill déjà résolu | 27 | 27 | 0 |
| Direct HH déjà résolu | 1 | 1 | 0 |
| VRBO sans backfill | 4 | 0 | 4 |
| Direct sans saisie HH | 38 | 0 | 38 |
| **Total** | **70** | **28** | **42** |

`RESERVATION_A_CONTROLER` : **70 → 42** (mesuré par le moteur réel, idempotent — 2 runs
consécutifs donnent 42). 0 bloquant. `Lot11` : 557 contrôles (541 BLOQUANT/5 A_CONTROLER/11 INFO,
environnement de copie partiel — Charges/IK/Acomptes vides indépendamment de cette mission, non
comparable à un total réel).

## 8. Résultat société

| Indicateur | Avant | Après | Delta |
|---|---:|---:|---:|
| Commission conciergerie | 41 208,44 € | 41 208,44 € | 0,00 € |
| Net propriétaire (avant charge fixe) | 215 397,04 € | 215 397,04 € | 0,00 € |
| REEL | 313 886,49 € | 313 886,49 € | 0,00 € |
| COMPTABLE | 303 362,33 € | 303 362,33 € | 0,00 € |
| HORS_COMPTA | 10 524,16 € | 10 524,16 € | 0,00 € |

**Delta = 0,00 € partout, expliqué** : les 28 réservations corrigées étaient déjà comptées dans
ces totaux avant le fix (le bug ne les excluait pas des calculs financiers, seulement de la liste
de contrôle — double-affichage, pas double-comptage financier).

## 9. Mois clôturés

27 des 42 restantes sont en mois OUVERT, 15 en mois CLOTURE. Aucune n'a nécessité de correction
HIST (contrairement à guestCount) : ce sont des cas de **donnée absente**, pas de donnée gelée
existante mais mal transportée — rien à corriger dans HIST pour ces 42, une correction future
nécessiterait une **saisie manuelle nouvelle** (`SAISIE_ReservationsHorsHostaway.xlsx`), pas une
correction de champ existant.

## 10. Les 42 restantes — répartition

10 logements distincts, 7 propriétaires distincts, 12 mois (2026-01→2027-02).

| Canal | Cause | Nombre |
|---|---|---:|
| Direct | Aucune saisie HH (`DIRECT_SANS_SAISIE_HH`) | 38 |
| VRBO | `paymentStatus` Hostaway Unknown, aucun backfill CSV disponible | 4 |

Aucune ne peut être résolue sans données supplémentaires (saisie manuelle via le mécanisme HH
déjà existant, ou nouveau backfill VRBO) — hors périmètre technique de cette mission, pas une
question de règle métier à trancher (la règle D054 existe déjà et fonctionne correctement pour
les cas où la donnée est fournie).

## 11. Verdict

SNAPSHOTS/RÉSERVATIONS CIBLÉES : 70/70 auditées. CODE CORRIGÉ : bug de double-comptage (5 lignes,
committé). SIMULATION : 70→42 (idempotent, 0 delta société). AUCUNE DONNÉE RÉELLE MODIFIÉE (pas
de PREUVE_A éligible à une écriture réelle — le fix est un correctif de code, pas une correction
de donnée). MOIS CLÔTURÉS : aucun réouvert, aucune correction HIST nécessaire. TESTS : 274 passed
(moteur) + régression app en cours. CLÔTURE : NO GO (inchangé). MODE RÉEL : NO GO — NON ACTIVÉ.

## 12. Prochaine action

42 restantes nécessitent une **saisie manuelle humaine** (38 Direct + 4 VRBO) via le mécanisme
HH existant — pas une décision de règle métier, une collecte de données. Aucune question
utilisateur nécessaire sur ce point : le système sait déjà quoi faire, il manque la donnée
source elle-même.

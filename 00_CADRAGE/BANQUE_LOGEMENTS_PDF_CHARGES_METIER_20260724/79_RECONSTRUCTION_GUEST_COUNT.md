# 79 — GUEST_COUNT_MANQUANT_PREPARATION_CANAPE : audit et constat (2026-08-10)

**Aucune donnée réelle modifiée. Aucun code modifié — le correctif nécessaire existe déjà depuis
le 20/06/2026. Aucune valeur de voyageurs inventée.** Cette mission traite uniquement
`GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` (553) ; les 59 `RESERVATION_EXCLUE_A_CONTROLER`
(VRBO/Direct) restent hors périmètre.

## 1. Déduplication

| Mesure | Nombre |
|---|---:|
| Lignes de contrôle | 553 |
| Réservations économiques distinctes (source Lot10, onglet `A_CONTROLER`) | **553** (pas de duplication) |
| Logements concernés | **4** — les seuls logements du parc ayant une règle de préparation canapé configurée dans `REF_Logements` (`seuil_voyageurs_preparation_canape` + `montant_preparation_canape`) |
| Mois distincts | 23 (2025-01 → 2027-02, période ouverte incluse) |

**Les 553 réservations couvrent la quasi-totalité de l'activité de ces 4 logements** (558
réservations totales sur ces logements dans `MASTER_CALC_Reservations_Resolues`, 558 avec
`guestCount` absent — 5 non comptées en contrôle car probablement annulées, traitées séparément).

## 2. Sémantique du guest count — non modifiée

`guest_count` = valeur brute `numberOfGuests` de l'API Hostaway, transportée telle quelle
(colonne `guestCount` conservée pour compatibilité des consommateurs aval, jamais recalculée,
jamais un total dérivé d'adultes+enfants ni une autre définition). Aucune ambiguïté dans le code,
aucun arbitrage nécessaire sur ce point.

## 3. Règle de préparation canapé — tracée, non modifiée

`lib_canape.calculate_canape_amount()` :
1. Le logement doit avoir une règle configurée (`seuil_voyageurs_preparation_canape` +
   `montant_preparation_canape` dans `REF_Logements`) — sinon `NON_APPLICABLE`, aucun contrôle.
2. Si une règle existe et que `guest_count` est absent → **`A_CONTROLER`**, montant **0**, jamais
   de valeur inventée.
3. Si `guest_count >= seuil` → préparation facturée. Sinon → `NON_ELIGIBLE`, montant 0.

C'est le chemin exact : `guest count absent` → règle canapé configurée sur le logement →
`A_CONTROLER` côté Lot10 → remonté par Lot11 comme `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE`.
**Le taux de commission n'intervient à aucun moment dans cette chaîne.**

## 4. Audit de la chaîne technique — le correctif existe déjà

| Étape | Champ attendu | Constat |
|---|---|---|
| Extraction Lot1 (`lot1_hostaway_extract.py`) | `res.get("numberOfGuests")` → colonnes `guestCount` + `numberOfGuests` (audit) | **Code correct**, corrigé par le commit `719169d` (« Correctif Hostaway - nombre voyageurs pour preparation canape », **2026-06-20**) |
| Résolution API (`lib_guestcount.resolve_api_guest_count`) | Combine `res.numberOfGuests` / `detail.numberOfGuests` | Code présent et cohérent avec le correctif |
| Lot4quater (réservations résolues) | Transporte `guestCount`/`source_guestCount` tel quel, `source_guestCount="HIST"` si branche historique | Transport correct, ne fabrique rien |
| Lot10 (`calculate_canape_amount`) | Utilise `guestCount` transporté | Comportement correct (§3) |

**Constat déterminant** : le fichier réel `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_Reservations.xlsx`
(copies) contient **0 valeur `guestCount` remplie sur 1391 réservations (100 % vide)**, et **ne
porte pas la colonne `numberOfGuests`** que le correctif du 20/06/2026 ajoute pourtant. Preuve
directe que ce fichier a été produit par une extraction **antérieure au correctif** — la date de
fichier (2026-07-24) ne reflète pas une ré-extraction post-correctif, seulement une copie/dernière
écriture du fichier existant.

**Il n'y a pas de bug de code à corriger. Le code est déjà bon.** La cause est une **absence de
ré-extraction Hostaway réelle depuis la correction** — un problème de fraîcheur de données, pas un
défaut applicatif.

## 5. Recherche de source locale fiable — résultat négatif

- Aucun cache de payload API brut stocké localement (`lot1_hostaway_extract.py` interroge l'API en
  direct, pas de mode simulation offline avec payloads sauvegardés).
- Aucune archive de `MASTER_FACT_HA_Reservations.xlsx` dans `99_ARCHIVES` — jamais versionné.
- Aucune autre source locale (base applicative, document daté) ne porte de nombre de voyageurs
  pour ces réservations.

| Niveau de preuve | Réservations |
|---|---:|
| PREUVE_A | **0** |
| PREUVE_B | 0 |
| AMBIGU | 0 |
| **ABSENT** | **553** |

**Aucune reconstruction n'est possible sans une nouvelle extraction Hostaway réelle** (accès API
réseau), qui est un writer réel, hors périmètre technique de cette mission (jamais de mode réel,
jamais d'appel réseau non autorisé).

## 6. Cas hors Hostaway

Aucune des 553 réservations concernées n'est hors Hostaway : les 4 logements avec règle canapé
n'ont que des réservations Hostaway/historique dans ce jeu. Pas d'audit de formulaire nécessaire
pour cette mission — aucun champ à ajouter, aucune décision différée créée.

## 7. Cas annulés

Aucune des 553 lignes `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` ne porte un statut d'annulation —
le contrôle canapé s'applique uniquement au flux `df_ha_norm`/`df_vrbo_norm`/`df_hh_norm` (réservations
normales intégrées), distinct du traitement `CANCELLED` (statut de payout séparé). Logique
cancellation non touchée, non testée à nouveau (hors sujet, rien n'y a changé).

## 8. Simulation — sans objet

Aucune correction de code, aucune donnée à injecter : rien à simuler.

| Mesure | Avant | Après |
|---|---:|---:|
| GUEST_COUNT_MANQUANT_PREPARATION_CANAPE | 553 | **553 (inchangé)** |
| RESERVATION_A_CONTROLER_SANS_COMMISSION | 612 | **612 (inchangé)** |
| TOTAL contrôles | 2002 | **2002 (inchangé)** |

Aucun effet économique : rien n'a été modifié.

## 9. Verdict

| Mesure | Valeur |
|---|---|
| GUEST_COUNT — avant | 553 |
| GUEST_COUNT — après | **553** |
| Résolues | **0** |
| Reste | **553**, cause unique : absence de ré-extraction Hostaway réelle depuis le correctif du 20/06/2026 |
| RESERVATION_A_CONTROLER_SANS_COMMISSION — après | **612** (dont 553 guest count + 59 VRBO/Direct payout, inchangé) |
| TOTAL CONTRÔLES — après | **2002** |

- GESTION_LOGEMENT_MISSING : 472, inchangé.
- BANQUE : 222, inchangé.
- CHARGES : 69, inchangé.
- **CLÔTURE : NO GO.**
- **PRÉPARATION MODE RÉEL : NO GO.**
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

## 10. Question utilisateur — une seule

Le code est déjà correct. La seule action qui pourrait résoudre les 553 (et par ricochet réduire
`RESERVATION_A_CONTROLER_SANS_COMMISSION` à potentiellement 59) est **une ré-extraction Hostaway
réelle** (`lot1_hostaway_extract.py`, sans `--dry-run`), qui appelle l'API Hostaway en direct et
écrit dans les sources métier réelles.

**Confirmes-tu l'autorisation d'une extraction Hostaway réelle**, sachant que :
- c'est un accès réseau externe réel, à exécuter uniquement avec tes identifiants API valides ;
- c'est un writer qui touche des sources métier réelles (`02_TRAVAIL/Lot1_Hostaway/*`) ;
- le résultat n'est pas garanti à 100 % — si l'API Hostaway elle-même ne fournit pas
  `numberOfGuests` pour d'anciennes réservations, certaines resteront `A_CONTROLER` même après
  ré-extraction (ce sera alors une limite de la source, pas un défaut du pipeline) ?

Si oui, ce sera une mission dédiée et distincte (hors périmètre technique de celle-ci).

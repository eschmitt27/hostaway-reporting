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

## 11. Suite (2026-08-10) — ré-extraction Hostaway réelle autorisée et exécutée

**Ré-extraction Hostaway réelle autorisée par l'utilisateur pour rafraîchir le master après
correction `numberOfGuests`. Cette autorisation ne constitue pas une activation générale du mode
réel.** Périmètre strict : lecture API + génération d'un nouveau
`MASTER_FACT_HA_Reservations.xlsx` + remplacement contrôlé de ce seul fichier, si comparaison
concluante.

### A. Continuité
HEAD attendu `457cae9` confirmé. Branche `feature/banque-logements-pdf-charges-metier`, master
`8b47807`. Worktree propre. Port 8000/PID 21136 intact avant et après. Mode réel applicatif OFF,
writers OFF — aucun flag `app/config.py` modifié.

### B. Backup ancien master
`99_ARCHIVES/LOT1_MASTER_HA_Reservations/MASTER_FACT_HA_Reservations_PRE_REEXTRACT_20260810_184807.xlsx`
— hash SHA256 identique à l'original avant copie, relu et vérifié. Ancien master : 1391 lignes,
1391 `reservation_id` distincts, 22 colonnes, `guestCount` 100 % vide.

### C. Extraction API
Exécutée dans une zone temporaire isolée (scripts + `.env` réel copiés, aucune écriture dans
`02_TRAVAIL` réel pendant l'extraction). `--check-auth` OK, `--dry-run` : 1810 réservations
attendues depuis 2026-01-01. Extraction complète : 1527 lignes écrites dans
`MASTER_FACT_HA_Reservations.xlsx` (processus interrompu ensuite pendant l'étape annexe
« tâches ménage », sans conséquence — le fichier cible était déjà intégralement écrit).
`MASTER_CALC_HA_Payout.xlsx` recalculé séparément via `--recalc-payout-only` dans la copie de
simulation (REF_Setup réel disponible à cette étape) pour fiabiliser la mesure d'impact.

### D/E/F. Volumes et guest count
| Indicateur | Ancien master | Nouvelle extraction |
|---|---:|---:|
| Réservations | 1391 | 1527 |
| Colonne `numberOfGuests` | absente | présente |
| `numberOfGuests` renseigné | — | 1527 / 1527 |
| `guestCount` renseigné | 0 / 1391 (0 %) | 1527 / 1527 (100 %) |
| `code_controle_guestCount` (anomalie) | — | 0 |

### G. Comparaison exhaustive ancien/nouveau (par `reservation_id`)
1374 communs, 17 disparues (ancien → absentes du nouveau), 153 nouvelles.
- 1308 communes : seul guestCount/numberOfGuests diffère (attendu, c'est l'objet du correctif).
- 66 communes : `status`/`updatedOn` différents (cycle de vie Hostaway normal, ex. `new`→`modified`).
- 1 commune : changement économique réel — résa `60066412`, séjour prolongé 5→7 nuits,
  `totalPrice` 298,36€→422,70€ et `airbnbExpectedPayout` 228,73€→323,15€, cohérent au prorata
  (≈59,7€/nuit ancien, ≈60,4€/nuit nouveau). Modification légitime côté Hostaway, pas une anomalie.

### H. Écarts autres que guest count — classés
- **17 disparues** : vérifiées **directement via l'API en direct** (appel `GET /v1/reservations/{id}`
  sur les 17 ID) → toutes `status=cancelled`, `cancellationAmount=None`. Conforme à la règle
  existante du code (`include_any = is_active or is_owner or (is_cancelled and cancel_amt>0)`,
  inchangée) : annulation sans payout depuis la dernière extraction → exclusion correcte, pas un
  bug. **ATTENDU/EXPLICABLE.**
- **153 nouvelles** : 118 `new` + 23 `modified` + 12 `ownerStay`, check-in de 2026-06-01 à
  2027-02-07 — nouvelles réservations créées depuis la dernière extraction. **ATTENDU** (activité
  commerciale normale sur ~1-2 mois d'écart).
- **1 changement économique** (§G) : **EXPLICABLE**, vérifié cohérent.

Aucun écart économique inexpliqué. Aucune donnée personnelle nouvelle exposée (mêmes colonnes
`guestName` déjà présentes avant, schéma inchangé sur ce point).

### I. Contrôles de structure
1527 `reservation_id` distincts sur 1527 lignes (0 doublon). 26 colonnes (22 + 4 nouvelles :
`numberOfGuests`, `source_guestCount`, `controle_guestCount`, `code_controle_guestCount`).
Schéma compatible aval (nom de colonne `guestCount` inchangé, colonnes ajoutées uniquement).

### J/K. Simulation pipeline sur copie complète (jamais sur le réel, `CALCULS_REAL_RUN_ENABLED` non
touché, jamais lancé sur les sources réelles)
Copie intégrale de `01_SOURCES_BRUTES` + `02_DONNEES_NORMALISEES` + `02_TRAVAIL` (scripts et
sorties existantes), nouveau master substitué, chaîne rejouée : lot4bis → lot4quater → lot9 → lot10
→ lot11, avec l'interpréteur des lots (Python 3.12 + pandas), `PROJECT_ROOT`/`--project-root`
pointant exclusivement vers la copie. Un stub `NORM_Banque` vide (0 ligne) a été créé uniquement
pour satisfaire la dépendance technique `CTR-9-001` de lot9 — le dossier Banque réel est
actuellement vide aussi (hors périmètre de cette autorisation, aucune ligne bancaire simulée ou
inventée, 0 réellement égal à 0). lot4bis : 0 bloquant. lot4quater : 258 mois ouverts (live), 1269
mois clôturés (HIST, gelés). lot9 : 0 doublon. lot10 : 0 bloquant, REEL=COMPTABLE+HC vérifié à
l'euro (313 886,49 € = 303 362,33 € + 10 524,16 €). lot11 : 0 doublon, jointures OK.

| Indicateur | Avant | Après | Delta |
|---|---:|---:|---:|
| `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` (Lot10) | 553 | **506** | **-47** |
| `RESERVATION_EXCLUE_A_CONTROLER` (VRBO/Direct, Lot10) | 59 | 70 | +11 (base élargie 1391→1527 résa, explicable) |
| Total A_CONTROLER Lot10 | 612 | 576 | -36 |

**Mécanisme vérifié par jointure exhaustive** (les 506 restantes croisées avec
`MASTER_CALC_Reservations_Resolues`) : **100 % des 506 lignes restantes sont en `etat_mois=CLOTURE`
et `guestCount` vide** — c'est-à-dire figées dans l'historique clôturé (`HIST_Reservations_Cloturees`,
upsert immuable par conception, jamais réécrit sur une ligne déjà historisée, y compris par ce
correctif). Les 47 résolues sont exactement les réservations de mois **ouverts**. Résolution
= **100 % des cas techniquement atteignables par une ré-extraction API** (mois ouverts) ; **0 %
des mois clôturés**, par construction délibérée du pipeline (immutabilité de la clôture), pas une
limite du correctif.

### L. Effets économiques (Lot10, copie de simulation, hors Banque/Charges/IK/Acomptes incomplets
dans la copie — non comparables au total agrégé réel)
| Indicateur | Ancien (réel, dernier run Lot10 disponible) | Nouveau (simulation) | Delta |
|---|---:|---:|---:|
| Lignes NORMAL (commissions) | 1349 | 1462 | +113 |
| Assiette commission | 240 777,75 € | 257 995,48 € | +17 217,73 € |
| Commission conciergerie | 41 486,44 € | 41 208,44 € | -278,00 € |
| Net propriétaire (avant charge fixe) | 199 291,31 € | 216 487,04 € | +17 195,73 € |
| Résultat REEL (global) | 291 779,67 € | 313 886,49 € | +22 106,82 € |
| Résultat COMPTABLE (global) | 281 255,51 € | 303 362,33 € | +22 106,82 € |
| Résultat HORS_COMPTA (global) | 10 524,16 € | 10 524,16 € | 0 (inchangé, attendu) |

Delta dominé par les 153 nouvelles réservations et les 17 annulées (activité commerciale normale
sur la période écoulée), pas principalement par le correctif guest count — les 47 résolutions
canapé ne représentent qu'une fraction marginale de ce delta. Pas d'anomalie : REEL=COMPTABLE+HC
vérifié à l'identique des deux côtés.

### M. Réservations encore sans guest count
506, toutes en mois clôturé (HIST gelé), cause unique et vérifiée : historisées avant le correctif
du 20/06/2026, jamais réécrites depuis (immutabilité de clôture, comportement voulu). Aucune
inventée. Une correction de ces 506 nécessiterait une procédure dédiée de correction post-clôture
— décision utilisateur distincte, hors périmètre de cette autorisation.

### N. Tests
`tests/test_canape.py` + `tests/test_guestcount_canape_flow.py` +
`tests/test_guestcount_canape_integration.py` (Python 3.12 + pandas) : 26 passed, 9 subtests
passed. `tests/test_reservations_hh.py` + `tests/test_pipeline_registry_paths.py` +
`tests/test_controles_runner_validation_positive.py` (miniconda, app) : 36 passed. **0 nouvel
échec.**

### O. Intégrité
950/950 fichiers de la baseline réelle contrôlés après opération. **2 diffs, tous deux attendus et
documentés** : `REF_Setup.xlsm` (déjà committé `4b49a9d`, mission antérieure) et
`MASTER_FACT_HA_Reservations.xlsx` (cette mission). 0 fichier manquant, 0 diff imprévu. `app.db`
réelle, Banque, et toute autre source : identiques. Port 8000/PID 21136 intact. Mode réel OFF.

### P. Master réel remplacé
**OUI.** Uniquement `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_Reservations.xlsx`. Copie atomique
depuis la nouvelle extraction validée, hash SHA256 relu et identique à la source
(`b3bde4a5…e57fa8`). 1527 lignes, 1527 `reservation_id` distincts, 26 colonnes confirmés après
écriture. Aucun autre fichier de `Lot1_Hostaway` (Payout, Details, Fees, FinanceFields, Listings,
Anomalies) n'a été remplacé — ils restent les anciens fichiers réels, désormais partiellement
désynchronisés du nouveau `Reservations` (1391 lignes vs 1527) **délibérément**, comme prévu par le
périmètre strict de l'autorisation (§13 : pas de relance du pipeline réel). Cette désynchronisation
partielle sera résorbée par un futur run officiel du pipeline complet, hors périmètre ici.

### Q. Verdict clôture
- `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` (mesure de simulation, pipeline réel non relancé) :
  553 → 506 (-47), mécanisme intégralement expliqué et vérifié (§ ci-dessus).
- **Aucun chiffre de clôture réel n'a changé** : le pipeline aval réel n'a pas été relancé (interdit
  explicitement, §13). Les fichiers de contrôle réels (`MASTER_CTRL_Coherence.xlsx`, export
  applicatif) restent ceux du dernier run réel tant qu'un pipeline officiel n'est pas rejoué.
- **CLÔTURE : NO GO** (inchangé — nécessite un run réel du pipeline aval, hors périmètre ici).
- **PRÉPARATION MODE RÉEL : NO GO** (inchangé).
- **MODE RÉEL : NO GO — NON ACTIVÉ.** Aucun flag `app/config.py` touché.

### R. Prochaine action exacte
Deux voies possibles, aucune engagée par cette mission :
1. **Relancer le pipeline aval réel** (lot4bis→lot4quater→lot9→lot10→lot11, sur sources réelles,
   avec un vrai `BANQUE_LOT8_IMPORT.xlsx` à reconstituer via le module Banque — actuellement
   vide en réel aussi, indépendamment de cette mission) pour que les 47 résolutions et les
   nouvelles réservations impactent réellement `MASTER_CTRL_Coherence.xlsx` et l'export de
   clôture. Nécessite une autorisation dédiée (writers CALCULS_REAL_RUN, hors périmètre ici).
2. **Décision utilisateur sur les 506 restantes** (mois clôturés, guest count gelé) : accepter
   qu'elles restent `A_CONTROLER` définitivement (comportement correct par design), ou définir une
   procédure de correction post-clôture explicite (comme cela a été fait pour
   `GESTION_LOGEMENT_MISSING`) — décision métier distincte, à formuler si souhaitée.

Aucune des deux n'est engagée automatiquement. Mode réel reste NO GO.

## 12. Suite (2026-08-11) — audit d'impact + correctif lot4ter + correction réelle des 506

Décision utilisateur : correction rétroactive **ciblée** des 506 snapshots historiques (pas de
réouverture globale, pas de recalcul aveugle depuis le live, pas de synchronisation LIVE→HIST
générale).

**Audit d'impact préalable (copies)** : 397/506 sans impact canapé (delta 0), 109/506 avec
correction nécessaire (+1 090,00 € canapé). Les 506 étant exclues de Commissions/NetProprietaire
(A_CONTROLER), leur résolution ajouterait 14 302,91 € de commission et 75 412,65 € de net
propriétaire jamais formellement reconnus — **résultat société global inchangé** (REEL/COMPTABLE/
HORS_COMPTA identiques au centime dans toutes les simulations : le Flux Lot9 compte déjà ces
revenus indépendamment du contrôle canapé Lot10). 0 relevé propriétaire existant pour ces 3
propriétaires/17 mois (vérifié en lecture seule sur `app.db` réelle).

**Découverte critique et corrigée** : `lot4ter_historiser_reservations_cloturees.py` réécrivait
HIST depuis une liste `COLS` fixe de 28 colonnes n'incluant pas `guestCount` — toute correction de
ce champ était silencieusement effacée au run normal suivant. Reproduit par un test rouge
(`tests/test_lot4ter_guestcount_persistence.py`, 4 cas), corrigé par l'ajout minimal de
`"guestCount"` à `COLS` + capture dans la construction de nouvelle ligne. Test rouge→vert.
Régression : 272 passed (moteur) + 432 passed/36 skipped (app), 0 nouvel échec. **Committé
(`9a0a6aa`) avant toute donnée réelle.**

**Preuve de persistance sur copies** : correction fail-closed des 506 (script
`correction_guestcount_hist_ciblee.py` — refuse tout champ hors `guestCount`, toute réservation
hors liste, toute valeur invalide, toute écriture réelle sans `AUTORISATION_ECRITURE_REELLE=1`
explicite) puis run **normal** de `lot4ter` corrigé : guestCount survit (506/506, 1269 lignes
inchangées, 17 mois toujours CLOTURE). Chaîne aval rejouée : GUEST_COUNT 506→0, A_CONTROLER
576→70 (VRBO/Direct seuls), résultat société identique. Idempotence du cycle complet vérifiée
(2e passage : mêmes compteurs exacts). Rollback vérifié (hash restauré exact, compteurs reviennent
à 576/506/70).

**Correction réelle appliquée** : backup horodaté (`99_ARCHIVES/HIST_Reservations_Cloturees/…
PRE_CORRECTION_GUESTCOUNT_20260811_144118.xlsx`, hash vérifié), écriture réelle (colonne
`guestCount` ajoutée, 506 valeurs, 1269 lignes inchangées, **0 diff sur les 28 colonnes
existantes** vérifié contre le backup), relecture confirmée. Journal append-only :
`99_ARCHIVES/JOURNAL_CORRECTIONS/journal_correction_guestcount_hist_20260811.json` (506 entrées,
aucune PII). Intégrité globale : 950/950 fichiers baseline, 3 diffs tous attendus et documentés.
Port 8000/PID 21136 intact.

**Pipeline aval réel non relancé** (interdit explicitement, `CALCULS_REAL_RUN_ENABLED` jamais
touché) — les sorties de clôture réelles (`MASTER_CTRL_Coherence.xlsx`, export applicatif)
n'intègrent pas encore la correction, ce sera l'effet d'un futur run autorisé séparément.

**Verdict** : SNAPSHOTS CIBLES 506/506. CHAMPS MODIFIÉS guestCount uniquement (schéma étendu, 0
autre colonne touchée). MOIS 17/17 toujours CLOTURE. GUEST_COUNT (simulation) 506→0. A_CONTROLER
(simulation) 576→70. Canapé +1 090,00 €. Commission +14 302,91 € (débloquée). Net propriétaire
+75 412,65 € (débloqué). Résultat société : delta 0,00 €. Rollback VALIDÉ. Idempotence VALIDÉE.
HIST réel modifié : **OUI**. Pipeline réel relancé : **NON**. **MODE RÉEL : NO GO — NON ACTIVÉ.**

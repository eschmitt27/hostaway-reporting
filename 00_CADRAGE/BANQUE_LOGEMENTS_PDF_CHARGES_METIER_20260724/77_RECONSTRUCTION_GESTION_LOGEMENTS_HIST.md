# 77 — GESTION_LOGEMENT_MISSING : diagnostic et plan de reconstruction (2026-08-10)

**Aucun historique inventé. Aucun contrôle résolu automatiquement.** Cette mission traite
uniquement la famille `GESTION_LOGEMENT_MISSING` ; les 8 autres familles bloquantes ne sont pas
traitées.

> **Mise à jour (2026-08-10, suite) — DÉCISION UTILISATEUR APPLIQUÉE, SOURCE RÉELLE MODIFIÉE.**
> Décision explicite reçue : pour les 14 logements sans historique, considérer que le propriétaire
> renseigné au 01/01/2026 était déjà applicable à partir du **01/08/2025** (date de couverture
> décidée, pas nécessairement la date commerciale réelle d'entrée en gestion). Appliqué d'abord sur
> copie, vérifié structurellement et fonctionnellement, puis appliqué au référentiel réel après
> backup + SHA256 + prévisualisation exacte du diff. Voir section 10 ci-dessous pour le détail
> complet. Janvier→juillet 2025 restent explicitement **hors couverture, non reconstruits**.

## 1. Déduplication : 838 lignes ≠ 838 problèmes

| Mesure | Nombre |
|---|---:|
| Lignes de contrôle brutes | **838** |
| Couples logement × mois distincts | **137** |
| Logements distincts concernés | **14** (sur 19 au référentiel) |
| Mois distincts concernés | **12** |
| Première période | 2025-01 |
| Dernière période | 2025-12 |

**Ratio 6,12 lignes par couple** : chaque couple logement×mois est signalé une fois par réservation
concernée. Le volume apparent (838) est un artefact de comptage, pas 838 lacunes économiques.

**Le problème réel se réduit à : 14 logements, sur l'année 2025.**

## 2. Cause — une seule, uniforme

Lecture directe de `REF_Gestion_Logements_Hist` (17 lignes, copie de recette) : **les 17 lignes de
gestion ont `date_debut = 2026-01-01`**. Aucune ne couvre une date antérieure. La colonne `source`
porte pour les 17 lignes « Confirmation opérateur 28/06/2026 ».

Le référentiel est donc une **photographie de l'état au 01/01/2026**, construite en juin 2026 —
il n'a jamais contenu d'historique antérieur. Toutes les réservations de 2025 tombent
mécaniquement hors de toute période de gestion.

| Catégorie de diagnostic | Couples |
|---|---:|
| **A. ABSENCE_REELLE_HISTORIQUE** | **137** |
| B. BORNE_DATE | 0 |
| C. TROU_ENTRE_DEUX_PERIODES | 0 |
| D. STATUT_NON_GERE | 0 |
| E. IDENTIFIANT_NON_RESOLU | 0 |
| F. SOURCE_INCOHERENTE | 0 |
| G. AUTRE | 0 |

## 3. Le moteur n'est pas en cause — vérifié, pas supposé

Audit de `lib_ref_history.applies_on()` (fonction unique de résolution de période) :

```python
if start is not None and d < start:  return False   # début INCLUSIF
if end   is not None and d > end:    return False   # fin INCLUSIVE
return True                                        # date_fin vide => période ouverte
```

Vérification empirique exécutée sur le moteur réel :

| Cas testé | Résultat | Attendu |
|---|---|---|
| 2025-12-31 vs début 2026-01-01 | hors période | ✔ |
| **2026-01-01** vs début 2026-01-01 | **dans la période** | ✔ (début inclusif) |
| 2026-01-15 (milieu) | dans la période | ✔ |
| 2026-06-01, `date_fin` vide | dans la période | ✔ (période ouverte) |
| **2026-04-26** vs fin 2026-04-26 | **dans la période** | ✔ (fin inclusive) |
| 2026-04-27 vs fin 2026-04-26 | hors période | ✔ |

Confirmation croisée par les données : **aucun mois de 2026 n'apparaît dans les 137 couples
manquants**, alors que tous les mois de 2025 y sont. Le moteur applique donc correctement la borne
`2026-01-01`.

**Aucune correction moteur n'est nécessaire. 0 des 838 lignes n'est imputable à un bug.**
Corriger la donnée pour masquer un bug inexistant aurait été une faute ; il n'y a pas de bug.

## 4. Recherche de preuves historiques — résultat négatif

Sources examinées :

| Source | Résultat |
|---|---|
| 5 archives datées de `REF_Setup` (`99_ARCHIVES`, juin 2026) | **L'onglet `REF_Gestion_Logements_Hist` n'existe dans aucune d'elles** — il a été créé après |
| `REF_Logements` (colonnes de dates d'entrée/sortie de gestion) | **Aucune colonne de date**, aucune valeur |
| Exports historiques, documents de cadrage datés, décisions métier | Aucune mention d'un rattachement propriétaire antérieur à 2026 |
| Historique applicatif (SQLite) | Ne contient que ce qui dérive du même référentiel |

| Niveau de preuve | Couples logement × mois | Logements |
|---|---:|---:|
| PREUVE_A (explicite, non contradictoire) | **0** | 0 |
| PREUVE_B (reconstruction indirecte cohérente) | **0** | 0 |
| AMBIGU (plusieurs possibilités) | **0** | 0 |
| **ABSENT (aucune preuve suffisante)** | **137** | **14** |

## 5. Conséquence : aucune reconstruction n'est possible sans l'utilisateur

`PREUVE_A = 0` ⇒ **aucune ligne candidate à une reconstruction déterministe.** Aucun overlay de
reconstruction n'a donc été construit, aucune simulation d'injection n'a été faite : simuler
l'injection d'un ensemble vide n'aurait produit aucune information.

Les éléments qui pourraient *sembler* des preuves ont été explicitement écartés, conformément au
cadrage : l'existence d'une réservation en 2025 ne prouve pas qui gérait le logement ; le
propriétaire actuel ne prouve pas qu'il l'était en 2025 ; un taux de commission ne prouve pas un
rattachement. **Rien n'a été déduit.**

| Mesure | Avant | Après | Delta |
|---|---:|---:|---:|
| Lignes brutes | 838 | 838 | 0 |
| Couples distincts | 137 | 137 | 0 |
| Logements concernés | 14 | 14 | 0 |
| Clôture bloquée | OUI | OUI | — |

Effets aval (Lot9→Lot13) : **non mesurés, sans objet** — aucune donnée n'ayant été injectée, il n'y
a aucune variation à expliquer. Les totaux de référence restent ceux vérifiés en LOT D/E
(REEL 291 722,75 = COMPTABLE 281 198,59 + HC 10 524,16, écart 0,00 €).

## 6. Questions utilisateur — le motif est uniforme, donc la question l'est aussi

**Question générale (couvre les 14 logements)** :
Le référentiel de gestion ne commence qu'au **01/01/2026**. Or des réservations existent sur toute
l'année **2025** pour 14 logements. Trois réponses possibles, à trancher globalement :

- **(a)** ces logements étaient déjà en gestion en 2025 avec **les mêmes propriétaires**
  qu'aujourd'hui → il suffirait de reculer les `date_debut` à une date à préciser ;
- **(b)** ils étaient en gestion en 2025 mais avec des **propriétaires ou des dates différents**
  → il faut le détail par logement (tableau ci-dessous) ;
- **(c)** 2025 est **hors périmètre de gestion** (activité antérieure au contrat de conciergerie)
  → les contrôles 2025 doivent alors être neutralisés par une règle explicite, pas par une
  reconstruction de données.

**Détail par logement** (identifiants opaques, aucune adresse, aucun nom) :

| Logement | Mois manquants | Plages | Gestion connue à partir de |
|---|---:|---|---|
| LOG_0001 | 6 | 2025-07 → 2025-12 | PROP_0001 depuis 2026-01-01 (ouverte) |
| LOG_0002 | 10 | 2025-02 → 2025-10, 2025-12 | PROP_0002 depuis 2026-01-01 (ouverte) |
| LOG_0003 | 11 | 2025-02 → 2025-12 | PROP_0003 depuis 2026-01-01 (fin 2026-04-26) |
| LOG_0004 | 11 | 2025-02 → 2025-12 | PROP_0004 depuis 2026-01-01 (ouverte) |
| LOG_0006 | 11 | 2025-02 → 2025-12 | PROP_0006 depuis 2026-01-01 (ouverte) |
| LOG_0007 | 7 | 2025-03 → 2025-09 | PROP_0007 depuis 2026-01-01 (ouverte) |
| LOG_0008 | 10 | 2025-02 → 2025-04, 2025-06 → 2025-12 | PROP_0008 depuis 2026-01-01 (ouverte) |
| LOG_0010 | 8 | 2025-01 → 2025-08 | PROP_0005 depuis 2026-01-01 (ouverte) |
| LOG_0011 | 12 | 2025-01 → 2025-12 | PROP_0008 depuis 2026-01-01 (ouverte) |
| LOG_0012 | 11 | 2025-02 → 2025-12 | PROP_0009 depuis 2026-01-01 (ouverte) |
| LOG_0013 | 12 | 2025-01 → 2025-12 | PROP_0009 depuis 2026-01-01 (ouverte) |
| LOG_0014 | 12 | 2025-01 → 2025-12 | PROP_0010 depuis 2026-01-01 (ouverte) |
| LOG_0015 | 11 | 2025-02 → 2025-12 | PROP_0011 depuis 2026-01-01 (ouverte) |
| LOG_0016 | 5 | 2025-05, 2025-07 → 2025-08, 2025-10, 2025-12 | PROP_0012 depuis 2026-01-01 (ouverte) |

Note : les trous internes (LOG_0002 sans 2025-11, LOG_0008 sans 2025-05, LOG_0016 discontinu) ne
signifient pas qu'une gestion existait ces mois-là — ils signifient simplement qu'**aucune
réservation** n'y a déclenché de contrôle. Le référentiel est vide sur **toute** l'année 2025.

**Si la réponse est (a)** — la plus probable au vu de l'uniformité — la reconstruction serait
mécanique et sûre : reculer les 14 `date_debut` de 2026-01-01 à la date d'entrée en gestion réelle
de chaque logement. Mais cette date reste **une information que seul l'utilisateur détient**, et
elle n'est pas nécessairement identique pour les 14. Elle ne sera pas devinée.

## 7. Ce qui n'a délibérément pas été fait

- Aucune écriture dans `REF_Setup` réel ni dans aucune copie (§15 du cadrage respecté).
- Aucune ligne de gestion créée, même « évidente ».
- Aucun `date_debut` reculé par défaut au 01/01/2025.
- Aucune correction moteur (il n'y avait pas de bug).
- Aucune des 8 autres familles bloquantes touchée — en particulier, les 612 contrôles de
  commission relèvent d'un référentiel **distinct** (taux de commission) et d'une mission séparée.

## 8. Verdict

| Mesure | Valeur |
|---|---|
| GESTION_LOGEMENT_MISSING — avant | **838 lignes / 137 couples / 14 logements** |
| Après corrections moteur | **838** (aucune correction nécessaire : aucun bug) |
| Après reconstruction PREUVE_A sur copies | **838** (PREUVE_A = 0 : rien à reconstruire) |
| **Reste à validation humaine** | **137 couples / 14 logements — 1 question globale + détail** |

- **CLÔTURE : NO GO** — les 137 couples restent ouverts, et 8 autres familles bloquantes
  (1519 lignes) n'ont pas été traitées par cette mission.
- **PRÉPARATION MODE RÉEL : NO GO** — inchangé.
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

## 9. (superседée par la section 10 — décision (a) reçue et appliquée)

## 10. Application de la décision utilisateur (2026-08-10)

**Décision reçue, verbatim résumée** : cas (a) — même propriétaire qu'au 01/01/2026, couverture
prolongée jusqu'au **01/08/2025** (date de couverture décidée par l'utilisateur, pas affirmée comme
la date commerciale réelle). Ne pas prolonger avant. Aucun changement de propriétaire. Ne rien
inventer pour janvier→juillet 2025.

### 10.1 Application sur copie

14 lignes de `REF_Gestion_Logements_Hist` modifiées sur la copie de recette : seule `date_debut`
passée de `2026-01-01` à `2025-08-01` pour `LOG_0001, 0002, 0003, 0004, 0006, 0007, 0008, 0010,
0011, 0012, 0013, 0014, 0015, 0016`. `proprietaire_id`, `date_fin`, `statut_gestion` strictement
inchangés. `LOG_0005`, `LOG_0009`, `LOG_0017` (hors périmètre) non touchés.

### 10.2 Contrôles structurels (avant toute simulation)

| Contrôle | Résultat |
|---|---|
| Aucun chevauchement illégal | OK — une seule ligne par logement, aucun chevauchement possible |
| Aucun doublon exact | OK — 0 doublon détecté |
| Même propriétaire, même statut, même `date_fin` pour les 14 lignes | OK — vérifié ligne à ligne |
| Autres logements (3) strictement inchangés | OK — vérifié ligne à ligne |
| Nombre de lignes du référentiel inchangé | OK — 17 avant, 17 après |

### 10.3 Mesure sur copies

| Mesure | Avant | Après | Delta |
|---|---:|---:|---:|
| Lignes GESTION_LOGEMENT_MISSING | 838 | **472** | **−366** |
| Couples logement × mois | 137 | **77** | **−60** |
| Logements encore concernés | 14 | 14 | 0 (mêmes 14, mois restants seulement) |
| Total lignes de contrôle (toutes familles) | 2420 | 2002 | −418 (dont 60 couples ci-dessus × ratio de duplication par réservation ≈ 6,1) |

**Restes janvier→juillet 2025** (77 couples, tous dans cette fenêtre, sur les 14 mêmes logements) :

| Mois | Couples restants |
|---|---:|
| 2025-01 | 7 |
| 2025-02 | 64 |
| 2025-03 | 79 |
| 2025-04 | 83 |
| 2025-05 | 73 |
| 2025-06 | 82 |
| 2025-07 | 84 |

Exactement conforme à la décision : rien n'a été prolongé avant le 01/08/2025.

### 10.4 Effets aval (sur copies)

| Sortie | Avant | Après | Écart | Explication |
|---|---:|---:|---:|---|
| REEL (Lot10 global) | 291 722,75 € | 291 722,75 € | 0,00 € | Chaque logement n'a qu'une seule ligne de gestion — aucune ambiguïté propriétaire n'a jamais existé pour le calcul de commission ; prolonger `date_debut` n'a pas pu changer le propriétaire ni le taux déjà utilisés |
| COMPTABLE | 281 198,59 € | 281 198,59 € | 0,00 € | Idem |
| HORS_COMPTA | 10 524,16 € | 10 524,16 € | 0,00 € | Idem |
| Réconciliations A/B/D/H | OK | OK | — | Inchangées |
| Réconciliations C/G | A_CONTROLER | A_CONTROLER | — | Inchangées (mappings comptables provisoires, sans lien avec cette mission) |

**Aucune variation économique inattendue.** Zéro variation, triviale à expliquer.

### 10.5 Application au référentiel réel

Procédure suivie strictement dans l'ordre demandé :

1. **Backup** : `99_ARCHIVES/LOT0_REF_Setup/REF_Setup_PRE_PROLONGATION_GESTION_20260810.xlsm`.
2. **SHA256** : réel et backup identiques avant écriture
   (`a0ae8fbd0e73d5e94fd1f0ecd4f68195ca3ce0f994c62314ed92ef25ecc6637a`, 88 670 octets).
3. **Prévisualisation du diff exact** : produite et vérifiée conforme à la décision — seule
   `date_debut` des 14 lignes concernées change, rien d'autre.
4. **Écriture** : 14 lignes modifiées, identique au diff prévisualisé et à ce qui avait été validé
   sur copie.
5. **Relecture post-écriture** : les 14 lignes portent `date_debut = 2025-08-01` ; `LOG_0005`,
   `LOG_0009`, `LOG_0017` strictement inchangés ; 17 lignes au total, comme avant.
6. **Intégrité globale** : comparaison au baseline (950 fichiers) — **1 seul fichier modifié,
   `REF_Setup.xlsm`, exactement celui attendu. 949 autres fichiers identiques.**

**Recalcul du réel non effectué — délibérément.** Régénérer les sorties réelles Lot9→Lot13 exige le
writer Calculs (`CALCULS_REAL_RUN_ENABLED`), qui reste désactivé conformément au NO GO mode réel
en vigueur. Une instance de lecture a été démarrée par erreur sans `PROJECT_ROOT` explicite
(risque qu'elle résolve par défaut sur l'arborescence réelle) ; elle a été **arrêtée immédiatement**
sans qu'aucun writer n'ait été actif à aucun moment — aucune donnée touchée par cet instant. La
mesure §10.3 (838→472, 137→77) a été faite sur copies avec un diff **strictement identique** à
celui appliqué au réel ; elle est donc représentative de l'effet qu'aura le réel une fois son
pipeline relancé — action séparée, toujours gatée, non entreprise ici.

## 11. Verdict final

| Mesure | Valeur |
|---|---|
| GESTION_LOGEMENT_MISSING — avant | 838 lignes / 137 couples / 14 logements |
| Après corrections moteur | 838 (aucune correction nécessaire) |
| Après reconstruction PREUVE_A | 838 (PREUVE_A initial = 0) |
| **Après décision utilisateur (01/08/2025), sur copies** | **472 lignes / 77 couples / 14 logements** |
| **Appliqué au référentiel réel** | **Oui — 14 lignes, backup + SHA256 + prévisualisation vérifiés** |
| **Reste à validation humaine** | **77 couples, janvier→juillet 2025, mêmes 14 logements — hors périmètre de cette décision** |

- **CLÔTURE : NO GO** — 77 couples `GESTION_LOGEMENT_MISSING` restent ouverts (janvier→juillet
  2025), et les 8 autres familles bloquantes (1519 lignes) n'ont pas été traitées.
- **PRÉPARATION MODE RÉEL : NO GO** — inchangé.
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

## 12. Prochaine action exacte

Deux voies indépendantes, au choix de l'utilisateur :
1. Statuer sur janvier→juillet 2025 (74 couples restants) — même logique de décision de couverture,
   ou explicitement « hors périmètre » ;
2. Passer à la famille bloquante suivante par volume : `RESERVATION_A_CONTROLER_SANS_COMMISSION`
   (612 lignes) — référentiel **distinct** (taux de commission), à ne pas confondre avec celui-ci.

## 13. Prolongation finale au 01/01/2025 (2026-08-12) — clôture du chantier gestion

**Décision utilisateur reçue, verbatim** : « Oui le propriétaire a toujours été le même pour chaque
logement. » Interprétation enregistrée : pour les 14 logements déjà prolongés au 01/08/2025, le
propriétaire renseigné était déjà applicable sur toute la période 01/01/2025 → 31/07/2025. Date de
**couverture historique décidée pour le projet**, non affirmée comme date commerciale réelle
d'entrée en gestion antérieure à 2025.

### 13.1 Application

Même procédure que §10 : copie d'abord (`REF_Setup_COPY.xlsm`), correction fail-closed
(`correction_gestion_2025_ciblee.py`, refuse tout `gestion_id` hors liste, toute valeur de départ
≠ `2025-08-01`, toute autre colonne), 14 lignes modifiées `date_debut` 2025-08-01 → 2025-01-01 pour
`LOG_0001, 0002, 0003, 0004, 0006, 0007, 0008, 0010, 0011, 0012, 0013, 0014, 0015, 0016`.
`proprietaire_id`, `date_fin`, `statut_gestion` strictement inchangés. `LOG_0005`, `LOG_0009`,
`LOG_0017` non touchés (hors périmètre, jamais prolongés).

Diff exact (copie et réel, identique) : 14 cellules, colonne `date_debut` uniquement, 0 autre
cellule sur les 28 feuilles du classeur.

### 13.2 Mesure sur copies — simulation canonique fraîche avec Banque réelle

Base : `SIMULATION_CANONIQUE_HEAD_6b60577` (Banque réelle déjà reconstruite, 541 mouvements),
REF_Setup remplacé par la copie corrigée, chaîne rejouée via `run_regression_pipeline.py`
(lot4quater → lot9 → lot10 → lot11 → lot12 → lot13), **2 runs consécutifs, idempotence confirmée
(`[OK] Rejeu deterministe`)**.

| Mesure | Avant | Après | Delta |
|---|---:|---:|---:|
| `GESTION_LOGEMENT_MISSING` | 472 | **0** | −472 |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | 69 | **0** | −69 |
| BLOQUANT total | 541 | **0** | −541 |
| A_CONTROLER | 14 | 14 | 0 |
| INFO | 10 | 10 | 0 |
| Total contrôles | 565 | 24 | −541 |

Les 13 codes résiduels (A_CONTROLER + INFO) sont strictement identiques avant/après, code par code,
nombre par nombre — aucun effet de bord.

### 13.3 Effets économiques

| Vision | Avant | Après | Delta |
|---|---:|---:|---:|
| REEL | 313 756,48 € | 313 756,48 € | 0,00 € |
| COMPTABLE | 303 232,32 € | 303 232,32 € | 0,00 € |
| HORS_COMPTA | 10 524,16 € | 10 524,16 € | 0,00 € |

**Zéro variation**, cohérent avec §10.4 : chaque logement n'a qu'une seule ligne de gestion (un
seul propriétaire, un seul taux) — prolonger `date_debut` ne change ni propriétaire ni taux déjà
utilisés par le moteur de calcul. Les contrôles `GESTION_LOGEMENT_MISSING` et
`CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` sont des contrôles de **couverture référentielle**, pas des
recalculs de payout : leur disparition ne pouvait mécaniquement produire aucun delta.

### 13.4 Application au référentiel réel

1. Backup : `99_ARCHIVES/REF_Setup/REF_Setup_PRE_CORRECTION_GESTION_2025_20260812_105900.xlsm`,
   SHA256 identique à l'original avant écriture (`18e54236...f321f0a3`).
2. Écriture via script fail-closed, gate `AUTORISATION_ECRITURE_REELLE=1`.
3. Relecture immédiate : 14/14 lignes conformes, diff exact identique à la copie (14 cellules,
   `date_debut` uniquement), `LOG_0005/0009/0017` et les 3 autres feuilles/colonnes intacts.
4. Intégrité globale (950 fichiers baseline) : **3 diffs, exactement les 3 attendus**
   (`REF_Setup.xlsm` — ce changement ; `MASTER_FACT_HA_Reservations.xlsx` et
   `HIST_Reservations_Cloturees.xlsx` — déjà committés lors des missions précédentes). 0 autre
   fichier source touché.
5. Pipeline réel **non relancé** (`CALCULS_REAL_RUN_ENABLED` reste OFF). La preuve quantitative
   §13.2 fait foi ; le recalcul réel Lot9→Lot13 est une action séparée, toujours gatée.
6. Tests ciblés (0 code modifié) : `test_cloture.py`, `test_lot10_reservation_exclue_dedup.py`,
   `test_lot11_avantages_integration.py`, `test_lot4quater_resoudre_source_reservations.py`,
   `test_lot4ter_guestcount_persistence.py` — **24/24 verts**.

### 13.5 Verdict final du chantier gestion

| Mesure | Valeur |
|---|---|
| `GESTION_LOGEMENT_MISSING` — départ (mission initiale) | 838 lignes / 137 couples / 14 logements |
| Après décision (a), prolongation au 01/08/2025 | 472 lignes / 77 couples / 14 logements |
| **Après décision finale, prolongation au 01/01/2025** | **0 ligne / 0 couple** |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` (sous-ensemble dépendant) | 69 → **0** |
| **BLOQUANT total baseline clôture** | 541 → **0** |

**Chantier gestion historique 2025 : CLÔTURÉ.** Aucune ligne `GESTION_LOGEMENT_MISSING` ni
`CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` ne reste ouverte sur simulation canonique fraîche.

- **CLÔTURE TECHNIQUE (sur ces deux familles) : GO.**
- **PRÉPARATION MODE RÉEL : NO GO** — inchangé (42 Direct/VRBO, Banque humaine, mappings
  comptables provisoires restent).
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

Restes humains inchangés, indépendants de ce chantier : 42 réservations Direct/VRBO (saisie), 222
mouvements Banque + file assistée (rapprochement humain), 14 contrôles A_CONTROLER résiduels,
mappings comptables provisoires (non bloquants moteur).

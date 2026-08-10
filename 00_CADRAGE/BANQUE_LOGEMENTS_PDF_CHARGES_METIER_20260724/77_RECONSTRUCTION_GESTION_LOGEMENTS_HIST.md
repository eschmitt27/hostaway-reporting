# 77 — GESTION_LOGEMENT_MISSING : diagnostic et plan de reconstruction (2026-08-10)

**Aucune source réelle modifiée. Aucun historique inventé. Aucun contrôle résolu automatiquement.**
Cette mission diagnostique uniquement la famille `GESTION_LOGEMENT_MISSING` ; les 8 autres familles
bloquantes ne sont pas traitées.

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

## 9. Prochaine action exacte

Obtenir de l'utilisateur la réponse à la question (a)/(b)/(c) du §6. Cette seule réponse débloque
mécaniquement les 137 couples : dans le cas (a), une mission courte de reconstruction contrôlée
suffit ; dans le cas (c), c'est une règle de périmètre à écrire, pas une donnée à reconstruire.

# 78 — RESERVATION_A_CONTROLER_SANS_COMMISSION : audit et constat (2026-08-10)

**Aucune donnée réelle modifiée. Aucune reconstruction nécessaire — le référentiel demandé existe
déjà.** Cette mission traite uniquement `RESERVATION_A_CONTROLER_SANS_COMMISSION` ; les autres
familles ne sont pas traitées.

## 0. Correction de la baseline des contrôles (gate préalable)

Le total réel après prolongation de `REF_Gestion_Logements_Hist` (mission précédente) est **2002**,
vérifié exhaustivement sur l'artefact de contrôles, **pas une erreur**. L'hypothèse de la mission
(2420−366=2054) supposait que seule `GESTION_LOGEMENT_MISSING` avait varié. En réalité,
**`CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` a aussi varié de −52** (121→69) : effet aval réel et
explicable, ce contrôle dépendant lui aussi de `REF_Gestion_Logements_Hist` (charges fixes
résolues via ce référentiel, `CTR-LOT10-16`). Tous les 16 codes comparés un à un ; aucun autre
n'a varié.

| Famille | Nombre |
|---|---:|
| GESTION_LOGEMENT_MISSING | 472 |
| RESERVATION_A_CONTROLER_SANS_COMMISSION | 612 |
| GUEST_COUNT_MANQUANT_PREPARATION_CANAPE | 553 |
| CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE (Banque) | 222 |
| CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE | 69 |
| Résiduelles (11 codes mineurs) | 74 |
| **TOTAL** | **2002** |

## 1. Audit des 612 — dédup et causes réelles

Source de vérité directe : onglet `A_CONTROLER` de `MASTER_CALC_Commissions.xlsx` (Lot10),
**612 lignes = 612 réservations distinctes** (pas de duplication, une ligne par réservation, clé
`reservation_calc_id`). Cause exacte (colonne `code_anomalie_lot10`) :

| Cause | Nombre |
|---|---:|
| `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` | 553 |
| `RESERVATION_EXCLUE_A_CONTROLER` (source VRBO 32 + Direct 27, `statut_calcul_payout=A_CONTROLER`) | 59 |
| `TAUX_COMMISSION_*` (tout code lié à un taux manquant) | **0** |
| **TOTAL** | **612** |

**Aucune des 612 lignes actuelles n'est causée par un taux de commission manquant.**

## 2. Pourquoi le taux ne peut pas être la cause — vérifié dans le code, pas supposé

`lot10_calculer_resultats.py`, fonction `_attach_commission_rate()` : si `resolve_commission_rate()`
échoue pour une seule ligne, le moteur fait `log.error(...); sys.exit(1)` — **arrêt dur du pipeline,
pas un A_CONTROLER ligne par ligne.** Les runs de cette session (idempotence, prolongation gestion)
ont toujours abouti à `SUCCES` : la preuve directe est qu'**aucune réservation actuellement traitée
n'a jamais déclenché ce blocage**, donc le référentiel de taux couvre déjà tout ce qui atteint ce
stade de calcul.

Les 553 `GUEST_COUNT_MANQUANT` et 59 `RESERVATION_EXCLUE_A_CONTROLER` (`statut_calcul_payout`,
assigné en amont dans `lot1_hostaway_extract.py`, indépendant de tout taux) sont **exclues avant
même d'atteindre la résolution de taux**.

## 3. Audit du référentiel `REF_Taux_Commission` — déjà construit exactement selon la règle

19 lignes, colonnes `taux_commission_id, proprietaire_id, logement_id, taux_commission, date_debut,
date_fin, actif, justification, commentaire`. **12/12 propriétaires couverts**, source
« Règle opérateur confirmée le 28/06/2026 » — construit le même jour que `REF_Gestion_Logements_Hist`.

| Propriétaire | Périodes |
|---|---|
| PROP_0002, 0003, 0004, 0006, 0011 (5) | Une seule période : `2025-01-01 → (ouverte)`, 15 % — taux inchangé |
| PROP_0001, 0005, 0007, 0008, 0009, 0010, 0012 (7) | Deux périodes : `2025-01-01 → 2026-01-31` à 15 %, puis `2026-02-01 → (ouverte)` au taux actuel propre (18/19/12 % selon le propriétaire) |

**Structure exactement conforme à la règle utilisateur** : couverture 2025-01-01→2026-01-31 à 15 %
pour tous, taux spécifique à partir du 01/02/2026 quand il diffère, ligne unique quand il ne diffère
pas. Contrôles structurels : 0 trou, 0 chevauchement (au plus 2 lignes contiguës par propriétaire,
bornes cohérentes), 0 doublon, aucune période avant 2025-01-01.

## 4. Simulation — sans objet

Aucune période à créer, aucun overlay, aucune copie modifiée : **le référentiel cible et le
référentiel actuel sont déjà identiques.** Mesure demandée par la mission :

| Mesure | Avant | Après |
|---|---:|---:|
| RESERVATION_A_CONTROLER_SANS_COMMISSION | 612 | **612 (inchangé)** |
| — dont taux absent | 0 | 0 |
| — dont guest count manquant | 553 | 553 (hors périmètre) |
| — dont payout VRBO/Direct non résolu | 59 | 59 (hors périmètre) |

**Résolues par la règle de taux : 0.** Ce n'est pas un échec de la règle — la règle est déjà
appliquée par le système ; les 612 restants sont bloqués par deux causes totalement indépendantes
du taux, explicitement hors périmètre de cette mission.

## 5. Effets aval — aucun, car aucune écriture

Aucune modification n'a été faite ; aucun effet à mesurer. Les totaux économiques
(REEL 291 722,75 € = COMPTABLE 281 198,59 € + HC 10 524,16 €, écart 0,00 €) restent ceux déjà
vérifiés lors des missions précédentes.

## 6. Le cas des 77 couples `GESTION_LOGEMENT_MISSING` restants (jan-juil 2025)

Vérifié : aucune des 612 lignes actuelles de `RESERVATION_A_CONTROLER_SANS_COMMISSION` ne provient
des 14 logements/77 couples encore sans historique de gestion pour janvier-juillet 2025 — le
calcul économique de Lot10 résout le propriétaire indépendamment de la couverture temporelle
vérifiée par Lot11 (constaté déjà lors de la mission précédente : la prolongation de gestion n'a
provoqué aucune variation économique). Aucun cas `PROPRIETAIRE_A_CONTROLER` lié au taux n'existe
donc dans ce périmètre.

## 7. Application réelle — sans objet

Rien à appliquer : le référentiel réel est déjà dans l'état cible. Aucun backup, aucune écriture,
aucun diff.

## 8. Verdict

| Mesure | Valeur |
|---|---|
| RESERVATION_A_CONTROLER_SANS_COMMISSION — avant | 612 |
| Après (règle de taux, déjà en place) | **612 — inchangé** |
| Résolues par cette mission | **0** (le référentiel était déjà complet) |
| Reste, cause GUEST_COUNT_MANQUANT (hors périmètre) | 553 |
| Reste, cause RESERVATION_EXCLUE_A_CONTROLER / payout VRBO-Direct (hors périmètre, cause distincte du taux) | 59 |

- GESTION_LOGEMENT_MISSING : **472, inchangé** (hors périmètre de cette mission).
- GUEST_COUNT : **553, inchangé**.
- BANQUE : **222, inchangé**.
- CHARGES : **69, inchangé** (déjà réduit par l'effet aval de la mission précédente).
- **CLÔTURE : NO GO.**
- **PRÉPARATION MODE RÉEL : NO GO.**
- **MODE RÉEL : NO GO — NON ACTIVÉ.**

## 9. Prochaine action exacte

La règle de taux ne débloque rien ici — **aucune question utilisateur nécessaire sur le sujet
taux**, il est clos. Deux voies possibles, indépendantes de cette mission :
1. **Prochaine famille par volume** : `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` (553 lignes,
   dont 553 des 612 commission actuelles) — la résoudre réduirait mécaniquement une bonne partie
   des deux familles à la fois.
2. Statuer sur les 59 `RESERVATION_EXCLUE_A_CONTROLER` (VRBO/Direct, statut de payout non résolu) —
   cause distincte, nécessitera probablement une décision utilisateur sur la source financière de
   ces réservations (hors Hostaway), à documenter séparément si elle est traitée.

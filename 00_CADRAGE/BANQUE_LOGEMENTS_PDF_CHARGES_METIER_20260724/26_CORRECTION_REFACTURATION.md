# 26 — Correction du défaut de refacturation (proprietaire_id = None)

## Origine exacte

`app/services/charges_preview_service.py`, fonction **`_build_row_data`** (branche « charge non
ménage guidée »). Le code posait **explicitement** :

```python
if len(finaux) == 1:
    affectation_type = "LOGEMENT"
    logement_id = finaux[0]
    proprietaire_id = None        # ← propriétaire jamais matérialisé
```

Le propriétaire était pourtant **déjà résolu**, de façon **historisée**, par le périmètre :
`compute_perimetre_logements()` construit `proprietaire_par_logement` en ne retenant que les lignes
de gestion actives sur le mois de la charge (`gestion_active_pour_mois`). L'information existait ;
elle n'était simplement pas écrite dans la ligne SAISIE.

## Traçage de la chaîne

| Étape | logement_id | proprietaire_id | refacturable | Statut |
|---|---|---|---|---|
| Formulaire | LOG_A1 | (non saisi) | OUI | ok |
| Périmètre (`compute_perimetre_logements`) | LOG_A1 | **PROP_A** (résolu, historisé) | OUI | ok |
| `_build_row_data` | LOG_A1 | **None** ← **PERTE ICI** | OUI | défaut |
| SAISIE_Charges_Flux | LOG_A1 | None | OUI | défaut propagé |
| Lot3 MASTER charges | LOG_A1 | None | OUI | défaut propagé |
| Lot9 flux CHG | LOG_A1 | None | — | charge visible (résultat logement −100) |
| Lot10 `aggregate_refacturable_charges` | LOG_A1 | None | OUI | **refacturation abandonnée** (non-inférence) |
| Préfacture finale | — | — | — | 0 € |

## Point de correction retenu : **Option A (au plus tôt, à la saisie)**

Décision : matérialiser le propriétaire dans `_build_row_data`, à partir de la résolution
historisée déjà calculée par le périmètre. Motifs :
- l'information est disponible et **déjà datée** (aucune nouvelle règle métier inventée) ;
- Lot10 conserve intacte sa règle de **non-inférence** (option C écartée) ;
- la charge porte son propriétaire dès la SAISIE → traçabilité et contrôles en amont.

## Règle appliquée (matérialisation stricte, jamais d'attribution arbitraire)

| Situation | Résultat |
|---|---|
| 1 logement, propriétaire actif résolu à la période | `proprietaire_id` **renseigné** |
| 1 logement, aucun propriétaire actif | `None` (jamais inventé) |
| N logements, **tous** du même propriétaire | propriétaire **renseigné** |
| N logements, propriétaires différents | `None` (ambiguïté → aucune attribution) |
| Charge propriétaire seul (sans logement) | propriétaire explicite conservé |
| Charge ménage / catégorie personnalisée | `None` (jamais refacturable) |

## Preuve chiffrée après correction (scénario B, pipeline complet Lot3+Lot9+Lot10)

| Indicateur | Avant | Après | Écart |
|---|--:|--:|--:|
| Résultat réel LOG_A1 | 1000 | **900** | −100 |
| Résultat comptable LOG_A1 | 1000 | **900** | −100 |
| Net PROP_A (exploitation) | 1660 | 1660 | 0 |
| **Somme à payer PROP_A** | 340 | **440** | **+100** |
| **Préfacture finale PROP_A** | 0 | **100** | **+100** |

Ligne MASTER après correction : `logement_id=LOG_A1`, **`proprietaire_id=PROP_A`**,
`refacturable=OUI`, `statut_controle=VALIDE`.

## Réconciliation des 100 € (aucun double comptage)

| Élément | Montant |
|---|--:|
| Charge enregistrée | 100 |
| Impact sur le résultat du logement | −100 |
| Créance sur le propriétaire (`charges_exceptionnelles_refacturees`) | +100 |
| Montant dû conciergerie (340 commission + 100 refac) | 440 |
| Préfacture finale | 100 |
| Effet économique final **conciergerie** | −100 (charge) +100 (refac) = **0** |
| Effet économique final **propriétaire** | net 1660 − 100 dû = **1560** |

Les 100 € apparaissent **une seule fois** en charge et **une seule fois** en créance. Le net
d'exploitation (1660) n'est pas re-diminué : la refacturation transite par le montant dû, pas par le
net — logique comptable réellement appliquée par Lot10.

## Tableau final — tous scénarios passés au pipeline complet (Lot3 + Lot9 + Lot10)

| Scénario | Résultat LOG_A1 réel | Résultat comptable | Net PROP_A | Somme à payer PROP_A | Préfacture finale |
|---|--:|--:|--:|--:|--:|
| **A** conciergerie IC, non refac. | 1000 → **900** | 1000 → **900** | 1660 (0) | 340 (0) | 0 (0) |
| **B** propriétaire refacturable | 1000 → **900** | 1000 → **900** | 1660 (0) | **340 → 440** | **0 → 100** |
| **C** payée hors banque conciergerie | 1000 → **900** | 1000 → **900** | 1660 (0) | 340 (0) | 0 (0) |
| **D** hors comptabilité (HC) | 1000 → **900** | **1000 (0)** | 1660 (0) | 340 (0) | 0 (0) |
| **H** réparti LOG_A1+LOG_A2 (même propriétaire) | 1000 (0) | 1000 (0) | 1660 (0) | 340 (0) | 0 (0) |
| **I** réparti LOG_A1+LOG_B1 (2 propriétaires) | 1000 (0) | 1000 (0) | 1660 (0) | 340 (0) | 0 (0) |

**Distinctions prouvées :**
- **A vs B** : seule B génère une préfacture (100 €) et augmente la somme due (+100). La
  refacturation fonctionne désormais de bout en bout.
- **A vs D** : D ne touche PAS le résultat comptable (règle HORS_COMPTA conservée).
- **C** : aucune préfacture, aucune somme due supplémentaire → aucun remboursement indu.
- **H / I** : la charge multi-logements devient une charge **GLOBAL** (une seule charge économique) :
  elle réduit le résultat conciergerie global (**14 060 → 13 960**, −100) mais n'est pas ventilée
  dans les résultats par logement de Lot10. La ventilation reste **analytique**, matérialisée dans
  `SAISIE_Charges_Impacts/AFFECTATIONS` : I → LOG_A1/PROP_A = **50**, LOG_B1/PROP_B = **50**
  (somme exacte 100, aucun montant ne passe d'un propriétaire à l'autre).

## Tests

`tests/test_charge_refacturable_proprietaire.py` — **6 tests** (rouge avant correction : 3 échecs) :
matérialisation logement unique, non refacturable, propriétaire inconnu jamais inventé,
multi-logements même propriétaire, multi-propriétaires (ambiguïté → None), charge ménage.

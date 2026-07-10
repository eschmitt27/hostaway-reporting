# Lot7 — Générateur des avantages associés (cadrage canonique)

> Statut : **moteur réel = Python/openpyxl** (Option A). Ce document fait foi pour Lot7.

## Ce qui génère réellement Lot7

Lot7 est **généré par Python / openpyxl**, de façon déterministe et testable.

- Moteur : `02_TRAVAIL/lot7_generateur_avantages.py`
- Logique d'attribution (dédup, priorité) : `02_TRAVAIL/lib_avantages.py`
- Sortie produite : onglet `MASTER_CALC_AVANTAGES` (15 colonnes) du classeur
  `Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx`.

**L'onglet `POWER_QUERY_CODE` est DOCUMENTAIRE.** Le classeur ne contient aucun Power Query
vivant : pas de `connections.xml`, pas de `DataMashup`, pas de `queryTables`, aucune connexion
actualisable. Il ne faut donc plus parler de « refresh Power Query réel » pour ce fichier : le
recalcul se fait en exécutant le générateur Python, pas en actualisant Excel.

## Sources durables lues (jamais écrites par le générateur)

| Source | Rôle |
|---|---|
| `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` (onglet `SAISIE`) | Avantage issu d'une charge, porté par la colonne `avantage_associe_id`. |
| `SOURCE_SAISIE` (onglet du classeur Lot7) | Saisie **strictement résiduelle** : avantages autonomes sans charge d'origine (virements `TYPE_FLUX_001`, IK `TYPE_FLUX_015`, remboursements `TYPE_FLUX_005`). |

`SOURCE_SAISIE` ne doit **jamais** recevoir une charge déjà présente dans Lot3 : une ligne
résiduelle dont `lien_origine` pointe une `charge_id` existante en Lot3 est **rejetée**
(anomalie `SAISIE_LOT7_SOURCE_DEJA_EXISTANTE`).

## Règle d'attribution de l'avantage d'une charge (une charge = une seule voie)

Les avantages issus des charges viennent de **`SAISIE_Charges_Flux.avantage_associe_id`**.
Le moyen de paiement (`mode_paiement_id`) et le bénéficiaire de l'avantage
(`avantage_associe_id`) sont **deux notions distinctes**.

1. `avantage_associe_id` renseigné → **totalité** du montant attribuée à cet associé, quel que
   soit le moyen de paiement (`PAY_001` banque pro inclus). Aucune autre règle avantage n'est
   appliquée à cette charge.
2. sinon → règle historique `TYPE_FLUX_002` (dépense perso) par `associe_id` de paiement.
3. **jamais les deux voies** pour une même `charge_id`.

- `PAY_003` / `PAY_004` **sans** `avantage_associe_id` → aucun avantage automatique.
- `PAY_001` **avec** `avantage_associe_id` → avantage pour `avantage_associe_id`.
- Une même `charge_id` n'est **jamais comptée deux fois** (dédup déterministe : première
  occurrence gardée, anomalie `CHARGE_ID_DOUBLON` tracée). Le générateur est **idempotent** :
  une seconde exécution produit exactement le même `MASTER_CALC_AVANTAGES` (100 € reste 100 €,
  jamais 200 €).

## Exécution (copies contrôlées uniquement)

```python
import lot7_generateur_avantages as gen
resume = gen.generer(saisie_charges_path, lot7_path, sortie_path)  # sortie_path = COPIE
```

- Le générateur **n'écrit jamais** le classeur métier réel : `sortie_path` doit être une copie.
- Les flags d'écriture réelle (`CHARGES_REAL_WRITE_ENABLED`) restent **désactivés** ; ce module
  ne les lit ni ne les modifie (il ne fait que lire des sources et écrire une copie fournie).

## Preuves

- `tests/test_lot7_generateur_avantages.py` — 5 cas exigés + idempotence, sur fixtures tmp.
- `tests/test_avantages_charges.py` — logique d'agrégation (`lib_avantages`).
- `tests/test_pq_avantage_lot7_reel.py` — preuve **complémentaire optionnelle** via Excel COM
  (skippée si Excel/win32com indisponible) ; ne conditionne pas Lot7.

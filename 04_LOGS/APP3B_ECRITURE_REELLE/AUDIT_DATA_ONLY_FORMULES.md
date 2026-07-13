# APP-3b / Commit 1 — Audit des lecteurs `data_only` et de la dépendance aux formules C/I/J/AD

> Audit **lecture seule**. Aucun fichier métier modifié, aucun flag touché, aucun writer codé.
> Périmètre : qui lit `SAISIE_Charges_Flux.xlsx` et `MASTER_FACT_MAN_Charges.xlsx`, avec quelles
> colonnes, et que se passe-t-il si Excel n'a pas recalculé le cache des formules après une
> écriture openpyxl.

## 0. Rappel du risque étudié

`openpyxl` **préserve** les formules mais **ne les recalcule pas** : la valeur en cache d'une cellule
formule reste celle d'avant, ou devient vide pour une ligne nouvellement écrite. `fullCalcOnLoad = True`
ne recalcule qu'à la **prochaine ouverture par Excel**. Tout lecteur `data_only=True` d'une colonne
formule lit donc **le cache**, pas la formule.

Colonnes formule de `SAISIE_Charges_Flux` (onglet `SAISIE`) :

| Col | Champ | Formule |
|---|---|---|
| `C` | `mois` | `=IF(B2="","",TEXT(B2,"YYYY-MM"))` |
| `I` | `impact_resultat_reel` | `=IF(H2="IC","OUI",IF(H2="HC","OUI",IF(H2="HR","NON","A_CONTROLER")))` |
| `J` | `impact_resultat_comptable` | `=IF(H2="IC","OUI",IF(H2="HC","NON",IF(H2="HR","NON","A_CONTROLER")))` |
| `AD` | `ROW_HASH` | `=IF(A2="","",TEXT(B2,"YYYYMMDD")&"|"&TEXT(D2,"0.00")&"|"&F2&"|"&M2)` |

## 1. Lecteurs identifiés de `SAISIE_Charges_Flux.xlsx` (lecture directe)

| Lecteur | `data_only` | Colonnes lues | Lit C/I/J/AD comme valeur ? | Verdict |
|---|---|---|---|---|
| `02_TRAVAIL/lot7_generateur_avantages.py` (l. 117) | `True` | charge_id, date_charge, mois, montant, type_flux_id, associe_id, avantage_associe_id, mode_paiement_id | `mois` **en repli seulement** | **IMMUNISÉ** |
| `02_TRAVAIL/lot6f_cout_complet_menages.py` (l. 57 `sh()`) | `True` | affectable_menage, **mois**, categorie_charge_id, montant | **OUI — `mois` (col C)** | **À RISQUE** |
| `02_TRAVAIL/lot11_controles_coherence.py` (via `gen.charger_charges_saisie`) | `True` | idem Lot7 | non (même dérivation) | **IMMUNISÉ** |
| `02_TRAVAIL/lib_controles_avantages.py` | — | opère sur les dicts déjà normalisés par Lot7 | non | **IMMUNISÉ** |
| `05_APPLICATION/app/readers/saisie_charges_reader.py` (l. 75/184/218) | `True` | `charge_id` (col A) pour `read_all_charge_ids` / `count_charges_with_prefix` | non | **IMMUNISÉ** |
| `05_APPLICATION/app/readers/saisie_charges_reader.py` (l. 162 `find_model_row`) | `False` | C/I/J/AD **en tant que formules** (c'est le but) | non (lit la formule, pas sa valeur) | **IMMUNISÉ** |
| `05_APPLICATION/app/services/charges_preview_service.py` | via reader | ligne modèle + comptage `charge_id` | non | **IMMUNISÉ** |

### Le seul lecteur à risque : Lot6f

```python
# 02_TRAVAIL/lot6f_cout_complet_menages.py — l. 214-220
for d in saisie_rows:
    if str(d.get("affectable_menage")) != "OUI" or str(d.get("mois"))[:7] != MONTH: continue
    cat = str(d.get("categorie_charge_id")); m = f(d.get("montant")) or 0
    if cat == "CHG_004": pool_conso += m
    elif cat in ("CHG_018",): pool_autres += m
    else: pool_courses += m
```

Si le cache de `C` n'est pas recalculé, `d["mois"]` vaut `None` → `"None"[:7] != MONTH` → **la charge
est ignorée**. Les pools courses / consommables / autres tombent à 0, le coût complet ménage est faux,
et l'erreur est **silencieuse** : le seul contrôle émis (`POOL_VIDE_NON_SAISI`) est de sévérité `INFO`
et son message dit « aucune charge ménage saisie » — c'est-à-dire exactement le mauvais diagnostic.

Même dépendance l. 226 pour le contrôle `DOUBLE_SOURCE_LAVAGE_A_CONTROLER`.

## 2. Lecteurs de `MASTER_FACT_MAN_Charges.xlsx`

| Lecteur | `data_only` | Colonnes utilisées |
|---|---|---|
| `lot10_calculer_resultats.py` (l. 176) → `lib_settlements.aggregate_refacturable_charges` | `True` | **mois**, montant, refacturable, statut_controle, proprietaire_id, logement_id |
| `lot11_controles_coherence.py` (l. 221) | `True` | charge_id, mode_paiement_id, montant |
| `05_APPLICATION/app/readers/charges_reader.py` (l. 49) | `True` | page Charges / Fournisseurs (D026 : source unique = MASTER, jamais la SAISIE) |
| `lot12_seed/remove_donnees_fictives.py` | `True` | charge_id |

Dans le MASTER, `mois` / `impact_resultat_reel` / `impact_resultat_comptable` sont des **colonnes de
données**, pas des formules : le cache de la SAISIE ne les concerne pas. **À condition que le MASTER
soit alimenté.** Ce qui nous amène au vrai sujet.

## 3. Le fait capital : rien n'alimente le MASTER Lot3

Inspection du classeur `02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx` :

```
connections.xml : False
DataMashup      : False
queryTables     : []
customXml       : []
onglets         : ['MASTER', 'VUE_MENAGE', 'POWER_QUERY_CODE']
nb lignes MASTER: 1
ligne 2         : '[Charge par Power Query — requete MASTER_FACT_MAN_Charges]'
```

**Aucun Power Query vivant** — exactement la même situation que le classeur Lot7 avant la mission
Option A. L'onglet `POWER_QUERY_CODE` est **documentaire**. Et il n'existe **aucun script `lot3_*.py`**
dans `02_TRAVAIL/` (vérifié : `ls 02_TRAVAIL/lot3*` → aucun résultat).

**Conséquence directe pour l'écriture réelle :** écrire une charge dans `SAISIE_Charges_Flux` ne la
propage **à rien**. Lot9, Lot10 (net propriétaire), Lot11 (contrôles charges), Lot12 (factures) et la
page Charges de l'app lisent tous le **MASTER**, qui restera vide. Seuls Lot7 (avantages) et le suivi
associé de Lot11 verraient la charge, parce qu'eux lisent la SAISIE en direct.

Autrement dit : **le writer réel, à lui seul, produirait une charge invisible pour le résultat et le
net propriétaire.** Ce n'est pas un défaut du writer — c'est un maillon manquant en amont de lui.

## 4. Risque si Excel n'a pas recalculé

| Lot | Impact réel |
|---|---|
| Lot7 avantages | aucun (dérive `mois` de `date_charge`) |
| Lot11 suivi associé | aucun (même dérivation) |
| App (prévisualisation, `charge_id`, ligne modèle) | aucun (colonnes manuelles uniquement) |
| **Lot6f coût complet ménages** | **charge ménage ignorée, pools à 0, résultat faux et silencieux** |
| Lot9 / Lot10 / Lot12 / page Charges | aucun **au titre du cache** — mais **tout** au titre du MASTER non alimenté (§3) |

## 5. Lots immunisés

Lot7 (générateur avantages), Lot11 (suivi associé), les readers et services de l'app. Tous parce
qu'ils ne lisent aucune colonne formule **comme valeur** : `mois` est systématiquement dérivé de
`date_charge` en Python, ou la colonne n'est pas lue.

## 6. Lots à adapter

1. **`lot6f_cout_complet_menages.py`** — seul lecteur qui dépend réellement du cache de `C`.
2. **Lot3 — à créer** (voir §7) : ce n'est pas une adaptation, c'est un maillon absent.

## 7. Recommandation avant le writer réel

**Le prérequis n'est pas le cache des formules. C'est le générateur Lot3.** Deux correctifs, par
ordre d'importance :

### 7.1 Créer un générateur Lot3 (Option A, même patron que Lot7)

Un `02_TRAVAIL/lot3_generateur_charges.py` déterministe : lit `SAISIE_Charges_Flux` (source durable),
écrit `MASTER_FACT_MAN_Charges` (onglet MASTER), et **dérive en Python** les colonnes aujourd'hui
portées par des formules Excel :

- `mois` = `date_charge[:7]` — jamais le cache de `C` ;
- `impact_resultat_reel` / `impact_resultat_comptable` = dérivés de `code_impact` (le mapping existe
  déjà : `lib_lot4a_reservations_hh.py` l. 310-311, `IMPACT_REEL` / `IMPACT_COMPTA`) ;
- `ROW_HASH` = recalculé en Python.

Effet : **le cache Excel devient sans objet pour toute la chaîne aval**, et l'écriture réelle
produit enfin une charge visible par Lot9/Lot10/Lot11/Lot12. C'est la même décision, et le même
patron, que ceux déjà validés pour Lot7.

### 7.2 Corriger Lot6f

Remplacer la dépendance à `d.get("mois")` par une dérivation depuis `date_charge` (une ligne), ou
faire lire le MASTER une fois celui-ci alimenté. Petit patch, supprime la dernière dépendance au
cache Excel du projet.

### 7.3 Ne pas dépendre du recalcul Excel

`fullCalcOnLoad = True` reste posé (confort : les formules se rafraîchissent à l'ouverture pour
l'humain qui regarde le classeur), mais **aucun lot ne doit en dépendre**. Une fois 7.1 et 7.2 faits,
c'est le cas.

## 8. Impact sur le plan de commits

Le plan validé (writer → orchestrateur → branchement) reste bon, mais il lui manque une marche, et
elle vient **avant** le writer :

| # | Contenu | Statut |
|---|---|---|
| 1 | **Cet audit** | fait |
| 1-bis | **Générateur Lot3 + correctif Lot6f** | **à décider — nouveau, prérequis** |
| 2 | `saisie_charges_writer` + tests unitaires | inchangé |
| 3 | Orchestrateur + rollback deux fichiers + tests transactionnels | inchangé |
| 4 | Branchement `persister_reel` + protocole rejoué sur copies | inchangé |

Sans le commit 1-bis, le writer réel serait techniquement correct et **métier-ement inutile** : il
écrirait une charge que le résultat et le net propriétaire ne verraient jamais.

Flags inchangés (`CHARGES_REAL_WRITE_ENABLED = False`). Aucun fichier métier réel touché.

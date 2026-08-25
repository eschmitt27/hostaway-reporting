# Moteur Commission pur — Mission 7 (2026-08-25)

## Ancienne chaîne

Le calcul commission/net propriétaire vivait inline dans `lot10_calculer_resultats.py::
build_commissions`, dupliqué **littéralement 3 fois** (une par branche de routage HOSTAWAY/HH/
VRBO) :

```python
df["commission_conciergerie"] = (df["assiette_commission"] * df["taux_commission"]).round(2)
df["net_proprietaire"] = (df["payout_calcule"] - df["menage_retenu"] - df["commission_conciergerie"]).round(2)
```

Chaque branche avait déjà résolu son assiette et son taux (par date économique, `resolve_
commission_rate`/`resolve_regle_version`, Mission 6 ter — inchangés) avant d'appliquer cette
formule, mais la formule elle-même n'était nulle part centralisée.

## Nouvelle chaîne

`02_TRAVAIL/lib_commission_engine.py` (nouveau, même famille que `lib_canape.py`,
`lib_settlements.py` — bibliothèques pures partagées par les scripts Lot*, distinctes de
`05_APPLICATION/app` par construction du projet : deux interpréteurs Python séparés, cf.
`lib_db_moteur.py` en tête de fichier, « lier les deux environnements » explicitement évité).

Deux fonctions pures, sans import pandas/sqlite3/fastapi :

```python
def calculer_commission_conciergerie(assiette, taux):
    return round(assiette * taux, 2)

def calculer_net_proprietaire(payout, menage, commission):
    return round(payout - menage - commission, 2)
```

Génériques par duck-typing (`round()` délègue à `Series.__round__`) : fonctionnent identiquement
sur des scalaires Python et des `pandas.Series` — les 3 branches de `build_commissions` continuent
de les appeler de façon vectorisée, sans boucle Python ni changement de performance.

## Ce qui reste dans Lot10 (pas dans le moteur)

- résolution du taux par date économique (`resolve_commission_rate`, inchangé) ;
- résolution/vérification de la version ASSIETTE_COMMISSION (`resolve_regle_version` +
  `_verifier_version_regle`, fail-closed, inchangé) ;
- **dérivation de l'assiette par branche** : HOSTAWAY accepte l'assiette fournie par le payout
  amont (Lot1/lot4quater) telle quelle ; HH calcule `total_percu - menage` ; VRBO calcule
  `assiette_resolu` ou, à défaut, `payout_resolu - menage_resolu`. Ce sont des étapes de
  **préparation de données** hétérogènes par nature de source (colonnes différentes selon le
  canal), pas la formule de commission elle-même — laissées dans Lot10 (étape « construction des
  inputs », pas le moteur), pour ne pas fabriquer un contrat artificiel unique qui masquerait ces
  différences réelles de source. Documenté ici plutôt qu'inventé silencieusement.

## Pourquoi deux formules de `net_proprietaire` produisent des valeurs différentes selon la branche

`calculer_net_proprietaire` est UNE seule fonction, mais les 3 branches ne lui passent pas les
mêmes valeurs source :

- HOSTAWAY/VRBO : `payout_calcule`/`menage_retenu` déjà arrondis au centime avant l'appel ;
- HH : `total_percu`/`menage` **bruts** (non arrondis) — comportement préexistant, préservé tel
  quel par cette extraction (pas une régression introduite ici, pas un bug corrigé non plus —
  hors mandat, refactor pur).

## Taux et assiette par date économique

Inchangé (Mission 6 ter) : une réservation résout toujours sa version/son taux à SA date d'arrivée,
jamais à la date du recalcul. Prouvé par `tests/test_regles_versionnees_production.py` (V2 de
fixture créée pour 2027 ne change jamais un résultat 2026, même rejoué après coup) — mission 7 ne
retouche pas ce mécanisme, seulement la formule finale d'application du taux à l'assiette.

## Fail-closed

Inchangé : `_verifier_version_regle` reste le seul garde-fou de version (registry d'implémentations
via le tuple `("V1",)` passé à chaque appel) — BLOQUANT (`sys.exit(1)`) si une version résolue
n'est pas dans ce tuple. Le moteur pur lui-même ne fait aucune vérification de version — il reçoit
des nombres déjà validés par l'appelant, conformément au principe « moteur calcule, application
constate ».

## Parité

Preuve par construction (même formule, relocalisée, appelée avec les mêmes valeurs) + tests
ciblés :

- `tests/test_commission_engine.py` (10 tests) : moteur pur isolé, sans DB/FastAPI/fichier,
  fonctionne sur scalaires et `pandas.Series`.
- `tests/test_lot10_commission_moteur_pur.py` (4 tests) : `build_commissions` (chaîne de
  production réelle, fixtures HOSTAWAY/HH/VRBO) produit exactement la valeur que donnerait un
  appel direct au moteur pur avec les mêmes assiette/taux/payout/ménage ; preuve qu'aucune formule
  dupliquée ne subsiste dans `lot10_calculer_resultats.py` (recherche littérale de l'ancien
  pattern inline, absente après extraction).
- `tests/test_regles_versionnees_production.py` (préexistant, Mission 6 ter, inchangé) : couvre
  déjà le scénario temporel V1/V2 et la non-régression du passé.

Écart économique : **0,00 €** (formule identique, pas de recalcul sur données réelles jugé
nécessaire — extraction strictement mécanique, prouvée par construction plutôt que rejouée sur
une copie complète du pipeline réel).

## Tests

Nouveaux : 10 (`test_commission_engine.py`) + 4 (`test_lot10_commission_moteur_pur.py`) = 14.
Aucun test existant modifié. Campagne finale : moteur **383 passed / 0 failed** (369 + 14),
application **2758 passed / 0 failed** (10 shards, 187 fichiers, inchangé — Lot10 appelé via
subprocess par certains tests applicatifs, tous verts).

## Migration

Aucune — aucun besoin de stockage nouveau, aucune colonne changée.

## Limites

- Dérivation de l'assiette par branche (HOSTAWAY/HH/VRBO) reste dans Lot10, pas dans un registry
  d'implémentations formel type `ASSIETTE_IMPLEMENTATIONS = {...}` — jugé prématuré : les 3
  branches ont des sources de données réellement différentes (colonnes différentes), pas 3
  variantes interchangeables d'une même formule paramétrée. Si une vraie V2 d'ASSIETTE_COMMISSION
  apparaît un jour, cette dérivation devra être revisitée à ce moment (pas avant, pour ne pas
  inventer une abstraction sans second cas d'usage réel).
- Parité prouvée par construction et tests ciblés, pas rejouée sur une copie complète du pipeline
  réel (Lot9→Lot13) — non jugé nécessaire vu la nature strictement mécanique de l'extraction
  (même formule, mêmes valeurs, relocalisée).

## Prochaine action

Aucune décidée par cette mission. STOP explicite — ne pas commencer le moteur Charges sans
nouvelle mission.

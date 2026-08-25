# Moteur Commission pur — Mission 7 (2026-08-25) + Mission 7 bis (2026-08-25)

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
- **dérivation de l'assiette HOSTAWAY** : accepte telle quelle la valeur fournie par le payout
  amont (Lot1/lot4quater), aucun calcul ici — pur pass-through technique (audité Mission 7 bis,
  voir section suivante).

## Mission 7 bis — audit de l'assiette restante : TECHNIQUE / MÉTIER / MIXTE

Verdict : **MIXTE**.

| Branche | Calcul actuel de l'assiette | Technique ou métier | Où il vit maintenant |
|---|---|---|---|
| HOSTAWAY | accepte la valeur du payout amont (Lot1/lot4quater) telle quelle, aucun calcul | **TECHNIQUE** (pass-through, 0 décision économique prise dans Lot10) | Lot10 (inchangé) |
| HH | `total_percu - menage` | **MÉTIER** (vraie décision : ce qui entre dans l'assiette de commission pour un paiement direct) | `lib_commission_engine.assiette_v1_paiement_direct` (extrait) |
| VRBO | `assiette_resolu` (historique clôturé, lot4quater) si présent, sinon `payout_resolu - menage_resolu` | **MIXTE** : la formule de repli est la même règle métier que HH (extraite, partagée) ; le CHOIX de préférer la valeur historique déjà résolue reste une décision technique de réconciliation de source (laquelle des deux sources fait foi), pas une formule économique distincte | formule dans le moteur ; préférence source dans Lot10 |

`lib_commission_engine.py` gagne une 3e fonction pure :

```python
def assiette_v1_paiement_direct(payout, menage):
    return round(payout - menage, 2)
```

Utilisée à l'identique par HH et par le repli VRBO — c'est la même règle économique réelle
(« assiette = montant perçu directement moins le ménage »), pas une coïncidence de code : elle ne
s'applique qu'aux canaux de PAIEMENT DIRECT (par opposition à HOSTAWAY, où le payout amont a déjà
sa propre logique, hors mandat de cette mission).

Le choix VRBO « valeur historique résolue si présente, sinon calculée » (`combine_first`, NaN-aware
par ligne) reste dans Lot10 : c'est une décision de **quelle source de données fait foi**
(réconciliation technique), pas une deuxième formule économique — la formule elle-même, une fois
choisie, est identique à celle de HH. Pas de registry `ASSIETTE_IMPLEMENTATIONS = {...}` créé pour
une seule vraie implémentation (§6 de la mission) : `assiette_v1_paiement_direct` EST la V1
canonique, appelable directement, sans indirection inutile tant qu'aucune V2 réelle n'existe.

**ASSIETTE_COMMISSION_V1 : CANONIQUE** pour les canaux de paiement direct (HH/VRBO) — la seule
partie de l'assiette qui était une vraie règle économique est maintenant dans le moteur.
HOSTAWAY reste, à raison, hors du moteur (rien à y déplacer : 0 décision).

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
nécessaire pour Mission 7 — extraction strictement mécanique). Mission 7 bis ajoute une preuve A/B
réelle (section suivante), qui confirme ce 0,00 € sur un jeu de données représentatif.

## Mission 7 bis — preuve A/B réelle

`tests/test_ab_moteur_commission.py` (nouveau, 5 tests) : recette **représentative** (fixture, pas
une copie du pipeline réel complet Lot9→Lot13 — §9 de la mission autorise explicitement une
« fixture représentative » comme alternative) — 15 réservations, 3 canaux (5 HOSTAWAY + 5 HH +
5 VRBO), 3 logements, 2 propriétaires, 2 taux de commission différents, montants variés incluant
des cas limites (ménage nul, montants non ronds).

**ANCIEN CALCUL** reconstitué à l'identique dans le test (isolé, jamais réactivé en production —
§9/§15 de la mission), comparé au résultat réel de `build_commissions` (chaîne de production,
moteur pur inclus) :

- lignes ancien : 15 — lignes nouveau : 15 — manquantes : 0 — supplémentaires : 0 ;
- diff assiette : 0 — diff taux : 0 — diff commission : 0 — diff net propriétaire : 0 ;
- agrégats (ce jeu de données précis, pas l'ancien chiffre historique 285/41 602,41 € qui portait
  sur un dataset différent) : assiette totale 2 598,46 €, commission totale 502,11 €, net total
  2 096,35 € — identiques ancien/nouveau ;
- **écart monétaire max : 0,00 €**.

Piège de rounding découvert en écrivant ce test (pas un bug de production) : `round()` builtin
Python et `pandas.Series.round()` peuvent diverger sur une valeur pile à la limite (ex.
`205.5 * 0.19` : builtin → 39.05, `Series.round()` → 39.04, écart de représentation flottante).
Lot10 a TOUJOURS opéré de façon vectorisée (jamais un scalaire nu) — le test reconstitue donc
l'« ancien calcul » via `Series.round()` explicitement, pour comparer au comportement réellement
exécuté historiquement, pas à un chemin scalaire qui n'a jamais tourné en production.

## Tests

Mission 7 : 10 (`test_commission_engine.py`) + 4 (`test_lot10_commission_moteur_pur.py`) = 14.
Mission 7 bis : +4 (`AssietteV1PaiementDirectTests`, mêmes fichiers `test_commission_engine.py`/
`test_lot10_commission_moteur_pur.py` étendus) + 5 (`test_ab_moteur_commission.py`) = 9. Total
nouveaux : 23. Aucun test existant supprimé. Campagne finale : moteur **392 passed / 0 failed**
(369 + 23), application **2758 passed / 0 failed** (10 shards, 187 fichiers, inchangé — Lot10
appelé via subprocess par certains tests applicatifs, tous verts).

## Migration

Aucune — aucun besoin de stockage nouveau, aucune colonne changée.

## Limites

- Registry `ASSIETTE_IMPLEMENTATIONS = {...}` non créé — une seule vraie implémentation
  (`assiette_v1_paiement_direct`) ne justifie pas une indirection supplémentaire (§6 de la
  mission) ; à revisiter si une vraie V2 apparaît.
- Préférence de source VRBO (historique résolu vs calculé) reste dans Lot10, décision de
  réconciliation technique documentée comme telle, pas une deuxième formule économique.
- Preuve A/B sur fixture représentative (15 lignes, 3 canaux, 2 taux) — pas sur une copie complète
  du pipeline réel Lot9→Lot13 (~1391 réservations réelles) : autorisé explicitement par la mission
  (§9), jugé suffisant pour une extraction strictement mécanique déjà prouvée par construction.

## Prochaine action

Aucune décidée par cette mission. STOP explicite — ne pas commencer le moteur Charges sans
nouvelle mission.

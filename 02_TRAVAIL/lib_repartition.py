"""Repartition monetaire canonique — aucun centime ne disparait.

POURQUOI CE MODULE EXISTE
Le depot portait QUATRE facons de repartir un montant, et trois resultats differents :

  · `charges_engine.repartir_egal`        — centimes, residu aux PREMIERS de l'ordre trie ;
  · `lib_charges_menage.ventiler_charge_menage` — centimes, le DERNIER absorbe tout le residu ;
  · `facture_ventilation_menage_service.ventiler` — idem, le dernier absorbe ;
  · `lot6f_cout_complet_menages`          — `round(montant * poids / total, 2)` par ligne,
                                            SANS rattrapage : 100,00 EUR sur 3 poids egaux
                                            ventilaient 99,99 EUR. Un centime n'etait attribue
                                            a personne.

Les trois premieres garantissaient la somme, mais pas de la meme maniere : « le dernier absorbe »
peut ecarter une ligne de plusieurs centimes de sa part exacte des qu'il y a beaucoup de lignes,
alors que « le residu aux plus proches » ne s'ecarte jamais de plus d'un centime. La quatrieme ne
garantissait rien.

Ce module rend UNE reponse, et c'est la seule que les moteurs doivent utiliser.

LA REGLE, EN QUATRE TEMPS
  1. convertir en CENTIMES (entiers) — jamais de flottants intermediaires ;
  2. donner a chacun la partie ENTIERE de sa part exacte ;
  3. distribuer les centimes restants, un par un, aux parts dont le reste fractionnaire est le
     plus grand — ce sont celles que l'arrondi a le plus lesees ;
  4. a reste fractionnaire EGAL, departager par la CLE TRIEE. Deterministe et documente : deux
     executions sur la meme donnee attribuent le centime au meme element, et cet element ne depend
     jamais de l'ordre d'une requete SQL.

Consequence directe du 4 : a poids egaux, tous les restes sont egaux, donc les premieres cles
triees recoivent les centimes. 100,00 EUR sur trois logements rendent bien 33,34 / 33,33 / 33,33.

INVARIANT, VRAI DANS TOUS LES CAS : somme(parts) == montant, au centime exact.

Module PUR : aucune dependance, aucune entree/sortie, aucun etat.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping


def _en_centimes(montant) -> int:
    """Montant euros -> centimes entiers. `round` a mi-chemin suit la convention du depot."""
    return int(round(float(montant or 0) * 100))


def repartir_centimes(total_cents: int, poids: Mapping[str, float]) -> dict[str, int]:
    """Repartit `total_cents` au prorata de `poids`. Rend {cle: centimes}, somme exacte.

    Les poids nuls ou negatifs sont ECARTES : une ligne sans poids ne recoit rien, et ne doit pas
    non plus capter un centime residuel. Si plus aucun poids ne subsiste, rend {} — l'appelant
    decide alors quoi faire, plutot que de se voir imposer une repartition arbitraire.
    """
    retenus = {str(k): float(v) for k, v in (poids or {}).items() if v and float(v) > 0}
    if not retenus or total_cents == 0:
        return {}

    total_poids = sum(retenus.values())
    signe = -1 if total_cents < 0 else 1
    reste_a_placer = abs(total_cents)

    # Etape 2 — part entiere. `int()` tronque, et l'operande est positif : c'est bien un plancher.
    bases: dict[str, int] = {}
    fractions: list[tuple[float, str]] = []
    for cle in sorted(retenus):
        exact = reste_a_placer * retenus[cle] / total_poids
        entier = int(exact)
        bases[cle] = entier
        fractions.append((exact - entier, cle))

    # Etape 3 et 4 — les centimes restants aux plus grands restes ; a egalite, a la cle triee.
    # Le tri est stable et `fractions` est deja construit dans l'ordre des cles : trier sur le
    # seul reste DECROISSANT preserve donc l'ordre des cles a egalite.
    manquants = reste_a_placer - sum(bases.values())
    for _, cle in sorted(fractions, key=lambda x: -x[0])[:manquants]:
        bases[cle] += 1

    return {cle: signe * cents for cle, cents in bases.items()}


def repartir(montant, poids: Mapping[str, float]) -> dict[str, float]:
    """Repartit `montant` (euros) au prorata de `poids`. Rend {cle: euros}, somme exacte."""
    return {cle: cents / 100.0
            for cle, cents in repartir_centimes(_en_centimes(montant), poids).items()}


def repartir_egal(montant, cles: Iterable[str]) -> dict[str, float]:
    """Cas particulier a poids egaux — meme regle, meme resultat, pas une seconde formule."""
    return repartir(montant, {str(c).strip(): 1.0 for c in (cles or []) if str(c).strip()})


def somme(parts) -> float:
    """Somme arrondie au centime. A comparer au montant source : l'ecart doit etre exactement 0."""
    if isinstance(parts, Mapping):
        valeurs: Iterable[Any] = parts.values()
    else:
        valeurs = parts
    return round(sum(float(v or 0) for v in valeurs), 2)

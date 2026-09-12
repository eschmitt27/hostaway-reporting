"""§45 — un numéro de téléphone se stocke sous UNE forme et se lit sous une autre.

LE CONSTAT
Le référentiel portait deux écritures du même genre de numéro, selon la table d'origine :

    ref_proprietaires   0610190367     (national, 10 chiffres, collé)
    ref_intervenants    33775793253    (indicatif pays sans « + », collé)

Aucune des deux ne se lit, et surtout : deux numéros identiques écrits différemment ne se
rapprochent pas. Comparer, dédoublonner ou chercher un correspondant devenait impossible sans
savoir de quelle table venait la valeur.

LA RÈGLE
On STOCKE en E.164 (`+33610190367`) — la forme canonique internationale, sans espace ni
ponctuation, celle qui compare bien. On AFFICHE en groupes de deux (`06 10 19 03 67`), la forme
que l'œil français lit. Le stockage n'est pas l'affichage, et c'est justement ce qui permet aux
deux d'être bons en même temps.

CE QUE CE MODULE NE FAIT JAMAIS
Compléter un numéro incomplet, ni deviner un indicatif pour un numéro qui n'en porte pas et ne
ressemble pas à un numéro français. Un numéro que la règle ne sait pas interpréter est rendu TEL
QUEL, signalé non normalisé : mieux vaut une valeur brute qu'un numéro inventé, qu'on appellerait.
"""
from __future__ import annotations

import re
from typing import Any

#: Indicatif du pays par défaut. La quasi-totalité des correspondants (propriétaires, intervenants,
#: prestataires) sont français ; un numéro déjà en forme internationale n'est jamais réinterprété.
INDICATIF_DEFAUT = "33"

#: Longueur d'un numéro national français, indicatif pays exclu : 9 chiffres après le 0 initial.
_LONGUEUR_NATIONALE_FR = 9

_NON_CHIFFRE = re.compile(r"[^\d+]")


def normaliser(brut: Any) -> dict[str, Any]:
    """`0610190367` ou `33 7 75 79 32 53` → forme canonique + forme lisible. FONCTION PURE.

    Retourne `{e164, affichage, valide, brut}`. `valide` dit seulement si la règle a su
    interpréter la valeur — ce n'est pas une vérification d'existence de la ligne téléphonique.
    """
    texte = "" if brut is None else str(brut).strip()
    if not texte:
        return {"e164": "", "affichage": "", "valide": False, "brut": texte}

    # `00` en tête est la forme internationale composée depuis la France : c'est un « + ».
    nettoye = _NON_CHIFFRE.sub("", texte.replace("(0)", ""))
    if nettoye.startswith("00"):
        nettoye = "+" + nettoye[2:]
    chiffres = nettoye.lstrip("+")

    if nettoye.startswith("+"):
        national = _national_francais(chiffres)
    elif chiffres.startswith("0") and len(chiffres) == _LONGUEUR_NATIONALE_FR + 1:
        national = chiffres[1:]
    elif chiffres.startswith(INDICATIF_DEFAUT) and \
            len(chiffres) == len(INDICATIF_DEFAUT) + _LONGUEUR_NATIONALE_FR:
        # `33775793253` : l'indicatif pays écrit sans « + ». C'est la forme du référentiel des
        # intervenants — interprétable sans ambiguïté, puisqu'un national français fait 10 chiffres
        # et commence par 0.
        national = chiffres[len(INDICATIF_DEFAUT):]
    else:
        national = ""

    if not national or len(national) != _LONGUEUR_NATIONALE_FR or not national.isdigit():
        # Numéro étranger, incomplet ou illisible : on ne l'invente pas. Un numéro déjà
        # international garde une forme E.164 débarrassée de sa ponctuation — il n'est pas
        # français, donc pas formatable en paires, mais il reste comparable.
        return {"e164": ("+" + chiffres) if nettoye.startswith("+") and chiffres.isdigit() else "",
                "affichage": texte, "valide": False, "brut": texte}

    return {
        "e164": f"+{INDICATIF_DEFAUT}{national}",
        "affichage": formater(national),
        "valide": True,
        "brut": texte,
    }


def _national_francais(chiffres: str) -> str:
    """Partie nationale d'un numéro déjà international. Vide si ce n'est pas un numéro français."""
    if chiffres.startswith(INDICATIF_DEFAUT):
        return chiffres[len(INDICATIF_DEFAUT):]
    return ""


def formater(national: str) -> str:
    """`610190367` → `06 10 19 03 67`. Groupes de deux, comme on les dicte et comme on les lit."""
    complet = "0" + str(national)
    return " ".join(complet[i:i + 2] for i in range(0, len(complet), 2))


def afficher(brut: Any) -> str:
    """Forme lisible d'un numéro stocké. Une valeur non interprétable est rendue telle quelle.

    C'est le point d'entrée des gabarits : `{{ p.telephone|telephone }}`.
    """
    return normaliser(brut)["affichage"]


def e164(brut: Any) -> str:
    """Forme de stockage. Vide si la règle n'a pas su interpréter la valeur — l'appelant décide
    alors de conserver le brut plutôt que d'enregistrer une valeur fausse."""
    return normaliser(brut)["e164"]

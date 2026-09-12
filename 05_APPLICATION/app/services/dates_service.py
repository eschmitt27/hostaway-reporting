"""§76 — une colonne de date ne contient qu'une seule forme de date : ISO `AAAA-MM-JJ`.

LE CONSTAT
Un balayage de toutes les colonnes de date de la base réelle a trouvé QUATRE valeurs écrites à la
française, dans trois tables :

    ecritures.date_ecriture              11/09/2026
    factures_proprietaires.date_facture  11/09/2026
    imputations_airbnb.date_imputation   15/08/2026, 12/08/2026

Ce n'est pas un détail d'affichage. `'11/09/2026' < '2026-09-12'` est VRAI en SQLite : une date
française se trie avant toutes les dates ISO, quelle que soit l'année. Un filtre « depuis le
1er septembre », un tri chronologique, un calcul d'ancienneté, un rapprochement par date : tout
se trompe silencieusement sur ces lignes, et rien ne le signale.

LA CAUSE
Des champs de saisie en texte libre portant « AAAA-MM-JJ » comme simple indication — ils
acceptaient tout. Ils sont désormais des sélecteurs de calendrier (§40), ce qui ferme la porte
côté écran. Ce module ferme l'autre : les chemins programmatiques, les imports, les reprises de
données.

CE QUE CE MODULE NE FAIT JAMAIS
Deviner un ordre ambigu. `03/04/2026` peut être le 3 avril ou le 4 mars ; seule la forme
française (jour d'abord) est reconnue, parce que c'est celle que produit cette application. Une
valeur qu'on ne sait pas lire est REFUSÉE, jamais réinterprétée au jugé.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any

_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_FRANCAIS = re.compile(r"^(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})$")
_COMPACT = re.compile(r"^(\d{4})(\d{2})(\d{2})$")


def normaliser(valeur: Any) -> str:
    """Rend la date en ISO `AAAA-MM-JJ`, ou une chaîne vide si la valeur n'est pas une date.

    Accepte : ISO (éventuellement suivi d'une heure), français `JJ/MM/AAAA` (avec `/`, `.` ou `-`),
    compact `AAAAMMJJ`, et un objet `date`. Tout le reste ressort vide — à l'appelant de refuser,
    plutôt qu'à ce module d'inventer.
    """
    if isinstance(valeur, date):
        return valeur.isoformat()
    texte = "" if valeur is None else str(valeur).strip()
    if not texte:
        return ""

    m = _ISO.match(texte)
    if m:
        return _valide(m.group(1), m.group(2), m.group(3))
    m = _FRANCAIS.match(texte)
    if m:
        return _valide(m.group(3), m.group(2), m.group(1))
    m = _COMPACT.match(texte)
    if m:
        return _valide(m.group(1), m.group(2), m.group(3))
    return ""


def _valide(annee: str, mois: str, jour: str) -> str:
    """Une date grammaticalement bien formée mais impossible (31/02) n'est pas une date."""
    try:
        return date(int(annee), int(mois), int(jour)).isoformat()
    except ValueError:
        return ""


def est_iso(valeur: Any) -> bool:
    """Vrai si la valeur est DÉJÀ dans la forme de stockage. Sert aux contrôles d'intégrité."""
    texte = "" if valeur is None else str(valeur).strip()
    return bool(texte) and bool(_ISO.match(texte)) and normaliser(texte) == texte[:10]


def exiger(valeur: Any, *, champ: str = "date") -> str:
    """Normalise, ou lève. À employer au moment d'ÉCRIRE en base.

    Lever plutôt que stocker une valeur douteuse : une date fausse en base ne se voit pas, et
    contamine ensuite tous les tris et filtres qui la touchent.
    """
    iso = normaliser(valeur)
    if not iso:
        raise ValueError(f"{champ} : « {valeur} » n'est pas une date exploitable "
                         f"(attendu AAAA-MM-JJ ou JJ/MM/AAAA)")
    return iso


def mois(valeur: Any) -> str:
    """`2026-09-12` → `2026-09`. Vide si la valeur n'est pas une date."""
    iso = normaliser(valeur)
    return iso[:7] if iso else ""

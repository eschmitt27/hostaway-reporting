"""§102 — aucun écran métier n'affiche un identifiant technique quand un libellé humain existe.

RÈGLE (recette utilisateur n°3, règle finale) : `PROP_0008`, `LOG_0005`, `INT_0003`, `CANAL_003`,
`TYPE_001`, `PAY_001`, `CHG_008`, `MTP-…` n'ont rien à faire dans un texte lu par l'utilisateur.
Les identifiants restent partout où ils servent — base, jointures, logs, observabilité, `value`
HTML des formulaires, écrans d'Administration des référentiels — mais pas comme LIBELLÉ.

CE QUE CE TEST REGARDE, ET POURQUOI SEULEMENT ÇA
Le texte VISIBLE, c'est-à-dire le HTML privé de ses balises, de ses attributs et de ses scripts.
Un `value="LOG_0005"` dans un `<option>` est légitime : c'est la valeur transmise au serveur, pas
ce que l'utilisateur lit. Chercher la chaîne dans le HTML brut ferait échouer le test sur du code
correct, et pousserait à « corriger » ce qui ne doit pas l'être.

Les écrans d'Administration sont exclus : ils existent précisément pour manipuler les
référentiels, identifiants compris.
"""
from __future__ import annotations

import re

import pytest

#: Familles d'identifiants techniques du projet.
MOTIFS = re.compile(
    r"\b(PROP|LOG|INT|CANAL|TYPE|PAY|CHG|ASSOC|MTP|FOUR|TLM|SRC|REC|IMP)[_-]\d{3,}\b")

#: Écrans métier réellement parcourus par l'utilisateur. Administration et observabilité en sont
#: absents : y montrer les identifiants est leur raison d'être.
ECRANS = [
    "/",
    "/logements",
    "/reservations",
    "/menages",
    "/charges-controle",
    "/fournisseurs",
    "/factures",
    "/factures-proprietaires",
    "/creances",
    "/comptes-proprietaires",
    "/proprietaires",
    "/comptabilite",
    "/clotures",
]


def _texte_visible(html: str) -> str:
    """Le HTML réduit à ce que l'œil lit : ni balises, ni attributs, ni script, ni style."""
    sans_script = re.sub(r"<(script|style)\b.*?</\1>", " ", html, flags=re.S | re.I)
    sans_commentaire = re.sub(r"<!--.*?-->", " ", sans_script, flags=re.S)
    sans_balise = re.sub(r"<[^>]+>", " ", sans_commentaire)
    return re.sub(r"\s+", " ", sans_balise)


@pytest.mark.parametrize("route", ECRANS)
def test_aucun_identifiant_technique_visible(client, route):
    reponse = client.get(route)
    if reponse.status_code in (301, 302, 303, 307, 308):
        pytest.skip(f"{route} redirige ({reponse.status_code}) — écran couvert ailleurs")
    assert reponse.status_code == 200, f"{route} → {reponse.status_code}"

    trouves = sorted(set(MOTIFS.findall(_texte_visible(reponse.text))))
    # `findall` avec un groupe ne rend que le préfixe : on re-cherche en entier pour le message.
    if trouves:
        complets = sorted(set(m.group(0) for m in MOTIFS.finditer(_texte_visible(reponse.text))))
        raise AssertionError(
            f"{route} affiche des identifiants techniques au lieu de libellés : {complets}")

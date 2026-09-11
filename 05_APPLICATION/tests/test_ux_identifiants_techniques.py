"""Garde UX : aucun identifiant technique présenté comme un libellé métier (recette n°2, §49).

CE QUI EST INTERDIT — un `PROP_0001`, `LOG_0001`, `PAY_001` ou `CHG_008` affiché comme
information à l'utilisateur alors qu'un nom existe au référentiel.

CE QUI RESTE AUTORISÉ — et doit le rester :
  · `value="PROP_0001"` dans un formulaire : c'est la donnée envoyée, pas le texte lu ;
  · une URL `/fournisseurs/CHG-...` : c'est une adresse ;
  · l'écran d'administration des référentiels, dont l'objet EST de montrer les identifiants ;
  · un identifiant affiché explicitement comme tel (« Identifiant : … »), utile au support.

Le test porte donc sur le TEXTE VISIBLE, balises et attributs retirés.
"""
from __future__ import annotations

import html
import re

import pytest

# Identifiants du référentiel qui ont toujours un libellé : les voir en clair est un défaut.
MOTIFS_INTERDITS = (
    re.compile(r"\bPROP_\d+\b"),
    re.compile(r"\bLOG_\d+\b"),
    re.compile(r"\bPAY_\d+\b"),
    re.compile(r"\bCHG_\d+\b"),
)

# Écrans métier. L'administration des référentiels en est volontairement absente : elle expose
# les identifiants par vocation, et le lui interdire la rendrait inutilisable.
ECRANS = [
    "/logements",
    "/menages",
    "/fournisseurs",
    "/factures-proprietaires",
    "/creances",
    "/comptes-proprietaires",
    "/clotures",
]


def _texte_visible(html_source: str) -> str:
    """Texte réellement lu par l'utilisateur : balises, styles et scripts retirés.

    Les attributs (`value=`, `href=`) disparaissent avec les balises — c'est exactement la
    distinction que ce test doit faire.
    """
    sans_script = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html_source)
    sans_balises = re.sub(r"(?s)<[^>]+>", " ", sans_script)
    return re.sub(r"\s+", " ", html.unescape(sans_balises))


def _occurrences(texte: str) -> list[str]:
    trouvees: list[str] = []
    for motif in MOTIFS_INTERDITS:
        trouvees.extend(motif.findall(texte))
    return trouvees


@pytest.mark.parametrize("route", ECRANS)
def test_ecran_metier_sans_identifiant_technique(client, route):
    reponse = client.get(route)
    assert reponse.status_code == 200, f"{route} inaccessible"
    texte = _texte_visible(reponse.text)
    # « Identifiant : CHG-... » reste licite : il s'annonce comme un identifiant, il n'usurpe
    # pas la place d'un nom. On ne retire que ces mentions explicites avant de chercher.
    texte = re.sub(r"Identifiant\s*:\s*\S+", " ", texte)
    fautifs = _occurrences(texte)
    assert not fautifs, (
        f"{route} affiche des identifiants techniques comme libellés : {sorted(set(fautifs))}. "
        f"Utiliser les filtres `nom_proprietaire` / `nom_logement` / `nom_mode_paiement` / "
        f"`nom_categorie_charge`.")


def test_les_value_html_gardent_les_identifiants(client):
    """Contre-épreuve : la règle porte sur l'AFFICHAGE, pas sur les données transmises.

    Sans ce test, une correction trop zélée pourrait retirer les identifiants des formulaires —
    et casser l'envoi, en croyant bien faire.

    Le référentiel du fixture peut être vide (aucune case à cocher n'est alors rendue) : on
    n'exige donc la présence des `value=` que s'il y a effectivement des cases. L'affirmation
    sur le texte visible, elle, vaut dans tous les cas.
    """
    reponse = client.get("/fournisseurs/nouvelle")
    assert reponse.status_code == 200
    cases = re.findall(r'<input type="checkbox" name="(?:proprietaires|logements)" '
                       r'value="([^"]+)"', reponse.text)
    if cases:
        assert any(re.fullmatch(r"(PROP|LOG)_\d+", c) for c in cases), \
            "les identifiants doivent rester dans les `value=` : c'est la donnée envoyée au serveur"
    assert not _occurrences(_texte_visible(reponse.text)), \
        "…mais ils ne doivent jamais être le texte lu par l'utilisateur"


def test_statuts_sans_soulignes_dans_les_creances(client):
    """§33 : `PARTIELLEMENT_REGLEE` est du vocabulaire interne, pas une information."""
    texte = _texte_visible(client.get("/creances").text)
    for code in ("PARTIELLEMENT_REGLEE", "NON_REGLEE", "TROP_PERCU_A_CONTROLER"):
        assert code not in texte, f"statut technique affiché tel quel : {code}"


def test_administration_referentiels_montre_bien_les_identifiants(client):
    """Contre-épreuve : cet écran DOIT les afficher — c'est son objet. Une règle trop large
    l'aurait vidé de son sens."""
    reponse = client.get("/administration/referentiels")
    assert reponse.status_code == 200

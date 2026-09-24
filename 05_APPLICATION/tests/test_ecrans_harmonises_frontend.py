"""Écrans harmonisés (Créances & dettes, Règlements, Relevé, Comptes propriétaires, Résultats,
Comptabilité, fiche Logement) — refonte de présentation uniquement.

Ce qui ne doit pas bouger, et que ces tests verrouillent :
  · les paramètres envoyés par chaque barre de filtres (mêmes `name`, même méthode GET) ;
  · le confinement des styles : `ecrans.css` ne peut toucher que les écrans qui portent `.ecran`.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

CSS = Path(__file__).resolve().parent.parent / "app" / "static" / "css" / "ecrans.css"

# Noms des champs de filtre AVANT la refonte, relevés dans les anciens gabarits.
FILTRES = {
    "/creances": {"proprietaire", "logement", "mois", "statut", "echues"},
    "/dettes": {"fournisseur", "statut", "echues"},
    "/comptabilite/balance": {"periode_debut", "periode_fin", "journal"},
    "/resultats": {"mois", "vision"},
    "/releves-proprietaires": {"mois", "proprietaire_id"},
}

ECRANS = ["/creances", "/dettes", "/echeancier", "/comptabilite", "/comptabilite/balance",
          "/resultats", "/releves-proprietaires", "/comptes-proprietaires"]


@pytest.mark.parametrize("url", ECRANS)
def test_ecran_harmonise_repond_et_porte_le_conteneur(client, url):
    r = client.get(url)
    assert r.status_code == 200, url
    page = BeautifulSoup(r.text, "html.parser")
    assert page.select_one(".ecran") is not None, f"{url} : conteneur `.ecran` absent"
    assert page.select_one('link[href="/static/css/ecrans.css"]') is not None


@pytest.mark.parametrize("url,attendus", sorted(FILTRES.items()))
def test_filtres_envoient_les_memes_parametres(client, url, attendus):
    page = BeautifulSoup(client.get(url).text, "html.parser")
    formulaires = [f for f in page.select("form") if (f.get("method") or "get").lower() == "get"
                   and f.select("[name]")]
    assert formulaires, f"{url} : barre de filtres absente"
    noms = {c["name"] for f in formulaires for c in f.select("input[name], select[name]")}
    assert noms == attendus, f"{url} : {sorted(noms ^ attendus)}"


def test_sous_navigation_creances_dettes(client):
    for url in ("/creances", "/dettes", "/echeancier"):
        page = BeautifulSoup(client.get(url).text, "html.parser")
        liens = {a["href"] for a in page.select(".ec-onglets a")}
        assert liens == {"/creances", "/dettes", "/echeancier"}
        assert page.select_one(f'.ec-onglets a[href="{url}"]')["aria-current"] == "page"


def test_styles_confines_aux_ecrans_harmonises():
    source = re.sub(r"/\*.*?\*/", "", CSS.read_text(encoding="utf-8"), flags=re.S)
    fautifs = []
    for bloc in re.findall(r"([^{}]+)\{", source):
        bloc = bloc.strip()
        if not bloc or bloc.startswith("@"):
            continue
        fautifs += [s.strip() for s in bloc.split(",") if not s.strip().startswith(".ecran")]
    assert not fautifs, f"sélecteurs non confinés : {fautifs}"

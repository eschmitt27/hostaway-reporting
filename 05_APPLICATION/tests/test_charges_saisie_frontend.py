"""Écran « Nouvelle saisie charge » (`/fournisseurs/nouvelle`) — refonte frontend.

La refonte ne touche QUE la présentation. Ces tests verrouillent ce qui ne doit pas bouger :

  · le contrat avec la route : les multi-sélections (propriétaires, logements, ménage) restent des
    cases à cocher natives, aux noms que la route traite comme des listes, aux valeurs = identifiants
    du référentiel. Le composant de recherche ne fait que les cocher ;
  · la page ne montre plus d'interrupteur technique (`CHARGES_REAL_WRITE_ENABLED = …`), sans qu'aucune
    garde d'écriture ne soit retirée (elles vivent côté serveur, non touchées) ;
  · les styles dédiés restent confinés à l'écran, et le script ne pointe que vers des éléments qui
    existent.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

import fixtures_referentiel as fx
from app.routes import fournisseurs as route_fournisseurs

APP_DIR = Path(__file__).resolve().parent.parent / "app"
CSS = APP_DIR / "static" / "css" / "charges_saisie.css"
JS = APP_DIR / "static" / "js" / "charges_saisie.js"

# Champs que `fournisseurs_nouvelle_previsualiser` transporte en LISTES (`MULTI` dans la route).
CHAMPS_MULTIPLES = {"logements", "proprietaires", "menage_intervenants",
                    "menage_logements", "menage_proprietaires"}


@pytest.fixture
def referentiel(tmp_db):
    fx.semer_parc_standard(tmp_db)
    fx.semer_referentiel_charges(tmp_db)
    return tmp_db


def _page(client) -> BeautifulSoup:
    reponse = client.get("/fournisseurs/nouvelle")
    assert reponse.status_code == 200
    return BeautifulSoup(reponse.text, "html.parser")


def test_multiselections_gardent_les_cases_natives_attendues_par_la_route(client, referentiel):
    page = _page(client)
    composants = page.select("[data-multiselect]")
    assert composants, "les listes de propriétaires / logements doivent être des multi-sélections"
    noms = set()
    for composant in composants:
        cases = composant.select('input[type="checkbox"]')
        assert cases, "un composant sans case native n'enverrait rien au serveur"
        noms |= {c["name"] for c in cases}
        # Le libellé visible du composant pointe vers un élément réel (accessibilité).
        assert page.find(id=composant["data-label-id"]) is not None
    assert noms <= CHAMPS_MULTIPLES, f"nom inconnu de la route : {noms - CHAMPS_MULTIPLES}"
    assert {"proprietaires", "logements", "menage_logements", "menage_proprietaires"} <= noms

    valeurs = {c["value"] for c in page.select('[data-multiselect] input[name="proprietaires"]')}
    assert valeurs == {"PROP_A", "PROP_B", "PROP_C"}
    valeurs = {c["value"] for c in page.select('[data-multiselect] input[name="logements"]')}
    assert valeurs == {"LOG_A1"}, "seuls les logements actifs sont proposés, comme avant"


def test_la_route_recoit_les_selections_multiples_en_listes(client, referentiel, monkeypatch):
    """Ce que la page envoie (plusieurs valeurs sous un même nom) arrive en listes au service."""
    recu = {}

    def capture(form_data):
        recu.update(form_data)
        return {"ok": False, "manifest": {"errors": []}}

    monkeypatch.setattr(route_fournisseurs, "previsualiser", capture)
    client.post("/fournisseurs/nouvelle/previsualiser", data={
        "date_charge": "2026-06-15", "montant": "100.00", "categorie_charge_id": "CHG_017",
        "code_impact": "IC", "mode_paiement_id": "PAY_001", "refacturable": "NON",
        "proprietaires": ["PROP_A", "PROP_B"], "logements": ["LOG_A1"],
    })
    assert recu["proprietaires"] == ["PROP_A", "PROP_B"]
    assert recu["logements"] == ["LOG_A1"]
    assert recu["montant"] == "100.00"


def test_une_erreur_ne_perd_pas_les_selections(client, referentiel):
    """Montant manquant : la page revient avec l'erreur ET les propriétaires / logements cochés."""
    reponse = client.post("/fournisseurs/nouvelle/previsualiser", data={
        "date_charge": "2026-06-15", "montant": "", "categorie_charge_id": "CHG_017",
        "code_impact": "IC", "mode_paiement_id": "PAY_001", "refacturable": "NON",
        "proprietaires": ["PROP_A", "PROP_C"], "logements": ["LOG_A1"],
    })
    assert reponse.status_code == 200
    page = BeautifulSoup(reponse.text, "html.parser")
    assert "Le montant est obligatoire." in page.select_one(".alert-error").get_text()
    coches = {(c["name"], c["value"]) for c in page.select('input[type="checkbox"][checked]')}
    assert coches == {("proprietaires", "PROP_A"), ("proprietaires", "PROP_C"), ("logements", "LOG_A1")}


def test_aucun_interrupteur_technique_affiche(client, referentiel):
    texte = _page(client).get_text(" ")
    assert "CHARGES_REAL_WRITE" not in texte
    assert "_ENABLED" not in texte


def test_ressources_dediees_servies_et_referencees(client, referentiel):
    html = client.get("/fournisseurs/nouvelle").text
    for chemin in ("/static/css/charges_saisie.css", "/static/js/charges_saisie.js"):
        assert chemin in html
        assert client.get(chemin).status_code == 200


def test_styles_confines_a_l_ecran():
    """Chaque sélecteur du CSS dédié commence par `.cs-saisie` : rien ne peut déborder sur un autre
    écran de l'application."""
    source = re.sub(r"/\*.*?\*/", "", CSS.read_text(encoding="utf-8"), flags=re.S)
    selecteurs = re.findall(r"([^{}]+)\{", source)
    fautifs = []
    for bloc in selecteurs:
        bloc = bloc.strip()
        if not bloc or bloc.startswith("@"):
            continue
        for selecteur in bloc.split(","):
            if not selecteur.strip().startswith(".cs-saisie"):
                fautifs.append(selecteur.strip())
    assert not fautifs, f"sélecteurs non confinés à l'écran : {fautifs}"


def test_le_script_ne_vise_que_des_elements_presents(client, referentiel):
    page = _page(client)
    ids = set(re.findall(r'\b(?:el|show|hide)\("([A-Za-z_]+)"\)', JS.read_text(encoding="utf-8")))
    assert ids, "le script de l'écran pilote bien des éléments par identifiant"
    manquants = sorted(i for i in ids if page.find(id=i) is None)
    assert not manquants, f"identifiants absents de la page : {manquants}"

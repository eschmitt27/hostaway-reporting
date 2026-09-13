"""Navigation : routes réelles répondent 200, aucun lien cassé dans sidebar.

APP-1 : /logements disponible. APP-2a : /reservations disponible (lecture seule).
APP-2 (ménages) : /menages disponible.
APP-3a : /fournisseurs disponible (lecture seule charges Lot3).
APP-3c : /proprietaires disponible (lecture seule relevés Lot10/Lot12).
Les 2 autres modules restent « À venir ».
"""


ROUTES_DISPONIBLES = ["/", "/logements", "/reservations", "/menages", "/fournisseurs", "/proprietaires",
                      "/sources-calculs",
                      # Intégration : quatre modules désormais disponibles.
                      "/banques-caisse", "/proprietaires-reglements", "/controles-cloture",
                      # APP-5C / APP-5D
                      "/clotures", "/pilotage-mensuel"]
# Chemins volontairement non construits (les modules ont un préfixe complet distinct).
ROUTES_FUTURES_SANS_LIEN = [
    "/banques",
    "/controles",
]


def test_modules_integres_repondent(client):
    """Les quatre modules intégrés répondent (non 404)."""
    for path in ("/menages", "/banques-caisse", "/proprietaires-reglements", "/controles-cloture"):
        assert client.get(path).status_code == 200, f"Module indisponible : {path}"


def test_accueil_200(client):
    r = client.get("/")
    assert r.status_code == 200


def test_sources_calculs_200(client):
    r = client.get("/sources-calculs")
    assert r.status_code == 200


def test_logements_200(client):
    r = client.get("/logements")
    assert r.status_code == 200


def test_reservations_200(client):
    r = client.get("/reservations")
    assert r.status_code == 200


def test_menages_200(client):
    r = client.get("/menages")
    assert r.status_code == 200


def test_health_repond(client):
    r = client.get("/health")
    assert r.status_code in (200, 503)


def test_sidebar_ne_contient_pas_href_futurs(client):
    r = client.get("/")
    assert r.status_code == 200
    for path in ROUTES_FUTURES_SANS_LIEN:
        assert f'href="{path}"' not in r.text, (
            f"Sidebar contient href vers route non construite : {path}"
        )


def test_sidebar_contient_href_logements(client):
    r = client.get("/")
    assert r.status_code == 200
    assert 'href="/logements"' in r.text, "Menu Logements doit être cliquable au Lot APP-1"


def test_sidebar_contient_href_reservations(client):
    r = client.get("/")
    assert r.status_code == 200
    assert 'href="/reservations"' in r.text, "Menu Réservations doit être cliquable au Lot APP-2a"


def test_sidebar_contient_href_menages(client):
    r = client.get("/")
    assert r.status_code == 200
    assert 'href="/menages"' in r.text, "Menu Ménages doit être cliquable au Lot APP-2"


def test_sidebar_contient_href_fournisseurs(client):
    r = client.get("/")
    assert r.status_code == 200
    assert 'href="/fournisseurs"' in r.text, "Menu Fournisseurs doit être cliquable au Lot APP-3a"


def test_sidebar_contient_href_proprietaires(client):
    r = client.get("/")
    assert r.status_code == 200
    # APP-3C : le menu « Propriétaires & règlements » pointe vers l'écran consolidé.
    assert 'href="/proprietaires-reglements"' in r.text, "Menu Propriétaires doit être cliquable"


def test_sidebar_contient_liens_modules_integres(client):
    """Intégration : les modules métier sont joignables depuis la barre latérale.

    Recette utilisateur n°2 (§44-46) : « Contrôles & clôture » n'y figure plus comme entrée
    autonome — son contenu est devenu une section de la page mensuelle de « Clôture mensuelle ».
    """
    r = client.get("/logements")
    assert r.status_code == 200
    for href in ('href="/menages"', 'href="/banques-caisse"',
                 'href="/proprietaires-reglements"', 'href="/clotures"'):
        assert href in r.text, f"Lien de module manquant dans la sidebar : {href}"
    # Banques et Contrôles ne sont plus « à venir » ; aucun badge futur ne doit subsister.
    assert "nav-badge-future" not in r.text
    assert "nav-item--future" not in r.text


def test_racine_redirige_vers_logements(client):
    """Recette utilisateur n°2 (§40) : l'accueil n'apportait plus de valeur — `/` mène directement
    au premier écran de travail. La vue d'accueil reste joignable sur `/accueil`."""
    r = client.get("/", follow_redirects=False)
    assert r.status_code in (302, 303, 307, 308)
    assert r.headers["location"] == "/logements"
    assert client.get("/accueil").status_code == 200


ROUTES_RECETTE_3_107 = ["/", "/health", "/logements", "/reservations", "/menages", "/charges",
                        "/factures-fournisseurs", "/factures-proprietaires", "/creances",
                        "/comptes-proprietaires", "/releves-proprietaires", "/comptabilite",
                        "/caisse", "/administration", "/observabilite/runs"]

ALIAS_VOLONTAIRES = {"/charges": "/fournisseurs", "/factures-fournisseurs": "/factures",
                     "/caisse": "/banques-caisse"}


def test_les_quinze_routes_de_la_recette_3_repondent_ou_redirigent_volontairement(client):
    """Recette utilisateur n°3 §107 — chacune répond, ou redirige vers un écran qui répond.

    `/charges`, `/factures-fournisseurs` et `/caisse` répondaient 404 : ce sont les noms des entrées
    de menu, dont les écrans vivent à une autre adresse. Aucun écran n'est dupliqué : l'adresse
    redirige vers l'écran existant."""
    for route in ROUTES_RECETTE_3_107:
        r = client.get(route, follow_redirects=False)
        assert r.status_code != 404, route
        if route in ALIAS_VOLONTAIRES:
            assert r.status_code == 308 and r.headers["location"] == ALIAS_VOLONTAIRES[route], route
        if r.status_code in (301, 302, 303, 307, 308):
            assert client.get(r.headers["location"]).status_code == 200, route
        else:
            assert r.status_code == 200, route


def test_header_global_sans_periode_sur_toutes_les_pages(client):
    """Élément global (header) : badge Période retiré partout, uniformément."""
    for path in ("/", "/logements", "/reservations", "/menages", "/sources-calculs"):
        r = client.get(path)
        assert r.status_code == 200
        assert "period-badge" not in r.text, f"Badge Période encore présent sur {path}"


def test_routes_futures_renvoient_404(client):
    """Aucune route fantôme enregistrée pour les modules à venir."""
    for path in ROUTES_FUTURES_SANS_LIEN:
        r = client.get(path)
        assert r.status_code == 404, (
            f"Route {path} retourne {r.status_code} au lieu de 404 "
            "(une route fantôme a été créée sans validation humaine)"
        )

"""Navigation : routes réelles répondent 200, aucun lien cassé dans sidebar.

APP-1 : /logements disponible. APP-2a : /reservations disponible (lecture seule).
Les 5 autres modules restent « À venir ».
"""


ROUTES_DISPONIBLES = ["/", "/logements", "/reservations", "/sources-calculs"]
ROUTES_FUTURES_SANS_LIEN = [
    "/proprietaires",
    "/fournisseurs",
    "/banques",
    "/menages",
    "/controles",
]


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


def test_sidebar_contient_badge_avenir(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "nav-badge-future" in r.text, "Badge 'À venir' absent de la sidebar"
    assert "nav-item--future" in r.text, "Classe nav-item--future absente de la sidebar"


def test_header_global_sans_periode_sur_toutes_les_pages(client):
    """Élément global (header) : badge Période retiré partout, uniformément."""
    for path in ("/", "/logements", "/reservations", "/sources-calculs"):
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

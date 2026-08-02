"""Sécurité — bandeau MODE RECETTE ne doit jamais exposer de chemin absolu ni de nom d'utilisateur.

Trouvé en recette globale (mission Banque, 2026-08-02) : `racine : {{ RECETTE_ROOT }}` et
`base : {{ RECETTE_DB }}` injectaient le chemin absolu complet (`C:\\Users\\<utilisateur>\\...`)
dans TOUTE page rendue en RECETTE_MODE — visible dans les rapports/captures partageables.
Corrigé : `app/main.py` n'expose plus que le nom logique de l'environnement (basename), jamais le
chemin complet ni le nom d'utilisateur Windows.
"""
from __future__ import annotations

import app.main as main_module


def test_recette_globals_ne_contiennent_aucun_chemin_absolu():
    g = main_module._recette_globals
    for cle in ("RECETTE_ROOT", "RECETTE_DB"):
        valeur = str(g[cle])
        assert ":\\" not in valeur and ":/" not in valeur, f"{cle} expose un chemin absolu : {valeur!r}"
        assert "\\" not in valeur.replace("/", ""), f"{cle} contient un séparateur de chemin Windows : {valeur!r}"


def test_recette_globals_ne_contiennent_pas_le_nom_utilisateur():
    import getpass
    utilisateur = getpass.getuser()
    g = main_module._recette_globals
    for cle in ("RECETTE_ROOT", "RECETTE_DB"):
        assert utilisateur.lower() not in str(g[cle]).lower(), f"{cle} expose le nom d'utilisateur"


def test_masquage_racine_conserve_uniquement_le_nom_logique(tmp_path):
    """La fonction de masquage transforme n'importe quel chemin absolu en simple nom logique,
    quel que soit l'environnement (pas seulement celui déjà figé dans _recette_globals)."""
    faux_chemin = tmp_path / "RECETTE_GLOBALE_20260101_000000" / "SOURCES_COPIEES"
    masque = main_module._nom_logique(faux_chemin)
    assert str(faux_chemin) not in masque
    assert ":\\" not in masque and ":/" not in masque
    assert masque  # jamais une chaîne vide non plus

"""Alias orthographiques réels — Google Sheet « Suivi ménage » (lot6b).

Exception nominative documentée par **D104** (`DECISIONS_METIER.md`) : la déclarante saisit parfois
« Kira » au lieu de « Kheira ». Isolé ici, hors de `lot6b_m04_menages_internes.py`, pour deux
raisons :

1. ce module n'est **jamais copié** vers `data_recette` par `recette/build_data_recette.py`
   (voir la liste d'exclusion explicite) : un jeu de recette fictif ne le voit jamais, et
   l'import échoue proprement (`ImportError` capturée par le moteur, alias vide) ;
2. la donnée réelle qu'il porte reste isolée dans un seul fichier, clairement nommé et documenté,
   au lieu d'être mêlée à la logique du moteur.

Clé = orthographe brute rencontrée dans la Google Sheet (normalisée par `norm()` au moment de la
consommation). Valeur = nom canonique, résolu ensuite via `REF_Intervenants.nom_normalise` — jamais
un identifiant en dur.
"""

ALIAS_PRENOMS_GOOGLE_SHEET = {
    "kira": "Kheira",
}

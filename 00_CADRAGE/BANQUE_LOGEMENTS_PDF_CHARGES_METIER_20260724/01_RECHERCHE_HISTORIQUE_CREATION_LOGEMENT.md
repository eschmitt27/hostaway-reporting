# 01 — Recherche historique : création de logement

## Méthode

Recherche sur toute la racine `C:\Users\Ewan\OneDrive\Documents\Conciergerie` :
- noms de fichiers (`*logement*form*`, `*create*logement*`, `*nouveau*logement*`…) ;
- contenu HTML/PY/MD (« nouveau logement », « ajouter un logement », « logement_form »…) ;
- dossiers `stitch` / `maquette` / `APPLICATION_LOCALE` ;
- + rappel de l'audit code précédent (13 pointes de branche).

## Élément retrouvé

| Élément retrouvé | Chemin | Type | Fonction réelle ou maquette | Réutilisable |
|---|---|---|---|---|
| Écran « Gestion des Logements » avec bouton **« + Ajouter un logement »** | `00_CADRAGE\APPLICATION_LOCALE\stitch\logements_chouette_patrimoine_uniformis\` (`code.html` + `screen.png`) | **Maquette Stitch** (design statique) | **Maquette uniquement** — bouton présent, mais aucun écran de formulaire de création n'est maquetté, et aucun code ne l'implémente | **Oui, comme référence design** |

La maquette montre la LISTE (colonnes : Nom interne, Adresse, Type de bien [Chalet/Appartement/
Maison], Propriétaire, Statut [Actif/En travaux/Inactif], Réf. Hostaway, Taux commission) + un
bouton « Ajouter un logement ». Le clic ne mène à aucun écran (pas de maquette de formulaire).

## Conclusion

La création de logement a été **intentionnellement prévue au design** (bouton dans la maquette
Stitch) mais **jamais construite** : aucun formulaire maquetté en détail, aucune route
`POST /logements`, aucune fonction `creer_logement`, aucun template — confirmé sur les 13 pointes de
branche (mission précédente) et par la présente recherche documentaire/visuelle.

→ Il faut donc la **construire** (étape 9). La maquette fournit les champs cibles et le style. Rien
à restaurer : il n'y a pas d'implémentation antérieure à récupérer.

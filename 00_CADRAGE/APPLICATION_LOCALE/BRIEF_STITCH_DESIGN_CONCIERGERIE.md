# BRIEF_STITCH_DESIGN_CONCIERGERIE

## 1. Objectif

Concevoir l’interface d’une application locale de pilotage pour une conciergerie immobilière.

L’application doit aider une seule personne à :
- suivre les réservations hors Hostaway ;
- contrôler les ménages ;
- générer et suivre les relevés propriétaires, factures clients et factures fournisseurs ;
- rapprocher la banque et suivre la caisse ;
- anticiper la trésorerie ;
- préparer la comptabilité et clôturer chaque mois ;
- administrer les propriétaires, prestataires, intervenants et logements.

L’interface est en français.

Le design doit être conçu d’abord pour un ordinateur de bureau (largeur de référence : 1440 px). Une version tablette peut être prévue, mais ne doit pas simplifier ou dégrader les flux de bureau.

Il n’y a pas de gestion de comptes, de rôles ni de droits d’accès dans cette version : un seul administrateur utilise l’outil.

---

## 2. Intention visuelle

Créer une application de gestion professionnelle, calme, lisible et efficace.

À rechercher :
- sensation de maîtrise, de fiabilité et d’organisation ;
- style sobre, moderne et haut de gamme ;
- forte hiérarchie de l’information ;
- beaucoup d’air, mais sans gaspiller la place ;
- chiffres, statuts, échéances et anomalies immédiatement repérables ;
- composants cohérents entre tous les écrans ;
- actions principales visibles sans devoir explorer plusieurs niveaux de menus.

À éviter :
- dashboard décoratif ou surchargé ;
- gradients voyants, style gaming ou cartes excessivement arrondies ;
- nombreuses modales empilées ;
- pages avec plus de deux niveaux de navigation ;
- menus techniques mélangés aux opérations quotidiennes ;
- icônes sans libellé ;
- couleurs seules pour signifier un statut ;
- tableaux avec trop de colonnes visibles par défaut.

Direction suggérée :
- fond blanc cassé ou gris très léger ;
- texte anthracite / bleu nuit ;
- une couleur principale sobre, par exemple bleu pétrole ou vert profond ;
- vert réservé aux validations et montants positifs ;
- orange réservé aux échéances, éléments en attente ou avertissements ;
- rouge réservé aux anomalies bloquantes ;
- surfaces et bordures discrètes ;
- typographie sans sérif très lisible, chiffres tabulaires si possible.

---

## 3. Architecture de navigation obligatoire

### Principes

- Barre latérale gauche fixe sur ordinateur.
- Sept menus principaux maximum.
- Deux niveaux de navigation maximum.
- Barre d’en-tête toujours visible avec recherche globale et période active.
- Un seul bouton d’action primaire contextualisé par écran.
- Les actions rares ou risquées sont dans `Actions avancées` de la fiche concernée.
- Les tableaux ouvrent une fiche détaillée au clic.
- Les fiches simples utilisent des onglets internes : `Synthèse`, `Détails`, `Liens`, `Documents`, `Historique`.
- Les documents complexes utilisent une page complète, pas une chaîne de modales.

### Menus

```text
Accueil
Activité
  Réservations hors Hostaway
  Ménages

Facturation & paiements
  Relevés propriétaires
  Factures clients
  Fournisseurs & charges

Banque & trésorerie
  Banque
  Caisse
  Prévisions de trésorerie
  Associés / avances

Pilotage & clôture
  À traiter
  Précomptabilité
  Clôture mensuelle
  Sauvegardes & archives

Référentiels
  Acteurs
  Logements
  Paramètres

Outils & sources
  Hostaway & calculs
  Anomalies source
```

---

## 4. Gabarit global des écrans

### Barre latérale gauche

Contient uniquement les menus et sous-menus.

Doit afficher :
- le nom ou logo de l’application ;
- les 7 domaines ;
- les sous-menus uniquement du domaine actif ;
- badges de quantité uniquement pour les éléments réellement à traiter ;
- un état réduit possible sur écran plus petit.

Ne pas mettre de boutons de création ou d’actions opérationnelles dans la barre latérale.

### En-tête supérieur

Toujours visible.

Contient :
- recherche globale : acteur, logement, réservation, facture, mouvement bancaire ;
- sélecteur de période active : mois ou plage de dates ;
- indicateur discret d’actualisation Hostaway si nécessaire ;
- aucune gestion de profil utilisateur.

La période active doit être visible sur tous les écrans qui dépendent du mois : facturation, trésorerie, clôture, accueil, contrôle des ménages.

### En-tête de contenu

Dans chaque écran :
- fil d’Ariane discret si utile ;
- titre clair ;
- sous-titre ou nombre d’éléments ;
- filtres essentiels ;
- bouton d’action primaire placé à droite.

Exemples :
- `+ Réservation`
- `+ Acteur`
- `+ Logement`
- `+ Facture ou charge`
- `Importer un relevé`
- `Générer les relevés`
- `Préparer la clôture`

---

## 5. Écran Accueil

L’accueil ne doit pas être un cockpit encombré. Il doit permettre de savoir immédiatement quoi traiter.

### Haut de page : 5 indicateurs utiles

1. Solde clients à recevoir
2. Solde fournisseurs à payer
3. Reversements propriétaires à effectuer
4. Solde banque + caisse
5. État de clôture du mois

### Zone centrale : « À traiter »

Liste priorisée de cartes ou lignes compactes :
- factures clients échues ;
- factures fournisseurs à payer ;
- lignes bancaires non classées ;
- écarts de ménage ;
- justificatifs manquants ;
- anomalies Hostaway ou de calcul ;
- blocages de clôture ;
- dossiers AirCover ou incidents si nécessaire.

Chaque ligne doit avoir :
- un libellé clair ;
- un statut textuel ;
- une échéance ou date ;
- le logement / acteur concerné ;
- un bouton ou lien `Ouvrir`.

### Bas de page : actions rapides

- `+ Réservation hors Hostaway`
- `+ Facture fournisseur / charge`
- `Importer un relevé bancaire`
- `Générer les relevés propriétaires`
- `Lancer le matching ménages`
- `Préparer la clôture`

---

## 6. Écrans de listes

Les écrans de listes sont le cœur de l’application.

### Règles

- Filtres essentiels ouverts sous le titre.
- Filtres avancés repliables.
- Colonnes métier priorisées, sans information technique inutile.
- Statut affiché avec texte et couleur.
- Ligne cliquable ouvrant la fiche.
- Recherche dans la liste si nécessaire.
- Tri par défaut pertinent : échéance, anomalie, date récente ou statut.
- État vide clair avec action de création ou d’import.
- État d’erreur clair avec explication et action suivante.
- Sélection multiple seulement lorsqu’elle apporte un vrai gain.

### Colonnes types

#### Réservations hors Hostaway
- Référence
- Logement
- Voyageur
- Dates
- Montant
- Acompte
- Solde
- Statut

#### Ménages
- Date
- Logement
- Intervenant
- Attendu
- Réalisé
- Déclaré
- Facturé
- Écart
- Statut

#### Relevés propriétaires
- Période
- Propriétaire
- Logement
- Montant net
- Reversement
- Statut
- Échéance

#### Factures clients
- Numéro
- Client
- Date
- Échéance
- Total
- Payé
- Solde
- Statut

#### Fournisseurs & charges
- Fournisseur
- Date
- Échéance
- Catégorie
- Logement / ventilation
- Montant
- Paiement
- Statut comptable

#### Banque
- Date
- Libellé
- Montant
- Sens
- Proposition de classement
- Statut de rapprochement

---

## 7. Fiches détaillées

### Fiche acteur

Types possibles :
- propriétaire ;
- intervenant interne ;
- prestataire externe ;
- associé.

La première étape de création doit seulement demander les informations essentielles. Les informations détaillées viennent ensuite dans des onglets.

Exemple d’onglets :
- `Synthèse`
- `Coordonnées`
- `Tarifs et conditions`
- `Logements / liens`
- `Documents`
- `Historique`

Le propriétaire peut être créé sans logement. Son RIB peut être absent à la création, mais doit être signalé comme manquant avant un reversement bancaire.

### Fiche logement

Onglets :
- `Synthèse`
- `Informations`
- `Propriétaire et commission`
- `Ménages et coûts standards`
- `Canaux / Hostaway`
- `Documents`
- `Historique`

Le prix standard de ménage doit être présenté comme une donnée du logement, liée au type de ménage et à une période de validité.

### Fiche réservation hors Hostaway

Présenter un parcours de saisie clair :
1. logement ;
2. dates ;
3. voyageur ;
4. montants ;
5. acompte éventuel ;
6. confirmation.

Préremplir automatiquement le propriétaire et les paramètres du logement après sélection du logement.

### Fiche facture fournisseur / charge

Présenter clairement :
- fournisseur ;
- montant ;
- date et échéance ;
- catégorie ;
- logement ou ventilation ;
- justificatif ;
- paiement bancaire associé ;
- statut comptable.

Une ligne bancaire ne doit jamais être présentée comme une charge automatiquement validée.

---

## 8. Documents complexes

### Relevé propriétaire / préfacture

Écran complet, avec une lecture verticale simple :

1. En-tête : propriétaire, logement, période, statut
2. Résumé financier : revenus, ménages, commission, ajustements, net propriétaire
3. Lignes de réservations
4. Ajustements : AirCover, reversement Airbnb, acomptes, extras, corrections
5. Paiements et reversements
6. Documents et justificatifs
7. Historique

Les ajustements doivent être des lignes structurées, pas du texte libre.

Chaque ajustement affiche :
- type ;
- montant ;
- sens ;
- réservation ou période concernée ;
- source ;
- justificatif ;
- motif ;
- impact réel / comptable.

### Rapprochement ménages

Écran de comparaison clair, idéalement en colonnes :

```text
Attendu | Réalisé Hostaway | Déclaré interne | Facturé externe | Écart | Action
```

Actions possibles :
- corriger une source ;
- relancer le matching ;
- valider ;
- outrepasser avec motif.

Le bouton principal est `Relancer le matching` ou `Corriger`, jamais `Outrepasser`.

### Clôture mensuelle

Écran de synthèse par module :

```text
Réservations
Payouts
Ménages
Relevés propriétaires
Factures fournisseurs
Banque
Caisse
Créances clients
Précomptabilité
```

Pour chaque bloc :
- statut ;
- nombre de blocages ;
- lien vers les éléments ;
- action de résolution.

La clôture doit afficher une progression lisible, sans donner l’illusion que le mois est définitivement propre si des dérogations existent.

---

## 9. États et statuts

Toujours afficher un libellé texte avec la couleur.

Exemples :
- `VALIDE`
- `À CONTRÔLER`
- `À PAYER`
- `PAYÉ`
- `IMPAYÉ`
- `PARTIELLEMENT PAYÉ`
- `PRÊT À CLÔTURER`
- `CLOTURÉ`
- `RÉOUVERT`
- `ARCHIVÉ`
- `BLOQUANT`

Les statuts bloquants doivent être visibles dans :
- la ligne de liste ;
- la fiche ;
- l’écran `À traiter` ;
- l’écran de clôture si la période est concernée.

---

## 10. Actions avancées

Présenter les actions sensibles dans un menu secondaire nommé `Actions avancées`.

Exemples :
- archiver ;
- détacher un rapprochement bancaire ;
- outrepasser un écart ménage ;
- forcer une clôture ;
- réouvrir un mois ;
- restaurer une sauvegarde dans une copie de travail.

Pour chaque action sensible :
- confirmer ;
- demander un motif ;
- rappeler la conséquence ;
- ne jamais la présenter comme action primaire.

---

## 11. Parcours prioritaires à designer

Créer les écrans et prototypes de ces parcours en priorité :

1. Ajouter un propriétaire
2. Ajouter un prestataire ménage externe
3. Créer un logement
4. Saisir une réservation hors Hostaway
5. Créer une facture fournisseur / charge
6. Rapprocher un paiement bancaire
7. Traiter un écart de ménage
8. Préparer un relevé propriétaire
9. Suivre une créance client
10. Voir la trésorerie à venir
11. Clôturer un mois
12. Réouvrir un mois

---

## 12. Livrables demandés à Stitch

Créer un design système et des écrans haute fidélité pour :

- la navigation globale ;
- l’accueil ;
- une liste type ;
- une fiche acteur ;
- une fiche logement ;
- une réservation hors Hostaway ;
- un relevé propriétaire ;
- une facture fournisseur / charge ;
- le rapprochement bancaire ;
- le rapprochement ménages ;
- la prévision de trésorerie ;
- la clôture mensuelle ;
- les états vides, erreurs, éléments bloquants et confirmations d’actions avancées.

Utiliser des données fictives réalistes de conciergerie à Toulouse :
- logements ;
- propriétaires ;
- prestataires ménage ;
- factures ;
- réservations ;
- lignes bancaires ;
- statuts ;
- montants en euros.

Ne pas intégrer de gestion des utilisateurs, de rôles ou de permissions dans le design.

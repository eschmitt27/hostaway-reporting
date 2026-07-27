# 47 — Socle analytique : ce qui est préparé, ce qui ne l'est pas

Le brief est explicite : « ne construis pas encore le tableau de bord analytique complet ». Ce tour
prépare uniquement le **modèle**, sans exploitation.

## Dimensions présentes sur chaque ligne d'écriture

`ecriture_lignes` porte, en plus du compte et de l'auxiliaire :

- `logement_id`
- `proprietaire_id`
- `reservation_id`

Ces colonnes existent depuis la migration `0021` (voir `43_CADRAGE_COMPTABILITE_APPLICATION.md`).
**Elles ne sont renseignées par aucun générateur actuel** : `generer_ecriture_achat` et
`generer_ecriture_banque` ne portent pas de logement/réservation (une facture fournisseur globale
n'est pas toujours rattachable à un logement unique — cf. §12 du cadrage, une facture peut couvrir
plusieurs affectations analytiques, jamais dupliquée en plusieurs charges pour autant).

## Dimensions déclarées par le brief, non couvertes ici

- activité, plateforme, catégorie : aucune colonne dédiée. `compte` (606000 générique) et
  `auxiliaire` couvrent partiellement « catégorie » et « fournisseur/prestataire », sans grain fin.
- réel/comptable/hors-compta : cette classification existe déjà dans Lot9/Lot10
  (`code_impact` IC/HC), **pas répliquée** dans les écritures. Le cadrage assume que la Comptabilité
  applicative ne recalcule pas cette vision — elle appartient à Lot10.

## Pourquoi rien n'est exploité

Un tableau de bord analytique sur des colonnes vides serait soit trompeur (affichage de zéros
invisibles à distinguer d'un vrai zéro), soit nécessiterait d'inventer un rattachement logement/
réservation pour une facture fournisseur qui n'en porte pas nécessairement un de façon univoque.
Aucune décision métier consultée ne tranche cette règle de ventilation — le cadrage le signale
explicitement plutôt que de l'improviser.

## Prochaine étape, si le socle analytique est repris

1. Décider la règle de ventilation d'une facture multi-logements vers les lignes d'écriture (le
   même principe que la ventilation des courses du module Ménages : proportionnelle à un poids
   documenté, jamais une répartition arbitraire).
2. Peupler `logement_id`/`proprietaire_id`/`reservation_id` sur les écritures ACHATS dont la
   charge liée porte déjà cette information (`SAISIE_Charges_Flux.logement_id`).
3. Construire un premier tableau de synthèse par dimension, sur des données réellement peuplées.

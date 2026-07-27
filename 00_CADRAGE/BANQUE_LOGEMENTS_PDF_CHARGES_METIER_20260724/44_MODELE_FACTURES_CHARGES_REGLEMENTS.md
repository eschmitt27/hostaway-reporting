# 44 — Modèle cible Factures / Charges / Règlements (issu de l'audit `42`)

## Ce qui change

Rien dans le comportement existant. Une seule addition, additive et rétrocompatible : la table
`facture_classification` (migration `0020`), qui porte `sens` (`RECUE`/`EMISE`) et `type_facture`
(`FOURNISSEUR`/`PROPRIETAIRE`/`VOYAGEUR`/`AVOIR_RECU`/`AVOIR_EMIS`) pour chaque facture SQLite.

Toutes les factures existantes et futures créées par `factures_service.creer()` sont classées
`RECUE`/`FOURNISSEUR` par défaut — c'est le seul circuit qui produit des factures en SQLite
aujourd'hui.

## Ce qui ne change pas

- Le circuit **propriétaire** reste entièrement porté par `lot12_generer_factures.py` (Excel,
  `FACT_FACTURE_ENTETE`/`FACT_FACTURE_LIGNES`), lu par `/proprietaires-reglements`. Il n'est **pas**
  migré vers SQLite : le recréer dupliquerait un moteur qui calcule déjà correctement commissions,
  forfaits, refacturations et compensations.
- Le circuit **voyageur/tiers** reste non construit — aucun besoin rencontré à ce jour.
- Facture, charge, dette/créance, règlement, mouvement bancaire restent des objets distincts,
  comme avant : cette migration ne change aucune règle de séparation, elle ajoute une étiquette.

## Statuts de facture — inchangés, déjà conformes au brief

`BROUILLON`, `A_CONTROLER`, `VALIDEE`, `PARTIELLEMENT_REGLEE`, `REGLEE`, `ANNULEE`, `LITIGE` — le
brief demande aussi `AVOIR` comme statut ; l'audit constate qu'« avoir » est aujourd'hui un **moyen
de règlement** (`reglements_fournisseurs.moyen = 'AVOIR'`), pas un statut de facture. Les deux
lectures coexistent sans conflit : un avoir reçu peut être classé `type_facture=AVOIR_RECU` (colonne
ajoutée ici) tout en gardant un statut de cycle de vie classique.

## Contrôles — état après audit

Voir le tableau détaillé dans `42`. Résumé : **9 des 13 contrôles demandés par le brief §14 étaient
déjà couverts**, 3 sont structurellement impossibles par construction (index uniques), 1 reste hors
périmètre SQLite (créance propriétaire, portée par l'Excel). Aucun code de contrôle n'a dû être
ajouté ce tour — l'audit a confirmé la solidité du modèle plutôt que d'y trouver des trous.

## Ce qui reste à faire, si le circuit propriétaire devait un jour entrer en SQLite

Non entrepris ce tour, noté pour mémoire : il faudrait un objet `facture_id_opaque` de type
`PROPRIETAIRE` référençant une ligne de `FACT_FACTURE_ENTETE`, sans dupliquer le calcul — un pont en
lecture seule, sur le modèle de `factures_banque_service.py`. Décision à prendre le jour où un besoin
réel se présente (ex. rapprochement bancaire du reversement propriétaire depuis l'application).

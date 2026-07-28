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

## Lignes de facture — multi-charges / multi-logements (migration `0022`, 2026-07-28)

Gap comblé, identifié dans `48_ROADMAP_RESTANTE_PROJET.md` : `factures.charge_id` (0017) impose
une charge unique par facture (index unique). Ajout **additif** : table `facture_lignes`
(`facture_id_opaque`, `charge_id`, `logement_id`, `montant_ht`/`tva`/`ttc`, `commentaire`), une ligne
par charge, avec la même règle qu'avant — une charge n'est jamais rattachée deux fois (index unique
sur `charge_id`).

Décision explicite (utilisateur) : `charge_id` reste sur `factures` pour le cas mono-charge
historique ; les nouvelles factures qui couvrent plusieurs charges/logements utilisent
`facture_lignes`. Les deux mécanismes sont mutuellement exclusifs par facture (contrôlé dans
`factures_service.ajouter_ligne` et `lier_charge`, code `E04_FACTURE_DEJA_MONO_CHARGE`).

Ne change rien au reste : `facture_lignes` ne crée jamais de charge, comme `lier_charge`. Le solde
et le montant réglé restent calculés depuis `montant_ttc` de la facture (inchangé) ; le total des
lignes est affiché à titre informatif (alerte non bloquante si écart avec `montant_ttc`).

Portée volontairement restreinte (décision utilisateur) : factures propriétaires émises et factures
tiers restent hors périmètre de ce tour, la décision de ne pas migrer lot12 vers SQLite (ci-dessus)
n'est pas remise en cause.

## Ce qui reste à faire, si le circuit propriétaire devait un jour entrer en SQLite

Non entrepris ce tour, noté pour mémoire : il faudrait un objet `facture_id_opaque` de type
`PROPRIETAIRE` référençant une ligne de `FACT_FACTURE_ENTETE`, sans dupliquer le calcul — un pont en
lecture seule, sur le modèle de `factures_banque_service.py`. Décision à prendre le jour où un besoin
réel se présente (ex. rapprochement bancaire du reversement propriétaire depuis l'application).

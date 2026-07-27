# 43 — Cadrage : premier socle Comptabilité applicatif

Mission 3. **Première verticale, pas le module complet.** N'est pas construit ce tour : analytique
complet, écrans Résultats, clôture comptable, TVA, journal VENTES/CAISSE/OD.

## Objectif

La Comptabilité applicative n'est **pas** une copie de Lot10. Lot10 calcule un résultat économique
(REEL/COMPTABLE/HORS_COMPTA) depuis les flux Excel — c'est un moteur de gestion. La Comptabilité
applicative est une **couche d'écritures en partie double**, dérivée des objets déjà existants
(facture, règlement, rapprochement bancaire), qui ne recalcule aucun résultat de gestion : elle
trace des débits/crédits équilibrés, avec origine, pour produire à terme des journaux légaux.

## Objets

| Objet | Rôle |
|---|---|
| Compte | ligne du plan comptable, configurable, jamais figé définitivement |
| Journal | classement des écritures (ACHATS, VENTES, BANQUE, CAISSE, OD) |
| Écriture | un événement comptable équilibré (débit total = crédit total) |
| Ligne d'écriture | une ligne de l'écriture : compte, auxiliaire éventuel, débit XOR crédit, dimensions analytiques |
| Auxiliaire | sous-compte nominatif (fournisseur, propriétaire, associé) |
| Pièce | référence de la pièce justificative (ex. `facture_id_opaque`) |
| Origine | l'objet métier qui a déclenché l'écriture (facture, règlement, mouvement bancaire) |

## Journaux (minimum du brief)

`ACHATS`, `VENTES`, `BANQUE`, `CAISSE`, `ODIVERSES` (opérations diverses).

**Ce tour construit uniquement ACHATS et BANQUE** — les deux journaux exigés par la verticale de
recette obligatoire (§22 du brief). VENTES, CAISSE, ODIVERSES sont déclarés (contrainte de
validation sur `ecritures.journal`) mais aucune génération n'est câblée pour eux.

## Statuts d'écriture

`PROPOSEE` → `VALIDEE` → `CONTREPASSEE`. Aucune suppression : une écriture erronée se contrepasse
(écriture miroir, débit/crédit inversés, origine = l'écriture annulée), jamais réécrite ni effacée.

## Plan de comptes — configurable, non figé

Seed minimal, décision **provisoire** (à valider métier avant tout usage réel) :

| Compte | Libellé | Type | Auxiliaire |
|---|---|---|---|
| 401000 | Fournisseurs | PASSIF | OUI |
| 411000 | Propriétaires (créance/compensation) | ACTIF | OUI |
| 512000 | Banque | ACTIF | NON |
| 606000 | Achats et charges externes (générique) | CHARGE | NON |

`606000` est un compte **générique par défaut** : le mapping catégorie de charge → compte fin n'est
pas arbitré (aucune décision métier consultée ne fixe un plan comptable définitif). Documenté comme
tel, pas présenté comme validé.

## Écritures proposées — règles

- générées à partir d'une facture fournisseur **validée** (ACHATS) ou d'un rapprochement bancaire
  **confirmé** (BANQUE) ;
- équilibre débit/crédit imposé en code, jamais laissé à la saisie ;
- origine conservée (`origine_type`, `origine_id_opaque`) ;
- **idempotence** : une contrainte unique sur `(journal, origine_type, origine_id_opaque)` interdit
  la double génération — regénérer pour la même origine est un no-op explicite, jamais un doublon ;
- validation explicite requise avant que l'écriture compte dans un solde de compte ;
- annulation uniquement par contrepassation tracée.

## Dimensions analytiques — préparées, non exploitées

Chaque ligne d'écriture peut porter `logement_id`, `proprietaire_id`, `reservation_id`,
`fournisseur_id_opaque`. Aucun tableau de bord analytique n'est construit sur ces colonnes ce tour
— elles ne servent qu'à ce que Mission 4 n'ait pas à changer le schéma.

## Ce que ce tour NE construit PAS

- écrans Résultats ;
- clôture comptable (distincte de la clôture applicative déjà existante du pilotage des calculs) ;
- contrôle TVA (aucune TVA n'est gérée par les factures fournisseurs actuelles au-delà du montant
  saisi) ;
- génération VENTES/CAISSE/ODIVERSES ;
- écran auxiliaires détaillé (une liste minimale seulement).

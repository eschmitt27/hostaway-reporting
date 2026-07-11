# Validation locale guestCount sans API

Date: 2026-06-30

## Perimetre

Aucun appel API Hostaway n'a ete relance pour cette validation.

La validation repose uniquement sur des tests locaux avec payloads Hostaway simules, conformes a la structure observee pendant l'audit API:

- `numberOfGuests` present dans la reponse liste reservation;
- `numberOfGuests` present dans la reponse detail reservation;
- champs Lot1 `numberOfGuests`, `guestCount`, `source_guestCount`, `controle_guestCount`, `code_controle_guestCount`;
- classeurs temporaires isoles produits par les tests pour Lot4bis, Lot4quater, Lot10, Lot11 et Lot12.

Aucun `MASTER_*` du projet source n'a ete regenere.

## Hierarchie validee

1. `numberOfGuests` liste API -> `guestCount`, `source_guestCount = API_LIST`.
2. Liste absente, detail API present -> `guestCount`, `source_guestCount = API_DETAIL`.
3. Liste et detail identiques -> `API_LIST` prioritaire.
4. Liste et detail contradictoires -> `guestCount` vide, `source_guestCount = CONFLIT_API`, code `GUEST_COUNT_CONFLICT_API_LIST_DETAIL`.
5. `NaN`, vide, negatif, decimal non entier ou texte invalide -> `guestCount` vide, code `GUEST_COUNT_INVALIDE_API`, jamais converti en zero.
6. Snapshot historique -> utilise uniquement en fallback quand aucune source API exploitable n'est disponible.

## Propagation locale validee

Les tests creent un projet temporaire et des classeurs minimaux simules, puis executent localement:

- Lot4bis;
- Lot4quater;
- Lot10;
- Lot11;
- Lot12.

Verifications effectuees:

- `guestCount` et `source_guestCount` presents dans `MASTER_CALC_Reservations` temporaire;
- `guestCount` et `source_guestCount` presents dans `MASTER_CALC_Reservations_Resolues` temporaire;
- preparation canape calculee seulement avec `guestCount` valide;
- preparation canape exclue de l'assiette et de la commission;
- preparation canape deduite du net proprietaire;
- ligne `PREPARATION_CANAPE` creee uniquement si montant positif.

## Corrections apres revue du travail interrompu

- Suppression du BOM `U+FEFF` introduit au debut de `lot4bis_charger_reservations.py`.
- Correction de l'assertion de schema Lot4bis de 25 a 26 colonnes apres ajout de `source_guestCount`.
- Ajout du helper testable `guest_count_columns_from_api(...)` dans Lot1 pour valider la logique Lot1 sans appel HTTP.
- Maintien du fallback snapshot uniquement dans Lot4bis pour anciens MASTER incomplets.

## Resultats

Commandes executees:

- `"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"`
- `git diff --check`
- `git diff --stat`
- `git status --short`

Resultats:

- unittest: `Ran 90 tests in 7.418s` / `OK`.
- `git diff --check`: OK, uniquement avertissements CRLF.
- Aucun appel API Hostaway relance.
- Aucun `MASTER_*` source regenere.

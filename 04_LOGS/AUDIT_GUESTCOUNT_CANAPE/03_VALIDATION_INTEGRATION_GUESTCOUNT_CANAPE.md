# Validation integration guestCount / preparation canape

Date: 2026-06-30

## 1. Preuve de jointure

La preuve detaillee est dans `04_LOGS/AUDIT_GUESTCOUNT_CANAPE/02_PREUVE_JOINTURE_GUESTCOUNT.md`.

Cle finale retenue pour le backfill: `MASTER_FACT_HA_Reservations.reservation_id` -> `MASTER_FACT_HA_ReservationDetails.reservation_id`, normalisee en texte et sans suffixe decimal Excel.

Les champs `id` et `hostawayReservationId` presents dans `json_snapshot` sont controles, mais ne sont pas supposes interchangeables avec la cle de rattachement.

Compteurs etablis sur donnees reelles:

- snapshots contenant `numberOfGuests`: 86
- snapshots JSON lisibles normalement: 0
- snapshots resolus uniquement par regex sur texte tronque: 86
- snapshots non exploitables: 0
- reservations reellement jointes par `reservation_id`: 86
- reservations avec `guestCount` final resolu apres Lot4bis en copie temporaire: 86

## 2. Integration isolee

Un test d'integration isole dans une copie temporaire fait passer des donnees minimales a travers:

- Lot4bis
- Lot4quater
- Lot10
- Lot11
- Lot12

Aucune sortie du projet source n'est regeneree par ce test.

Scenarios verifies sur les fichiers Excel temporaires produits:

- LOG_0006 avec `guestCount = 3`: `preparation_canape_voyageurs = 10`, assiette commission inchangee, commission inchangee, net proprietaire diminue de 10, ligne `PREPARATION_CANAPE` en prefacture.
- LOG_0006 avec `guestCount` absent: controle `A_CONTROLER`, code `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE`, aucun montant canape automatique, aucune ligne `PREPARATION_CANAPE`.
- Reservation annulee/exclue: aucun montant canape.
- `LOGEMENT_DIVERS` / hors parc technique: aucun montant canape et aucune facture proprietaire.

## 3. Stock historique incomplet

Les 505 reservations concernees par la regle canape mais sans `guestCount` restent a controler explicitement.

Regle appliquee:

- `guestCount` absent sur logement concerne -> `A_CONTROLER`
- code controle -> `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE`
- supplement non applique automatiquement
- pas de completion a zero
- pas de facturation canape
- pas de deduction du net proprietaire
- aucune deduction depuis prix, voyageurs nommes, linge ou autre donnee indirecte

Compteurs source constates:

- reservations avec `guestCount` resolu: 86
- reservations avec `guestCount` absent: 1305
- reservations concernees canape: 558
- reservations concernees canape avec `guestCount` resolu: 53
- reservations concernees canape sans `guestCount`: 505

## 4. Tests et controles

Commandes executees avec l'interpreteur de reference `C:\Program Files\Python312\python.exe`:

- `"C:\Program Files\Python312\python.exe" -m unittest discover -s tests -p "test_*.py"`
- `git diff --check`
- `git diff --stat`
- `git status --short`

Resultats:

- unittest: `Ran 83 tests in 6.839s` / `OK`
- aucun skip critique pour `test_import_side_effects.py`
- `git diff --check`: OK, uniquement avertissements CRLF attendus
- recette complete non lancee
- pipeline global non lance

## 5. Sorties source

Aucune sortie `MASTER_*` ou export du projet source n'a ete regeneree ou modifiee pour cette validation.

Les tests d'integration ecrivent uniquement dans des repertoires temporaires isoles.

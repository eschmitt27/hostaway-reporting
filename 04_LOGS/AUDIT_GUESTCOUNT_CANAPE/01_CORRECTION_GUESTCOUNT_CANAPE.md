# Correction guestCount et preparation canape

Date: 2026-06-30
Perimetre: flux Hostaway guestCount -> preparation canape -> net proprietaire -> prefacture.

## Cause racine

Le champ `numberOfGuests` etait disponible dans les details Hostaway, mais le flux ne l'exploitait pas de bout en bout:

- Lot 1 ecrivait la ligne `MASTER_FACT_HA_Reservations` avant l'appel detail, donc `guestCount` restait vide quand `numberOfGuests` n'etait pas dans la liste reservation.
- Les snapshots de `MASTER_FACT_HA_ReservationDetails.xlsx` contiennent `numberOfGuests`, mais ils sont tronques a 4000 caracteres; un parse JSON strict peut echouer. Un fallback texte deterministe est necessaire.
- Lot 4bis ne relisait pas les details pour backfiller `guestCount`.
- Lot 4quater ne propageait pas `guestCount` dans `MASTER_CALC_Reservations_Resolues`.
- Lot 10 importait `calculate_canape_amount`, mais ne l'appelait pas; les colonnes `preparation_canape_voyageurs` et `controle_preparation_canape` etaient prevues mais non alimentees.

## Corrections code

- Ajout de `02_TRAVAIL/lib_guestcount.py`:
  - resolution canonique `guestCount` sans jamais convertir l'absence en 0;
  - conservation d'un `0` explicite fourni par la source;
  - extraction depuis snapshot JSON complet ou tronque via fallback regex cible sur `numberOfGuests` / `guestCount`.

- `lot1_hostaway_extract.py`:
  - utilise la resolution canonique depuis la reservation;
  - met a jour `guestCount` apres appel detail quand le detail apporte `numberOfGuests`.

- `lot4bis_charger_reservations.py`:
  - lit `MASTER_FACT_HA_ReservationDetails.xlsx`;
  - indexe `numberOfGuests` par `reservation_id`;
  - backfill `guestCount` avec hierarchie reservation -> snapshot;
  - supprime l'usage de `or` pour ne pas perdre un `0` explicite.

- `lot4quater_resoudre_source_reservations.py`:
  - ajoute `guestCount` dans les colonnes resolues.

- `lot10_calculer_resultats.py`:
  - appelle `calculate_canape_amount(...)` sur les reservations normales apres exclusion HORS_PARC_TECHNIQUE / statuts invalides;
  - alimente `preparation_canape_voyageurs`, `controle_preparation_canape`, `source_preparation_canape`;
  - deduit le supplement du `net_proprietaire`;
  - ne modifie ni `assiette_commission` ni `commission_conciergerie`;
  - ajoute le controle `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` quand `guestCount` manque pour un logement concerne.

- `lot11_controles_coherence.py`:
  - remonte explicitement `GUEST_COUNT_MANQUANT_PREPARATION_CANAPE` dans les controles.

- `lot13_export_powerbi.py`:
  - expose `preparation_canape_voyageurs`, `controle_preparation_canape` dans `PBI_Commissions`;
  - expose `total_preparation_canape_mois` dans `PBI_Net_Proprietaire`;
  - `guestCount` n'est pas exporte Power BI car le filet anti-sensible bloque les colonnes contenant `guest`.

## Tables alimentees apres regeneration ciblee

Les scripts alimenteront apres execution ciblee dans l'ordre:

1. `MASTER_FACT_HA_Reservations.xlsx` : `guestCount` depuis reservation ou detail futur.
2. `MASTER_CALC_Reservations.xlsx` : `guestCount` backfill depuis snapshots existants.
3. `MASTER_CALC_Reservations_Resolues.xlsx` : `guestCount` propage.
4. `MASTER_CALC_Commissions.xlsx` : `guestCount`, `preparation_canape_voyageurs`, `controle_preparation_canape`, `source_preparation_canape`.
5. `MASTER_CALC_NetProprietaire.xlsx / REGLEMENT` : `total_preparation_canape_mois`.
6. `MASTER_FACT_Proprietaires.xlsx / FACT_FACTURE_LIGNES` : ligne `PREPARATION_CANAPE` si montant positif.
7. Exports Power BI : champs preparation canape visibles dans commissions et net proprietaire.

## Compteurs sur sorties actuelles non regenerees

Ces compteurs sont calcules en lecture seule sur les fichiers actuels, sans regeneration:

- Reservations avec `guestCount` resoluble via hierarchie actuelle: 86.
- Reservations encore sans `guestCount`: 1305.
- Reservations rattachees a un logement concerne canape: 558.
- Reservations concernees canape avec `guestCount` resoluble: 53.
- Reservations concernees canape encore sans `guestCount`: 505.

## Tests

Commandes executees:

```text
python -m unittest tests.test_guestcount_canape_flow tests.test_canape tests.test_import_side_effects
```

Resultat:

```text
Ran 25 tests
OK (skipped=1)
```

Le skip concerne uniquement le test d'import complet en copie temporaire quand les dependances pipeline `openpyxl` / `pandas` ne sont pas installees dans l'environnement Python courant. Les tests statiques de garde et les tests fonctionnels `guestCount` / canape passent.

Verification syntaxique sans ecriture pyc:

```text
OK sur les scripts et tests modifies
```

## Sorties generees

Aucun classeur `MASTER_*` ni export CSV n'a ete regenere pendant cette correction.
Aucune sauvegarde de fichier Excel suivi Git n'a donc ete necessaire a cette etape.

La regeneration devra etre faite ensuite de facon ciblee, pas via pipeline global, apres validation humaine.

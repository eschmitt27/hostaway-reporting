# Preuve de jointure guestCount

Perimetre: `MASTER_FACT_HA_Reservations.xlsx`, `MASTER_FACT_HA_ReservationDetails.xlsx`, champ `json_snapshot`.

## Cle de rattachement prouvee

La jointure retenue est `MASTER_FACT_HA_Reservations.reservation_id` vers `MASTER_FACT_HA_ReservationDetails.reservation_id`, normalisee en texte sans suffixe decimal Excel. Les champs JSON `id` et `hostawayReservationId` sont controles mais ne sont pas utilises comme cle principale.

## Echantillon de reservations retrouvees

| reservation_id table reservations | cle ligne detail | id json_snapshot | hostawayReservationId json_snapshot | numberOfGuests | cle finale utilisee | resultat jointure |
| --- | --- | --- | --- | ---: | --- | --- |
| 60539937 | 60539937 | None | None | 1 | 60539937 | OK reservation_id == detail.reservation_id |
| 60438998 | 60438998 | None | None | 1 | 60438998 | OK reservation_id == detail.reservation_id |
| 60550389 | 60550389 | None | None | 2 | 60550389 | OK reservation_id == detail.reservation_id |
| 58132903 | 58132903 | None | None | 1 | 58132903 | OK reservation_id == detail.reservation_id |
| 59977743 | 59977743 | None | None | 3 | 59977743 | OK reservation_id == detail.reservation_id |
| 58796697 | 58796697 | None | None | 1 | 58796697 | OK reservation_id == detail.reservation_id |
| 60438654 | 60438654 | None | None | 1 | 60438654 | OK reservation_id == detail.reservation_id |
| 60330887 | 60330887 | None | None | 3 | 60330887 | OK reservation_id == detail.reservation_id |
| 60299082 | 60299082 | None | None | 3 | 60299082 | OK reservation_id == detail.reservation_id |
| 60296863 | 60296863 | None | None | 3 | 60296863 | OK reservation_id == detail.reservation_id |

## Compteurs

- snapshots contenant `numberOfGuests`: 86
- snapshots JSON lisibles normalement: 0
- snapshots resolus uniquement par regex sur texte tronque: 86
- snapshots non exploitables: 0
- reservations reellement jointes par `reservation_id`: 86
- reservations avec `guestCount` final resolu apres Lot4bis en copie temporaire: 86

## Conclusion

La cle de jointure operationnelle est bien `reservation_id` cote table reservations et cote table details. `id` et `hostawayReservationId` du JSON servent de controle de coherence quand le JSON est lisible, mais les snapshots tronques imposent un fallback texte cible pour extraire `numberOfGuests`.

# Inventaire champs API reservations Hostaway

Date audit: 2026-06-30T15:55:06

## 1. Endpoints reellement interroges

| Usage | Endpoint | Parametres |
| --- | --- | --- |
| Liste reservations | GET /v1/reservations | dateFrom=2026-01-01, limit=100, offset pagine |
| Detail reservation | GET /v1/reservations/{id} | un appel par reservation auditee |

Les donnees financieres et frais rattaches sont observees comme blocs imbriques dans les reponses reservation (`financeField` / `financeFields` / `reservationFees`), pas via un endpoint separe dans Lot1.

## 2. Couverture analysee

| Mesure | Valeur |
| --- | --- |
| dateFrom utilise par Lot1 | 2026-01-01 |
| count API liste | 1659 |
| reservations liste auditees | 1659 |
| details GET audites | 1659 |
| snapshots JSON complets conserves | 0 dans le projet; traitement statistique en memoire uniquement |

## 3. Inventaire complet des chemins de champs

| endpoint | chemin | types observes | present | vide ou absent | exemples anonymises | colonne actuelle | statut |
| --- | --- | --- | --- | --- | --- | --- | --- |
| liste | adults | int:1565, null:94 | 1659 | 94 | 1; 2; 4 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbCancellationPolicy | str:1324, null:335 | 1659 | 335 | moderate; flexible; long_term_flexible |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbExpectedPayoutAmount | float:1267, null:335, int:57 | 1659 | 335 | 245.83; 100.93; 227.5 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbListingBasePrice | int:954, float:370, null:335 | 1659 | 335 | 262; 84; 196 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbListingCancellationHostFee | float:1267, null:335, int:57 | 1659 | 335 | 56.17; 23.07; 8.5 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbListingCancellationPayout | float:1267, null:335, int:57 | 1659 | 335 | 245.83; 100.93; 227.5 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbListingCleaningFee | int:1319, null:337, float:3 | 1659 | 337 | 40; 35; 50 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbListingHostFee | float:1267, null:335, int:57 | 1659 | 335 | 56.17; 23.07; 8.5 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbListingSecurityPrice | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbOccupancyTaxAmountPaidToHost | int:1324, null:335 | 1659 | 335 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbTotalPaidAmount | float:1206, null:335, int:118 | 1659 | 335 | 0; 227.5; 143.26 |  | PRESENT_API_NON_EXPORTE |
| liste | airbnbTransientOccupancyTaxPaidAmount | float:1269, null:335, int:55 | 1659 | 335 | 18.87; 6.05; 14.11 |  | PRESENT_API_NON_EXPORTE |
| liste | arrivalDate | str:1659 | 1659 | 0 | 2026-07-05; 2026-06-29; 2026-06-01 | MASTER_FACT_HA_Reservations.checkInDate | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | assigneeUserId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingCancellationPolicy | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomBookerCompany | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomBookerName | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomIsGeniusMember | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomNoShowFeeWaived | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomNoShowReportedAt | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomSmokingPreference | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | bookingcomSpecialRequests | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | braintreeGuestId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | braintreeMessage | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | cancellationDate | null:1550, str:109 | 1659 | 1550 | 2026-06-17 12:16:02; 2026-06-19 11:58:32; 2026-05-28 02:40:43 |  | PRESENT_API_NON_EXPORTE |
| liste | cancellationPolicy | str:1659 | 1659 | 0 | Flexible; Moderate; firm |  | PRESENT_API_NON_EXPORTE |
| liste | cancellationPolicyDetails | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | cancellationPolicyId | int:1556, null:103 | 1659 | 103 | 755319; 79386; 79381 |  | PRESENT_API_NON_EXPORTE |
| liste | cancelledBy | null:1647, str:12 | 1659 | 1647 | host |  | PRESENT_API_NON_EXPORTE |
| liste | ccExpirationMonth | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | ccExpirationYear | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | ccName | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | ccNumber | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | ccNumberEndingDigits | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | channelCommissionAmount | null:1539, float:119, int:1 | 1659 | 1539 | 58.34; 82.61; 103.76 |  | PRESENT_API_NON_EXPORTE |
| liste | channelId | int:1659 | 1659 | 0 | 2000; 2018; 2005 |  | PRESENT_API_NON_EXPORTE |
| liste | channelName | str:1659 | 1659 | 0 | direct; airbnbOfficial; bookingcom |  | PRESENT_API_NON_EXPORTE |
| liste | channelReservationId | str:1659 | 1659 | 0 | 181258-482324-2000-9814743674; 480142-guest-1710009300205583863-confirmation-HMDECYJZQZ; 480142-guest-279192287-confirmation-HM2H4BN8XT |  | PRESENT_API_NON_EXPORTE |
| liste | checkInTime | int:1659 | 1659 | 0 | 16; 2; 12 |  | PRESENT_API_NON_EXPORTE |
| liste | checkOutTime | int:1659 | 1659 | 0 | 11; 3 |  | PRESENT_API_NON_EXPORTE |
| liste | children | int:1462, null:197 | 1659 | 197 | 0; 1; 2 |  | PRESENT_API_NON_EXPORTE |
| liste | claimStatus | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | cleaningFee | int:1620, null:36, float:3 | 1659 | 36 | 40; 35; 50 |  | PRESENT_API_NON_EXPORTE |
| liste | comment | str:1460, null:199 | 1659 | 1517 |  |  | PRESENT_API_NON_EXPORTE |
| liste | confirmationCode | str:1324, null:335 | 1659 | 335 | HMDECYJZQZ; HM2H4BN8XT; HME3NWN2TC |  | PRESENT_API_NON_EXPORTE |
| liste | currency | str:1659 | 1659 | 0 | EUR | MASTER_FACT_HA_ReservationFinanceFields.currency, MASTER_FACT_HA_ReservationFees.currency | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | customFieldValues | list:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | customerIcalId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | customerIcalName | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | customerUserId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | cvc | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | departureDate | str:1659 | 1659 | 0 | 2026-07-10; 2026-07-05; 2026-06-03 | MASTER_FACT_HA_Reservations.checkOutDate | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | doorCode | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | doorCodeInstruction | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | doorCodeVendor | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | externalPropertyId | str:1565, null:94 | 1659 | 94 | 1048479781060748883; 1194708341597408068; 1447831924070233156 |  | PRESENT_API_NON_EXPORTE |
| liste | externalUnitId | null:1523, str:136 | 1659 | 1523 | 1549936001; 1550036001; 1548005201 |  | PRESENT_API_NON_EXPORTE |
| liste | financeField | list:1659 | 1659 | 1659 |  | MASTER_FACT_HA_ReservationFinanceFields.financeField_name | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | guestAddress | null:1537, str:122 | 1659 | 1656 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestAuthHash | str:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestCity | null:1529, str:130 | 1659 | 1583 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestCountry | null:1527, str:132 | 1659 | 1527 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestEmail | null:1524, str:135 | 1659 | 1524 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestExternalAccountId | str:1429, null:230 | 1659 | 230 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestFirstName | null:944, str:715 | 1659 | 944 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestLastName | null:1067, str:592 | 1659 | 1067 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestLocale | null:1073, str:586 | 1659 | 1073 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestName | str:1659 | 1659 | 869 |  | MASTER_FACT_HA_Reservations.guestName | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | guestNote | null:1539, str:120 | 1659 | 1539 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestPaymentCardIsVirtual | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestPicture | str:1288, null:371 | 1659 | 371 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestPortalRevampUrl | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestPortalUrl | str:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestRecommendations | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestTrips | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestWork | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | guestZipCode | null:1537, str:122 | 1659 | 1656 |  |  | PRESENT_API_NON_EXPORTE |
| liste | hostNote | null:1649, str:10 | 1659 | 1658 |  |  | PRESENT_API_NON_EXPORTE |
| liste | hostProxyEmail | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | hostawayCommissionAmount | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | hostawayReservationId | str:1659 | 1659 | 0 | 61255258; 61079901; 60297323 |  | PRESENT_API_NON_EXPORTE |
| liste | id | int:1659 | 1659 | 0 | 61255258; 61079901; 60297323 | MASTER_FACT_HA_Reservations.reservation_id, MASTER_FACT_HA_ReservationDetails.reservation_id, MASTER_FACT_HA_ReservationFinanceFields.reservation_id, MASTER_FACT_HA_ReservationFees.reservation_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | infants | int:1429, null:230 | 1659 | 230 | 0; 1; 2 |  | PRESENT_API_NON_EXPORTE |
| liste | insertedOn | str:1659 | 1659 | 0 | 2026-06-21 10:03:28; 2026-06-17 08:46:58; 2026-06-01 20:44:49 |  | PRESENT_API_NON_EXPORTE |
| liste | insuranceCoverageEnd | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | insuranceCoverageStart | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | insurancePolicyId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | insuranceStatus | str:1659 | 1659 | 0 | not_eligible |  | PRESENT_API_NON_EXPORTE |
| liste | isArchived | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | isDatesUnspecified | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestIdentityVerified | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestVerifiedByEmail | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestVerifiedByFacebook | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestVerifiedByGovernmentId | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestVerifiedByPhone | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestVerifiedByReviews | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isGuestVerifiedByWorkEmail | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isInitial | int:1659 | 1659 | 0 | 0; 1 |  | PRESENT_API_NON_EXPORTE |
| liste | isInstantBooked | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | isManuallyChecked | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | isPaid | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | isPinned | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | isProcessed | int:1659 | 1659 | 0 | 1 |  | PRESENT_API_NON_EXPORTE |
| liste | isStarred | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | latestActivityOn | str:1659 | 1659 | 0 | 2026-06-30 10:38:52; 2026-06-17 12:16:02; 2026-06-01 20:48:36 |  | PRESENT_API_NON_EXPORTE |
| liste | listingCustomFields | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | listingMapId | int:1659 | 1659 | 0 | 482324; 480142; 487144 | MASTER_FACT_HA_Reservations.listingMapId | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | listingName | str:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | localeForMessaging | null:1073, str:586 | 1659 | 1073 | en; fr; es |  | PRESENT_API_NON_EXPORTE |
| liste | localeForMessagingSource | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | marriottCancellationPolicy | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | nights | int:1659 | 1659 | 0 | 5; 6; 2 | MASTER_FACT_HA_Reservations.nights | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | numberOfGuests | int:1659 | 1659 | 0 | 2; 1; 4 | MASTER_FACT_HA_Reservations.guestCount | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | originalChannel | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | paymentStatus | str:1659 | 1659 | 0 | Unknown; Paid; Partially paid | MASTER_FACT_HA_Reservations.paymentStatus | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | pendingExpireDate | null:1482, str:177 | 1659 | 1482 | 2026-06-18 08:54:12; 2026-06-02 20:44:49; 2026-06-20 11:39:17 |  | PRESENT_API_NON_EXPORTE |
| liste | pets | int:1324, null:335 | 1659 | 335 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | phone | null:1099, str:560 | 1659 | 1099 |  |  | PRESENT_API_NON_EXPORTE |
| liste | previousArrivalDate | null:1628, str:31 | 1659 | 1628 | 2026-07-06; 2026-06-23; 2026-10-19 |  | PRESENT_API_NON_EXPORTE |
| liste | previousDepartureDate | null:1628, str:31 | 1659 | 1628 | 2026-07-10; 2026-07-02; 2026-10-24 |  | PRESENT_API_NON_EXPORTE |
| liste | remainingBalance | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | rentalAgreementFileUrl | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | reservationAgreement | str:1659 | 1659 | 0 | not_required; signed |  | PRESENT_API_NON_EXPORTE |
| liste | reservationCouponId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | reservationDate | str:1635, null:24 | 1659 | 24 | 2026-06-21 10:03:28; 2026-06-17 08:46:57; 2026-06-01 20:44:49 |  | PRESENT_API_NON_EXPORTE |
| liste | reservationFees | list:1659 | 1659 | 1659 |  | MASTER_FACT_HA_ReservationFees.fee_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | reservationId | str:1659 | 1659 | 0 | 181258-482324-2000-9814743674; 480142-guest-1710009300205583863-confirmation-HMDECYJZQZ; 480142-guest-279192287-confirmation-HM2H4BN8XT |  | PRESENT_API_NON_EXPORTE |
| liste | reservationUnit | list:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | securityDepositFee | null:1599, int:60 | 1659 | 1599 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | source | null:1659 | 1659 | 1659 |  | MASTER_FACT_HA_Reservations.source | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | status | str:1659 | 1659 | 0 | modified; cancelled; declined | MASTER_FACT_HA_Reservations.status | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | stripeGuestId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | stripeMessage | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| liste | taxAmount | int:1334, null:325 | 1659 | 325 | 0 |  | PRESENT_API_NON_EXPORTE |
| liste | totalPrice | float:1476, int:183 | 1659 | 0 | 300; 247.8; 130.05 | MASTER_FACT_HA_Reservations.totalPrice | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | updatedOn | str:1659 | 1659 | 0 | 2026-06-30 10:38:53; 2026-06-30 08:10:47; 2026-06-30 08:10:46 | MASTER_FACT_HA_Reservations.updatedOn | EXPORTE_MAIS_PERDU_EN_AVAL |
| liste | vrboCancellationPolicy | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | adults | int:1565, null:94 | 1659 | 94 | 1; 2; 4 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbCancellationPolicy | str:1324, null:335 | 1659 | 335 | moderate; flexible; long_term_flexible |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbExpectedPayoutAmount | float:1267, null:335, int:57 | 1659 | 335 | 245.83; 100.93; 227.5 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbListingBasePrice | int:954, float:370, null:335 | 1659 | 335 | 262; 84; 196 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbListingCancellationHostFee | float:1267, null:335, int:57 | 1659 | 335 | 56.17; 23.07; 8.5 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbListingCancellationPayout | float:1267, null:335, int:57 | 1659 | 335 | 245.83; 100.93; 227.5 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbListingCleaningFee | int:1319, null:337, float:3 | 1659 | 337 | 40; 35; 50 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbListingHostFee | float:1267, null:335, int:57 | 1659 | 335 | 56.17; 23.07; 8.5 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbListingSecurityPrice | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbOccupancyTaxAmountPaidToHost | int:1324, null:335 | 1659 | 335 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbTotalPaidAmount | float:1206, null:335, int:118 | 1659 | 335 | 0; 227.5; 143.26 |  | PRESENT_API_NON_EXPORTE |
| detail | airbnbTransientOccupancyTaxPaidAmount | float:1269, null:335, int:55 | 1659 | 335 | 18.87; 6.05; 14.11 |  | PRESENT_API_NON_EXPORTE |
| detail | arrivalDate | str:1659 | 1659 | 0 | 2026-07-05; 2026-06-29; 2026-06-01 | MASTER_FACT_HA_Reservations.checkInDate | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | assigneeUserId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingCancellationPolicy | null:1580, str:79 | 1659 | 1580 | 16; 14 |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomBookerCompany | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomBookerName | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomIsGeniusMember | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomNoShowFeeWaived | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomNoShowReportedAt | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomSmokingPreference | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | bookingcomSpecialRequests | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | braintreeGuestId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | braintreeMessage | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationDate | null:1550, str:109 | 1659 | 1550 | 2026-06-17 12:16:02; 2026-06-19 11:58:32; 2026-05-28 02:40:43 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicy | str:1659 | 1659 | 0 | moderate; firm; strict |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails | dict:1556, null:103 | 1659 | 103 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.accountId | int:1556 | 1556 | 103 | 181258; 0 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem | list:1556 | 1556 | 108 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[] | dict:3001 | 3001 | -1342 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].accountId | int:3001 | 3001 | -1342 | 181258; 0 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].cancellationPolicyId | int:3001 | 3001 | -1342 | 755319; 79386; 79381 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].event | str:3001 | 3001 | -1342 | arrival |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].id | int:3001 | 3001 | -1342 | 1067727; 1067728; 112090 | MASTER_FACT_HA_Reservations.reservation_id, MASTER_FACT_HA_ReservationDetails.reservation_id, MASTER_FACT_HA_ReservationFinanceFields.reservation_id, MASTER_FACT_HA_ReservationFees.reservation_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].insertedOn | str:3001 | 3001 | -1342 | 2026-01-10 10:02:19; 2022-09-27 07:31:00; 2022-09-27 07:30:20 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].refundAmount | int:3001 | 3001 | -1342 | 100; 50 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].refundField | str:3001 | 3001 | -1342 | totalPrice |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].refundType | str:3001 | 3001 | -1342 | percentage |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].timeDelta | int:3001 | 3001 | -1342 | -1209600; -604800; -432000 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.cancellationPolicyItem[].updatedOn | str:3001 | 3001 | -1342 | 2026-01-10 10:02:19; 2022-09-27 07:31:00; 2022-09-27 07:30:20 | MASTER_FACT_HA_Reservations.updatedOn | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | cancellationPolicyDetails.channelId | int:1556 | 1556 | 103 | 2000; 2018; 2005 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.channelSpecificCode | str:1556 | 1556 | 103 | flexible; moderate; 16 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.id | int:1556 | 1556 | 103 | 755319; 79386; 79381 | MASTER_FACT_HA_Reservations.reservation_id, MASTER_FACT_HA_ReservationDetails.reservation_id, MASTER_FACT_HA_ReservationFinanceFields.reservation_id, MASTER_FACT_HA_ReservationFees.reservation_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | cancellationPolicyDetails.insertedOn | str:1556 | 1556 | 103 | 2026-01-10 10:02:19; 2022-09-27 07:31:00; 2022-09-27 07:30:20 |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.longTerm | bool:1556 | 1556 | 103 | False |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.name | str:1556 | 1556 | 103 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cancellationPolicyDetails.updatedOn | str:1556 | 1556 | 103 | 2026-01-10 10:02:19; 2022-09-27 07:31:00; 2022-09-27 07:30:20 | MASTER_FACT_HA_Reservations.updatedOn | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | cancellationPolicyId | int:1556, null:103 | 1659 | 103 | 755319; 79386; 79381 |  | PRESENT_API_NON_EXPORTE |
| detail | cancelledBy | null:1647, str:12 | 1659 | 1647 | host |  | PRESENT_API_NON_EXPORTE |
| detail | ccExpirationMonth | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | ccExpirationYear | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | ccName | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | ccNumber | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | ccNumberEndingDigits | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | channelCommissionAmount | null:1539, float:119, int:1 | 1659 | 1539 | 58.34; 82.61; 103.76 |  | PRESENT_API_NON_EXPORTE |
| detail | channelId | int:1659 | 1659 | 0 | 2000; 2018; 2005 |  | PRESENT_API_NON_EXPORTE |
| detail | channelName | str:1659 | 1659 | 0 | direct; airbnbOfficial; bookingcom |  | PRESENT_API_NON_EXPORTE |
| detail | channelReservationId | str:1659 | 1659 | 0 | 181258-482324-2000-9814743674; 480142-guest-1710009300205583863-confirmation-HMDECYJZQZ; 480142-guest-279192287-confirmation-HM2H4BN8XT |  | PRESENT_API_NON_EXPORTE |
| detail | checkInTime | int:1659 | 1659 | 0 | 16; 2; 12 |  | PRESENT_API_NON_EXPORTE |
| detail | checkOutTime | int:1659 | 1659 | 0 | 11; 3 |  | PRESENT_API_NON_EXPORTE |
| detail | children | int:1462, null:197 | 1659 | 197 | 0; 1; 2 |  | PRESENT_API_NON_EXPORTE |
| detail | claimStatus | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cleaningFee | int:1620, null:36, float:3 | 1659 | 36 | 40; 35; 50 |  | PRESENT_API_NON_EXPORTE |
| detail | comment | str:1460, null:199 | 1659 | 1517 |  |  | PRESENT_API_NON_EXPORTE |
| detail | confirmationCode | str:1324, null:335 | 1659 | 335 | HMDECYJZQZ; HM2H4BN8XT; HME3NWN2TC |  | PRESENT_API_NON_EXPORTE |
| detail | currency | str:1659 | 1659 | 0 | EUR | MASTER_FACT_HA_ReservationFinanceFields.currency, MASTER_FACT_HA_ReservationFees.currency | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | customFieldValues | list:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | customerIcalId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | customerIcalName | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | customerUserId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | cvc | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | departureDate | str:1659 | 1659 | 0 | 2026-07-10; 2026-07-05; 2026-06-03 | MASTER_FACT_HA_Reservations.checkOutDate | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | doorCode | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | doorCodeInstruction | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | doorCodeVendor | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | externalPropertyId | str:1565, null:94 | 1659 | 94 | 1048479781060748883; 1194708341597408068; 1447831924070233156 |  | PRESENT_API_NON_EXPORTE |
| detail | externalUnitId | null:1523, str:136 | 1659 | 1523 | 1549936001; 1550036001; 1548005201 |  | PRESENT_API_NON_EXPORTE |
| detail | financeField | list:1659 | 1659 | 54 |  | MASTER_FACT_HA_ReservationFinanceFields.financeField_name | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | financeField[] | dict:14385 | 14385 | -12726 |  | MASTER_FACT_HA_ReservationFinanceFields.financeField_name | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | financeField[].alias | null:14385 | 14385 | 1659 |  |  | A_VERIFIER |
| detail | financeField[].id | int:14385 | 14385 | -12726 | 261051835; 261051836; 261051837 | MASTER_FACT_HA_Reservations.reservation_id, MASTER_FACT_HA_ReservationDetails.reservation_id, MASTER_FACT_HA_ReservationFinanceFields.reservation_id, MASTER_FACT_HA_ReservationFees.reservation_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | financeField[].isDeleted | int:14385 | 14385 | -12726 | 0; 1 |  | A_VERIFIER |
| detail | financeField[].isIncludedInTotalPrice | int:14385 | 14385 | -12726 | 1; 0 |  | A_VERIFIER |
| detail | financeField[].isMandatory | int:14385 | 14385 | -12726 | 1; 0 |  | A_VERIFIER |
| detail | financeField[].isOverriddenByUser | int:14385 | 14385 | -12726 | 1; 0 |  | A_VERIFIER |
| detail | financeField[].isQuantitySelectable | int:14385 | 14385 | -12726 | 0 |  | A_VERIFIER |
| detail | financeField[].listingFeeSettingId | null:14385 | 14385 | 1659 |  |  | A_VERIFIER |
| detail | financeField[].name | str:14385 | 14385 | -12726 |  |  | A_VERIFIER |
| detail | financeField[].quantity | null:14385 | 14385 | 1659 |  |  | A_VERIFIER |
| detail | financeField[].title | str:14385 | 14385 | -12726 | Base rate; Price for extra person; Cleaning fee |  | A_VERIFIER |
| detail | financeField[].total | float:11059, int:3326 | 14385 | -12726 | 300; 40; 0 |  | A_VERIFIER |
| detail | financeField[].type | str:14385 | 14385 | -12726 | accommodation; fee; totals |  | A_VERIFIER |
| detail | financeField[].units | int:14385 | 14385 | -12726 | 1 |  | A_VERIFIER |
| detail | financeField[].value | float:11059, int:3295, null:31 | 14385 | -12695 | 300; 40; 0 |  | A_VERIFIER |
| detail | guestAddress | null:1537, str:122 | 1659 | 1656 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestAuthHash | str:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestCity | null:1529, str:130 | 1659 | 1583 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestCountry | null:1527, str:132 | 1659 | 1527 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestEmail | null:1524, str:135 | 1659 | 1524 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestExternalAccountId | str:1429, null:230 | 1659 | 230 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestFirstName | null:944, str:715 | 1659 | 944 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestLastName | null:1067, str:592 | 1659 | 1067 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestLocale | null:1073, str:586 | 1659 | 1073 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestName | str:1659 | 1659 | 869 |  | MASTER_FACT_HA_Reservations.guestName | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | guestNote | null:1539, str:120 | 1659 | 1539 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestPaymentCardIsVirtual | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestPicture | str:1288, null:371 | 1659 | 371 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestPortalRevampUrl | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestPortalUrl | str:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestRecommendations | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestTrips | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestWork | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | guestZipCode | null:1537, str:122 | 1659 | 1656 |  |  | PRESENT_API_NON_EXPORTE |
| detail | hostNote | null:1649, str:10 | 1659 | 1658 |  |  | PRESENT_API_NON_EXPORTE |
| detail | hostProxyEmail | str:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | hostawayCommissionAmount | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | hostawayReservationId | str:1659 | 1659 | 0 | 61255258; 61079901; 60297323 |  | PRESENT_API_NON_EXPORTE |
| detail | id | int:1659 | 1659 | 0 | 61255258; 61079901; 60297323 | MASTER_FACT_HA_Reservations.reservation_id, MASTER_FACT_HA_ReservationDetails.reservation_id, MASTER_FACT_HA_ReservationFinanceFields.reservation_id, MASTER_FACT_HA_ReservationFees.reservation_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | infants | int:1429, null:230 | 1659 | 230 | 0; 1; 2 |  | PRESENT_API_NON_EXPORTE |
| detail | insertedOn | str:1659 | 1659 | 0 | 2026-06-21 10:03:28; 2026-06-17 08:46:58; 2026-06-01 20:44:49 |  | PRESENT_API_NON_EXPORTE |
| detail | insuranceCoverageEnd | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | insuranceCoverageStart | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | insurancePolicyId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | insuranceStatus | str:1659 | 1659 | 0 | not_eligible |  | PRESENT_API_NON_EXPORTE |
| detail | isArchived | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | isDatesUnspecified | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestIdentityVerified | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestVerifiedByEmail | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestVerifiedByFacebook | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestVerifiedByGovernmentId | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestVerifiedByPhone | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestVerifiedByReviews | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isGuestVerifiedByWorkEmail | int:1659 | 1659 | 0 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isInitial | int:1659 | 1659 | 0 | 0; 1 |  | PRESENT_API_NON_EXPORTE |
| detail | isInstantBooked | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | isManuallyChecked | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | isPaid | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | isPinned | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | isProcessed | int:1659 | 1659 | 0 | 1 |  | PRESENT_API_NON_EXPORTE |
| detail | isStarred | int:1659 | 1659 | 0 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | latestActivityOn | str:1659 | 1659 | 0 | 2026-06-30 10:38:52; 2026-06-17 12:16:02; 2026-06-01 20:48:36 |  | PRESENT_API_NON_EXPORTE |
| detail | listingCustomFields | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | listingMapId | int:1659 | 1659 | 0 | 482324; 480142; 487144 | MASTER_FACT_HA_Reservations.listingMapId | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | listingName | str:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | localeForMessaging | null:1073, str:586 | 1659 | 1073 | en; fr; es |  | PRESENT_API_NON_EXPORTE |
| detail | localeForMessagingSource | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | marriottCancellationPolicy | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | nights | int:1659 | 1659 | 0 | 5; 6; 2 | MASTER_FACT_HA_Reservations.nights | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | numberOfGuests | int:1659 | 1659 | 0 | 2; 1; 4 | MASTER_FACT_HA_Reservations.guestCount | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | originalChannel | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | paymentStatus | str:1659 | 1659 | 0 | Unknown; Paid; Partially paid | MASTER_FACT_HA_Reservations.paymentStatus | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | pendingExpireDate | null:1482, str:177 | 1659 | 1482 | 2026-06-18 08:54:12; 2026-06-02 20:44:49; 2026-06-20 11:39:17 |  | PRESENT_API_NON_EXPORTE |
| detail | pets | int:1324, null:335 | 1659 | 335 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | phone | null:1099, str:560 | 1659 | 1099 |  |  | PRESENT_API_NON_EXPORTE |
| detail | previousArrivalDate | null:1628, str:31 | 1659 | 1628 | 2026-07-06; 2026-06-23; 2026-10-19 |  | PRESENT_API_NON_EXPORTE |
| detail | previousDepartureDate | null:1628, str:31 | 1659 | 1628 | 2026-07-10; 2026-07-02; 2026-10-24 |  | PRESENT_API_NON_EXPORTE |
| detail | remainingBalance | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | rentalAgreementFileUrl | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | reservationAgreement | str:1659 | 1659 | 0 | not_required; signed |  | PRESENT_API_NON_EXPORTE |
| detail | reservationCouponId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | reservationDate | str:1635, null:24 | 1659 | 24 | 2026-06-21 10:03:28; 2026-06-17 08:46:57; 2026-06-01 20:44:49 |  | PRESENT_API_NON_EXPORTE |
| detail | reservationFees | list:1659 | 1659 | 1528 |  | MASTER_FACT_HA_ReservationFees.fee_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationFees[] | dict:764 | 764 | 895 |  | MASTER_FACT_HA_ReservationFees.fee_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationFees[].accountId | int:764 | 764 | 895 | 181258 |  | A_VERIFIER |
| detail | reservationFees[].amount | float:518, int:246 | 764 | 895 | 55; 26.2; 14.15 | MASTER_FACT_HA_ReservationFees.amount | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationFees[].currency | str:764 | 764 | 895 | EUR | MASTER_FACT_HA_ReservationFinanceFields.currency, MASTER_FACT_HA_ReservationFees.currency | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationFees[].externalReservationUnitId | null:764 | 764 | 1659 |  |  | A_VERIFIER |
| detail | reservationFees[].feeType | str:764 | 764 | 895 | guest; hotel |  | A_VERIFIER |
| detail | reservationFees[].id | int:764 | 764 | 895 | 80671540; 80671541; 80671542 | MASTER_FACT_HA_Reservations.reservation_id, MASTER_FACT_HA_ReservationDetails.reservation_id, MASTER_FACT_HA_ReservationFinanceFields.reservation_id, MASTER_FACT_HA_ReservationFees.reservation_id | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationFees[].insertedOn | str:764 | 764 | 895 | 2026-04-11 18:42:24; 2026-04-17 14:41:57; 2026-06-24 23:22:55 |  | A_VERIFIER |
| detail | reservationFees[].isImported | int:764 | 764 | 895 | 1 |  | A_VERIFIER |
| detail | reservationFees[].isIncluded | int:764 | 764 | 895 | 1; 0 |  | A_VERIFIER |
| detail | reservationFees[].isPerNight | int:764 | 764 | 895 | 0 |  | A_VERIFIER |
| detail | reservationFees[].isPerPerson | int:764 | 764 | 895 | 0 |  | A_VERIFIER |
| detail | reservationFees[].listingMapId | int:764 | 764 | 895 | 480139; 480140; 493047 | MASTER_FACT_HA_Reservations.listingMapId | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationFees[].name | str:764 | 764 | 895 |  |  | A_VERIFIER |
| detail | reservationFees[].percentage | int:262, float:262, null:240 | 764 | 1135 | 10; 7.2; 7.19 |  | A_VERIFIER |
| detail | reservationFees[].reservationId | int:764 | 764 | 895 | 57594520; 57852998; 61594179 |  | A_VERIFIER |
| detail | reservationFees[].updatedOn | str:764 | 764 | 895 | 2026-04-11 18:42:24; 2026-04-17 14:41:57; 2026-06-24 23:22:55 | MASTER_FACT_HA_Reservations.updatedOn | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | reservationId | str:1659 | 1659 | 0 | 181258-482324-2000-9814743674; 480142-guest-1710009300205583863-confirmation-HMDECYJZQZ; 480142-guest-279192287-confirmation-HM2H4BN8XT |  | PRESENT_API_NON_EXPORTE |
| detail | reservationUnit | list:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | securityDepositFee | null:1599, int:60 | 1659 | 1599 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | source | null:1659 | 1659 | 1659 |  | MASTER_FACT_HA_Reservations.source | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | status | str:1659 | 1659 | 0 | modified; cancelled; declined | MASTER_FACT_HA_Reservations.status | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | stripeGuestId | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | stripeMessage | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |
| detail | taxAmount | int:1334, null:325 | 1659 | 325 | 0 |  | PRESENT_API_NON_EXPORTE |
| detail | totalPrice | float:1476, int:183 | 1659 | 0 | 300; 247.8; 130.05 | MASTER_FACT_HA_Reservations.totalPrice | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | updatedOn | str:1659 | 1659 | 0 | 2026-06-30 10:38:53; 2026-06-30 08:10:47; 2026-06-30 08:10:46 | MASTER_FACT_HA_Reservations.updatedOn | EXPORTE_MAIS_PERDU_EN_AVAL |
| detail | vrboCancellationPolicy | null:1659 | 1659 | 1659 |  |  | PRESENT_API_NON_EXPORTE |

## 4. Comparaison ancien / intermediaire / recent

| periode | arrivee min | arrivee max | reservations | numberOfGuests liste | numberOfGuests detail | adults/children/infants presents | reconstituable prouve | aucune donnee |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ANCIEN | 2025-01-26 | 2025-08-25 | 553 | 553 | 553 | 536 | 0 | 0 |
| INTERMEDIAIRE | 2025-08-25 | 2026-03-28 | 553 | 553 | 553 | 530 | 0 | 0 |
| RECENT | 2026-03-28 | 2027-02-11 | 553 | 553 | 553 | 499 | 0 | 0 |

### Par canal

| periode | canal | reservations | numberOfGuests liste | numberOfGuests detail | aucune donnee |
| --- | --- | --- | --- | --- | --- |
| ANCIEN | airbnbOfficial | 536 | 536 | 536 | 0 |
| ANCIEN | vrboical | 17 | 17 | 17 | 0 |
| INTERMEDIAIRE | airbnbOfficial | 516 | 516 | 516 | 0 |
| INTERMEDIAIRE | direct | 16 | 16 | 16 | 0 |
| INTERMEDIAIRE | bookingcom | 14 | 14 | 14 | 0 |
| INTERMEDIAIRE | vrboical | 7 | 7 | 7 | 0 |
| RECENT | airbnbOfficial | 377 | 377 | 377 | 0 |
| RECENT | bookingcom | 122 | 122 | 122 | 0 |
| RECENT | direct | 44 | 44 | 44 | 0 |
| RECENT | vrboical | 10 | 10 | 10 | 0 |

### Par statut

| periode | statut | reservations | numberOfGuests liste | numberOfGuests detail | aucune donnee |
| --- | --- | --- | --- | --- | --- |
| ANCIEN | new | 552 | 552 | 552 | 0 |
| ANCIEN | inquiryNotPossible | 1 | 1 | 1 | 0 |
| INTERMEDIAIRE | new | 478 | 478 | 478 | 0 |
| INTERMEDIAIRE | modified | 26 | 26 | 26 | 0 |
| INTERMEDIAIRE | inquiry | 18 | 18 | 18 | 0 |
| INTERMEDIAIRE | cancelled | 13 | 13 | 13 | 0 |
| INTERMEDIAIRE | declined | 7 | 7 | 7 | 0 |
| INTERMEDIAIRE | inquiryNotPossible | 4 | 4 | 4 | 0 |
| INTERMEDIAIRE | inquiryPreapproved | 4 | 4 | 4 | 0 |
| INTERMEDIAIRE | expired | 2 | 2 | 2 | 0 |
| INTERMEDIAIRE | ownerStay | 1 | 1 | 1 | 0 |
| RECENT | new | 323 | 323 | 323 | 0 |
| RECENT | cancelled | 96 | 96 | 96 | 0 |
| RECENT | inquiry | 54 | 54 | 54 | 0 |
| RECENT | modified | 45 | 45 | 45 | 0 |
| RECENT | declined | 12 | 12 | 12 | 0 |
| RECENT | ownerStay | 12 | 12 | 12 | 0 |
| RECENT | inquiryPreapproved | 7 | 7 | 7 | 0 |
| RECENT | expired | 4 | 4 | 4 | 0 |

## 5. Disponibilite reelle de numberOfGuests

| Indicateur | Valeur |
| --- | --- |
| reservations auditees | 1659 |
| numberOfGuests dans liste | 1659 |
| numberOfGuests dans detail | 1659 |
| adults / children / infants presents | 1565 |
| nombre voyageurs reconstituable de facon prouvee | 0 |
| aucune donnee exploitable | 0 |

Hierarchie appliquee: liste `numberOfGuests`, puis detail `numberOfGuests`, puis champ officiel equivalent seulement si prouve. Aucun equivalent officiel additionnable n'a ete prouve dans cet audit.

## 6. Autres champs voyageurs potentiellement exploitables

Les chemins `adults`, `children`, `infants` ou variantes ne sont pas promus comme alternative: leur sens exact et leur addition au total voyageurs ne sont pas prouves par les reponses observees. Aucune reconstitution n'est retenue.

## 7. Champs API perdus pendant l'extraction

| chemin API | endpoint | diagnostic |
| --- | --- | --- |
| adults | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| children | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| customFieldValues | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].alias | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].isDeleted | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].isIncludedInTotalPrice | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].isMandatory | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].isOverriddenByUser | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].isQuantitySelectable | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].listingFeeSettingId | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].name | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].quantity | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].title | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].total | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].type | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].units | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| financeField[].value | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestAddress | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestAuthHash | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestCity | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestCountry | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestEmail | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestExternalAccountId | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestFirstName | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestLastName | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestLocale | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestNote | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestPaymentCardIsVirtual | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestPicture | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestPortalRevampUrl | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestPortalUrl | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestRecommendations | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestTrips | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestWork | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| guestZipCode | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| hostawayReservationId | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| infants | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| pets | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationAgreement | liste, detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].accountId | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].externalReservationUnitId | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].feeType | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].insertedOn | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].isImported | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].isIncluded | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].isPerNight | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].isPerPerson | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].name | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].percentage | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |
| reservationFees[].reservationId | detail | PRESENT_API_NON_EXPORTE ou A_VERIFIER |

Perte structurelle certaine: `MASTER_FACT_HA_ReservationDetails.json_snapshot` tronque le detail a 4000 caracteres. L'audit brut montre que l'information doit etre preservee par champs atomiques utiles ou snapshot hors Git anonymise, jamais par JSON tronque comme preuve complete.

## 8. Conclusion

- `numberOfGuests` est present dans l'API liste et detail pour toutes les reservations auditees.
- Aucune reservation auditee sans `numberOfGuests` observee dans les reponses API actuelles.
- Probleme extraction confirme si une colonne existe dans la table FACT mais n'est pas propagee vers `MASTER_CALC_Reservations`.
- Probleme d'anciennete a evaluer sur les bornes ci-dessus: les tranches sont derivees automatiquement des dates d'arrivee auditees.
- Probleme canal/statut: voir les tableaux par canal et statut; aucun nombre voyageur n'est deduit indirectement.

## 9. Proposition de correction future, sans execution

1. Remplacer le snapshot tronque comme source de preuve par une extraction atomique des champs reservation utiles: `numberOfGuests`, champs voyageurs officiels, blocs financiers et frais normalises.
2. Conserver `guestCount` comme colonne canonique aval, alimentee uniquement depuis `numberOfGuests` liste puis detail.
3. Ajouter un controle bloquant/a-controler pour toute reservation eligible canape sans donnee voyageurs exploitable, sans completer a zero.
4. Ne pas utiliser `adults` / `children` / `infants` tant qu'une equivalence officielle n'est pas prouvee.
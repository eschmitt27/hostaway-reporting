# Sources mai 2026 ? r?cup?rer

Aucune r?servation, charge, mouvement bancaire, acompte, AirCover ou imputation Airbnb n'a ?t? cr??, copi? ou modifi?.

| Source recherch?e | Fichier ou emplacement trouv? | P?riode r?ellement couverte | Nombre de lignes de mai | Utilisable directement ou non | Action n?cessaire |
|---|---|---:|---:|---|---|
| R?servations Hostaway de mai 2026 | `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_Reservations.xlsx` / `data` | 2025-01 -> 2027-02 | 171 | OUI - snapshot Hostaway local exploitable | Utiliser ce snapshot pour la recette ou refaire un export Hostaway mai si une version plus r?cente est souhait?e. Ne rien copier maintenant. |
| T?ches / m?nages Hostaway de mai 2026 | `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx` / `MASTER_ENRICHI` | 2026-02 -> 2027-02 | 102 | OUI - snapshot t?ches Hostaway exploitable | Utiliser ce snapshot pour les contr?les m?nages Hostaway de mai; ne pas importer juin ? la place. |
| Charges r?elles de mai 2026 | `01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx` / `SAISIE` | non d?termin?e | 0 | NON | Saisir/importer les charges r?elles de mai dans `SAISIE_Charges_Flux.xlsx`. Le fichier existe mais ne contient aucune ligne mai. |
| Relev? ou export bancaire de mai 2026 | `02_TRAVAIL/Lot8_Banque/BANQUE_LOT8_IMPORT.xlsx` / `BRUT_Banque` | 2026-02 -> 2026-04 | 0 | NON | Exporter/importer le relev? bancaire couvrant mai 2026 dans Lot8. Le fichier actuel couvre des mouvements hors mai et ne contient aucune ligne mai. |
| Acomptes propri?taires de mai 2026 | `01_SOURCES_BRUTES/AcomptesProprietaires/SAISIE_AcomptesProprietaires.xlsx` / `SAISIE` | non d?termin?e | 0 | NON_APPLICABLE sauf acompte r?el | Si un acompte propri?taire de mai existe, le saisir avec propri?taire, logement, mois, montant et justificatif. Aucune ligne mai trouv?e. |
| AirCover r?el de mai 2026 | `01_SOURCES_BRUTES/AirCover/SAISIE_AirCover.xlsx` / `MASTER` | non d?termin?e | 0 | NON_APPLICABLE sauf cas r?el | Si un AirCover mai existe, saisir b?n?ficiaire r?el, montant, justificatif et traitement. Aucune ligne mai trouv?e. |
| Versements Airbnb ? imputer de mai 2026 | `01_SOURCES_BRUTES/ImputationsAirbnb/SAISIE_ImputationsAirbnb.xlsx` / `MASTER` | non d?termin?e | 0 | NON_APPLICABLE sauf versement ? imputer | Apr?s import banque mai, saisir uniquement les versements Airbnb rattach?s avec certitude ? propri?taire + logement + mois + document. Aucune ligne mai trouv?e. |

## Sources compl?mentaires trouv?es

| ?l?ment | Fichier | P?riode | Lignes mai | Usage |
|---|---|---:|---:|---|
| Payout/finance Hostaway mai | `02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx` / `data` | 2025-01 -> 2027-02 | 89 | Sortie calcul?e du snapshot Hostaway, utile pour recette mais pas source brute. |
| D?tails r?servation Hostaway mai | `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_ReservationDetails.xlsx` / `data` | 2026-05 -> 2026-06 | 29 | Snapshot d?tails partiel, utile pour contr?le si besoin. |
| R?servations hors Hostaway mai | `01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx` / `SAISIE` | 2026-05 -> 2026-07 | 1 | Saisie source hors Hostaway ? v?rifier/valider avant recette. |
| M?nages internes mai | `02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx` / `SOURCE_RAW` | 2026-05 -> 2026-05 | 5 | Source normalis?e M04 existante. |
| M?nages externes mai | `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx` / `SOURCE_RAW` | 2026-05 -> 2026-05 | 13 | Extraction de factures prestataires existante. |
| R?servations calcul?es mai | `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx` / `VUE_FLUX` | 2025-01 -> 2027-02 | 96 | Sortie d?riv?e, contr?le uniquement. |
| Historique r?servations mai | `02_DONNEES_NORMALISEES/historique_reservations/HIST_Reservations_Cloturees.xlsx` / `HIST_Reservations_Cloturees` | 2025-01 -> 2026-07 | 103 | Historique d?riv?, contr?le uniquement. |

## ? importer ou saisir avant recette

- Charges r?elles de mai 2026 : Saisir/importer les charges r?elles de mai dans `SAISIE_Charges_Flux.xlsx`. Le fichier existe mais ne contient aucune ligne mai.
- Relev? ou export bancaire de mai 2026 : Exporter/importer le relev? bancaire couvrant mai 2026 dans Lot8. Le fichier actuel couvre des mouvements hors mai et ne contient aucune ligne mai.
- Acomptes propri?taires de mai 2026 : Si un acompte propri?taire de mai existe, le saisir avec propri?taire, logement, mois, montant et justificatif. Aucune ligne mai trouv?e.
- AirCover r?el de mai 2026 : Si un AirCover mai existe, saisir b?n?ficiaire r?el, montant, justificatif et traitement. Aucune ligne mai trouv?e.
- Versements Airbnb ? imputer de mai 2026 : Apr?s import banque mai, saisir uniquement les versements Airbnb rattach?s avec certitude ? propri?taire + logement + mois + document. Aucune ligne mai trouv?e.
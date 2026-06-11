# JOURNAL_CONTROLES.md
> Un contrôle exécuté sur données réelles = une entrée. Vide tant qu'aucun lot n'a tourné sur fichier réel.

---

## Format d'entrée

```
Date       : AAAA-MM-JJ
Lot        : Lot X — Nom
Code       : CODE_CONTROLE (convention : voir §Référence ci-dessous)
Sévérité   : BLOQUANT | A_CONTROLER | INFO
Fichier    : fichier testé
Résultat   : description précise
Statut     : OUVERT | CORRIGÉ | IGNORE_JUSTIFIE
Commentaire: note
```

---

## Contrôles exécutés

---

### CTR-2026-06-001

```
Date       : 2026-06-07
Lot        : Lot 0 — Stabiliser REF_Setup
Code       : AUDIT_REF_SETUP_LOT0_INITIAL
Sévérité   : BLOQUANT
Fichier    : 01_SOURCES_BRUTES/REF_Setup.xlsx.xlsm (chemin pré-correction)
Résultat   : Audit sur données réelles. 19 onglets présents, 0 doublon de clé, 0 date série brute.
             6 anomalies bloquantes détectées :
             B1 - Mojibake (REF_Associes, REF_Codes_Impact, REF_Types_Flux, REF_Types_Affectation — 26 cellules)
             B2 - REF_Statuts : VALIDE / BLOQUANT / IGNORE_JUSTIFIE absents
             B3 - REF_Statuts_Payout : onglet absent
             B4 - REF_Cloture_Mensuelle : onglet absent
             B5 - REF_Parametres_Generaux : 4 params manquants (TAUX_HORAIRE_MENAGE_INTERNE,
                  ARRONDI_DECIMALES, TOLERANCE_ARRONDI_LIGNE_EUR, TOLERANCE_ARRONDI_CUMUL_EUR)
             B6 - REF_Intervenants : colonnes nom_normalise / date_debut_validite / date_fin_validite
                  absentes, type_intervenant en minuscules au lieu de MAJUSCULES
             Anomalie importante I1 : APPARTEMENT_DIVERS et LOGEMENT_DIVERS absents de REF_Logements
             ANO-2026-06-003 (dates série) : sans objet, aucune série brute détectée
Statut     : CORRIGÉ (voir CTR-2026-06-002)
Commentaire: Lot 0 non validable avant correction. 6 bloquants + 2 onglets à créer.
```

---

### CTR-2026-06-002

```
Date       : 2026-06-07
Lot        : Lot 0 — Stabiliser REF_Setup
Code       : CORRECTIONS_REF_SETUP_LOT0
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (chemin canonique post-correction)
Résultat   : Corrections B1–B6 + I1 appliquées par script Python (lot0_corrections.py).
             Sauvegarde horodatée créée : 99_ARCHIVES/LOT0_REF_Setup/REF_Setup_BACKUP_20260607_113223.xlsx.xlsm
             Fichier déplacé vers chemin canonique : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
             B1 - Mojibake corrigé : 26 cellules (4 onglets)
             B2 - REF_Statuts : VALIDE / BLOQUANT / IGNORE_JUSTIFIE ajoutés (famille statut_controle)
             B3 - REF_Statuts_Payout : créé (6 valeurs fermées)
             B4 - REF_Cloture_Mensuelle : créé (structure vide, 7 colonnes)
             B5 - REF_Parametres_Generaux : 4 params ajoutés (TAUX_HORAIRE_MENAGE_INTERNE=10,
                  ARRONDI_DECIMALES=2, TOLERANCE_ARRONDI_LIGNE_EUR=0.10, TOLERANCE_ARRONDI_CUMUL_EUR=1.00)
             B6 - REF_Intervenants : 3 colonnes ajoutées, type_intervenant normalisé en MAJUSCULES,
                  nom_normalise calculé (IMENE, KHEIRA, MOUNIR, AISSATA, IMRANE)
             I1 - REF_Logements : APPARTEMENT_DIVERS et LOGEMENT_DIVERS ajoutés
Statut     : CORRIGÉ (audit post-correction : CTR-2026-06-003)
Commentaire: Script reproductible conservé dans 02_TRAVAIL/lot0_corrections.py
```

---

### CTR-2026-06-003

```
Date       : 2026-06-07
Lot        : Lot 0 — Stabiliser REF_Setup
Code       : AUDIT_REF_SETUP_LOT0_POST_CORRECTION
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : Audit post-correction complet. 21 onglets (19 originaux + REF_Statuts_Payout + REF_Cloture_Mensuelle).
             B1 - Mojibake : 0 cellule résiduelle — OK
             B2 - REF_Statuts : VALIDE / BLOQUANT / IGNORE_JUSTIFIE / A_CONTROLER présents — OK
             B3 - REF_Statuts_Payout : 6 valeurs fermées — OK
             B4 - REF_Cloture_Mensuelle : 7 colonnes, structure vide — OK
             B5 - REF_Parametres_Generaux : 7 params dont les 4 requis — OK
             B6 - REF_Intervenants : 11 colonnes, tous types MAJUSCULES, nom_normalise renseigné — OK
             I1 - REF_Logements : 19 logements dont APPARTEMENT_DIVERS et LOGEMENT_DIVERS — OK
             0 doublon de clé sur 8 onglets vérifiés — OK
             0 date série brute — OK
             Points restants non bloquants : CARTE_002 suffixe XXXX (Lot 8), mode_facturation A_DEFINIR (Lot 12).
             Questions métier tranchées : QM1 (coûts standards = exécution seule), QM2 (A_DEFINIR OK), QM3 (A_CONTROLER OK).
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 0 techniquement validable. Attente accord humain avant marquage FAIT dans ETAT_AVANCEMENT.md.
```

---

### CTR-2026-06-004

```
Date       : 2026-06-08
Lot        : Lot 1 — Module Hostaway (extraction + payout)
Code       : AUDIT_LOT1_HOSTAWAY_POST_FIX_FINANCEFIELD
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot1_Hostaway/ (run 20260608_134253)
Résultat   : Extraction complète Lot 1 sur données réelles. Fix financeField appliqué (1 ligne :
             _ff_from_res(detail) au lieu de parse_finance_fields(detail.get("money", {}))).
             17 listings (14 actifs, 3 archivés : 485104, 515523, 556954).
             1586 réservations API. 1391 traitées, 195 sautées (annulations sans montant — correct).
             86 appels détail Booking uniquement (6% — mode minimal validé).
             676 finance fields extraits (100% Booking — Airbnb via airbnbExpectedPayoutAmount).
             Payout : 1321 NORMAL, 59 A_CONTROLER (32 VRBO Unknown + 27 DIRECT), 0 INCOMPLET, 0 ABSENT.
             86 Booking 100% NORMAL via formule H2 (totalPriceFromChannel_formula) — fix confirmé.
             1235 Airbnb 100% NORMAL via airbnbExpectedPayoutAmount.
             55 anomalies A_CONTROLER (0 BLOQUANT) :
             - 32 VRBO_MONTANT_NON_RENSEIGNE (→ Lot 4)
             - 23 LISTING_ORPHELIN_A_CONTROLER (listings 515523 + 556954 absents REF — → Lot 2)
             CleaningTasks SKIPPED — à traiter Lot 6a via --only-cleaning-tasks.
             Points résiduels non bloquants :
             - ANO-004 mis à jour 29→32 VRBO
             - ANO-014 créée (listing 556954 T3 Montaudran, archivé, absent REF_Logements)
             - 59 A_CONTROLER à saisir manuellement (Lots 4 / 4bis)
Statut     : VALIDE
Commentaire: Lot 1 validé humainement le 2026-06-08. Script : 02_TRAVAIL/lot1_hostaway_extract.py.
```

---

### CTR-2026-06-005

```
Date       : 2026-06-08
Lot        : Lot 2 — Réconciliation logements
Code       : AUDIT_LOT2_MAPPING_LOGEMENTS
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (REF_Logements + REF_Mapping_Logements)
             02_TRAVAIL/Lot1_Hostaway/MASTER_REF_HA_Listings.xlsx
Résultat   : Réconciliation REF_Logements ↔ MASTER_REF_HA_Listings.
             17 listings Hostaway — 17/17 mappés, 0 orphelin résiduel.
             4 anomalies ANO-001/005/006/014 résolues par correction de mapping (pas de création de logement).
             Cause commune : deux logements (LOG_0009, LOG_0016) avaient été retirés puis recréés
             dans Hostaway → nouveaux listingMapId (556954, 515523) non encore reflétés dans le REF.
             Corrections appliquées :
             - LOG_0009 : hostaway_listing_id 497801 → 556954 ; nom "T3 Montaudran" ; commentaire ancien nom.
             - LOG_0016 : hostaway_listing_id 480780 → 515523 ; sur_hostaway NON → OUI.
             - REF_Mapping_Logements : 5 lignes ajoutées (MAP_LOG_0082 à 0086), total 86 lignes.
             - Anciens IDs 480780 et 497801 conservés comme alias historiques actifs (MAP_LOG_0039/0073).
             Contrôles vérifiés :
             - 0 listingMapId actif Hostaway non mappé.
             - Anciens IDs 480780 / 497801 résolvables → logement_id correct.
             - LOG_0009 et LOG_0016 : hostaway_listing_id, sur_hostaway, nom conformes post-correction.
             Google Sheet Suivi ménage : reporté au Lot 6b (M04) — non traité ici.
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 2 techniquement validé. Attente accord humain avant marquage FAIT dans ETAT_AVANCEMENT.md.
             Backup : 99_ARCHIVES/LOT2_Mapping/REF_Setup_BACKUP_20260608_153507.xlsm
```

---

### CTR-2026-06-006

```
Date       : 2026-06-08
Lot        : Lot 3 — SAISIE_Charges_Flux.xlsx
Code       : AUDIT_LOT3_SAISIE_MASTER_CHARGES
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
             01_SOURCES_BRUTES/Charges/SAISIE_Charges_Flux.xlsx
             02_TRAVAIL/Lot3_Charges/MASTER_FACT_MAN_Charges.xlsx
Résultat   : Contrôle final Lot 3 — 21 points vérifiés, 0 FAIL.
             REF_Setup.xlsm :
             - REF_Categories_Charges : 23 lignes, col 9 filtre_vue_menage présente.
             - CHG_016 filtre_vue_menage=NON ; CHG_021/022/023 présents ; CHG_023 filtre_vue_menage=OUI.
             - REF_Types_Flux : TYPE_FLUX_009 (ACHAT_MENAGE), 010 (FRAIS_LOCAL),
               011 (CHARGE_EXCEPTIONNELLE_REFACTURABLE), 012 (CHARGE_RECURRENTE_REFACTURABLE) présents.
             - REF_Charges_Recurrentes : REC_001 → TYPE_FLUX_012, REC_002 → TYPE_FLUX_010.
             SAISIE_Charges_Flux.xlsx :
             - 4 onglets (SAISIE, REF_LOCALE, CONTROLES_SAISIE, README).
             - 31 colonnes SAISIE conformes au cadrage validé (groupes 1 à 11).
             - 18 listes REF_LOCALE avec Named Ranges.
             - 13 contrôles CONTROLES_SAISIE.
             - Formules calculées sur 500 lignes : mois, impact_resultat_reel,
               impact_resultat_comptable, ROW_HASH.
             MASTER_FACT_MAN_Charges.xlsx :
             - 3 onglets (MASTER 37 cols, VUE_MENAGE 37 cols identiques, POWER_QUERY_CODE).
             - 6 colonnes PQ présentes : sens, filtre_vue_menage, source_module,
               source_table, source_pk, date_integration.
             - 4 requêtes M-code présentes dans POWER_QUERY_CODE.
             - Filtre VUE : [filtre_vue_menage]="OUI" AND [statut_controle]="VALIDE".
             Git status : 5 fichiers modifiés (tous Lot 3), 0 supprimé, 0 Lot 4.
Statut     : VALIDÉ
Commentaire: Lot 3 validé humainement le 2026-06-08. Marquage FAIT autorisé.
             Décisions D044 (statuts) et D045 (REF_Charges_Recurrentes) verrouillées.
             REFACTURATION → sens=CHARGE validé (charge avancée, récupérée sur propriétaire).
```

### CTR-2026-06-007

```
Date       : 2026-06-09
Lot        : Lot 4 — Réservations hors Hostaway
Code       : AUDIT_LOT4_SAISIE_MASTER_RESHH_STRUCTURE
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (lecture V1/V2)
             01_SOURCES_BRUTES/ReservationsHH/SAISIE_ReservationsHorsHostaway.xlsx
             02_TRAVAIL/Lot4_ReservationsHH/MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx
Résultat   : Audit structurel Lot 4 — 0 FAIL.
             V1 REF_Canaux_Reservation : CANAL_003 VRBO / CANAL_004 Direct / CANAL_005 Autre présents, actifs.
               Aucune modification REF_Setup.xlsm nécessaire.
             V2 REF_Proprietaires : colonne taux_commission présente (12 proprios actifs, valeurs réelles 0.15–0.19).
               REF_Logements sans taux → source = REF_PROPRIETAIRE via VLOOKUP.
             SAISIE_ReservationsHorsHostaway.xlsx :
               - 4 onglets (SAISIE, REF_LOCALE, CONTROLES_SAISIE, README).
               - 30 colonnes SAISIE conformes au cadrage validé (groupes 1 à 9).
               - 11 listes REF_LOCALE + lookup taux 2-col (lst_PropTauxLookup) pour VLOOKUP taux_commission.
               - 11 DV (10 plan + niveau_anomalie pour cohérence Lot 3).
               - 13 contrôles CONTROLES_SAISIE (CTR-L4-01 à CTR-L4-13).
               - Formules calculées 500 lignes : mois, nuits, taux_commission, taux_commission_source,
                 commission, acompte_facture, impact_resultat_reel, impact_resultat_comptable, ROW_HASH.
               - MFC : rouge = BLOQUANT / orange = A_CONTROLER (sur colonne AB niveau_anomalie).
             MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx :
               - 3 onglets (MASTER 34 cols, VUE_ACTIVE 34 cols, POWER_QUERY_CODE).
               - 4 colonnes PQ : source_module, source_table, source_pk, date_integration.
               - 3 requêtes M-code : SAISIE_HH_Source, MASTER_FACT_MAN_ReservationsHorsHostaway, VUE_ACTIVE_ReservationsHH.
               - Filtre VUE_ACTIVE : [statut_controle]="VALIDE".
             Décisions verrouillées : D046–D051 (QM-L4-01 à QM-L4-06).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — table vide, saisie non encore effectuée.
             La saisie des 32 VRBO (ANO-004) et des réservations directes se fait manuellement.
             Lot 4 peut être marqué FAIT après validation humaine du fichier produit.
```

### CTR-2026-06-008

```
Date       : 2026-06-09
Lot        : Lot 4bis — Table commune des réservations
Code       : AUDIT_LOT4BIS_MASTER_CALC_RESERVATIONS_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx
             02_TRAVAIL/lot4bis_master_calc_reservations.py
Résultat   : Contrôle structurel Lot 4bis — 0 FAIL.
             Script Python exécuté avec succès.
             MASTER_CALC_Reservations.xlsx créé :
             - 3 onglets : MASTER (24 cols) / VUE_FLUX (24 cols) / POWER_QUERY_CODE.
             - 24 colonnes : 2 identification + 6 rattachement + 3 séjour + 2 financier
               + 3 impact + 4 statut (incl. niveau_anomalie + code_anomalie) + 4 système PQ.
             - 7 requêtes M-code : HA_Reservations_Source / HA_Payout_Source / HH_Source /
               REF_Mapping_Source / REF_Logements_Source / MASTER_CALC_Reservations /
               VUE_FLUX_Reservations.
             Anti-double-comptage : 7 scénarios (S1–S7) — lignes HA liées à HH exclues
               de la branche HA avant empilement.
             2 contrôles BLOQUANTS : RESERVATION_DOUBLON_HOSTAWAY_HH + RESERVATION_CALC_ID_DUPLIQUE.
             6 contrôles A_CONTROLER : RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH /
               RESERVATION_VRBO_MONTANT_NON_RENSEIGNE (ANO-004) / RESERVATION_PAYOUT_MANQUANT /
               RESERVATION_LOGEMENT_NON_MAPPE / RESERVATION_MAPPING_MULTIPLE /
               RESERVATION_HH_NON_VALIDE.
             VUE_FLUX : filtre VALIDE + impact_resultat_reel=OUI + montant_retenu≠0 + non nul.
             Décisions verrouillées : D052–D057 (QM-L4b-01 à QM-L4b-06).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — table vide, données non encore chargées via PQ.
             La saisie HH et le refresh PQ doivent être effectués avant Lot 9.
             Lot 4bis peut être marqué FAIT après validation humaine.
```

### CTR-2026-06-009

```
Date       : 2026-06-09
Lot        : Lot 5 — Acomptes propriétaires
Code       : AUDIT_LOT5_SAISIE_MASTER_ACOMPTES_STRUCTURE
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/AcomptesProprietaires/SAISIE_AcomptesProprietaires.xlsx
             02_TRAVAIL/Lot5_AcomptesProprietaires/MASTER_FACT_MAN_AcomptesProprietaires.xlsx
             02_TRAVAIL/lot5_master_acomptes_proprietaires.py
Résultat   : Contrôle structurel Lot 5 — 0 FAIL.
             Script Python exécuté avec succès.
             REF_Setup.xlsm : non modifié — TYPE_FLUX_006 (ACOMPTE_FACTURE_PROPRIETAIRE)
               déjà présent, aucun ajout nécessaire.
             SAISIE_AcomptesProprietaires.xlsx :
               - 4 onglets : SAISIE (18 cols) / REF_LOCALE / CONTROLES_SAISIE / README.
               - 18 colonnes SAISIE conformes : 2 identification + 6 rattachement + 2 financier
                 + 1 mode + 3 impact + 4 statut.
               - 5 DV : source_acompte (liste fermée) / statut_controle (liste fermée) /
                 proprietaire_id (REF_LOCALE) / logement_id (REF_LOCALE) / mode_paiement_id (REF_LOCALE).
               - REF_LOCALE : 12 propriétaires actifs / 16 logements actifs avec proprietaire_id /
                 5 modes de paiement actifs.
               - 10 contrôles CONTROLES_SAISIE : 5 BLOQUANT + 5 A_CONTROLER.
             MASTER_FACT_MAN_AcomptesProprietaires.xlsx :
               - 3 onglets : MASTER (22 cols) / VUE_ACTIVE (22 cols) / POWER_QUERY_CODE.
               - 22 colonnes MASTER : 18 SAISIE + 4 PQ (source_module / source_table / source_pk / date_integration).
               - 5 requêtes M-code : Q1_SAISIE_ACC_Source / Q2_HH_Acomptes_Ref /
                 Q3_REF_Proprietaires_Source / Q4_MASTER_FACT_MAN_AcomptesProprietaires /
                 Q5_VUE_ACTIVE_AcomptesProprietaires.
               - source_table = SAISIE_AcomptesProprietaires (toujours) — D064.
               - source_pk = acompte_id (toujours) — D064.
               - source_hh_id = reservation_hh_id si HH_RESERVATION / null sinon — D064.
               - Filtre VUE_ACTIVE : statut_controle = VALIDE.
             Décisions verrouillées : D058–D064 (QM-L5-01 à QM-L5-07).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — table vide, saisie non encore effectuée.
             Aucun backup REF_Setup.xlsm nécessaire (aucune modification).
             report_mois_suivant supprimé (D061) — report_mois_precedent conservé informatif.
             Lot 5 peut être marqué FAIT après validation humaine du fichier produit.
```

---

### CTR-2026-06-010

```
Date       : 2026-06-09
Lot        : Lot 6a — Hostaway CleaningTasks comptage ménages
Code       : AUDIT_LOT6A_CLEANING_TASKS_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx
             02_TRAVAIL/lot6a_cleaning_tasks_comptage.py
Résultat   : Contrôle structurel Lot 6a — 0 FAIL.
             Extraction segmentée par listingMapId (D065 — méthode fiable).
             Anti-plafond : 17 requêtes, 0 segment >= 500, exhaustivité prouvée.
             REF_Setup.xlsm : non modifié.

             Extraction :
               - Requêtes API      : 17 (1 par listing REF_Logements actifs + inactifs)
               - Tâches brutes     : 500
               - Tâches uniques    : 500 (après déduplication par task_id)
               - Doublons supprimés: 0
               - Segments plafonnés: aucun (max=51 pour listings 480139/480140)

             data (11 cols, 500 tâches brutes) :
               - H6 : cost=NULL sur 500/500 tâches dans onglets.
                 Note : 22/500 tâches avaient un coût dans l'API (50–70 EUR) — forcé NULL.
               - Dates : 2026-02 → 2027-02. Jan 2026 absent (D066 : normal).
               - 14 listings actifs, 3 listings avec 0 tâche (482324, 556954, 515523).
               - 1 tâche sans reservation_id, 0 sans listingMapId, 0 sans scheduled_date.
               - autoTaskId distincts : 15.

             MASTER_ENRICHI (21 cols, 500 lignes) :
               - BLOQUANT=0, A_CONTROLER=39, OK=461.
               - A_CONTROLER : 22 TASK_LOGEMENT_INACTIF (LOG_0003/485104, D068)
                 + 16 TASK_STATUT_PENDING (D067) + 1 TASK_SANS_RESERVATION.
               - Statuts : réalisé=325, prévu=74, A_CONTROLER=16, annulé=85.
               - type_ligne_menage_id=TLM_001 par défaut (D069).

             VUE_COMPTAGE (11 cols, 95 lignes mois×logement) :
               - BLOQUANT=0, 325 ménages réalisés (tous mois).
               - Mois couverts : 2026-02 → 2026-06 (complets), 2026-07+ (planifiés).

             POWER_QUERY_CODE : code M de référence (Q1 à Q4).
             Décisions verrouillées : D065–D069.
Statut     : VALIDÉ
Commentaire: Extraction segmentée remplace le single call défaillant (offset ignoré).
             Lot 6a remplace l'extraction --only-cleaning-tasks du Lot 1 (était vide).
             LOG_0003 (485104, actif=NON) : 22 tâches historiques mappées, A_CONTROLER.
             Lot 6a peut être marqué FAIT après validation humaine.
```

---

### CTR-2026-06-011

```
Date       : 2026-06-09
Lot        : Lot 6b — M04 Ménages internes main-d'œuvre
Code       : AUDIT_LOT6B_M04_MENAGES_STRUCTURE
Sévérité   : INFO
Fichier    : 02_DONNEES_NORMALISEES/menages/M04_MENAGES_PowerQuery.xlsx
             02_TRAVAIL/lot6b_m04_menages_internes.py
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (modification TYPE_FLUX_013)
Résultat   : Contrôle structurel Lot 6b — 0 FAIL.
             Script Python exécuté le 2026-06-09 (tag 20260609_152826).

             REF_Setup.xlsm :
               - Backup créé : 99_ARCHIVES/LOT6B_Menages/REF_Setup_BACKUP_20260609_152826.xlsm
               - TYPE_FLUX_013 (COUT_MO_INTERNE_MENAGE) ajouté dans REF_Types_Flux.
               - Colonnes : HC / NON / NON / NON / OUI confirmés.

             M04_MENAGES_PowerQuery.xlsx :
               - 8 onglets : SOURCE_RAW / PARAM_TAUX_INTERVENANTS / PARAMETRES_M04 /
                 MASTER / VUE_ACTIVE / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE / README
               - MASTER : 34 colonnes (2 IDENT + 8 RATT + 2 INTERV + 10 CALCUL + 5 FLUX + 3 STATUT + 4 SYSTEME)
               - PARAM_TAUX_INTERVENANTS : 5 intervenants (INT_0001-005), taux 10€/h INTERNE.
               - PARAMETRES_M04 : SEUIL_ECART_STANDARD_MENAGE = 10, actif=OUI.
               - POWER_QUERY_CODE : 10 requêtes M (Q1..Q9 + Q6B).
               - SOURCE_RAW : squelette vide, colonnes attendues documentées (D070).
               - Chemins PQ placeholder C:\CHEMIN_A_ADAPTER\ — à adapter localement.

             Contrôles documentés :
               BLOQUANTS : MENAGE_SANS_LOGEMENT_ID / M04_SCHEMA_SOURCE_INVALIDE /
                           MENAGE_INTERNE_CODE_IMPACT_NON_HC
               A_CONTROLER : MENAGE_EXTERNE_DANS_M04 / TYPE_INTERVENANT_ABSENT /
                             TAUX_ABSENT_INTERVENANT_INTERNE / TAUX_MULTIPLE_INTERVENANT /
                             MENAGE_RANGEMENT_A_CONTROLER / MENAGE_DOUBLON_POTENTIEL /
                             MENAGE_ECART_NEGATIF_IMPORTANT / MENAGE_ECART_HOSTAWAY_M04

             Décisions verrouillées : D070–D078 (QM-L6b-01 à QM-L6b-05 + corrections).
Statut     : VALIDÉ
Commentaire: Contrôle structurel uniquement — SOURCE_RAW vide, saisie GSheet non encore effectuée.
             Adaptation chemins PQ requise avant premier run Power Query.
             REC_002 cle_repartition mise à jour : COUT_STANDARD_MENAGES_MOIS (ancienne valeur : NOMBRE_MENAGES).
             Validation humaine Lot 6b — aucune action différée sur REC_002.
             Lot 6b peut être marqué FAIT après validation humaine du fichier produit.
```

---

### CTR-2026-06-012

```
Date       : 2026-06-09
Lot        : Lot 6c — Ménages externes (factures PDF prestataires)
Code       : AUDIT_LOT6C_MENAGES_EXTERNES_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx
             02_TRAVAIL/lot6c_menages_externes.py
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (modifications D086)
             01_SOURCES_BRUTES/MenagesExternes/Factures_PDF/Facture mai Aissata.pdf
             01_SOURCES_BRUTES/MenagesExternes/Factures_PDF/Facture mai Mounir.pdf
Résultat   : Contrôle structurel Lot 6c — 0 BLOQUANT, 9 A_CONTROLER, 4 VALIDE.
             Script Python exécuté le 2026-06-09 (tag 20260609_170606).

             REF_Setup.xlsm :
               - Backup : 99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm
               - REF_Intervenants : +3 colonnes (nom_legal, siret_rcs, email_facturation).
                 INT_0003 Mounir → MH Entreprise / RCS 792015919.
                 INT_0004 Aissata → Kandia DIABATE / SIRET 10147251200017.
               - REF_Types_Flux : TYPE_FLUX_014 (COUT_REEL_MENAGE_EXTERNE) ajouté.

             MASTER_FACT_MEN_MenagesExternes.xlsx :
               - 7 onglets : SOURCE_RAW / PARAMETRES / MASTER / VUE_ACTIVE /
                 VUE_ECART_HOSTAWAY / POWER_QUERY_CODE / README
               - SOURCE_RAW : 13 lignes (9 Aissata + 4 Mounir — ligne 8 splittée en 8a/8b)
               - MASTER : 13 lignes — 49 colonnes (11 blocs)
               - VUE_ACTIVE : 4 lignes VALIDE (lignes 1, 7, 8a, 8b Aissata — dates précises)
               - VUE_ECART_HOSTAWAY : 16 logements mois=2026-05

             Réconciliation factures :
               - FAC-2026-05-AISSATA-001 : somme=1 439,00€ = total — écart=0,00€ VALIDE
               - FAC-2026-05-MOUNIR-001  : somme=942,00€   = total — écart=0,00€ VALIDE

             Anomalies A_CONTROLER (9 lignes) :
               - 5 lignes Aissata : MENAGE_EXTERNE_DATE_ABSENTE (D087 Option B)
               - 3 lignes Mounir  : MENAGE_EXTERNE_DATE_ABSENTE
               - 1 ligne Mounir T2-65 Gabriel : MENAGE_EXTERNE_MONTANT_NUL +
                 MENAGE_EXTERNE_LOGEMENT_INACTIF (0 ménage, logement inactif 2026-04-26)

             Observations factures :
               - Studio Puits vert : présent chez Aissata (10 pass.) ET Mounir (1 pass.) — non doublon.
               - T3 20 rue Amiral Galache (LOG_0016) : facturé T3 par Aissata, REF = T2. Noté commentaire.
               - Prix Mounir (32/36/52/65€) différents des coûts standards REF — normal.
               - Prix Aissata (29/39/55€) = coûts standards REF — coïncidence, non bloquant.

             Décisions verrouillées : D079–D088 (D-6c-01 à D-6c-10).
             POWER_QUERY_CODE : 7 requêtes M (Q1–Q7).
Statut     : VALIDÉ
Commentaire: Contrôle structurel et données mai 2026 peuplées. 9 lignes A_CONTROLER en attente
             de dates précises de ménage auprès des prestataires (D087 Option B).
             REF_Mapping_Logements à compléter avec les noms factures prestataires pour futurs runs PQ.
             TYPE_FLUX_014 intégrera MASTER_CALC_Flux au Lot 9.
             Lot 6c peut être marqué FAIT après validation humaine du fichier produit.
```

---

### CTR-2026-06-016

```
Date       : 2026-06-11
Lot        : Lot 4bis — Table commune réservations (correctif peuplement)
Code       : CORRECTIF_LOT4BIS_PEUPLEMENT_MASTER_CALC_RESERVATIONS
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot4bis_charger_reservations.py (créé)
             02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx (modifié)
             .gitignore (modifié — ajout 99_ARCHIVES/LOT4BIS_TableCommune/)
Résultat   : MASTER_CALC_Reservations.xlsx peuplé par script Python reproductible.
             Squelette Power Query (non exécutable) remplacé par approche script Python.
             Backup créé : 99_ARCHIVES/LOT4BIS_TableCommune/MASTER_CALC_Reservations_BACKUP_20260611_200317.xlsx

             Volumes produits :
               MASTER   : 1 391 lignes (24 colonnes)
               VUE_FLUX : 1 321 lignes (VALIDE + impact_resultat_reel=OUI + montant≠0)

             Répartition par source :
               HOSTAWAY_AIRBNB            : 1 235 (S1)
               HOSTAWAY_BOOKING           :    86 (S2)
               HOSTAWAY_DIRECT_HH         :    27 (cas non couvert D054 — validé 2026-06-11)
               HOSTAWAY_VRBO_A_CONTROLER  :    32 (S5)
               OWNERSTAY_EXCLU            :    11 (S7)
               HH (S3/S4/S6)             :     0 (SAISIE_ReservationsHorsHostaway vide)

             Répartition par statut_controle :
               VALIDE         : 1 321
               A_CONTROLER    :    59
               EXCLU_RESULTAT :    11

             Répartition par code_impact :
               IC : 1 321
               HC :    59
               HR :    11

             Anomalies A_CONTROLER :
               DIRECT_SANS_SAISIE_HH      : 27 — DIRECT Hostaway sans saisie HH, à saisir
               VRBO_MONTANT_NON_RENSEIGNE : 32 — VRBO sans montant, en attente saisie HH
               LOGEMENT_INACTIF           :  0 — règle date_sortie_gestion appliquée

             Règle date_sortie_gestion validée (2026-06-11) :
               - date_arrivee < date_sortie ET date_depart <= date_sortie → VALIDE
               - date_arrivee < date_sortie ET date_depart > date_sortie  → A_CONTROLER (SEJOUR_CHEVAUCHE_SORTIE_GESTION)
               - date_arrivee >= date_sortie                              → A_CONTROLER (LOGEMENT_INACTIF)
               - date_sortie absente ET actif=NON                         → A_CONTROLER (LOGEMENT_INACTIF)

             LOG_0003 (T2 - 65 Gabriel, sorti gestion 2026-04-26) :
               76 réservations historiques — toutes VALIDE (arrivées avant date_sortie)
               0 SEJOUR_CHEVAUCHE_SORTIE_GESTION (tous départs avant 2026-04-26)

             Contrôles BLOQUANTS : 0 détecté
             Données personnelles voyageur dans MASTER/VUE_FLUX : AUCUNE (vérifié)
             Fichiers sources : NON modifiés
             HH : 0 ligne (SAISIE_ReservationsHorsHostaway vide — saisie manuelle à venir)
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Script reproductible. Relancer lot4bis_charger_reservations.py après toute
             modification de SAISIE_ReservationsHorsHostaway pour inclure les HH.
             Lot 9 reste BLOQUÉ jusqu'à validation et commit du correctif.
```

---

### CTR-2026-06-015

```
Date       : 2026-06-09
Lot        : Lot 7 — IK & Avantages associés
Code       : AUDIT_LOT7_IK_AVANTAGES_STRUCTURE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot7_IK_Avantages/MASTER_FACT_MAN_IK_Avantages.xlsx
             02_TRAVAIL/lot7_ik_avantages.py
             01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : Construction structurelle Lot 7 — structure vide, 0 donnée fictive.

             REF_Setup.xlsm :
               - Backup : 99_ARCHIVES/LOT7_IK_Avantages/REF_Setup_BACKUP_20260609_192200.xlsm
               - REF_Types_Flux : TYPE_FLUX_015 (INDEMNITE_KILOMETRIQUE) ajouté.
                 IC — avantage_brut=OUI — justificatif obligatoire.

             MASTER_FACT_MAN_IK_Avantages.xlsx :
               - 6 onglets : SOURCE_SAISIE / PARAMETRES / MASTER_SAISIE /
                 MASTER_CALC_AVANTAGES / POWER_QUERY_CODE / README
               - MASTER_SAISIE      : 22 colonnes — structure vide
               - MASTER_CALC_AVANTAGES : 15 colonnes — structure vide — multi-mois
               - SOURCE_SAISIE      : 9 colonnes saisie brute + ligne instructions
               - PARAMETRES         : REF_Associes (2 associés) + REF_Types_Flux
                 (7 types pertinents) + REF_Modes_Paiement (5 modes) +
                 valeurs fermées statuts / niveau_anomalie / type_remboursement
               - POWER_QUERY_CODE   : 5 requêtes M (Q1-Q5)

             Décisions verrouillées : D089–D095 (D-7-01 à D-7-07).
             Pré-correction : D096 (LOG_0016 T2→T3, CTR-2026-06-014).

             Contrôles implémentés dans Q3 :
               BLOQUANTS (6) : AVANTAGE_ASSOCIE_SANS_ASSOCIE_ID /
                 AVANTAGE_ASSOCIE_MONTANT_INVALIDE / AVANTAGE_SANS_TYPE_FLUX /
                 MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES /
                 DOUBLE_COMPTAGE_SAISIE_ET_DERIVE /
                 SAISIE_LOT7_SOURCE_DEJA_EXISTANTE
               A_CONTROLER (6) : REMBOURSEMENT_SANS_LIEN_ORIGINE /
                 REMBOURSEMENT_SENS_ABSENT / VIREMENT_ASSOCIE_SANS_LIEN_BANQUE /
                 IK_SANS_JUSTIFICATIF / AVANTAGE_NET_NEGATIF /
                 TYPE_FLUX_IMPACT_INCOHERENT

             Sources dérivées (à 0 jusqu au peuplement) :
               - avantage_brut_depenses_perso      : Lot 3 TYPE_FLUX_002
               - avantage_brut_montant_recupere_hh : Lot 4 via reservation_hh_id
               - charges_payees_pour_societe        : Lot 3 TYPE_FLUX_004+008

             Points résiduels non bloquants :
               - Q4/Q5 : jointures Lot 3 et Lot 4 codées mais
                 sources à 0 — à activer quand Lot 3 et Lot 4 peuplés.
               - Virements associés mai 2026 : à saisir manuellement
                 quand données disponibles (banque non traitée).

             Lot 7 structure validée. Données à peuplier par saisie.
Statut     : VALIDÉ
Commentaire: Aucune donnée fictive. Toutes les colonnes et contrôles sont en place.
             TYPE_FLUX_015 actif dans REF_Setup.xlsm.
             Lot 8 (banque) alimentera les virements associés sans ressaisie.
```

---

### CTR-2026-06-014

```
Date       : 2026-06-09
Lot        : Correction référentiel — pré-Lot 7
Code       : CORRECTION_REF_LOG0016_TYPE_LOGEMENT
Sévérité   : INFO
Fichier    : 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm
Résultat   : Correction type logement LOG_0016 (Cyprien / Clarisse).

             REF_Setup.xlsm :
               - Backup : 99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_REFCORR_LOG0016_20260609_190848.xlsm
               - REF_Logements LOG_0016 :
                   type_logement_id  : TYPE_002 → TYPE_003
                   nom_logement_officiel : T2 - Cyprien (Clarisse) → T3 - Cyprien (Clarisse)
                   nom_court         : T2 - Cyprien → T3 - Cyprien
               - REF_Mapping_Logements :
                   MAP_LOG_0075 (Facture ménage externe) : T2 - Cyprien (Clarisse) → T3 - Cyprien (Clarisse)
                   MAP_LOG_0076 (Nom court interne)      : T2 - Cyprien → T3 - Cyprien
                   MAP_LOG_0074 (Hostaway internalName)  : non modifié (donnée source externe)
                   MAP_LOG_0083 (Hostaway listing public): non modifié (pas de mention T2/T3)

             Impact lots commités :
               - Lot 6a (CleaningTasks) : LOG_0016 absent du MASTER_ENRICHI — aucun recalcul.
               - Lot 6b (M04)           : LOG_0016 absent de M04 — aucun recalcul.
               - Lot 6c (ménages ext.)  : LOG_0016 présent (MENEXT-2026-05-AISSATA-008/009).
                   type_logement_id absent du MASTER Lot 6c — structure non impactée.
                   montant_ligne_ttc = 55€ (tarif T3) déjà correct — aucun recalcul financier.
               - Lot 7                  : construction non démarrée — référentiel propre.

             Coût standard applicable post-correction :
               COUT_MEN_003 = 55€ (TYPE_003 T3) — au lieu de COUT_MEN_002 = 39€ (TYPE_002 T2).

             Décision verrouillée : D096.
Statut     : VALIDÉ
Commentaire: Correction purement référentielle. Aucun fichier MASTER à recalculer.
             L'internalName Hostaway "T2 - Cyprien (Clarisse)" reste à corriger côté Hostaway si nécessaire.
```

---

## Référence des codes de contrôle (source : ARCHITECTURE_DONNEES.md §18)

> Convention : codes tirés de l'architecture. Ne pas inventer de nouveaux codes sans les ajouter ici ET dans l'architecture.

### BLOQUANTS (calcul exclu, clôture / facturation impossible)

| Code | Module | Déclencheur |
|---|---|---|
| `BOOKING_PAYOUT_INCOMPLET` | Hostaway | Réservation Booking active sans payout calculable |
| `ACOMPTE_NON_RATTACHE_FACTURE` | Acomptes | Acompte sans facture_ref renseignée |
| `MONTANT_RECUPERE_HH_NON_REPRIS_AVANTAGES` | HH / Avantages | montant_recupere HH non reflété dans les avantages associés |
| `CHARGE_LOGEMENT_SANS_LOGEMENT_ID` | Charges | Charge avec affectation LOGEMENT mais logement_id absent |
| `CHARGE_PERSO_SANS_ASSOCIE` | Charges | Charge perso/liquide sans associe_id |
| `RESERVATION_HH_SANS_PROPRIETAIRE` | HH | Réservation hors Hostaway sans proprietaire_id |
| `ACOMPTE_HH_INCOHERENT` | HH | Acompte ≠ Total − Ménage − Commission − Reversé |
| `MENAGE_SANS_LOGEMENT_ID` | M04 / Ménages | Appartement ménage non rattaché à un logement du référentiel |
| `MENAGE_INTERNE_CODE_IMPACT_NON_HC` | M04 | Ligne M04 avec code_impact ≠ HC |
| `BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY` | Banque | Tentative double comptage banque ↔ Hostaway |
| `BANQUE_DATE_INEXPLOITABLE` | Banque | Ligne bancaire sans aucune date exploitable (Date ET Valeur absentes ou non parsables) |
| `BANQUE_DEBIT_CREDIT_VIDES` | Banque | Débit et Crédit simultanément vides |
| `BANQUE_DEBIT_CREDIT_DOUBLES` | Banque | Débit et Crédit simultanément renseignés |
| `BANQUE_MONTANT_NON_NUMERIQUE` | Banque | Montant non convertible en nombre |
| `CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Au moins une ligne non classée ouverte sur le mois |
| `M04_SCHEMA_SOURCE_INVALIDE` | M04 | Colonne obligatoire absente dans Google Sheet ou requête PQ échoue |
| `RESERVATION_DOUBLON_HOSTAWAY_HH` | Table commune | reservation_id_hostaway rattaché à 2+ lignes sans lien explicite |
| `LOCAL_50_DOUBLE_COMPTAGE_POTENTIEL` | Obsolète | **Remplacé par `ACHATS_DEJA_EN_SAISIE_CHARGES`** |
| `ACHATS_DEJA_EN_SAISIE_CHARGES` | M04 / Charges | Charge présente dans `SAISIE_Charges_Flux` aussi injectée depuis M04 |
| `ACOMPTE_AIRBNB_INCLUS_NET_EXPLOITATION` | Exploitation | `acompte_conciergerie_recu_via_airbnb` comptabilisé dans le revenu net — bloquant |
| `ACHAT_EXCEPTIONNEL_INCLUS_NET_EXPLOITATION` | Exploitation | Achat ou charge exceptionnelle inclus dans `revenu_net_exploitation` — bloquant |
| `CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE` | Exploitation | Charge non récurrente dans `charge_fixe_mensuelle` — bloquant |
| `PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT` | Exploitation | Paiement déduit du `total_payout` au lieu du `reste_a_payer` — bloquant |
| `CONFUSION_PAYOUT_SOLDE_FACTURE` | Exploitation | Confusion entre `total_payout`, `montant_du_conciergerie`, `reste_a_payer` — bloquant |
| `PK_MANQUANTE_OU_DOUBLONNEE` | Transverse | PK absente ou en doublon dans une table master |

### À CONTRÔLER (intégrés au calcul provisoire, non bloquants)

| Code | Module | Déclencheur |
|---|---|---|
| `LISTING_ORPHELIN_A_CONTROLER` | Hostaway | listingMapId dans export, absent de REF_Logements |
| `REFERENTIEL_ORPHELIN` | REF | Logement sur_hostaway=OUI absent de l'export Hostaway |
| `VRBO_MONTANT_NON_RENSEIGNE` | VRBO | Réservation VRBO Unknown sans saisie manuelle |
| `RESERVATION_HOSTAWAY_DIRECT_AVEC_MONTANT_SANS_HH` | Table commune | direct Hostaway avec totalPrice > 0, pas de ligne HH |
| `CANCELLED_AVEC_MONTANT` | Hostaway | Réservation annulée avec montant → règle D030 s'applique automatiquement (statut ANNULE_AVEC_PAYOUT) |
| `BANQUE_FICHIER_PERIODE_INCOHERENTE` | Banque | Dates réelles du fichier hors période nominale du nom |
| `BANQUE_LIGNE_SANS_DATE` | Banque | Colonne Date vide mais Valeur présente (non bloquant si Valeur exploitable) |
| `BANQUE_DEVISE_NON_EUR` | Banque | Devise ≠ EUR |
| `DOUBLON_BANCAIRE_POTENTIEL` | Banque | Empreinte bancaire déjà connue |
| `LIGNE_BANCAIRE_NON_CLASSEE` | Banque | Aucune règle ni classification fiable |
| `IA_CONFIANCE_INSUFFISANTE` | Banque | Score de confiance IA sous le seuil |
| `MENAGE_SANS_COUT_STANDARD` | Ménages | Type ou coût standard absent du référentiel |
| `ECART_ARRONDI_LIGNE_SUPERIEUR_TOLERANCE` | Transverse | Écart d'arrondi > 0,10 € sur une ligne (D035) |
| `ECART_ARRONDI_FACTURE_SUPERIEUR_TOLERANCE` | Transverse | Écart d'arrondi cumulé > 1,00 € sur une facture / propriétaire / mois (D035) |
| `MENAGE_ECART_NEGATIF_IMPORTANT` | Ménages | Écart coût réel vs standard > seuil |
| `MENAGE_DOUBLON_POTENTIEL` | Ménages | Même mois × logement × intervenant en double |
| `MENAGE_STATUT_NON_VALIDE` | M04 | Ligne ménage non validée, exclue du calcul |
| `TYPE_INTERVENANT_ABSENT` | Ménages | Intervenant sans type (prioritaire sur identité exacte) |

---

## Note sur BANQUE_LIGNE_SANS_DATE vs BANQUE_DATE_INEXPLOITABLE
- `BANQUE_LIGNE_SANS_DATE` : colonne Date vide mais colonne Valeur présente et parsable → **A_CONTROLER** (non bloquant)
- `BANQUE_DATE_INEXPLOITABLE` : Date ET Valeur absentes ou toutes deux non parsables → **BLOQUANT** (ligne inutilisable)

# ETAT_AVANCEMENT.md
> Fichier de mémoire inter-sessions. À lire en PREMIER à chaque reprise. À mettre à jour en FIN de session.

---

## Dernière mise à jour
Date : 2026-06-11
Session : Session 16 — Lot 9 Table de flux unifiée MASTER_CALC_Flux
Agent : Claude Code (claude-sonnet-4-6)

---

## Lot en cours
Lot : 9 — Table de flux unifiée MASTER_CALC_Flux
Statut : **EN_ATTENTE_VALIDATION_HUMAINE** — script exécuté, 1333 flux, 10 contrôles OK, commit non fait

> Contrôles inscrits :
> - Lot 0 : CTR-2026-06-001 (audit initial), CTR-2026-06-002 (corrections), CTR-2026-06-003 (post-correction — tout vert)
> - Lot 1 : CTR-2026-06-004 (extraction + payout validés — 2026-06-08)
> - Lot 2 : CTR-2026-06-005 (mapping logements — 17/17 OK — validé humainement 2026-06-08)
> - Lot 3 FAIT (2026-06-08) : REF_Setup.xlsm mis à jour (5 onglets). SAISIE_Charges_Flux.xlsx créé (4 onglets, 31 cols, 18 DV, 13 contrôles). MASTER_FACT_MAN_Charges.xlsx créé (37 cols, 4 requêtes PQ). CTR-2026-06-006 inscrit.
> - Lot 4 (2026-06-09) : SAISIE_ReservationsHorsHostaway.xlsx créé (4 onglets, 30 cols, 11 DV, 13 contrôles, VLOOKUP taux). MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx créé (34 cols, 3 requêtes PQ). CTR-2026-06-007 inscrit. Décisions D046–D051 verrouillées.
> - Lot 4bis squelette (2026-06-09) : MASTER_CALC_Reservations.xlsx créé (3 onglets, 24 cols, 7 requêtes PQ, anti-double-comptage 7 scénarios, 2 BLOQUANTS + 6 A_CONTROLER). CTR-2026-06-008 inscrit. Décisions D052–D057 verrouillées.
> - Lot 4bis correctif FAIT (2026-06-11) : MASTER_CALC_Reservations.xlsx peuplé par script Python (1 391 lignes MASTER / 1 321 VUE_FLUX). CTR-2026-06-016 inscrit. Commit 3835f21. Règle date_sortie_gestion validée.
> - Lot 5 (2026-06-09) : SAISIE_AcomptesProprietaires.xlsx créé (4 onglets, 18 cols, 5 DV, 10 contrôles). MASTER_FACT_MAN_AcomptesProprietaires.xlsx créé (22 cols, 5 requêtes PQ). REF_Setup.xlsm non modifié (TYPE_FLUX_006 déjà présent). CTR-2026-06-009 inscrit. Décisions D058–D064 verrouillées.
> - Lot 6a (2026-06-09) : MASTER_FACT_HA_CleaningTasks_Discovery.xlsx peuplé (4 onglets : data 500 tâches / MASTER_ENRICHI 21 cols / VUE_COMPTAGE 11 cols / POWER_QUERY_CODE). 0 BLOQUANT, 325 ménages réalisés. CTR-2026-06-010 inscrit. Décisions D065–D069 verrouillées.
> - Lot 9 (2026-06-11) : MASTER_CALC_Flux.xlsx créé (1 333 flux, 22 cols). TYPE_FLUX_017 créé. CTR-2026-06-017 inscrit. EN_ATTENTE_VALIDATION_HUMAINE.

**Points résiduels non bloquants à traiter dans les lots suivants :**
- `CARTE_002` suffixe `XXXX` (carte Ewan) → à renseigner au **Lot 8** avant traitement des exports bancaires.
- `mode_facturation = A_DEFINIR` pour tous les propriétaires → à définir au **Lot 12** avant facturation.
- CleaningTasks FAIT au Lot 6a (500 tâches, 325 réalisés, 0 BLOQUANT).
- 59 réservations A_CONTROLER (32 VRBO + 27 DIRECT) → saisie manuelle aux **Lots 4 / 4bis**.

---

## Ce qui est terminé

- **Fichiers de cadrage mis à jour et cohérents (Session 2, pack V2)** :
  - CLAUDE.md, README_PROJET.md, REGLES_METIER.md, ARCHITECTURE_DONNEES.md, PLAN_CONSTRUCTION.md
  - ETAT_AVANCEMENT.md, DECISIONS_METIER.md, JOURNAL_CONTROLES.md, JOURNAL_ANOMALIES.md
- **Lot 0 — REF_Setup FAIT (2026-06-07)** :
  - Audit réel exécuté sur `REF_Setup.xlsm` (19 onglets, 0 doublon clé, 0 date série brute).
  - 6 corrections bloquantes appliquées (B1 mojibake, B2 statuts, B3 REF_Statuts_Payout, B4 REF_Cloture_Mensuelle, B5 paramètres, B6 intervenants).
  - Codes hors-parc APPARTEMENT_DIVERS + LOGEMENT_DIVERS ajoutés dans REF_Logements.
  - Fichier déplacé vers chemin canonique : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`.
  - Sauvegarde : `99_ARCHIVES/LOT0_REF_Setup/REF_Setup_BACKUP_20260607_113223.xlsx.xlsm`.
  - Contrôle inscrit : CTR-2026-06-003 (audit post-correction tout vert).
  - Décisions métier tranchées : QM1 (coûts standards = exécution seule, D037 confirmé), QM2 (A_DEFINIR OK), QM3 (CHG_012 reste A_CONTROLER).
- **Lot 1 — Module Hostaway FAIT (2026-06-08)** : extraction validée (run 20260608_134253 — 1391 réservations traitées, 17 listings, 55 anomalies A_CONTROLER, 0 BLOQUANT). Fix financeField appliqué (`_ff_from_res`). Payout : 1321 NORMAL, 59 A_CONTROLER (32 VRBO + 27 DIRECT). Contrôle inscrit : CTR-2026-06-004. CleaningTasks SKIPPED → Lot 6a.
- **Lot 2 — Mapping logements FAIT (2026-06-08)** : réconciliation REF_Logements ↔ MASTER_REF_HA_Listings. 17/17 listings mappés, 0 orphelin. ANO-001/005/006/014 CORRIGÉES par correction de mapping (pas de création de logement). LOG_0009 : hid 497801→556954, nom "T3 Montaudran". LOG_0016 : hid 480780→515523, sur_hostaway NON→OUI. 5 nouvelles lignes REF_Mapping (MAP_LOG_0082–0086, total 86 lignes). Anciens IDs 480780 et 497801 conservés comme alias historiques actifs (actif=OUI). Contrôle inscrit : CTR-2026-06-005. Validé humainement 2026-06-08.
- **Lot 3 — SAISIE_Charges_Flux FAIT (2026-06-08)** :
  - REF_Setup.xlsm mis à jour : REF_Categories_Charges (+filtre_vue_menage +CHG_021/022/023), REF_Types_Flux (+TYPE_FLUX_009-012), REF_Types_Affectation (+AFF_GLOBAL/NON_AFFECTABLE), REF_Statuts (D044 : STAT_022 désactivé, STAT_024-029), REF_Charges_Recurrentes (nouvel onglet, REC_001/002). Backup : 99_ARCHIVES/LOT3_Charges/REF_Setup_BACKUP_20260608_191019.xlsm.
  - SAISIE_Charges_Flux.xlsx créé (01_SOURCES_BRUTES/Charges/) : 31 colonnes, 18 listes déroulantes, formules calculées, 13 contrôles, MFC niveau_anomalie.
  - MASTER_FACT_MAN_Charges.xlsx créé (02_TRAVAIL/Lot3_Charges/) : 37 colonnes (31 SAISIE + sens/filtre_vue_menage/source_module/source_table/source_pk/date_integration). VUE_MENAGE = filtre filtre_vue_menage=OUI AND statut_controle=VALIDE. 4 requêtes M-code Power Query.
  - Décisions verrouillées : D044 (séparation statut_controle/niveau_anomalie), D045 (REF_Charges_Recurrentes paramétrable). REFACTURATION→sens=CHARGE validé.
  - Contrôle inscrit : CTR-2026-06-006 (21 points, 0 FAIL). Validé humainement 2026-06-08.
- **Lot 4 — Réservations hors Hostaway FAIT (2026-06-09)** :
  - SAISIE_ReservationsHorsHostaway.xlsx créé (4 onglets, 30 cols, 11 DV, 13 contrôles, VLOOKUP taux_commission). Chemin : `01_SOURCES_BRUTES/ReservationsHH/`.
  - MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx créé (3 onglets, 34 cols, 3 requêtes PQ). Chemin : `02_TRAVAIL/Lot4_ReservationsHH/`.
  - REF_Setup.xlsm non modifié (V1+V2 OK). Contrôle inscrit : CTR-2026-06-007. Décisions D046–D051.
- **Lot 4bis — Table commune réservations FAIT (2026-06-09)** :
  - MASTER_CALC_Reservations.xlsx créé (3 onglets, 24 cols, 7 requêtes PQ). Chemin : `02_TRAVAIL/Lot4bis_TableCommune/`.
  - Anti-double-comptage 7 scénarios. 2 BLOQUANTS + 6 A_CONTROLER détectés en PQ. VUE_FLUX filtre VALIDE+OUI+montant≠0.
  - Contrôle inscrit : CTR-2026-06-008. Décisions D052–D057.
- **Lot 5 — Acomptes propriétaires FAIT (2026-06-09)** :
  - SAISIE_AcomptesProprietaires.xlsx créé (4 onglets, 18 cols, 5 DV, 10 contrôles). Chemin : `01_SOURCES_BRUTES/AcomptesProprietaires/`.
  - MASTER_FACT_MAN_AcomptesProprietaires.xlsx créé (3 onglets, 22 cols, 5 requêtes PQ). Chemin : `02_TRAVAIL/Lot5_AcomptesProprietaires/`.
  - REF_Setup.xlsm non modifié : TYPE_FLUX_006 (ACOMPTE_FACTURE_PROPRIETAIRE) déjà présent.
  - report_mois_suivant supprimé (D061). source_pk = acompte_id toujours (D064). TYPE_FLUX_013 non créé.
  - Contrôle inscrit : CTR-2026-06-009. Décisions D058–D064.
- **Lot 6a — Hostaway CleaningTasks comptage ménages FAIT (2026-06-09)** :
  - MASTER_FACT_HA_CleaningTasks_Discovery.xlsx peuplé (4 onglets). Chemin : `02_TRAVAIL/Lot1_Hostaway/`.
  - data (11 cols, 500 tâches brutes, H6 cost=NULL). MASTER_ENRICHI (21 cols, 0 BLOQUANT, 39 A_CONTROLER).
  - VUE_COMPTAGE (11 cols, 95 lignes mois×logement, 325 ménages réalisés). POWER_QUERY_CODE (Q1–Q4).
  - D065 : API /v1/tasks = 500 max, single call. D066 : Jan 2026 absent = normal.
  - D067 : confirmed=prévu. D068 : logement inactif=A_CONTROLER. D069 : TLM_001 par défaut.
  - Contrôle inscrit : CTR-2026-06-010. Décisions D065–D069.
- **Lot 6c — Ménages externes FAIT (2026-06-09)** :
  - MASTER_FACT_MEN_MenagesExternes.xlsx créé (7 onglets, 49 cols). Chemin : `02_TRAVAIL/Lot6c_MenagesExternes/`.
  - SOURCE_RAW : 13 lignes (9 Aissata fac.2026-37 / 4 Mounir fac.0003 — mai 2026). Ligne 8 splittée en 8a/8b.
  - MASTER : 13 lignes — 4 VALIDE / 9 A_CONTROLER / 0 BLOQUANT.
  - VUE_ACTIVE : 4 lignes (lignes avec date_menage précise). 9 lignes A_CONTROLER = MENAGE_EXTERNE_DATE_ABSENTE.
  - VUE_ECART_HOSTAWAY : 16 logements mois=2026-05.
  - Réconciliation : FAC-2026-05-AISSATA-001 écart=0,00€ / FAC-2026-05-MOUNIR-001 écart=0,00€ — VALIDE.
  - REF_Setup.xlsm : TYPE_FLUX_014 (COUT_REEL_MENAGE_EXTERNE) ajouté. REF_Intervenants +3 colonnes (nom_legal, siret_rcs, email_facturation).
  - Backup : 99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm.
  - FRANCHISE_TVA confirmé : Aissata et Mounir. taux_tva=0, HT=TTC.
  - Kandia DIABATE = INT_0004 (Aissata) / MH Entreprise = INT_0003 (Mounir).
  - CTR-2026-06-012 inscrit. Décisions D079–D088.
- **Lot 6b — M04 Ménages internes FAIT (2026-06-09)** :
  - M04_MENAGES_PowerQuery.xlsx créé (squelette). Chemin : `02_DONNEES_NORMALISEES/menages/`.
  - 8 onglets : SOURCE_RAW / PARAM_TAUX_INTERVENANTS / PARAMETRES_M04 / MASTER (34 cols) / VUE_ACTIVE / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE (10 requêtes) / README.
  - PARAM_TAUX : INT_0001 Imène 10€, INT_0002 Kheira 10€, INT_0003/0004/0005 EXTERNE.
  - PARAMETRES_M04 : SEUIL_ECART_STANDARD_MENAGE=10 paramétrable.
  - REF_Setup.xlsm : TYPE_FLUX_013 (COUT_MO_INTERNE_MENAGE) ajouté. Backup : 99_ARCHIVES/LOT6B_Menages/REF_Setup_BACKUP_20260609_152826.xlsm.
  - Clé répartition future : COUT_STANDARD_MENAGES_MOIS (non NOMBRE_MENAGES). cout_standard_total_ligne = base pondération, non comptable (D076).
  - Contrôle inscrit : CTR-2026-06-011. Décisions D070–D078.
  - SOURCE_RAW vide — coller données GSheet + adapter chemins PQ avant premier run.
  - REC_002 cle_repartition mise à jour : COUT_STANDARD_MENAGES_MOIS (ancienne valeur : NOMBRE_MENAGES, D076, validation humaine Lot 6b).

> **Règle D029 — IRRÉVOCABLE** : aucun lot ne peut être marqué FAIT sans entrée dans JOURNAL_CONTROLES.

---

## Ce qui a été modifié (session 16 — Lot 9, 2026-06-11)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-017 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 16, Lot 9)
- Cadrage : `.gitignore` (ajout `99_ARCHIVES/LOT9_FluxUnifie/`)
- Référentiel : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
  (TYPE_FLUX_017 = REVENU_RESERVATION_HOSTAWAY ajouté — onglet REF_Types_Flux)
- Backup REF_Setup : `99_ARCHIVES/LOT9_FluxUnifie/REF_Setup_backup_lot9_20260611_213323.xlsm`
- Script créé : `02_TRAVAIL/lot9_construire_flux.py`
  (construit MASTER_CALC_Flux depuis 3 sources : RES 1321 + MEN 4 + BNQ 8 = 1333 flux)
- Créé : `02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx`
  (22 colonnes, 1333 flux, tous VALIDE, tous IC, 10 contrôles BLOQUANTS passés)

Volumes produits :
  - RES : 1321 flux (TYPE_FLUX_017, PRODUIT, IC — réservations Airbnb/Booking)
  - MEN : 4 flux (TYPE_FLUX_014, CHARGE, IC — ménages externes Aissata, 2026-05)
  - BNQ : 8 flux (TYPE_FLUX_016, CHARGE, IC — frais bancaires, commentaire générique)
  - Total : 1333 flux VALIDE / 0 doublon / 0 montant négatif

Sécurité bancaire : aucune donnée brute (libellé, compte, IBAN) dans MASTER_CALC_Flux.xlsx [OK]
Statut : EN_ATTENTE_VALIDATION_HUMAINE — commit non fait
Fichiers sources NON modifiés (HA_Reservations, Payout, HH, Banque brute)

## Ce qui a été modifié (session 15 — Lot 4bis correctif, 2026-06-11)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-016 inscrit)
- Script créé : `02_TRAVAIL/lot4bis_charger_reservations.py`
- Modifié : `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`
  (MASTER 1 391 lignes / VUE_FLUX 1 321 lignes / POWER_QUERY_CODE conservé)
- Backup : `99_ARCHIVES/LOT4BIS_TableCommune/MASTER_CALC_Reservations_BACKUP_20260611_200317.xlsx`
- Commit 3835f21 (5 fichiers : .gitignore, ETAT_AVANCEMENT, JOURNAL_CONTROLES, script, xlsx)

---

## Ce qui a été modifié (cette session — Lot 6c)
- Cadrage : `DECISIONS_METIER.md` (D079–D088 ajoutés — D-6c-01 à D-6c-10)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-012 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 13, Lot 6c)
- Script : `02_TRAVAIL/lot6c_menages_externes.py` (créé + exécuté)
- Créé : `02_TRAVAIL/Lot6c_MenagesExternes/MASTER_FACT_MEN_MenagesExternes.xlsx`
  (7 onglets : SOURCE_RAW 13L / PARAMETRES / MASTER 49cols / VUE_ACTIVE 4L / VUE_ECART_HOSTAWAY / POWER_QUERY_CODE 7Q / README)
- Modifié : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
  (REF_Intervenants +3 cols INT_0003/0004 ; REF_Types_Flux +TYPE_FLUX_014)
- Backup : `99_ARCHIVES/LOT6C_MenagesExternes/REF_Setup_BACKUP_20260609_170606.xlsm`
- Fichiers Lots 3, 4, 4bis, 5, 6a, 6b : NON modifiés

## Ce qui a été modifié (cette session — Lot 6a)
- Cadrage : `DECISIONS_METIER.md` (D065–D069 ajoutés — QM-L6a-API/Jan/02/inactif/04)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-010 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 11, Lot 6a)
- Script : `02_TRAVAIL/lot6a_cleaning_tasks_comptage.py` (créé + exécuté)
- Modifié : `02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`
  (4 onglets : data 500 tâches 11 cols, MASTER_ENRICHI 21 cols, VUE_COMPTAGE 11 cols, POWER_QUERY_CODE)
- REF_Setup.xlsm : NON modifié
- Fichier CleaningTasks issu du Lot 1 enrichi au Lot 6a : MODIFIÉ (`02_TRAVAIL/Lot1_Hostaway/MASTER_FACT_HA_CleaningTasks_Discovery.xlsx`)
- REF_Setup.xlsm : NON modifié
- Fichiers Lots 3, 4, 4bis, 5 : NON modifiés

---

## Ce qui a été testé sur données réelles
- Test : Audit Lot 0 complet (pre + post correction) sur `REF_Setup.xlsm` réel
- Statut : VALIDÉ — CTR-2026-06-003
- Test : Extraction Lot 1 Hostaway complète post-fix financeField (run 20260608_134253, 121.8 s)
- Statut : VALIDÉ — CTR-2026-06-004

---

## Anomalies connues (résumé — détail dans JOURNAL_ANOMALIES.md)

| ID | Code | Sévérité | Statut |
|---|---|---|---|
| ANO-001 | LISTING_ORPHELIN (515523) | A_CONTROLER | **CORRIGÉ 2026-06-08** — alias → LOG_0016 |
| ANO-002 | ENCODAGE_CASSE | A_CONTROLER | **CORRIGÉ 2026-06-07** |
| ANO-003 | DATES_SERIE_EXCEL | A_CONTROLER | **SANS OBJET — aucune série brute** |
| ANO-004 | VRBO_MONTANT_NON_RENSEIGNE (×32) | A_CONTROLER | OUVERT — Lot 4 |
| ANO-005 | REFERENTIEL_ORPHELIN (497801) | A_CONTROLER | **CORRIGÉ 2026-06-08** — ancien ID LOG_0009 |
| ANO-006 | LISTING_CONFIRME_HORS_HOSTAWAY (480780) | INFO | **CORRIGÉ 2026-06-08** — ancien ID LOG_0016 |
| ANO-007 | REF_STATUTS_VALEURS_CONTROLE_MANQUANTES | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-008 | REF_STATUTS_PAYOUT_ABSENT | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-009 | REF_CLOTURE_MENSUELLE_ABSENTE | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-010 | REF_PARAMETRES_GENERAUX_INCOMPLETS | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-011 | REF_INTERVENANTS_SCHEMA_INCOMPLET | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-012 | REF_LOGEMENTS_CODES_HORS_PARC_ABSENTS | A_CONTROLER | **CORRIGÉ 2026-06-07** |
| ANO-013 | REF_CARTES_PAIEMENT_SUFFIXE_MANQUANT | A_CONTROLER | IGNORE_JUSTIFIE — Lot 8 |
| ANO-014 | LISTING_ORPHELIN_A_CONTROLER (556954) | A_CONTROLER | **CORRIGÉ 2026-06-08** — alias → LOG_0009 |

> **4 cas listingMapId résolus au Lot 2** — voir JOURNAL_ANOMALIES.md (note synthèse mise à jour).

---

## Décisions prises (résumé — détail dans DECISIONS_METIER.md)
- D001 à D020 : décisions architecture validées (payout, commission, codes impact, upsert…)
- D021 : Statuts payout fermés — `REF_Statuts_Payout` à créer au Lot 0
- D022 : `REF_Statuts` fermé — à créer au Lot 0
- D023 : obsolète — remplacée par D035 (double seuil 0,10 €/1,00 €)
- D024 : `REF_Cloture_Mensuelle` — structure créée au Lot 0, exploitée au Lot 8
- D025 : Frontière Lot 3 / Lot 7 — IK uniquement dans `MASTER_FACT_MAN_IK_Avantages`
- D026 : `SAISIE_Charges_Flux.xlsx` = source unique achats/charges (exclut IK)
- D027 : Suppression définitive `Courses` / `Coût du lavage` / `achats` de M04 — IRRÉVOCABLE
- D016-REV : Forfait local 50 € quitte M04 → `SAISIE_Charges_Flux.xlsx`
- D028 : Coût complet ménage hors M04 via `VUE_ACHATS_MENAGE_VALIDES`
- D029 : Aucun lot ne peut être FAIT sans contrôle dans JOURNAL_CONTROLES — IRRÉVOCABLE
- D030 : Cancellation payout — `BaseCommission = CancellationPayout`, pas de ménage déduit
- D031 : `revenu_net_exploitation_proprietaire` — indicateur économique pur, formule verrouillée
- D032 : `acompte_conciergerie_recu_via_airbnb` — bloc règlement uniquement, jamais exploitation
- D033 : Séparation exploitation / règlement — deux blocs non communicants
- D034 : `charges_exceptionnelles_refacturees` — bloc règlement uniquement
- D035 : Convention d'arrondi — double seuil 0,10 €/ligne / 1,00 €/cumulé — VERROUILLÉ
- D036 : IK en montant direct (pas de barème auto au démarrage) — VERROUILLÉ
- D037 : REF_Couts_Standards_Menage = exécution seule, valeurs à revalider Lot 0 — VERROUILLÉ
- D038 : Rangement dans M04 = main-d'œuvre uniquement — VERROUILLÉ
- D039 : charge_fixe_mensuelle paramétrable dans REF_Logements, 0 si absent — VERROUILLÉ
- D040 : Structure sortie facture (FACT_FACTURE_ENTETE/LIGNES, Excel contrôle, **aucun PDF au démarrage**) — VERROUILLÉ
- D041 : Incidents voyageurs — catégorie `CHG_021` dans `SAISIE_Charges_Flux.xlsx`, `reservation_id` obligatoire — VERROUILLÉ (P02)
- D042 : AirCover — 3 flux distincts (remboursement propriétaire hors comptes / prestation `CHG_022` facturée bloc règlement / impact résultat ligne par ligne) — VERROUILLÉ (P03)
- D043 : Priorité Excel avant Power BI — aucun dashboard `.pbix` livré par les lots — VERROUILLÉ (P32)
- D044 : Séparation statut_controle / niveau_anomalie — `statut_controle` : VALIDE/A_CONTROLER/EXCLU_RESULTAT/A_VENTILER (Lot 3+) ; `niveau_anomalie` : INFO/A_CONTROLER/BLOQUANT — VERROUILLÉ (DM-L3-01)
- D045 : REF_Charges_Recurrentes — table des montants paramétrables (forfaits, loyers) ; aucun montant fixe codé en dur dans formules/PQ/scripts — VERROUILLÉ
- D052 : logement_id via JOIN REF_Mapping_Logements (listingMapId, HA) ; proprietaire_id via JOIN REF_Logements ; anomalies LOGEMENT_NON_MAPPE + MAPPING_MULTIPLE — VERROUILLÉ (QM-L4b-01)
- D053 : MASTER_CALC_Reservations 24 cols ; source 7 valeurs ; source_montant 5 valeurs ; +niveau_anomalie +code_anomalie ; contrôle RESERVATION_HH_NON_VALIDE — VERROUILLÉ (QM-L4b-02)
- D054 : Anti-double-comptage 7 scénarios ; DOUBLON HA-HH BLOQUANT ; CALC_ID_DUPLIQUE BLOQUANT — VERROUILLÉ (QM-L4b-03)
- D055 : mois = TEXT YYYY-MM depuis checkInDate (HA) / direct (HH) — VERROUILLÉ (QM-L4b-04)
- D056 : PK RES-AAAA-MM-HA-NNN / RES-AAAA-MM-HH-NNN ; compteur reset 001/mois/branche — VERROUILLÉ (QM-L4b-05)
- D057 : chemins 02_TRAVAIL/Lot4bis_TableCommune/ ; 3 onglets ; 7 requêtes PQ — VERROUILLÉ (QM-L4b-06)
- D058 : périmètre sources Lot 5 — HH_RESERVATION / VIREMENT_DIRECT / AUTRE — VERROUILLÉ (QM-L5-01)
- D059 : facture_ref = FAC-AAAA-MM-PROP-NNN, provisoire, BLOQUANT si absente — VERROUILLÉ (QM-L5-02)
- D060 : granularité proprietaire_id+logement_id+mois+facture_ref+source ; 22 cols MASTER — VERROUILLÉ (QM-L5-03)
- D061 : report_mois_suivant supprimé Lot 5 ; report_mois_precedent informatif, déféré Lot 10/12 — VERROUILLÉ (QM-L5-04)
- D062 : chemins Lot 5 ; TYPE_FLUX_006 existant, aucune modif REF_Setup — VERROUILLÉ (QM-L5-05)
- D063 : PK ACC-AAAA-MM-NNN, reset 001/mois, stable, saisi manuellement — VERROUILLÉ (QM-L5-06)
- D064 : source_pk = acompte_id toujours ; source_hh_id = reservation_hh_id si HH — VERROUILLÉ (QM-L5-07)

---

## Prochaine action obligatoire
```
Lot 9 (2026-06-11) — CTR-2026-06-017 inscrit — EN_ATTENTE_VALIDATION_HUMAINE
Action : valider le bilan 16 points, puis git add + commit (5 fichiers : .gitignore,
         ETAT_AVANCEMENT.md, JOURNAL_CONTROLES.md, REF_Setup.xlsm,
         lot9_construire_flux.py, MASTER_CALC_Flux.xlsx)
Après commit : Lot 9 FAIT → Lot 10 débloqué (résultats réel/comptable/hors compta)
```

---

## Lecture prochaine session (discipline contextuelle)

Si la prochaine session est **validation + commit Lot 9** :
```text
À OUVRIR :
- CLAUDE.md (intégral, court)
- ETAT_AVANCEMENT.md (ce fichier)
- PLAN_CONSTRUCTION.md → uniquement Lot 9
- ARCHITECTURE_DONNEES.md → §2.3, §14
- REGLES_METIER.md → §1, §2 (H4), §8
- Tables calculées déjà produites (MASTER_CALC_Reservations, MASTER_FACT_MAN_Charges, etc.)

À NE PAS OUVRIR :
- README_PROJET.md (sauf onboarding)
- Sources brutes PDF/API
```

Cette discipline est appliquée à chaque nouveau lot, en s'appuyant sur la matrice `CLAUDE.md §5.bis`.

---

## Interdictions / points sensibles
- Ne pas modifier : sources brutes (Banque, Hostaway, PDF)
- Ne pas utiliser : REF_Setup.xlsx (nom incorrect — le fichier est REF_Setup.xlsm)
- Ne pas passer au Lot 2 sans audit Lot 0 validé sur fichier réel
- **Ne jamais marquer un lot FAIT sans entrée dans JOURNAL_CONTROLES (D029)**
- Ne pas écraser : tables master existantes sans sauvegarde
- Ne pas fusionner ménages internes / externes / tâches Hostaway
- Ne pas saisir IK/virements associés dans SAISIE_Charges_Flux.xlsx (appartient au Lot 7)
- **Ne jamais réintroduire Courses, Coût du lavage, onglet achats dans M04 (D027 — irrévocable)**
- Ne pas mettre achats/consommables/linge/matériel dans M04 (appartient à SAISIE_Charges_Flux)
- Ne pas modifier revenu_net_exploitation via des acomptes ou paiements reçus (D031/D033)
- Ne jamais intégrer une charge exceptionnelle dans charge_fixe_mensuelle (D039/EP3)
- Ne jamais comparer un coût standard complet à un coût d'exécution M04 (D037)
- Ne jamais inclure une charge exceptionnelle refacturée dans `revenu_net_exploitation_proprietaire` (D034/EP7/P21)
- Ne jamais traiter un remboursement AirCover perçu par le propriétaire comme un payout (D042/AC5)
- Ne jamais livrer un dashboard Power BI dans un lot (D043/PBI2)

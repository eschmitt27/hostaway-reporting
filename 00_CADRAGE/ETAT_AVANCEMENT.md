# ETAT_AVANCEMENT.md
> Fichier de mémoire inter-sessions. À lire en PREMIER à chaque reprise. À mettre à jour en FIN de session.

---

## Dernière mise à jour
Date : 2026-06-14
Session : Session 20 — Correctif Lot 9 ingestion Charges/M04 + infrastructure test Lot 12
Agent : Claude Code (claude-opus-4-8 / claude-sonnet-4-6)

---

## Lot en cours
Lot : 11 — Contrôles de cohérence globaux
Statut : **EN_ATTENTE_VALIDATION_HUMAINE** — 51 contrôles générés (CTR-2026-06-020), commit non fait

> Contrôles inscrits :
> - Lot 0 : CTR-2026-06-001 (audit initial), CTR-2026-06-002 (corrections), CTR-2026-06-003 (post-correction — tout vert)
> - Lot 1 : CTR-2026-06-004 (extraction + payout validés — 2026-06-08)
> - Lot 2 : CTR-2026-06-005 (mapping logements — 17/17 OK — validé humainement 2026-06-08)
> - Lot 11 : CTR-2026-06-020 (contrôles globaux — 0 BLOQUANT / 44 A_CONTROLER / 7 INFO — après audit F1/F2)
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

## Ce qui a été modifié (session 20 — Correctif Lot 9 ingestion Charges/M04 + infra test Lot 12, 2026-06-14)

**Objectif :** câbler dans le Flux les sources jusqu'ici absentes (Charges, M04) + créer une
infrastructure de données fictives pour tester le pipeline jusqu'au Lot 12.

- Modifié : `02_TRAVAIL/lot9_construire_flux.py`
  - Ingestion Charges (Lot 3, VALIDE) — sens/code_impact/type portés par la ligne
  - Ingestion M04 ménages internes (Lot 6b, VALIDE, HC, TYPE_FLUX_013)
  - IK exclu du Flux (décision « IK hors Flux / vue dédiée », §15.3)
  - Acomptes différés (passe Lot 10)
  - Garde-fou CTR-9-011 : portion RES (TYPE_FLUX_017) inchangée
- Créé : `02_TRAVAIL/lot12_seed_donnees_fictives.py` (seed idempotent + backup + tag obligatoire)
- Créé : `02_TRAVAIL/lot12_remove_donnees_fictives.py` (suppression par ID ZZ_TEST_ / tag + vérif 0 résiduel)
- Modifié : `.gitignore` (ignore `99_ARCHIVES/LOT12_TEST_DATA/` — backups fictif, règle #10)
- Modifié : `00_CADRAGE/JOURNAL_CONTROLES.md` (CTR-2026-06-021)
- Modifié : `00_CADRAGE/ETAT_AVANCEMENT.md` (ce fichier — session 20)

**Fichiers à ne SURTOUT PAS committer :**
- `99_ARCHIVES/LOT12_TEST_DATA/` (backups contenant du fictif — déjà ignoré git)
- Tous les fichiers Excel de données (Flux, Resultats, Commissions, NetProprietaire, CTRL_Coherence,
  Charges, M04, MenExt, ReservationsHH, Reservations4bis, REF_Setup.xlsm, banque)

**Résultat audit (avant commit) :**
- 0 donnée fictive résiduelle (scan ZZ_TEST_ / tag sur 30 Excel suivis)
- 0 Excel suivi modifié (git diff = uniquement .gitignore + lot9_construire_flux.py + docs)
- Sources amont non modifiées (lecture seule)
- Correctif Lot 9 **neutre** tant que Charges/M04 sont vides (baseline 1 333 inchangée)

**Test fictif réalisé :** baseline 1 333 → avec fictif 1 340 → après suppression 1 333 → 0 résiduel.

**Findings Lot 10 à traiter ensuite (passe Lot 10) :**
1. HH sans payout Hostaway → gérer par montant saisi (pas payout)
2. Lot 10 GLOBAL HORS_COMPTA reste 0 même avec flux HC → casse REEL = COMPTABLE + HC
3. Charges globales / non affectables (sans logement_id) perdues dans l'agrégation
4. Acomptes uniquement dans le bloc REGLEMENT, jamais dans l'exploitation (D031/D032/D033)

**PRÉREQUIS BLOQUANT :** ne pas peupler Charges/M04 avec des données réelles HC avant correctif Lot 10.

---

## Ce qui a été modifié (session 19 — Lot 11 Contrôles de cohérence globaux, 2026-06-14)

- Script créé : `02_TRAVAIL/lot11_controles_coherence.py`
  - Banque adaptative (D-LOT11-01 Option C) — 132 lignes NORM_Banque lues localement
  - Re-vérification indépendante (D-LOT11-04) — pas de reprise Lot 10
  - 6 onglets produits : MASTER / BLOQUANTS_OUVERTS / A_CONTROLER_OUVERTS / DASHBOARD_MOIS / RAPPROCHEMENT_PAYOUT_BANQUE / CAISSE_THEORIQUE
- Créé : `02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx`
  - MASTER : 51 contrôles générés
  - BLOQUANTS_OUVERTS : **0 ligne** — 0 bloquant
  - A_CONTROLER_OUVERTS : 44 lignes
  - DASHBOARD_MOIS : 4 lignes (2026-02/03/04 + TRANSVERSE) — mois vides arbitraires supprimés (F3)
  - RAPPROCHEMENT_PAYOUT_BANQUE : 3 mois (indicatif — écarts normaux)
  - CAISSE_THEORIQUE : 6 lignes avec flag CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES
- Corrections post-audit (2026-06-14) :
  - F1 : `facturation_lot12_ok` = OUI uniquement si 0 BLOQUANT + 0 A_CONTROLER + mois banque CLOTURE.
    Résultat : AUCUN mois facturable (tout NON_SOUS_RESERVE_A_CONTROLER).
  - F2 : détection placeholder Power Query → CHARGES/M04/IK/HH correctement vides → INFO 3 → 7.
  - F3 : code mort supprimé, print dynamique, DASHBOARD sans mois vides.
- Chiffres clés :
  - 0 BLOQUANT — aucun blocage structurel
  - 44 A_CONTROLER (23 LISTING_ORPHELIN + 14 CHARGE_FIXE_DATE + 3 CLOTURE_BANQUE + 4 autres)
  - 7 INFO (HC_ZERO_SOURCES_VIDES ×6 [CHARGES/M04/ACOMPTES/IK/HH/TRANSVERSE] + MODE_FACTURATION_A_DEFINIR)
  - REEL = COMPTABLE + HORS_COMPTA = 283 515.60 + 0 = 283 515.60 € ✓
  - Banque : BANQUE_DISPONIBLE — 52 lignes RAPPROCHEMENT_REQUIS sur 3 mois
  - Caisse théorique : 0 € (sources HH/Charges/Acomptes vides)
  - facturation_lot12_ok : NON sur tous les mois (0 OUI)
- Sources amont : aucune modification (toutes read_only=True)
- Commit non fait — EN_ATTENTE_VALIDATION_HUMAINE

---

## Ce qui a été modifié (session 18 — Lot 10 Résultats / commissions / net propriétaire, 2026-06-13)

- Script créé : `02_TRAVAIL/lot10_calculer_resultats.py`
  - Jointure confirmée : Flux.source_pk → Reservations.reservation_calc_id → reservation_id_hostaway → Payout.reservation_id
  - 6 contrôles BLOQUANTS / 4 contrôles A_CONTROLER / 19 points CTR rapport
- Créé : `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx`
  - Onglet COMMISSIONS : 1 321 réservations NORMAL (Airbnb + Booking)
  - Onglet A_CONTROLER : 59 réservations exclues (VRBO + Direct)
- Créé : `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx`
  - PAR_MOIS_LOGEMENT : 497 lignes total (248 REEL + 248 COMPTABLE + 1 HORS_COMPTA placeholder)
  - PAR_MOIS_PROPRIETAIRE : 404 lignes total (202 REEL + 202 COMPTABLE)
  - GLOBAL : 3 lignes (1 REEL + 1 COMPTABLE + 1 HORS_COMPTA) — REEL = COMPTABLE = 283 515.60 € / HC = 0 (sources vides)
- Créé : `02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx`
  - EXPLOITATION : 1 321 réservations, charge_fixe=0 par réservation
  - REGLEMENT : 269 lignes mois × logement, charge fixe Option A
  - VUE_MOIS : 220 lignes mois × propriétaire
- Charge fixe mensuelle (Option A, D-LOT10-04 validé) :
  - 233 lignes générées / 13 logements avec forfait > 0
  - 12 logements flaggés CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE (premier mois Flux < 2026-01)
  - 1 logement flaggé LOG_SANS_FLUX_017 : LOG_0009 (forfait=40€ mais 0 réservation TYPE_FLUX_017)
  - Total charge fixe générée : 7 645.00 €
- Chiffres clés :
  - Total payout NORMAL : 283 709.60 €
  - Total ménage retenu : 55 619.00 €
  - Total assiette commission : 228 090.60 €
  - Total commission conciergerie : 39 167.42 €
  - Total net proprio avant charge fixe : 188 923.18 €
  - Total net proprio après charge fixe : 181 278.18 €
  - Résultat REEL global (Flux) : 283 515.60 €
  - Résultat HORS_COMPTA : 0 € (sources vides — M04, Charges, IK non alimentés)
- 0 BLOQUANT détecté — CTR-2026-06-019 inscrit
- Sources amont non modifiées (Flux, Reservations, Payout, REF_Setup — lecture seule)
- Commit non fait — EN_ATTENTE_VALIDATION_HUMAINE

---

## Ce qui a été modifié (session 17 — Correctif Lot 1 Payout final, 2026-06-13)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-018 inscrit — correctif final date-aware + REF historique)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 17, correctif Lot 1 final)
- Cadrage : `.gitignore` (99_ARCHIVES/LOT1_Hostaway/ ajouté)
- Référentiel : `01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm`
  - REF_Couts_Standards_Menage : +5 lignes historiques 2025 (COUT_STD_2025_TYPE_001–005)
  - Période : 2025-01-01 → 2025-12-31 / Montants identiques 2026 (29/39/55/69/110€)
  - Lignes 2026 (COUT_MEN_001–005) inchangées. Aucun chevauchement.
  - Backup : `99_ARCHIVES/REF_Setup_BACKUP_20260613_175507.xlsm`
- Script refactoré : `02_TRAVAIL/lot1_hostaway_extract.py`
  - `_build_cost_ref_df()` : colonnes date_debut_validite + date_fin_validite + cout_standard_id
  - `load_menage_cost_ref()` : retourne DataFrame (plus dict simple)
  - `_lookup_menage_by_date()` : sélection date-aware + détection doublons BLOQUANT
  - `PayoutCalculator` : refactoring complet → 5-tuples + meta_dict traçabilité
  - `_META_NON_APPLICABLE` : traçabilité annulations / VRBO / Direct
  - `recalc_payout_only()` : date-aware par ligne via checkInDate + 8 colonnes tracabilité
  - Anomalies : COUT_STANDARD_MENAGE_ABSENT, COUT_STANDARD_MENAGE_DOUBLON_VALIDITE (BLOQUANT)
  - Mode `--recalc-payout-only --payout-source <path>` conservé
- Recalculé (correctif final) : `02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx`
  - Source : backup propre MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx
  - 1 235 lignes Airbnb NORMAL + 86 lignes Booking NORMAL corrigées
  - menage_retenu Airbnb : 0.00 € → 51 727.00 € (REF_Setup date-aware)
  - menage_retenu Booking : 3 810.00 € → 3 892.00 € (REF_Setup date-aware, delta +82€)
  - assiette Airbnb : 263 043.22 € → 211 316.22 €
  - assiette Booking : 16 856.38 € → 16 774.38 €
  - 8 colonnes traçabilité ajoutées (menage_retenu_source, cout_standard_id, snapshot, dates, logement_id, type_id, date_reference)
  - CTR-15 doublons validité : 0 / CTR-16 Airbnb avec cout_std_id : 1235/1235 / CTR-17 Booking : 86/86
  - CTR-18 snapshot==menage_retenu : 1321/1321 / CTR-19 date_reference : 1321/1321
  - 0 ligne sans cout_standard [OK] / Annulations intactes [OK] (D030)
  - Impact estimé commissions (~15%) : −7 771.35 € (taux réels à confirmer Lot 10)
- Backup pré-correctif : `99_ARCHIVES/LOT1_Hostaway/MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx`

Note progression correctifs :
  #1 cleaningFee_res : invalidé (prix voyageur ≠ coût standard).
  #2 REF_Setup sans dates : insuffisant (814 réservations 2025 sans cout_standard).
  Final : date-aware + REF historique 2025 → 0 manquant, traçabilité complète.

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
Lot 10 — 2026-06-13 — CTR-2026-06-019 — EN_ATTENTE_VALIDATION_HUMAINE
Action : valider les 19 contrôles CTR-LOT10-01 à CTR-LOT10-19, puis git add + commit.

Fichiers à committer (6) :
  00_CADRAGE/ETAT_AVANCEMENT.md
  00_CADRAGE/JOURNAL_CONTROLES.md
  02_TRAVAIL/lot10_calculer_resultats.py
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
  02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx

19 contrôles à valider :
  CTR-LOT10-01  Flux lus                                : 1 333
  CTR-LOT10-02  Reservations NORMAL commissions         : 1 321
  CTR-LOT10-03  A_CONTROLER exclus                      : 59
  CTR-LOT10-04  Total payout NORMAL                     : 283 709.60 €
  CTR-LOT10-05  Total ménage retenu                     : 55 619.00 €
  CTR-LOT10-06  Total assiette commission                : 228 090.60 €
  CTR-LOT10-07  Total commission conciergerie            : 39 167.42 €
  CTR-LOT10-08  Total net proprio avant charge fixe      : 188 923.18 €
  CTR-LOT10-09  Total charge fixe générée                : 7 645.00 €
  CTR-LOT10-10  Total net proprio après charge fixe      : 181 278.18 €
  CTR-LOT10-11  Résultat REEL global (Flux)             : 283 515.60 €
  CTR-LOT10-12  Résultat COMPTABLE global               : 283 515.60 €
  CTR-LOT10-13  Résultat HORS_COMPTA                    : 0.00 € [HC_ZERO_SOURCES_VIDES]
  CTR-LOT10-14  PAR_MOIS_LOGEMENT total / dont REEL      : 497 lignes / 248 REEL
  CTR-LOT10-15  PAR_MOIS_PROPRIETAIRE total / dont REEL : 404 lignes / 202 REEL
  CTR-LOT10-16  CHARGE_FIXE_DATE_ENTREE_GESTION_INCO.   : 12 logements (attendu — REF 2026-01)
  CTR-LOT10-17  LOG_SANS_FLUX_017                       : 1 (LOG_0009 — forfait=40€, 0 réservation Flux)
  CTR-LOT10-18  Contrôles BLOQUANTS                     : 0
  CTR-LOT10-19  Sources amont lecture seule              : OK

Points à décider après Lot 10 (ne bloquent pas le commit) :
  - LOG_0009 : forfait=40€ mais 0 réservation Flux → confirmer si logement actif / en gestion
  - date_entree_gestion REF_Logements : toutes à 2026-01-01 → correction REF_Setup ultérieure
  - mode_facturation = A_DEFINIR (12 propriétaires) → Lot 12
```

---

## Prochaine action obligatoire
```
Lot 11 — 2026-06-14 — CTR-2026-06-020 — EN_ATTENTE_VALIDATION_HUMAINE
Action : valider les 12 contrôles CTR-LOT11 ci-dessous, puis git add + commit.

Fichiers à committer (4) :
  00_CADRAGE/ETAT_AVANCEMENT.md
  00_CADRAGE/JOURNAL_CONTROLES.md
  02_TRAVAIL/lot11_controles_coherence.py
  02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx

12 contrôles à valider (après corrections audit F1/F2/F3) :
  CTR-LOT11-01  Total contrôles générés               : 51
  CTR-LOT11-02  BLOQUANTS ouverts                     : 0
  CTR-LOT11-03  A_CONTROLER ouverts                   : 44
  CTR-LOT11-04  INFO                                  : 7
  CTR-LOT11-05  LISTING_ORPHELIN_A_CONTROLER           : 23 (réservations listingMapId 515523/556954)
  CTR-LOT11-06  CHARGE_FIXE_DATE_ENTREE_GESTION_INCO. : 14 logements (vs 12 Lot 10 — Lot 11 plus exhaustif)
  CTR-LOT11-07  CLOTURE_IMPOSSIBLE_LIGNE_BANC.         : 3 mois (2026-02/03/04 RAPPROCHEMENT_REQUIS)
  CTR-LOT11-08  REEL = COMPTABLE + HORS_COMPTA         : 283515.60 = 283515.60 + 0 ✓
  CTR-LOT11-09  Banque statut                         : BANQUE_DISPONIBLE
  CTR-LOT11-10  Caisse théorique                      : 0.00 EUR (CAISSE_NON_REPRESENTATIVE)
  CTR-LOT11-11  Sources amont lecture seule            : OK (aucune modification)
  CTR-LOT11-12  facturation_lot12_ok                  : NON sur tous les mois (0 OUI) — F1 appliqué

Points résiduels (non bloquants pour commit) :
  - 44 A_CONTROLER à traiter/valider avant Lot 12 (priorité : VRBO, HH, acomptes, clôture banque)
  - LOG_0009 : investigation séparée
  - date_entree_gestion : correctif REF séparé
  - mode_facturation : Lot 12
  - Banque : export Airbnb requis pour finaliser RAPPROCH_AIRBNB_ATTENTE
```

---

## Lecture prochaine session (discipline contextuelle)

Si la prochaine session est **validation + commit Lot 11** :
```text
À OUVRIR :
- CLAUDE.md (intégral, court)
- ETAT_AVANCEMENT.md (ce fichier)
- PLAN_CONSTRUCTION.md → uniquement Lot 11
- ARCHITECTURE_DONNEES.md → §18
- REGLES_METIER.md → §1, §6, §8, §11

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

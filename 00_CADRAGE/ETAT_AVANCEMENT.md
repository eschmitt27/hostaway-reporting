# ETAT_AVANCEMENT.md
> Fichier de mémoire inter-sessions. À lire en PREMIER à chaque reprise. À mettre à jour en FIN de session.

---

## Dernière mise à jour
Date : 2026-06-09
Session : Session 9 — Lot 4bis MASTER_CALC_Reservations (construction terminée)
Agent : Claude Code (claude-sonnet-4-6)

---

## Lot en cours
Lot : 4bis — MASTER_CALC_Reservations (table commune des réservations)
Statut : **FAIT** (Lots 0, 1, 2, 3, 4 et 4bis FAITS — en attente validation humaine Lot 4bis)

> Contrôles inscrits :
> - Lot 0 : CTR-2026-06-001 (audit initial), CTR-2026-06-002 (corrections), CTR-2026-06-003 (post-correction — tout vert)
> - Lot 1 : CTR-2026-06-004 (extraction + payout validés — 2026-06-08)
> - Lot 2 : CTR-2026-06-005 (mapping logements — 17/17 OK — validé humainement 2026-06-08)
> - Lot 3 FAIT (2026-06-08) : REF_Setup.xlsm mis à jour (5 onglets). SAISIE_Charges_Flux.xlsx créé (4 onglets, 31 cols, 18 DV, 13 contrôles). MASTER_FACT_MAN_Charges.xlsx créé (37 cols, 4 requêtes PQ). CTR-2026-06-006 inscrit.
> - Lot 4 (2026-06-09) : SAISIE_ReservationsHorsHostaway.xlsx créé (4 onglets, 30 cols, 11 DV, 13 contrôles, VLOOKUP taux). MASTER_FACT_MAN_ReservationsHorsHostaway.xlsx créé (34 cols, 3 requêtes PQ). CTR-2026-06-007 inscrit. Décisions D046–D051 verrouillées.
> - Lot 4bis (2026-06-09) : MASTER_CALC_Reservations.xlsx créé (3 onglets, 24 cols, 7 requêtes PQ, anti-double-comptage 7 scénarios, 2 BLOQUANTS + 6 A_CONTROLER). CTR-2026-06-008 inscrit. Décisions D052–D057 verrouillées.

**Points résiduels non bloquants à traiter dans les lots suivants :**
- `CARTE_002` suffixe `XXXX` (carte Ewan) → à renseigner au **Lot 8** avant traitement des exports bancaires.
- `mode_facturation = A_DEFINIR` pour tous les propriétaires → à définir au **Lot 12** avant facturation.
- CleaningTasks SKIPPED → à extraire via `--only-cleaning-tasks` au **Lot 6a**.
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

> **Règle D029 — IRRÉVOCABLE** : aucun lot ne peut être marqué FAIT sans entrée dans JOURNAL_CONTROLES.

---

## Ce qui a été modifié (cette session — Lot 4bis)
- Cadrage : `DECISIONS_METIER.md` (D052–D057 ajoutés — QM-L4b-01 à QM-L4b-06)
- Cadrage : `JOURNAL_CONTROLES.md` (CTR-2026-06-008 inscrit)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier — session 9, Lot 4bis)
- Script : `02_TRAVAIL/lot4bis_master_calc_reservations.py` (créé + exécuté)
- Créé : `02_TRAVAIL/Lot4bis_TableCommune/MASTER_CALC_Reservations.xlsx`
  (3 onglets : MASTER 24 cols, VUE_FLUX filtre VALIDE+OUI+montant≠0, POWER_QUERY_CODE 7 requêtes M)
- Dossier créé : `02_TRAVAIL/Lot4bis_TableCommune/`
- REF_Setup.xlsm : NON modifié
- Fichiers Lots 1, 3, 4 : NON modifiés

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

---

## Prochaine action obligatoire
```
Lot 4bis FAIT (2026-06-09) — CTR-2026-06-008 inscrit — en attente validation humaine
Prochain lot : Lot 5 — Acomptes propriétaires
```

---

## Lecture prochaine session (discipline contextuelle)

Pour la prochaine session (Lot 5 — Acomptes propriétaires), l'assistant doit ouvrir uniquement :

```text
À OUVRIR :
- CLAUDE.md (intégral, court)
- ETAT_AVANCEMENT.md (ce fichier)
- PLAN_CONSTRUCTION.md → uniquement Lot 5
- ARCHITECTURE_DONNEES.md → §10.4 (acomptes propriétaires)
- Sortie Lot 4 (VUE_ACTIVE MASTER_FACT_MAN_ReservationsHorsHostaway) + REF_Proprietaires + REF_Logements

À NE PAS OUVRIR (économie de contexte) :
- README_PROJET.md (sauf onboarding)
- OBJECTIF_PROJET_PILOTAGE_CONCIERGERIE_V3.md
- Les sections d'ARCHITECTURE_DONNEES.md hors §10.4
- Tous les autres lots du PLAN
- Données brutes Banque / fichiers Lot 1 détaillés
- MASTER_CALC_Reservations (Lot 4bis) — non utilisé au Lot 5
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

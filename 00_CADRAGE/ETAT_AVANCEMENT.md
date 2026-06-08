# ETAT_AVANCEMENT.md
> Fichier de mémoire inter-sessions. À lire en PREMIER à chaque reprise. À mettre à jour en FIN de session.

---

## Dernière mise à jour
Date : 2026-06-08
Session : Session 5 — Validation Lot 1 Hostaway (fix financeField + audit extraction)
Agent : Claude Code (claude-sonnet-4-6)

---

## Lot en cours
Lot : 2 — Mapping logements
Statut : **À DÉMARRER** (Lots 0 et 1 FAIT)

> Contrôles inscrits :
> - Lot 0 : CTR-2026-06-001 (audit initial), CTR-2026-06-002 (corrections), CTR-2026-06-003 (post-correction — tout vert)
> - Lot 1 : CTR-2026-06-004 (extraction + payout validés — 2026-06-08)
> Prochaine étape : attente feu vert humain pour démarrer Lot 2.

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

> **Règle D029 — IRRÉVOCABLE** : aucun lot ne peut être marqué FAIT sans entrée dans JOURNAL_CONTROLES.

---

## Ce qui a été modifié (cette session)
- Scripts : `02_TRAVAIL/lot1_hostaway_extract.py` (fix financeField 1 ligne : `_ff_from_res(detail)`)
- Données Lot 1 : `02_TRAVAIL/Lot1_Hostaway/` (8 fichiers, run 20260608_134253)
- Journaux : `JOURNAL_CONTROLES.md` (CTR-2026-06-004 ajouté), `JOURNAL_ANOMALIES.md` (ANO-004 mis à jour 29→32, ANO-014 créée, Note 3→4 orphelins)
- Cadrage : `ETAT_AVANCEMENT.md` (ce fichier)

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
| ANO-001 | LISTING_ORPHELIN (515523) | A_CONTROLER | OUVERT — Lot 2 |
| ANO-002 | ENCODAGE_CASSE | A_CONTROLER | **CORRIGÉ 2026-06-07** |
| ANO-003 | DATES_SERIE_EXCEL | A_CONTROLER | **SANS OBJET — aucune série brute** |
| ANO-004 | VRBO_MONTANT_NON_RENSEIGNE (×32) | A_CONTROLER | OUVERT — Lot 4 |
| ANO-005 | REFERENTIEL_ORPHELIN (497801) | A_CONTROLER | OUVERT — Lot 2 |
| ANO-006 | LISTING_CONFIRME_HORS_HOSTAWAY (480780) | INFO | OUVERT — À confirmer Lot 2 |
| ANO-007 | REF_STATUTS_VALEURS_CONTROLE_MANQUANTES | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-008 | REF_STATUTS_PAYOUT_ABSENT | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-009 | REF_CLOTURE_MENSUELLE_ABSENTE | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-010 | REF_PARAMETRES_GENERAUX_INCOMPLETS | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-011 | REF_INTERVENANTS_SCHEMA_INCOMPLET | BLOQUANT | **CORRIGÉ 2026-06-07** |
| ANO-012 | REF_LOGEMENTS_CODES_HORS_PARC_ABSENTS | A_CONTROLER | **CORRIGÉ 2026-06-07** |
| ANO-013 | REF_CARTES_PAIEMENT_SUFFIXE_MANQUANT | A_CONTROLER | IGNORE_JUSTIFIE — Lot 8 |
| ANO-014 | LISTING_ORPHELIN_A_CONTROLER (556954) | A_CONTROLER | OUVERT — Lot 2 |

> **Les 4 cas listingMapId sont distincts** — 515523, 556954, 497801, 480780 — voir JOURNAL_ANOMALIES.md.

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
- D041 : Incidents voyageurs — catégorie `INCIDENT_VOYAGEUR` dans `SAISIE_Charges_Flux.xlsx`, `reservation_id` obligatoire — VERROUILLÉ (P02)
- D042 : AirCover — 3 flux distincts (remboursement propriétaire hors comptes / prestation facturée bloc règlement / impact résultat ligne par ligne) — VERROUILLÉ (P03)
- D043 : Priorité Excel avant Power BI — aucun dashboard `.pbix` livré par les lots — VERROUILLÉ (P32)

---

## Prochaine action obligatoire
```
Lots 0 et 1 FAITS. En attente feu vert humain pour démarrer Lot 2.

Lot 2 — Mapping logements :
- Pré-requis : Lots 0 et 1 FAITS ✓
- Objectif : réconcilier REF_Logements ↔ listings Hostaway, traiter les 4 cas orphelins
  (515523 + 556954 → ORPHELIN_A_CONTROLER à ajouter REF ; 497801 → REF vers absent Hostaway ;
  480780 → sur_hostaway=NON à confirmer), inscrire contrôle dans JOURNAL_CONTROLES.
- Lecture ciblée : PLAN Lot 2 ; REGLES §9 ; ARCHI §4, §16.3, §18.3, §20.
```

---

## Lecture prochaine session (discipline contextuelle)

Pour la prochaine session (Lot 2), l'assistant doit ouvrir uniquement :

```text
À OUVRIR :
- CLAUDE.md (intégral, court)
- ETAT_AVANCEMENT.md (ce fichier)
- PLAN_CONSTRUCTION.md → uniquement Lot 2
- REGLES_METIER.md → §9
- ARCHITECTURE_DONNEES.md → §4, §16.3, §18.3, §20 (via table des matières)
- JOURNAL_ANOMALIES.md → ANO-001, ANO-005, ANO-006, ANO-014 (4 cas listing/REF orphelins)

À NE PAS OUVRIR (économie de contexte) :
- README_PROJET.md (sauf onboarding)
- OBJECTIF_PROJET_PILOTAGE_CONCIERGERIE_V3.md (vision générale, déjà lue)
- Les sections d'ARCHITECTURE_DONNEES.md hors §4/§16.3/§18.3/§20
- Tous les autres lots du PLAN
- Données brutes Hostaway (Lot 1 déjà extrait)
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

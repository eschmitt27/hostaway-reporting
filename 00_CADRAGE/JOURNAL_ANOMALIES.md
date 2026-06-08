# JOURNAL_ANOMALIES.md
> Anomalies détectées et leur statut de résolution. Chaque anomalie = une entrée unique.

---

## Format d'entrée

```
ID         : ANO-AAAA-MM-XXX
Date       : AAAA-MM-JJ
Lot        : Lot X — Nom
Code       : CODE_CONTROLE (cf. JOURNAL_CONTROLES.md)
Sévérité   : BLOQUANT | A_CONTROLER | INFO
Source     : table ou fichier concerné
PK         : clé de la ligne concernée si applicable
Description: description précise
Statut     : OUVERT | EN_COURS | CORRIGÉ | IGNORE_JUSTIFIE
Résolution : action prise ou raison d'ignorer
```

---

## Anomalies ouvertes

### ANO-2026-06-001
Date : 2026-06-04 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : OUVERT
Code : LISTING_ORPHELIN_A_CONTROLER
Source : MASTER_REF_HA_Listings
PK : listingMapId = 515523
Description : Logement présent dans l'export Hostaway, ABSENT de REF_Logements.
Cas : vrai logement ancien ou désactivé (pas une erreur de saisie).
Traitement attendu : ajouter au REF_Setup.xlsm au Lot 2 avec statut explicite.
⚠ Ne pas ignorer définitivement. Ne pas intégrer comme logement actif avant arbitrage.
Résolution : En attente — Lot 2

---

### ANO-2026-06-002
Date : 2026-06-04 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-07
Code : ENCODAGE_CASSE
Source : REF_Setup.xlsm — onglets REF_Associes, REF_Codes_Impact, REF_Types_Flux, REF_Types_Affectation
PK : multiple (26 cellules touchées)
Description : Caractères mal encodés (mojibake UTF-8→latin-1, ex. « associÃ© » au lieu de « associé »).
Onglet REF_Types_Affectation ajouté à la liste — absent du signalement initial.
Traitement appliqué le 2026-06-07 : correction par script Python (lot0_corrections.py), traitement paire par paire (U+00C3+U+00xx → codepoint UTF-8 correct).
Résolution : CORRIGÉ — 0 cellule résiduelle confirmée par audit post-correction CTR-2026-06-003.

---

### ANO-2026-06-003
Date : 2026-06-04 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : CORRIGÉ / SANS OBJET
Code : DATES_SERIE_EXCEL
Source : REF_Setup.xlsm — onglets avec colonnes date
PK : multiple
Description : Signalement préventif de dates potentiellement stockées en numéro de série Excel.
Audit réel du 2026-06-07 : AUCUNE date série brute détectée. Toutes les dates sont correctement
stockées comme objets datetime par Excel. Anomalie sans objet sur données réelles.
Résolution : SANS OBJET — confirmé par audit CTR-2026-06-001.

---

### ANO-2026-06-004
Date : 2026-06-04 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : OUVERT
Code : VRBO_MONTANT_NON_RENSEIGNE
Source : MASTER_FACT_HA_Reservations
PK : 32 réservations vrboical — paymentStatus = Unknown (29 au run 20260523, 32 au run 20260608 — +3 nouvelles réservations VRBO)
Description : Montant financier indisponible dans Hostaway pour ces 32 réservations.
Traitement attendu : saisie manuelle dans MASTER_FACT_MAN_ReservationsHorsHostaway au Lot 4.
Résolution : En attente — Lot 4

---

### ANO-2026-06-005
Date : 2026-06-04 | Lot : 2 — Réconciliation | Sévérité : A_CONTROLER | Statut : OUVERT
Code : REFERENTIEL_ORPHELIN
Source : REF_Setup.xlsm — REF_Logements
PK : listingMapId = 497801
Description : listingMapId 497801 présent dans REF_Logements (sur_hostaway = OUI ?) mais ABSENT de l'export Hostaway.
Cas : logement désactivé, archivé, ou erreur de saisie dans le référentiel.
⚠ Cas DIFFÉRENT de ANO-001 : ici c'est le référentiel qui pointe vers Hostaway, pas l'inverse.
Traitement attendu : trancher au Lot 2 — désactiver dans REF_Logements ou vérifier si listingMapId est correct.
Résolution : En attente — Lot 2

---

### ANO-2026-06-006
Date : 2026-06-04 | Lot : 2 — Réconciliation | Sévérité : INFO | Statut : À CONFIRMER
Code : LISTING_CONFIRME_HORS_HOSTAWAY
Source : REF_Setup.xlsm — REF_Logements
PK : logement_id correspondant à listingMapId = 480780
Description : Logement avec sur_hostaway = NON dans REF_Logements. Cohérent architecturalement (logement géré sans Hostaway).
⚠ Cas DIFFÉRENT de ANO-001 et ANO-005 : ce n'est pas un orphelin, c'est un logement volontairement hors Hostaway.
Traitement attendu : confirmer au Lot 2 que sur_hostaway = NON est bien intentionnel et documenter.
Résolution : En attente — confirmation Lot 2

---

## Note — les 4 cas listingMapId sont distincts

| PK | Sens de l'anomalie | Nature | Action |
|---|---|---|---|
| 515523 | Hostaway → absent REF | Ancien logement archivé | Ajouter au REF avec statut ORPHELIN_A_CONTROLER (Lot 2) |
| 556954 | Hostaway → absent REF | Ancien logement archivé (nouveau — ANO-014) | Ajouter au REF avec statut ORPHELIN_A_CONTROLER (Lot 2) |
| 497801 | REF → absent Hostaway | REF pointe vers un listing inexistant | Vérifier/désactiver dans REF (Lot 2) |
| 480780 | REF sur_hostaway=NON | Logement volontairement hors Hostaway | Confirmer l'intention (Lot 2) |

---

---

### ANO-2026-06-007
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_STATUTS_VALEURS_CONTROLE_MANQUANTES
Source : REF_Setup.xlsm — REF_Statuts
PK : famille statut_controle
Description : Valeurs fermées de statut_controle manquantes dans REF_Statuts : VALIDE, BLOQUANT, IGNORE_JUSTIFIE.
A_CONTROLER présent (famille import) mais les 3 autres requises par PLAN Lot 0 absentes.
Sans ces valeurs, les listes déroulantes de saisie ne peuvent pas pointer vers le référentiel.
Résolution : CORRIGÉ — VALIDE (STAT_021), BLOQUANT (STAT_022), IGNORE_JUSTIFIE (STAT_023) ajoutés en famille statut_controle.

---

### ANO-2026-06-008
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_STATUTS_PAYOUT_ABSENT
Source : REF_Setup.xlsm
PK : onglet REF_Statuts_Payout
Description : Onglet REF_Statuts_Payout requis par PLAN Lot 0 (Archi §23.2bis, D021) absent du fichier.
Sans cet onglet, le Lot 1 (Hostaway) ne peut pas affecter les statuts de calcul payout.
Résolution : CORRIGÉ — onglet créé avec 6 valeurs (NORMAL, ANNULE_SANS_PAYOUT, ANNULE_AVEC_PAYOUT, PAYOUT_ABSENT, PAYOUT_INCOMPLET, A_CONTROLER).

---

### ANO-2026-06-009
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_CLOTURE_MENSUELLE_ABSENTE
Source : REF_Setup.xlsm
PK : onglet REF_Cloture_Mensuelle
Description : Onglet REF_Cloture_Mensuelle requis par PLAN Lot 0 (D024) absent. Structure vide requise
pour que le Lot 8 puisse l'alimenter (statuts mois OUVERT/EN_CONTROLE/CLOTURE).
Résolution : CORRIGÉ — onglet créé (structure vide, 7 colonnes : mois, statut_mois, date_passage_controle, date_cloture, nb_lignes_bancaires_non_classees, nb_controles_bloquants_ouverts, commentaire).

---

### ANO-2026-06-010
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_PARAMETRES_GENERAUX_INCOMPLETS
Source : REF_Setup.xlsm — REF_Parametres_Generaux
PK : TAUX_HORAIRE_MENAGE_INTERNE, ARRONDI_DECIMALES, TOLERANCE_ARRONDI_LIGNE_EUR, TOLERANCE_ARRONDI_CUMUL_EUR
Description : 4 paramètres requis par PLAN Lot 0 absents. TAUX_HORAIRE_MENAGE_INTERNE requis pour Lot 6b
(M04 code le taux en dur à 10 EUR/h et doit migrer vers ce référentiel). Les 3 paramètres d'arrondi (D035)
requis pour les contrôles d'arrondi des Lots 10/11.
Résolution : CORRIGÉ — 4 params ajoutés (PARAM_004 à PARAM_007) avec valeurs 10 / 2 / 0.10 / 1.00.

---

### ANO-2026-06-011
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : BLOQUANT | Statut : CORRIGÉ 2026-06-07
Code : REF_INTERVENANTS_SCHEMA_INCOMPLET
Source : REF_Setup.xlsm — REF_Intervenants
PK : INT_0001 à INT_0005
Description : 3 colonnes requises absentes (nom_normalise, date_debut_validite, date_fin_validite).
type_intervenant en minuscules ('Interne'/'Externe') au lieu de MAJUSCULES ('INTERNE'/'EXTERNE').
Sans nom_normalise, le mapping Google Sheet → M04 (Lot 6b) est impossible.
Résolution : CORRIGÉ — 3 colonnes ajoutées, type_intervenant normalisé en MAJUSCULES,
nom_normalise calculé sans accents (IMENE, KHEIRA, MOUNIR, AISSATA, IMRANE).

---

### ANO-2026-06-012
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : CORRIGÉ 2026-06-07
Code : REF_LOGEMENTS_CODES_HORS_PARC_ABSENTS
Source : REF_Setup.xlsm — REF_Logements
PK : APPARTEMENT_DIVERS, LOGEMENT_DIVERS
Description : Codes techniques hors parc requis par PLAN Lot 0 absents. Nécessaires pour Lots 2 et 6b
(cas mapping orphelin, appartements hors parc réel). Sans eux, le système ne peut pas affecter un logement_id
aux flux hors parc sans violer la règle interdisant APPARTEMENT_DIVERS pour masquer un mauvais mapping.
Résolution : CORRIGÉ — 2 logements techniques ajoutés (actif=OUI, sur_hostaway=NON, forfait=0,
commentaire explicite "jamais utilisé pour masquer un mauvais mapping").

---

## Anomalies corrigées

*(voir statut CORRIGÉ sur ANO-002, ANO-003, ANO-007 à ANO-012 ci-dessus)*

---

### ANO-2026-06-014
Date : 2026-06-08 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : OUVERT
Code : LISTING_ORPHELIN_A_CONTROLER
Source : MASTER_REF_HA_Listings / MASTER_CTRL_HA_Anomalies
PK : listingMapId = 556954
Description : Listing 556954 (T3 Montaudran, Toulouse) présent dans l'export Hostaway (specialStatus=archived, actif=NON),
ABSENT de REF_Logements. Détecté au run 20260608_134253 (CTR-2026-06-004).
22 réservations historiques attachées — incluses dans les 23 LISTING_ORPHELIN_A_CONTROLER du run.
Cas analogue à ANO-001 (515523) : ancien logement archivé, jamais saisi dans le référentiel.
⚠ Ne pas intégrer comme logement actif avant arbitrage.
Traitement attendu : ajouter dans REF_Setup.xlsm / REF_Logements au Lot 2 avec statut ORPHELIN_A_CONTROLER.
Résolution : En attente — Lot 2

---

## Anomalies ignorées justifiées

### ANO-2026-06-013
Date : 2026-06-07 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : IGNORE_JUSTIFIE
Code : REF_CARTES_PAIEMENT_SUFFIXE_MANQUANT
Source : REF_Setup.xlsm — REF_Cartes_Paiement
PK : CARTE_002 (Ewan)
Description : suffixe_carte = 'XXXX' (placeholder). Le vrai suffixe de la carte d'Ewan n'est pas renseigné.
Justification : Non bloquant avant Lot 8. Le suffixe sert uniquement au rapprochement bancaire (Lot 8).
Action à faire au Lot 8 : remplacer 'XXXX' par le vrai suffixe avant traitement des exports bancaires.

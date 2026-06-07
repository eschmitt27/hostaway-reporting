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
Date : 2026-06-04 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : OUVERT
Code : ENCODAGE_CASSE
Source : REF_Setup.xlsm — onglets REF_Associes, REF_Codes_Impact, REF_Types_Flux
PK : multiple
Description : Caractères mal encodés (« associÃ© » au lieu de « associé »). Cause probable : export UTF-8 re-ouvert en Latin-1.
Traitement attendu : corriger à la source dans REF_Setup.xlsm avant toute jointure sur ces colonnes.
Résolution : En attente — Lot 0

---

### ANO-2026-06-003
Date : 2026-06-04 | Lot : 0 — REF_Setup | Sévérité : A_CONTROLER | Statut : OUVERT
Code : DATES_SERIE_EXCEL
Source : REF_Setup.xlsm — onglets avec colonnes date
PK : multiple
Description : Certaines dates stockées en numéro de série Excel (ex. 46023 = 2026-01-01).
Traitement attendu : normaliser au format AAAA-MM-JJ à l'import dans tous les scripts.
Résolution : En attente — Lot 0

---

### ANO-2026-06-004
Date : 2026-06-04 | Lot : 1 — Hostaway | Sévérité : A_CONTROLER | Statut : OUVERT
Code : VRBO_MONTANT_NON_RENSEIGNE
Source : MASTER_FACT_HA_Reservations
PK : 29 réservations vrboical — paymentStatus = Unknown
Description : Montant financier indisponible dans Hostaway pour ces 29 réservations.
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

## Note — les 3 orphelins ne sont pas équivalents

| PK | Sens de l'anomalie | Nature | Action |
|---|---|---|---|
| 515523 | Hostaway → absent REF | Ancien logement désactivé | Ajouter au REF avec statut ORPHELIN_A_CONTROLER (Lot 2) |
| 497801 | REF → absent Hostaway | REF pointe vers un listing inexistant | Vérifier/désactiver dans REF (Lot 2) |
| 480780 | REF sur_hostaway=NON | Logement volontairement hors Hostaway | Confirmer l'intention (Lot 2) |

---

## Anomalies corrigées

*(vide)*

---

## Anomalies ignorées justifiées

*(vide)*

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

### CTR-2026-06-023

```
Date       : 2026-06-14
Lot        : Lot 12 — Préfactures propriétaires
Code       : LOT12_PREFACTURES_PROPRIETAIRES
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot12_generer_factures.py
             02_TRAVAIL/Lot12_Factures/MASTER_FACT_Proprietaires.xlsx
Résultat   : Module Lot 12 créé — préfactures propriétaires (lecture seule sorties Lot 10/11).
             5 onglets : FACT_FACTURE_ENTETE / FACT_FACTURE_LIGNES / CONTROLE_MENSUEL /
                         DASHBOARD_FACTURATION / A_CONTROLER
             269 préfactures générées / 3 228 lignes facture (12 lignes par préfacture, §17.3)
             0 facture finale générée
             statut_generation : PREFACTURE_CONTROLE (269/269)
             statut_facture    : NON_FACTURABLE_A_CONTROLER (269/269)
             Balises visibles si données manquantes (logo/SIRET/adresse/mode/banque/...)
             GLOBAL_NON_AFFECTE exclu des factures propriétaires (frais bancaires société, hors REGLEMENT)
             Granularité prop × logement × mois ; numérotation PREF-AAAA-MM-PROP-LOG-NNN
             Contrôles : 0 BLOQUANT / 60 A_CONTROLER (59 réservations exclues + 1 MODE_FACTURATION_A_DEFINIR)
             0 donnée fictive résiduelle ; sources amont non modifiées
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
```

**Décisions appliquées (validées 2026-06-15) :** D-LOT12-01 à D-LOT12-08.

**Test fictif complet (cycle seed → pipeline → lot12 → remove) :**
- seed exécuté (Charges + M04 + MenExt + HH + Acomptes)
- pipeline relancé jusqu'à Lot 12 (lot4bis → lot9 → lot10 → lot11 → lot12)
- HH fictives visibles dans les préfactures (PROP_0001/LOG_0001/2026-03 : L1 payout 1 388,02 incluant HH_001, nb_resa 10)
- acompte fictif `ZZ_TEST_ACOMPTE_001` (VALIDE) visible en règlement : L11 = 222,22 € ; reste_à_payer 367,75 = 589,97 − 222,22
- acompte fictif `ZZ_TEST_ACOMPTE_002` (A_CONTROLER) exclu du règlement : L11 = 0
- flux HC fictifs : statut non final maintenu ; REEL = COMPTABLE + HC OK (282 775,85 = 283 220,29 + (−444,44)) ; lot11 0 BLOQUANT
- 0 facture finale même avec fictif ; 12 lignes par préfacture ; GLOBAL_NON_AFFECTE hors facture
- L5 (revenu net exploitation) ≠ L9 (reste à payer) — pas de confusion exploitation/règlement (D033)
- remove exécuté ; retour baseline réel (1333 flux / REEL 283 442,51 / lot11 51 contrôles 0 BLOQUANT / lot12 269 préfactures)
- 0 `ZZ_TEST_` / 0 `__TEST_FICTIF_LOT12_A_SUPPRIMER__` ; 11 xlsx données restaurés à l'état commité

**Limites actuelles (Lot 12 = préfactures uniquement) :**
Aucune facture finale tant que `facturation_lot12_ok != OUI` (DASHBOARD_MOIS, Lot 11). Blocages :
- 44 A_CONTROLER ouverts Lot 11
- modes de facturation propriétaires à définir (12 props `mode_facturation = A_DEFINIR`)
- banque non clôturée (52 lignes RAPPROCHEMENT_REQUIS)
- sources réelles M04 / Charges / IK / Acomptes encore partielles ou vides (HORS_COMPTA réel = 0 → balise DONNEES_PARTIELLES)
- coordonnées société / logo / SIRET sous balises (absents de REF_Parametres_Generaux)

---

### CTR-2026-06-022

```
Date       : 2026-06-14
Lot        : Lot 10 — Correctif HH / HC / charges globales / acomptes
Code       : CORRECTIF_LOT10_HH_HC_GLOBAL_ACOMPTES
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot10_calculer_resultats.py
             02_TRAVAIL/lot12_seed_donnees_fictives.py
             02_TRAVAIL/lot12_remove_donnees_fictives.py
Résultat   : 4 défauts révélés par les tests fictifs (CTR-2026-06-021) corrigés :
               1. HH / VRBO ne cherchent plus de payout Hostaway.
                  - branche HH : montant depuis la saisie (total_percu, menage, taux_commission)
                  - commission HH recalculée : (total_percu - menage) × taux_commission
                  - JOINTURE_PAYOUT_MANQUANTE limité à la branche Hostaway
                  - source_type = HH ; integration seulement si total_percu renseigné + VALIDE (D-LOT10C-05)
               2. GLOBAL HORS_COMPTA n'est plus forcé à 0.
                  - vision HC calculée depuis df_hc (flux inclure_resultat_hors_compta=OUI)
                  - contrôle ajouté CTR-LOT10-20 : REEL = COMPTABLE + HORS_COMPTA
                    (BLOQUANT REEL_DIFF_COMPTABLE_PLUS_HC si écart > 1,00 EUR)
               3. Charges sans logement/proprietaire visibles en GLOBAL_NON_AFFECTE.
                  - sentinelle (plus de groupby dropna) ; pas de ventilation arbitraire
               4. Acomptes lus depuis MASTER_FACT_MAN_AcomptesProprietaires (VALIDE).
                  - intégrés UNIQUEMENT dans REGLEMENT (autres_acomptes_recus)
                  - n'impactent jamais revenu_net_exploitation / commission / net / REEL / COMPTABLE
                  - granularité mois × logement × proprietaire, fallback proprietaire (D-LOT10C-02)
             Scripts fictifs Lot 12 mis à jour (HH réactivé + acomptes ajoutés).
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
```

**Décisions appliquées (validées 2026-06-14) :**
- D-LOT10C-01 : commission HH recalculée `(total_percu − menage) × taux` ; flag A_CONTROLER si écart saisie/recalcul > 0,10 €
- D-LOT10C-02 : acomptes granularité mois × logement × proprietaire (fallback proprietaire), REGLEMENT seulement
- D-LOT10C-03 : charges globales/non-affectables → ligne dédiée GLOBAL_NON_AFFECTE (pas de ventilation)
- D-LOT10C-04 : charges Lot 3 n'impactent PAS revenu_net_exploitation (formule fermée D031), seulement REEL/COMPTABLE
- D-LOT10C-05 : HH/VRBO intégrées seulement si total_percu renseigné + VALIDE, sinon A_CONTROLER

**Contrôles ajoutés :** CTR-LOT10-20 (REEL=COMPTABLE+HC), CTR-LOT10-21 (commissions HH), CTR-LOT10-22 (contrôles HH), CTR-LOT10-23 (lignes GLOBAL_NON_AFFECTE), CTR-LOT10-24 (acomptes REGLEMENT).

**CHANGEMENT DE BASELINE — justifié (correction, pas régression) :**
```
Ancien REEL : 283 515,60 €
Nouveau REEL : 283 442,51 €
Écart        : 73,09 €
```
Justification : 8 frais bancaires (`TYPE_FLUX_016`, `sens=CHARGE`, sans `logement_id` ni `proprietaire_id`,
source `BANQUE_LOT8_IMPORT_NORM_Banque`) étaient **perdus silencieusement** par l'agrégation
(`groupby dropna=True`). Désormais visibles dans `GLOBAL_NON_AFFECTE` et inclus au total.
Les 8 lignes : 9,00 + 18,00 + 0,54 + 9,00 + 9,00 + 9,00 + 18,00 + 0,55 = **73,09 €**.
Égalité prouvée : `283 515,60 − 283 442,51 = 73,09 €`.
**Ce n'est PAS une régression** : c'est la correction du drop silencieux (défaut #3).
Produits RES Hostaway inchangés (283 709,60 €) — garde-fou respecté.

**Non-régression Hostaway confirmée (audit) :**
1321 NORMAL / payout 283 709,60 / ménage 55 619,00 / assiette 228 090,60 /
commission 39 167,42 / net avant charge 188 923,18 € — tous inchangés.

**NOTE DE CORRECTION DOCUMENTAIRE (CTR-2026-06-019) :**
L'ancien commentaire CTR-2026-06-019 indiquait à tort que les `194 €` de charges incluaient
« 4 ménages externes + 8 frais bancaires ». **Correction** : les `194 €` correspondaient aux
**4 ménages externes seulement** ; les `73,09 €` de frais bancaires étaient **perdus** avant ce
correctif. Total charges REEL réel = 194,00 + 73,09 = 267,09 €.

**Résultats test fictif (cycle complet, avant restauration) :**
- baseline 1333 flux / REEL 283 442,51 / REEL=COMPTABLE+HC OK / 0 BLOQUANT
- avec fictif 1342 flux / NORMAL 1323 (HA 1321 + HH 2) / REEL 282 775,85 / COMPTABLE 283 220,29 /
  HORS_COMPTA −444,44 / REEL=COMPTABLE+HC OK (écart 0) / GLOBAL_NON_AFFECTE 3 / acomptes 222,22 REGLEMENT / lot11 0 BLOQUANT
- après suppression : retour 1333 / REEL 283 442,51 / 0 fictif résiduel
- 11 xlsx données restaurés à l'état commité (0 Excel suivi modifié)

---

### CTR-2026-06-021

```
Date       : 2026-06-14
Lot        : Lot 9 — Correctif ingestion Charges + M04 + outils test Lot 12
Code       : CORRECTIF_LOT9_INGESTION_CHARGES_M04
Sévérité   : INFO
Fichier    : 02_TRAVAIL/lot9_construire_flux.py
             02_TRAVAIL/lot12_seed_donnees_fictives.py
             02_TRAVAIL/lot12_remove_donnees_fictives.py
Résultat   : Lot 9 étend l'ingestion du Flux avec deux sources jusqu'ici non câblées :
               - Charges (Lot 3, MASTER_FACT_MAN_Charges) — statut_controle = VALIDE
                 sens / code_impact / type_flux portés par la ligne
               - M04 ménages internes (Lot 6b, M04_MENAGES_PowerQuery) — statut_controle = VALIDE
                 CHARGE, code_impact = HC (M2 verrouillé), TYPE_FLUX_013
             IK exclu du Flux (conforme décision « IK hors Flux / vue dédiée », §15.3).
             Acomptes différés à la passe Lot 10.
             Garde-fou ajouté : CTR-9-011 — portion RES (TYPE_FLUX_017) inchangée (BLOQUANT si écart).
             Test fictif réalisé (seed / pipeline / remove) :
               - baseline               : 1 333 flux
               - avec données fictives   : 1 340 flux (5 CHG + 1 M04 + 1 MenExt + 2 HH différées exclues)
               - après suppression       : retour 1 333 flux
               - 0 donnée fictive résiduelle (scan ZZ_TEST_ / tag sur 30 Excel suivis)
             Sources amont en lecture seule.
             Données Excel restaurées à l'état commité (git checkout après test).
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
```

**Outils de test fictif Lot 12 :**
- `lot12_seed_donnees_fictives.py` : insertion idempotente, backup avant écriture (99_ARCHIVES/LOT12_TEST_DATA/), tag obligatoire, append-only (aucune ligne réelle modifiée).
- `lot12_remove_donnees_fictives.py` : suppression par ID `ZZ_TEST_` OU tag `__TEST_FICTIF_LOT12_A_SUPPRIMER__`, backup avant suppression, vérification 0 résiduel.
- IDs fictifs : préfixe `ZZ_TEST_`. Tag : `__TEST_FICTIF_LOT12_A_SUPPRIMER__`. Montants reconnaissables : 111.11 / 222.22 / 333.33.
- Backups fictif (`99_ARCHIVES/LOT12_TEST_DATA/`) ignorés git (règle #10 — jamais versionnés).

**PRÉREQUIS BLOQUANT — ne PAS peupler Charges/M04 avec des données réelles HC avant le correctif Lot 10.**
Les tests fictifs ont révélé 4 points à traiter dans la passe Lot 10 :
1. Lot 10 `GLOBAL HORS_COMPTA` reste à 0 même si des flux HC existent → casse l'égalité REEL = COMPTABLE + HC (capté en BLOQUANT par CTR-LOT11 — preuve que les contrôles fonctionnent).
2. Certaines charges globales / non affectables (sans `logement_id`) sont perdues dans l'agrégation des résultats Lot 10.
3. HH (réservations hors Hostaway) doit être géré par le montant saisi (total_perçu − ménage − commission), pas par le payout Hostaway (absent → JOINTURE_PAYOUT_MANQUANTE).
4. Acomptes doivent aller uniquement dans le bloc REGLEMENT, jamais dans l'exploitation (D031/D032/D033).

Tant que Charges/M04 restent vides, le correctif Lot 9 est **neutre** (baseline 1 333 inchangée, vérifié).

---

### CTR-2026-06-020

```
Date       : 2026-06-14
Lot        : Lot 11 — Contrôles de cohérence globaux
Code       : CONSTRUCTION_LOT11_CONTROLES_COHERENCE
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot11_Controles/MASTER_CTRL_Coherence.xlsx
             02_TRAVAIL/lot11_controles_coherence.py
Résultat   : Script exécuté. 51 contrôles générés. 0 BLOQUANT. 44 A_CONTROLER. 7 INFO.
Statut     : OUVERT — EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Résultats partiels — M04/Charges/IK/HH/Acomptes non encore alimentés.
             0 BLOQUANT : aucun blocage structurel.
             44 A_CONTROLER à traiter/valider humainement avant facturation Lot 12.
             AUCUN mois facturable (facturation_lot12_ok = NON_SOUS_RESERVE_A_CONTROLER partout).
             Banque disponible localement (BANQUE_DISPONIBLE).
```

**Corrections appliquées après audit (2026-06-14) :**
- F1 (obligatoire) : `facturation_lot12_ok` = OUI uniquement si 0 BLOQUANT ET 0 A_CONTROLER ET mois banque CLOTURE.
  Sinon valeur prudente (NON_BLOQUANT_OUVERT / NON_SOUS_RESERVE_A_CONTROLER / NON_CLOTURE_INCOMPLETE / NON_DONNEES_INCOMPLETES).
- F2 (obligatoire) : `_is_empty()` détecte désormais les lignes placeholder Power Query
  ("Alimenté par Power Query", "Charge par Power Query", "# formule", préfixes techniques).
  CHARGES/M04/IK/HH désormais correctement détectées VIDES → INFO 3 → 7.
- F3 (cosmétique) : code mort `vrbo_ac` supprimé ; print "sur N mois banque" dynamique ;
  DASHBOARD limité aux mois porteurs de contrôles + mois banque + TRANSVERSE (plus de mois vides arbitraires).

**Décisions appliquées :**
- D-LOT11-01 Banque : Option C adaptative — fichier Lot 8 lu en local, contrôles banque exécutés
- D-LOT11-02 REF_Cloture_Mensuelle : non modifiée (dashboard dans DASHBOARD_MOIS)
- D-LOT11-03 Rapprochement payout/banque : indicatif, non bloquant (statut INFORMATIF)
- D-LOT11-04 Re-vérification indépendante depuis tables sources (pas de reprise Lot 10)
- D-LOT11-05 CAISSE_THEORIQUE : produite avec 0 + flag CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES

**Contrôles BLOQUANTS : 0**
```
CTR-LOT11-BL-01  PK_MANQUANTE_OU_DOUBLONNEE                    : 0 — OK (Flux/Res/Payout/Commissions)
CTR-LOT11-BL-02  DOUBLON_RESERVATION_FLUX                      : 0 — OK
CTR-LOT11-BL-03  JOINTURE_RESERVATIONS_MANQUANTE               : 0 — OK
CTR-LOT11-BL-04  JOINTURE_PAYOUT_MANQUANTE                     : 0 — OK
CTR-LOT11-BL-05  REVENU_NET_EXPLOITATION_INCOHERENT            : 0 — OK (formule payout-menage-com-cf)
CTR-LOT11-BL-06  CONFUSION_PAYOUT_SOLDE_FACTURE                : 0 — OK
CTR-LOT11-BL-07  PAIEMENT_DEJA_RECU_DEDUIT_DU_PAYOUT          : 0 — OK
CTR-LOT11-BL-08  COMMISSION_INCOHERENTE                        : 0 — OK
CTR-LOT11-BL-09  ASSIETTE_COMMISSION_INCOHERENTE               : 0 — OK
CTR-LOT11-BL-10  REEL_INCOHERENT_VS_COMPTABLE_PLUS_HC         : 0 — OK (283515.60 = 283515.60 + 0)
CTR-LOT11-BL-11  CHARGE_EXCEPTIONNELLE_DANS_CHARGE_FIXE       : 0 — OK (forfait REF = charge_fixe REGLEMENT)
CTR-LOT11-BL-12  BANQUE_PAYOUT_POTENTIEL_DEJA_HOSTAWAY        : 0 — OK (0 flux PRODUIT banque)
CTR-LOT11-BL-13  COMMISSION_SANS_TAUX                         : 0 — OK (tous taux renseignés)
```

**Contrôles A_CONTROLER : 44 (répartition)**
```
CTR-LOT11-AC-01  LISTING_ORPHELIN_A_CONTROLER        : 23 lignes (reservations listingMapId 515523 / 556954)
                 → Source : MASTER_CTRL_HA_Anomalies — depuis Lot 1, statut OUVERT
                 → Action : confirmer logement inactif + supprimer ou mapper dans REF_Logements
CTR-LOT11-AC-02  CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE : 14 logements
                 → 14 vs 12 en Lot 10 : Lot 11 vérifie TOUS les logements avec TYPE_FLUX_017
                   (Lot 10 vérifiait seulement logements avec forfait>0)
                 → Action : corriger date_entree_gestion dans REF_Logements (lot REF séparé)
CTR-LOT11-AC-03  CLOTURE_IMPOSSIBLE_LIGNE_BANCAIRE_NON_CLASSEE : 3 (mois 2026-02/03/04)
                 → 52 lignes RAPPROCHEMENT_REQUIS dans NORM_Banque
                 → Action : exporter données Airbnb, rapprocher RAPPROCH_AIRBNB_ATTENTE (Lot 8c)
CTR-LOT11-AC-04  LOG_SANS_FLUX_017                   : 1 (LOG_0009 — forfait=40€, 0 réservation Flux)
                 → Re-détecté indépendamment — confirmé
                 → Action : vérifier si LOG_0009 actif / a eu des réservations
CTR-LOT11-AC-05  VRBO_MONTANT_NON_RENSEIGNE          : 1 entrée agrégée (32 réservations VRBO)
                 → Action : saisie manuelle dans SAISIE_ReservationsHorsHostaway.xlsx (Lots 4/4bis)
CTR-LOT11-AC-06  RESERVATION_A_CONTROLER_SANS_COMMISSION : 1 entrée agrégée (59 réservations)
                 → Net propriétaire incomplet pour 4% des réservations
                 → Action : Lots 4/4bis + décision par réservation
CTR-LOT11-AC-07  MENAGE_EXTERNE_DATE_ABSENTE         : 1 (9 lignes Aissata fac.2026-37)
                 → Action : préciser date_menage auprès du prestataire
```

**Contrôles INFO : 7**
```
CTR-LOT11-IN-01  HC_ZERO_SOURCES_VIDES (CHARGES)     : SAISIE_Charges_Flux non peuplée — placeholder PQ
CTR-LOT11-IN-02  HC_ZERO_SOURCES_VIDES (M04)         : M04 SOURCE_RAW vide — placeholder PQ
CTR-LOT11-IN-03  HC_ZERO_SOURCES_VIDES (ACOMPTES)    : acomptes non saisis — 0 attendu
CTR-LOT11-IN-04  HC_ZERO_SOURCES_VIDES (IK)          : IK/avantages non saisis — placeholder formule
CTR-LOT11-IN-05  HC_ZERO_SOURCES_VIDES (HH)          : ReservationsHorsHostaway vide — placeholder PQ
CTR-LOT11-IN-06  HC_ZERO_SOURCES_VIDES (TRANSVERSE)  : HORS_COMPTA = 0 global — M04/Charges/IK vides
CTR-LOT11-IN-07  MODE_FACTURATION_A_DEFINIR          : 12 propriétaires — bloquant pour Lot 12
```
> Note F2 : INFO 3 → 7 après correction de la détection placeholder Power Query.
> CHARGES/M04/IK/HH ne contenaient qu'une ligne d'instruction technique (pas de donnée métier).

**DASHBOARD_MOIS (4 lignes — F1/F3 appliqués)**
```
mois        BL  AC INFO  statut_banque  facturation_lot12_ok
2026-02      0   1   0    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
2026-03      0   1   0    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
2026-04      0   1   0    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
TRANSVERSE   0  41   7    OUVERT         NON_SOUS_RESERVE_A_CONTROLER
```
> AUCUN mois marqué OUI. Facturation Lot 12 interdite tant que A_CONTROLER non traités
> et clôture banque non validée (REGLES §11 C5/C6/C7).

**Rapprochement payout / banque (indicatif)**
```
Banque statut : BANQUE_DISPONIBLE (fichier Lot 8 présent localement)
Mois 2026-02  : banque=197.03€ / HA=14020.13€ / écart=-13823.10€
Mois 2026-03  : banque=1983.79€ / HA=16752.21€ / écart=-14768.42€
Mois 2026-04  : banque=1341.36€ / HA=18888.65€ / écart=-17547.29€
NOTE : écarts normaux — mois Hostaway = check-in date, banque = date virement réel
       (décalage 1-2 mois). 39 virements Airbnb EN_ATTENTE_EXPORT_AIRBNB dans RAPPROCH_AIRBNB_ATTENTE.
```

**Caisse théorique**
```
Solde : 0.00 EUR — CAISSE_NON_REPRESENTATIVE_SOURCES_VIDES
HH/Charges/Acomptes vides → structure stable pour recalculs futurs.
```

**Sources amont :** AUCUNE modification (toutes read_only=True)
- MASTER_CALC_Flux.xlsx ✓   MASTER_CALC_Reservations.xlsx ✓   MASTER_CALC_HA_Payout.xlsx ✓
- MASTER_CALC_Commissions.xlsx ✓   MASTER_CALC_Resultats.xlsx ✓   MASTER_CALC_NetProprietaire.xlsx ✓
- REF_Setup.xlsm ✓   Lot8_Banque/ ✓   Fichiers bancaires bruts ✓

---

### CTR-2026-06-019

```
Date       : 2026-06-13
Lot        : Lot 10 — Résultats, commissions & net propriétaire
Code       : CONSTRUCTION_LOT10_COMMISSIONS_RESULTATS_NET
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
             02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
             02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx

Script     : 02_TRAVAIL/lot10_calculer_resultats.py

Jointure confirmée par test valeurs réelles :
  MASTER_CALC_Flux.source_pk         = "RES-2025-01-HA-001" (reservation_calc_id)
    -> MASTER_CALC_Reservations.reservation_calc_id
    -> MASTER_CALC_Reservations.reservation_id_hostaway (entier, ex. 60504160)
    -> MASTER_CALC_HA_Payout.reservation_id             (même format entier)

Contrôles exécutés (19 points) :
  CTR-LOT10-01  Flux lus depuis MASTER_CALC_Flux              : 1 333 [OK]
  CTR-LOT10-02  Reservations NORMAL intégrées aux commissions  : 1 321 [OK]
  CTR-LOT10-03  Reservations A_CONTROLER exclues               : 59 [OK]
  CTR-LOT10-04  Total payout calculé (NORMAL)                  : 283 709.60 €
  CTR-LOT10-05  Total ménage retenu                            : 55 619.00 €
  CTR-LOT10-06  Total assiette commission                       : 228 090.60 €
                Vérification : 283 709.60 - 55 619.00 = 228 090.60 [OK]
  CTR-LOT10-07  Total commission conciergerie                   : 39 167.42 €
  CTR-LOT10-08  Total net propriétaire avant charge fixe        : 188 923.18 €
                Vérification : 228 090.60 - 39 167.42 = 188 923.18 [OK]
  CTR-LOT10-09  Total charge fixe mensuelle générée             : 7 645.00 €
                (233 lignes mois x logement / 13 logements avec forfait > 0)
  CTR-LOT10-10  Total net propriétaire après charge fixe        : 181 278.18 €
                Vérification : 188 923.18 - 7 645.00 = 181 278.18 [OK]
  CTR-LOT10-11  Résultat REEL global (Flux)                    : 283 515.60 €
                Note : différence avec payout (194.00 €) = charges Flux (4 ménages + 8 frais bancaires)
  CTR-LOT10-12  Résultat COMPTABLE global (Flux)               : 283 515.60 €
                REEL = COMPTABLE [attendu — tout IC à ce stade]
  CTR-LOT10-13  Résultat HORS_COMPTA global (Flux)             : 0.00 € [HC_ZERO_SOURCES_VIDES]
                Sources vides : M04 / Charges / IK / Acomptes non alimentés
  CTR-LOT10-14  PAR_MOIS_LOGEMENT total / dont REEL            : 497 lignes / 248 REEL [OK]
                (248 REEL + 248 COMPTABLE + 1 HORS_COMPTA placeholder)
  CTR-LOT10-15  PAR_MOIS_PROPRIETAIRE total / dont REEL        : 404 lignes / 202 REEL [OK]
                (202 REEL + 202 COMPTABLE)
  CTR-LOT10-16  CHARGE_FIXE_DATE_ENTREE_GESTION_INCOHERENTE    : 12 logements [A_CONTROLER — attendu]
                Cause : date_entree_gestion REF = 2026-01-01 pour tous logements,
                        mais réservations Flux remontent à 2025.
                        date_entree_gestion = date création SAS/référentiel, pas début réel de gestion.
                        Décision : ne pas corriger REF_Setup maintenant — lot séparé si nécessaire.
  CTR-LOT10-17  LOG_SANS_FLUX_017                              : 1 logement [A_CONTROLER]
                LOG_0009 (T3 Montaudran) : forfait=40€ mais aucune réservation TYPE_FLUX_017 dans Flux.
                A vérifier : logement effectivement en gestion ? Données manquantes ?
  CTR-LOT10-18  Contrôles BLOQUANTS détectés                   : 0 [OK]
  CTR-LOT10-19  Sources amont (lecture seule)                  : [OK]
                MASTER_CALC_Flux / MASTER_CALC_Reservations / MASTER_CALC_HA_Payout / REF_Setup.xlsm
                non modifiés — vérifiés par git status (0 fichier M)

Charge fixe mensuelle — règle appliquée (Option A, D-LOT10-04) :
  - Source : REF_Logements.forfait_logiciel_consommables_mensuel
  - Exposé : charge_fixe_mensuelle
  - Début  : premier mois TYPE_FLUX_017 dans MASTER_CALC_Flux par logement
  - Fin    : date_sortie_gestion si actif=NON, sinon dernier mois TYPE_FLUX_017
  - Cas LOG_0003 (actif=NON, sortie=2026-04-26) : charge fixe arrêtée à 2026-04 [OK]
  - Logements à forfait=0 : aucune ligne générée (LOG_0004, LOG_0016, LOG_0017, divers)

Séparation exploitation / règlement :
  - EXPLOITATION : charge_fixe=0 par réservation (non proratisée — D-LOT10-01)
  - REGLEMENT    : charge_fixe 1x par mois x logement actif
  - Acomptes     : 0 (sources vides — A_CONTROLER sur tous les reste_a_payer)

Fichiers créés :
  - 02_TRAVAIL/lot10_calculer_resultats.py
  - 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Commissions.xlsx
    (onglets : COMMISSIONS 1321L + A_CONTROLER 59L)
  - 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_Resultats.xlsx
    (onglets : PAR_MOIS_LOGEMENT + PAR_MOIS_PROPRIETAIRE + GLOBAL)
  - 02_TRAVAIL/Lot10_Resultats/MASTER_CALC_NetProprietaire.xlsx
    (onglets : EXPLOITATION + REGLEMENT 269L + VUE_MOIS 220L)

Fichiers non modifiés : tout le reste (Flux, Reservations, Payout, REF_Setup, banque, Lot 9)
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 11 ne peut pas démarrer avant validation + commit Lot 10 (D029).
             LOG_0009 et date_entree_gestion à traiter en lot séparé REF_Setup ultérieur.
```

---

### CTR-2026-06-017

```
Date       : 2026-06-11
Lot        : Lot 9 — Table de flux unifiée MASTER_CALC_Flux
Code       : CONSTRUCTION_MASTER_CALC_FLUX_LOT9
Sévérité   : INFO
Fichier    : 02_TRAVAIL/Lot9_FluxUnifie/MASTER_CALC_Flux.xlsx
Résultat   : Construction sur données réelles. Script lot9_construire_flux.py exécuté.
             10 contrôles BLOQUANTS tous OK.

             Volumes produits :
               - RES : 1 321 flux (TYPE_FLUX_017, PRODUIT, IC — VUE_FLUX MASTER_CALC_Reservations)
               - MEN : 4 flux (TYPE_FLUX_014, CHARGE, IC — MenagesExternes VALIDE 2026-05)
               - BNQ : 8 flux (TYPE_FLUX_016, CHARGE, IC — frais bancaires Lot 8 VALIDE)
               - TOTAL : 1 333 flux / 22 colonnes / 0 doublon / 0 montant négatif

             Sources :
               - MASTER_CALC_Reservations VUE_FLUX : 1 321 lignes VALIDE
                 (AIRBNB 1235 / BOOKING 86 — tous IC, PRODUIT, TYPE_FLUX_017)
               - MASTER_FACT_MEN_MenagesExternes MASTER VALIDE : 4 lignes
                 (MENEXT-2026-05-AISSATA-001/007/008/009 — IC, CHARGE, 29+55+55+55=194€)
               - BANQUE_LOT8_IMPORT NORM_Banque TYPE_FLUX_016 VALIDE : 8 lignes
                 (frais bancaires — IC, CHARGE, commentaire générique sans libellé brut)

             Contrôles BLOQUANTS :
               CTR-9-001 : 3 fichiers sources présents [OK]
               CTR-9-002 : VUE_FLUX non vide [OK]
               CTR-9-003 : VUE_FLUX volume >= 1000 (1321) [OK]
               CTR-9-004 : 0 montant négatif [OK]
               CTR-9-005 : tous sens valides (PRODUIT/CHARGE) [OK]
               CTR-9-006 : tous code_impact valides (IC uniquement) [OK]
               CTR-9-007 : 1333 flux_id uniques [OK]
               CTR-9-008 : 0 doublon source technique [OK]
               CTR-9-009 : volume = 1321+4+8 = 1333 [OK]
               CTR-9-010 : aucune colonne bancaire sensible dans schema [OK]

             Sécurité bancaire : libelle, libelle_brut, compte_id absents de MASTER_CALC_Flux.xlsx [OK]
             Commentaire frais bancaires : "Frais bancaires validés Lot 8" (générique) [OK]

             TYPE_FLUX_017 = REVENU_RESERVATION_HOSTAWAY créé dans REF_Setup.xlsm.
             Backup REF_Setup : 99_ARCHIVES/LOT9_FluxUnifie/REF_Setup_backup_lot9_20260611_213323.xlsm

             Répartition par mois : 2025-01 (7) à 2027-02 (2) — 23 mois couverts
Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Sources vides à ce stade : IK (0), Acomptes (0), Charges (0), M04 (0).
             Ces sources alimenteront MASTER_CALC_Flux lors de leur remplissage — re-run idempotent.
```

---

### CTR-2026-06-018

```
Date       : 2026-06-13
Lot        : Lot 1 — Correctif Payout final : menage_retenu date-aware + REF historique 2025
Code       : CORRECTIF_LOT1_PAYOUT_MENAGE_REF_SETUP
Sévérité   : BLOQUANT (bug bloquant Lot 10)
Fichier    : 02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx

--- Diagnostic initial ---
Bug identifié : menage_retenu = 0 pour 1 235 réservations Airbnb NORMAL.
Cause : _airbnb() lisait ffd.get("cleaningFee") — finance fields Airbnb vides → 0.0.

--- Correctif #1 (invalidé) ---
Tentative : menage_retenu = cleaningFee_res (champ API Hostaway, prix plateforme voyageur).
Résultat diagnostic : cleaningFee_res = prix facturé au voyageur (Studio 40€ vs std 29€, etc.)
                       ≠ coût standard conciergerie. 1 233 / 1 235 lignes avec écart individuel.
Décision humaine : cleaningFee_res non valide comme source de menage_retenu.
                   Règle métier : menage_retenu = REF_Couts_Standards_Menage uniquement.

--- Correctif #2 (intermédiaire, insuffisant) ---
Source : REF_Couts_Standards_Menage sans contrôle de validité temporelle.
Problème : 814 réservations Airbnb NORMAL avec checkInDate < 2026-01-01 tombaient à
           menage_retenu = 0 dès qu'un lookup date-aware était appliqué.
           Coût standard REF_Setup en vigueur depuis 2026-01-01 uniquement → trou historique 2025.
Supplanté par correctif final.

--- Correctif final — RETENU (2026-06-13) ---
Approche en deux volets :
  1. Lookup date-aware : menage_retenu sélectionné selon
       date_debut_validite <= checkInDate AND (date_fin_validite IS NULL OR checkInDate <= date_fin_validite)
       Date de référence = checkInDate de la réservation (jamais date du jour / date recalcul).
  2. Extension REF_Couts_Standards_Menage avec 5 lignes historiques 2025 :
       COUT_STD_2025_TYPE_001 | TYPE_001 | 29€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_002 | TYPE_002 | 39€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_003 | TYPE_003 | 55€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_004 | TYPE_004 | 69€  | 2025-01-01 → 2025-12-31
       COUT_STD_2025_TYPE_005 | TYPE_005 | 110€ | 2025-01-01 → 2025-12-31
     Mêmes montants que 2026 — aucun tarif différent connu pour 2025.
     Lignes 2026 (COUT_MEN_001 à COUT_MEN_005 — date_fin_validite NULL) inchangées.
     Aucun chevauchement : 2025-12-31 < 2026-01-01.

Périmètre : AIRBNB NORMAL + BOOKING NORMAL (règle homogène toutes plateformes).
Annulations avec payout : menage_retenu = 0 conservé (D030 irrévocable).
Script : lot1_hostaway_extract.py — PayoutCalculator utilise _lookup_menage_by_date()
         (DataFrame date-aware remplace dict simple). Retourne 5-tuple + meta_dict.
Mode : --recalc-payout-only --payout-source <backup> (sans relance API).
Backup source : 99_ARCHIVES/LOT1_Hostaway/MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx
REF_Setup backup : 99_ARCHIVES/REF_Setup_BACKUP_20260613_175507.xlsm

8 colonnes de traçabilité ajoutées dans MASTER_CALC_HA_Payout.xlsx :
  menage_retenu_source, cout_standard_id, cout_standard_menage_snapshot,
  cout_standard_date_debut_validite, cout_standard_date_fin_validite,
  logement_id_snapshot, type_logement_id_snapshot, date_reference_cout_menage

19 contrôles obligatoires (correctif final) :
  CTR-1   Lignes Airbnb NORMAL traitées         : 1 235 [OK]
  CTR-2   Lignes Booking NORMAL traitées        : 86 [OK]
  CTR-3   menage_retenu Airbnb AVANT (bug=0)    : 0.00 €
  CTR-4   menage_retenu Airbnb APRÈS REF_Setup  : 51 727.00 €
  CTR-5   menage_retenu Booking AVANT           : 3 810.00 € (était cleaningFee_res)
  CTR-6   menage_retenu Booking APRÈS REF_Setup : 3 892.00 € (delta +82€)
  CTR-7   assiette Airbnb AVANT                 : 263 043.22 €
  CTR-8   assiette Airbnb APRÈS                 : 211 316.22 € (delta -51 727€)
  CTR-9   assiette Booking AVANT                : 16 856.38 €
  CTR-10  assiette Booking APRÈS                : 16 774.38 € (delta -82€)
  CTR-11  Écart cleaningFee_res vs cout_std AB  : -501.00 € (cout_std légèrement > cln_res 2026)
  CTR-12  Lignes NORMAL sans cout_standard      : 0 [OK]
  CTR-13  Annulations avec payout intactes      : OK (D030)
  CTR-14  Impact estimé commissions (~15%)      : -7 771.35 € (assiette réduite 51 809€)
           API non relancée                     : OUI
           Source utilisée                      : MASTER_CALC_HA_Payout_BACKUP_20260613_114610.xlsx
  CTR-15  Doublons validité BLOQUANT            : 0 [OK]
  CTR-16  Airbnb NORMAL avec cout_standard_id   : 1 235 / 1 235 [OK]
  CTR-17  Booking NORMAL avec cout_standard_id  : 86 / 86 [OK]
  CTR-18  Snapshot == menage_retenu (NORMAL)    : 1 321 / 1 321 [OK]
  CTR-19  date_reference non vide (NORMAL)      : 1 321 / 1 321 [OK]
           git status                           : 6 fichiers M — aucun commit

Anomalie connexe (non bloquante ce lot) :
  LOG_0009 "T3 Montaudran" type = TYPE_002 (T2 — 39€) alors que nom dit T3.
  Impact nul sur correctif (listingMapId concerné bien couvert par cout_standard TYPE_002).
  À décider séparément (correction REF_Logements ou maintien).

Fichiers modifiés :
  - 01_SOURCES_BRUTES/REF_Setup/REF_Setup.xlsm (+5 lignes historiques 2025 REF_Couts_Standards_Menage)
  - 02_TRAVAIL/lot1_hostaway_extract.py (refactor complet : _build_cost_ref_df dates,
    load_menage_cost_ref→DataFrame, _lookup_menage_by_date nouveau, PayoutCalculator 5-tuples,
    _META_NON_APPLICABLE, recalc_payout_only date-aware + 8 col, main 5-tuple + nouveaux champs)
  - 02_TRAVAIL/Lot1_Hostaway/MASTER_CALC_HA_Payout.xlsx (correctif final appliqué, 8 col tracabilité)
  - .gitignore (99_ARCHIVES/LOT1_Hostaway/ ajouté)
  - 00_CADRAGE/ETAT_AVANCEMENT.md (session 17)
  - 00_CADRAGE/JOURNAL_CONTROLES.md (ce fichier)

Statut     : EN_ATTENTE_VALIDATION_HUMAINE
Commentaire: Lot 10 reste bloqué jusqu'à validation humaine du correctif final.
             Impact réel commissions = taux propriétaire × (51 727 + 82) € — à confirmer Lot 10.
             Taux de commission dans REF_Proprietaires ; charges fixes mensuelles dans REF_Logements — non dans ce fichier.
```

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

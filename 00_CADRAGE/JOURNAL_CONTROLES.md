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
